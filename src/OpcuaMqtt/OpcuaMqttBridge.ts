
import {
    AttributeIds,
    ClientMonitoredItemBase,
    ClientMonitoredItemGroup,
    ClientSession,
    ClientSubscription,
    ConnectionStrategyOptions,
    DataType,
    ExtraDataTypeManager,
    DataValue,
    MessageSecurityMode,
    NodeId,
    OPCUAClient,
    OPCUAClientOptions,
    ReadValueIdOptions,
    resolveNodeId,
    SecurityPolicy,
    StatusCode,
    StatusCodes,
    TimestampsToReturn,
    VariantArrayType,
    WriteValueOptions,
    CreateSubscriptionRequestOptions,
    MonitoringParametersOptions,
    MonitoringMode,
    DataChangeFilter,
    DataChangeTrigger,
    DeadbandType,
    BrowseDescriptionLike,
    BrowseDirection,
    MonitoredItem,
    ClientMonitoredItem,
} from 'node-opcua';

import mqtt, { MqttClient } from 'mqtt';
import CodesysOpcuaDriver from './codesys-opcua-driver';
import { BridgeCmds, DeviceActionRequestData, DeviceId, DeviceTags, MqttTopics, TopicData, buildFullTopicPath, initialMachine } from '@kuriousdesign/machine-sdk';
import { DeviceRegistration, MachineTags, PlcNamespaces, nodeListString, nodeTypeString } from '@kuriousdesign/machine-sdk';

interface HeartBeatConnection {
    connectionId: string;
    heartbeatValue: number;
    timer: NodeJS.Timeout;
    heartbeatInterval?: NodeJS.Timeout;
}

const connectionStrategy: ConnectionStrategyOptions = {
    initialDelay: 1000,
    maxDelay: 4000,
    maxRetry: -1  // -1 means infinite retries
};

const opcuaOptions: OPCUAClientOptions = {
    applicationName: 'OpcuaMqttBridge',
    connectionStrategy: connectionStrategy,
    securityMode: MessageSecurityMode.None,
    securityPolicy: SecurityPolicy.None,
    endpointMustExist: true,
    keepSessionAlive: true,
};

const PUBLISHING_INTERVAL = 1000; // in ms

const subscriptionOptions: CreateSubscriptionRequestOptions = {
    maxNotificationsPerPublish: 2000,
    publishingEnabled: true,
    requestedLifetimeCount: 100,
    requestedMaxKeepAliveCount: 10,
    requestedPublishingInterval: PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
};

const filter = new DataChangeFilter({
    trigger: DataChangeTrigger.StatusValue, // Report on any of Status, Value, or Timestamp
    deadbandType: DeadbandType.None,                 // No deadband suppression
    deadbandValue: 0
});

const optionsGroup: MonitoringParametersOptions = {
    discardOldest: true,
    queueSize: 1,
    samplingInterval: PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
    filter
};

interface MonitoredItemToMonitor {
    attributeId: number;
    nodeId: string;
}

const MAX_ITEMS_PER_GROUP = 50; // you found 100 as the practical limit; keep here for easy adjust

class OpcuaMqttBridge {
    private mqttClient: MqttClient | null = null;
    private opcuaClient: OPCUAClient | null = null;
    private opcuaSession: ClientSession | null = null;
    private opcuaControllerName: string;
    private codesysOpcuaDriver: CodesysOpcuaDriver | null = null;
    private opcuaHeartBeatConnection: HeartBeatConnection | null = null;
    private mqttClientHeartBeatConnections: Map<string, HeartBeatConnection> | null = null;
    private nodeListPrefix: string;
    private nodeTypePrefix: string;
    private mqttOptions: unknown;
    private mqttBrokerUrl: string;
    private opcuaEndpoint: string;
    private bridgeHealthIsOk: boolean = false;
    private mqttBrokerIsConnected: boolean = false;
    private opcuaServerIsConnected: boolean = false;
    private registeredDevices: DeviceRegistration[] = [];
    private deviceMap: Map<number, DeviceRegistration> = new Map();
    private dataTypeManager: ExtraDataTypeManager | null = null;
    private nodeIdToMqttTopicMap = new Map<string, string>();

    // track all subscriptions & groups so we can clean up
    private opcuaSubscriptions: ClientSubscription[] = [];
    private monitoredItemGroups: ClientMonitoredItemGroup[] = [];

    // legacy single fields retained for compatibility (not used in multi-sub approach)
    private opcuaSubscription0: ClientSubscription | null = null;
    private opcuaSubscription1: ClientSubscription | null = null;
    private monitoredItemGroup: ClientMonitoredItemGroup | null = null;

    private timerConnectionStatusForMqtt: NodeJS.Timeout | null = null;
    private lastPlcHeartbeatValue: number = 0;
    private shuttingDown: boolean = false;

    constructor(mqttBrokerUrl: string, mqttOptions: unknown, opcuaEndpoint: string, opcuaControllerName: string) {
        console.log('Initializing OpcuaMqttBridge...');
        this.mqttOptions = mqttOptions;
        this.opcuaEndpoint = opcuaEndpoint;
        this.mqttBrokerUrl = mqttBrokerUrl;
        this.opcuaControllerName = opcuaControllerName;
        this.nodeTypePrefix = nodeTypeString + this.opcuaControllerName + '.Application.';
        this.nodeListPrefix = nodeListString + this.opcuaControllerName + '.Application.';
        this.timerConnectionStatusForMqtt = setInterval(() => {
            this.publishBridgeConnectionStatus();
        }, 1000);

        this.initializeMqttClient()
            .then(() => this.initializeOpcuaClient())
            .then(() => this.retrieveRegisteredDevices())
            .then(() => {
                // populate nodeIdToMqttTopicMap exactly like before
                console.log('create machine entries for nodeIdToMqttTopicMap');
                Object.entries(initialMachine).map(([key, value]) => {
                    const tag = key;
                    const nodeId = PlcNamespaces.Machine + '.' + tag;
                    const topic = PlcNamespaces.Machine.toLowerCase() + '/' + tag.toLowerCase();
                    this.nodeIdToMqttTopicMap.set(nodeId, topic);
                });

                console.log('create device entries for nodeIdToMqttTopicMap');
                this.registeredDevices.forEach((device) => {
                    const deviceNodeId = PlcNamespaces.Machine + '.' + MachineTags.deviceStore + '[' + device.id + ']';
                    const deviceTopic = buildFullTopicPath(device, this.deviceMap);
                    let everyOtherTag = 1;

                    Object.values(DeviceTags).map((tag: string) => {
                        const nodeId = deviceNodeId + '.' + tag;
                        const topic = deviceTopic + '/' + tag.toLowerCase().replace('.', '/');
                        everyOtherTag += 1;
                    });

                    // device log
                    const deviceLogNodeId = PlcNamespaces.Machine + '.' + MachineTags.deviceLogs + '[' + device.id + ']';
                    const deviceLogTopic = deviceTopic + '/log';
                    this.nodeIdToMqttTopicMap.set(deviceLogNodeId, deviceLogTopic);

                    // device sts
                    const deviceStsNodeId = PlcNamespaces.Machine + '.' + device.mnemonic.toLowerCase() + 'Sts';
                    const deviceStsTopic = deviceTopic + '/sts';
                    if (!(device.mnemonic === 'SYS' || device.mnemonic === 'CON' || device.mnemonic === 'HMI')) {
                        this.nodeIdToMqttTopicMap.set(deviceStsNodeId, deviceStsTopic);
                    }

                    // subscribe to device HMI action request topic
                    this.subscribeToMqttTopicDeviceHmiActionRequest(device);
                });

                this.nodeIdToMqttTopicMap.forEach((topic, nodeId) => {
                    console.log(`  ${nodeId} => ${topic}`);
                });

                // subscribe using chunked groups
                this.subscribeToMonitoredItems()
                    .then(() => console.log('Both MQTT and OPC UA clients initialized'))
                    .catch(err => console.error('Error subscribing to monitored items:', err));
            })
            .catch((err) => {
                // If any of the initialization steps failed, log it
                console.error('Initialization chain failed:', err);
            });
    }

    // -----------------------------
    // Helper: build complete itemsToMonitor list
    // -----------------------------
    private createItemsToMonitor(): MonitoredItemToMonitor[] {
        const items: MonitoredItemToMonitor[] = [];
        for (const [nodeId] of this.nodeIdToMqttTopicMap) {
            items.push({
                attributeId: AttributeIds.Value,
                nodeId: `${this.nodeListPrefix}${nodeId}`,
            });
        }
        return items;
    }

    // -----------------------------
    // Main subscribe: chunk & create subscriptions + groups
    // -----------------------------
    private async subscribeToMonitoredItems(): Promise<void> {
        if (!this.opcuaSession) {
            throw new Error("OPC UA session is not initialized");
        }

        // First, clean up any existing subscriptions/groups if re-subscribing
        await this.terminateAllSubscriptions();

        const allItems = this.createItemsToMonitor();
        console.log('Subscribing to monitored items', allItems.length, 'items to monitor...');

        // Chunk items into groups of MAX_ITEMS_PER_GROUP
        for (let i = 0; i < allItems.length; i += MAX_ITEMS_PER_GROUP) {
            const chunk = allItems.slice(i, i + MAX_ITEMS_PER_GROUP);
            const groupIndex = Math.floor(i / MAX_ITEMS_PER_GROUP) + 1;
            console.log(`Creating subscription group ${groupIndex} with ${chunk.length} items...`);

            // Validate nodes in chunk (read test). Build a filtered array of valid items.
            const validatedItems: { attributeId: number; nodeId: string }[] = [];
            for (const item of chunk) {
                try {
                    const data = await this.opcuaSession.read({
                        nodeId: item.nodeId,
                        attributeId: AttributeIds.Value,
                    } as ReadValueIdOptions);

                    if (data && data.statusCode && data.statusCode === StatusCodes.Good) {
                        // good to monitor
                        validatedItems.push(item);
                    } else {
                        console.warn(`Skipping invalid/unsupported node for monitoring: ${item.nodeId} (status=${data?.statusCode?.toString()})`);
                    }
                } catch (err) {
                    // If read fails with BadNotSupported or other issue, skip the item gracefully
                    const errMsg = (err instanceof Error) ? err.message : String(err);
                    console.warn(`Read test failed for ${item.nodeId}: ${errMsg}. Skipping monitoring for this node.`);
                }
            }

            if (validatedItems.length === 0) {
                console.log(`No valid items in group ${groupIndex}, skipping subscription creation.`);
                continue;
            }

            try {
                const subscription = await this.opcuaSession.createSubscription2(subscriptionOptions);
                this.opcuaSubscriptions.push(subscription);

                const monitoredGroup = ClientMonitoredItemGroup.create(
                    subscription,
                    validatedItems,
                    optionsGroup,
                    TimestampsToReturn.Both
                );

                this.monitoredItemGroups.push(monitoredGroup);

                monitoredGroup.on('initialized', () => {
                    console.log(`Monitored group ${groupIndex} initialized with ${validatedItems.length} items`);
                });

                monitoredGroup.on('changed', (monitoredItem: ClientMonitoredItemBase, dataValue: DataValue) => {
                    this.handleMonitoredItemChange(monitoredItem, dataValue);
                });

                // ensure reporting mode is set (forces notifications)
                try {
                    await monitoredGroup.setMonitoringMode(MonitoringMode.Reporting);
                    console.log(`Monitored group ${groupIndex} set to Reporting mode`);
                } catch (setModeErr) {
                    console.warn(`Failed to set Reporting mode for group ${groupIndex}:`, setModeErr);
                }

            } catch (err) {
                console.error(`Failed to create subscription/monitored group ${groupIndex}:`, err);
            }
        }

        console.log(`✅ Subscribed via ${this.opcuaSubscriptions.length} subscriptions and ${this.monitoredItemGroups.length} monitored groups`);
    }

    // -----------------------------
    // Individual monitor method (kept for reference)
    // -----------------------------
    private async subscribeToMonitoredItemsIndividually(): Promise<void> {
        if (!this.opcuaSession) {
            throw new Error("OPC UA session is not initialized");
        }
        console.log('Subscribing to monitored items individually', this.nodeIdToMqttTopicMap.size, 'items to monitor...');
        this.opcuaSubscription0 = await this.opcuaSession.createSubscription2(subscriptionOptions);
        const itemsToMonitor = this.createItemsToMonitor();

        console.log("Monitoring", itemsToMonitor.length, "items (individual creation)");

        for (const item of itemsToMonitor) {
            try {
                const monitoredItem = await ClientMonitoredItem.create(
                    this.opcuaSubscription0!,
                    item,
                    optionsGroup,
                    TimestampsToReturn.Both
                );

                monitoredItem.on('err', (message: string) => {
                    console.error('Monitored item error:', message);
                });

                if (this.opcuaSubscription0) {
                    this.opcuaSubscription0.on('changed', (monitoredItem: ClientMonitoredItemBase, dataValue: DataValue) => {
                        this.handleMonitoredItemChange(monitoredItem, dataValue);
                    });
                }
            } catch (err) {
                console.error("Failed to create monitored item for", item.nodeId, err);
            }
        }
    }

    // -----------------------------
    // Data change handler (keeps original logic)
    // -----------------------------
    private async handleMonitoredItemChange(monitoredItem: ClientMonitoredItemBase, dataValue: DataValue): Promise<void> {
        try {
            const newValue = this.decipherOpcuaValue(dataValue);
            // monitoredItem.itemToMonitor.nodeId may be a NodeId object or string; ensure string
            const fullNodeId = monitoredItem.itemToMonitor?.nodeId?.toString ? monitoredItem.itemToMonitor.nodeId.toString() : String(monitoredItem.itemToMonitor?.nodeId);
            // strip away Node Id Down to just namespace and tag
            const strippedPrefix = this.nodeListPrefix.replace('ns=4;s=', '');
            const nodeId = fullNodeId.replace(this.nodeListPrefix, '');
            const topic = this.nodeIdToMqttTopicMap.get(nodeId);
            console.log(`Monitored item changed: nodeId=${nodeId}, value=${newValue}, topic=${topic}`);

            if (!topic) {
                console.error('No valid MQTT topic found for nodeId:', nodeId, 'full:', fullNodeId);
            } else if (topic && this.mqttClient && this.bridgeHealthIsOk) {
                const message: TopicData = {
                    timestamp: Date.now(),
                    payload: newValue
                };
                this.mqttClient.publish(topic, JSON.stringify(message));
                if (topic === "machine/heartbeatplc" && newValue % 30 === 0) {
                    console.log('Machine.heartbeatPlc:', newValue);
                }
            } else if (!this.bridgeHealthIsOk) {
                // not publishing when bridge health is not ok
            }
        } catch (error) {
            console.error('Error processing monitored item change:', error);
        }
    }

    // -----------------------------
    // Device action request subscription (unchanged)
    // -----------------------------
    private async subscribeToMqttTopicDeviceHmiActionRequest(device: DeviceRegistration): Promise<void> {
        if (!this.opcuaSession) {
            throw new Error("OPC UA session is not initialized");
        }
        if (!this.mqttClient) {
            throw new Error("MQTT client is not initialized");
        }
        if (!this.deviceMap || this.deviceMap.size === 0) {
            throw new Error("Device map is not initialized or empty");
        }

        const deviceTopic = buildFullTopicPath(device, this.deviceMap);
        const topic = deviceTopic + '/apiOpcua/hmiReq';
        console.log('Subscribing to device action request topic:', topic);

        this.mqttClient.subscribe(topic, { qos: 1 }, (err) => {
            if (err) {
                console.error('Failed to subscribe to device action request topic:', err.message);
            } else {
                // subscribed
            }
        });
    }

    private async handleHmiActionRequest(device: DeviceRegistration, message: DeviceActionRequestData): Promise<void> {
        console.log('Handling HMI Action Request for device:', device, 'with message:', message);

        if (!this.opcuaSession) {
            console.error('OPC UA session is not initialized');
            return;
        }

        try {
            this.codesysOpcuaDriver?.requestAction(device.id, message.ActionType, message.ActionId, message.ParamArray);
        } catch (error) {
            console.error('Failed to write to OPC UA nodes, Error:', error);
        }
    }

    // -----------------------------
    // MQTT init (unchanged)
    // -----------------------------
    private async initializeMqttClient(): Promise<void> {
        console.log('connecting to mqtt broker at: ' + this.mqttBrokerUrl);
        this.mqttClient = mqtt.connect(this.mqttBrokerUrl, this.mqttOptions as mqtt.IClientOptions);
        this.mqttClient.on('connect', () => {
            console.log('✅ MQTT client connected to broker');
            this.updateMqttConnectionStatus(true);
            this.mqttClient!.subscribe(MqttTopics.BRIDGE_CMD, (err) => {
                if (err) {
                    console.error('Failed to subscribe to bridge commands:', err.message);
                } else {
                    console.log('✅ Subscribed to bridge commands');
                }
            });

            this.mqttClient!.on('message', (topic, message) => {
                if (topic === MqttTopics.BRIDGE_CMD) {
                    const command = JSON.parse(message.toString()) as TopicData;
                    this.handleBridgeCommand(command);
                } else {
                    if (topic.endsWith('/apiOpcua/hmiReq')) {
                        const hmiActionReqData = JSON.parse(message.toString()) as DeviceActionRequestData;
                        const topicParts = topic.split('/');
                        const deviceIdStr = topicParts[topicParts.length - 3];
                        const deviceId = Number(deviceIdStr);
                        if (isNaN(deviceId)) {
                            console.error('Invalid deviceId extracted from topic:', topic);
                            return;
                        }
                        const device = this.deviceMap.get(deviceId);
                        if (!device) {
                            console.error('No device found for deviceId:', deviceId);
                            return;
                        }
                        this.handleHmiActionRequest(device, hmiActionReqData);
                    }
                }
            });
        });
        this.mqttClient.on('error', (err) => {
            console.error('MQTT error:', err.message);
            this.updateMqttConnectionStatus(false);
        });
        this.mqttClient.on('close', () => {
            console.log('MQTT connection closed');
            this.updateMqttConnectionStatus(false);
        });
    }

    // -----------------------------
    // Bridge command handler (unchanged)
    // -----------------------------
    private async handleBridgeCommand(message: TopicData): Promise<void> {
        const cmdData = message.payload as { cmd: BridgeCmds };
        console.log('Received bridge command:', cmdData.cmd);

        switch (cmdData.cmd) {
            case BridgeCmds.CONNECT:
                if (this.deviceMap.size > 0) {
                    const message: TopicData = {
                        timestamp: Date.now(),
                        payload: Array.from(this.deviceMap.entries())
                    };
                    if (this.mqttClient) {
                        console.log("Publishing deviceMap to bridge");
                        this.mqttClient.publish(MqttTopics.DEVICE_MAP, JSON.stringify(message));
                    } else {
                        console.warn("Cannot publish deviceMap, mqttClient not connected");
                    }
                } else {
                    console.log("DeviceMap not yet available, cannot publish to bridge/deviceMap");
                }
                break;
            case BridgeCmds.DISCONNECT:
                break;
            default:
                console.warn('Unknown bridge command:', cmdData.cmd);
        }
    }

    // -----------------------------
    // OPC UA client init (mostly unchanged)
    // -----------------------------
    private async initializeOpcuaClient(): Promise<void> {
        this.opcuaClient = OPCUAClient.create(opcuaOptions);

        this.opcuaClient.on("backoff", (retry, delay) => {
            console.warn(`OPC UA retry #${retry}, next attempt in ${delay} ms`);
            if (this.mqttBrokerIsConnected && this.mqttClient) {
                const message: TopicData = {
                    timestamp: Date.now(),
                    payload: "OPC UA connection lost"
                };
                this.mqttClient.publish(MqttTopics.BRIDGE_STATUS, JSON.stringify(message));
            }
        });

        this.opcuaClient.on("connection_lost", () => {
            console.warn("OPC UA connection lost");
            this.updateOpcuaConnectionStatus(false);
        });

        this.opcuaClient.on("after_reconnection", () => {
            console.log("✅ OPC UA reconnected");
        });

        try {
            console.log("connecting to opcua endpoint: " + this.opcuaEndpoint);
            await this.opcuaClient.connect(this.opcuaEndpoint);
            console.log("✅ OPC UA client connected");

            this.opcuaSession = await this.opcuaClient.createSession();
            console.log("✅ OPC UA session created");
            this.codesysOpcuaDriver = new CodesysOpcuaDriver(DeviceId.HMI, this.opcuaSession, this.opcuaControllerName);
            console.log("✅ CODESYS OPC UA driver created");
            this.dataTypeManager = new ExtraDataTypeManager();

            // create heartbeat with opcua tag that gets value every 1000ms
            this.opcuaHeartBeatConnection = {
                connectionId: 'opcua-heartbeat',
                heartbeatValue: 0,
                timer: setInterval(async () => {
                    if (this.opcuaSession && this.opcuaHeartBeatConnection) {
                        const heartbeatPlcNodeId = this.concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatPLC);
                        const heartbeatValue = await this.readOpcuaValue(heartbeatPlcNodeId);
                        if (heartbeatValue === null || this.lastPlcHeartbeatValue === heartbeatValue) {
                            if (this.opcuaServerIsConnected) {
                                console.warn('OPC UA Heartbeat value not changing, connection may be lost');
                            }
                            this.updateOpcuaConnectionStatus(false);
                            return;
                        }
                        this.updateOpcuaConnectionStatus(true);
                        this.opcuaHeartBeatConnection.heartbeatValue = heartbeatValue as number;
                        this.lastPlcHeartbeatValue = heartbeatValue as number;
                        const heartbeatHmiNodeId = this.concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatHMI);
                        await this.writeOpcuaValue(heartbeatHmiNodeId, heartbeatValue, DataType.Byte);
                    }
                }, 1000)
            };

            // Wait for OPC UA connection to be established by heartbeat
            while (!this.opcuaServerIsConnected) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
            console.log("✅ OPC UA connection established, proceeding...");

        } catch (err: any) {
            console.error("❌ Failed to connect to OPC UA:", err?.message ?? err);

            // Retry after delay
            setTimeout(() => {
                this.initializeOpcuaClient();
            }, 5000);
        }
    }

    // -----------------------------
    // Registered devices retrieval (unchanged)
    // -----------------------------
    private async retrieveRegisteredDevices(): Promise<DeviceRegistration[]> {
        if (!this.opcuaServerIsConnected) {
            console.log("OPC UA server not connected, cannot retrieve registered devices");
            return [];
        }
        console.log("Retrieving registered devices from OPC UA...");
        const registeredDevicesNodeId = this.concatNodeId(PlcNamespaces.Machine, MachineTags.registeredDevices);
        const heartbeatPlcNodeId = this.concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatPLC);

        const heartbeatPlc = await this.readOpcuaValue(heartbeatPlcNodeId) as number;
        const registeredDevices = await this.readOpcuaValue(registeredDevicesNodeId) as DeviceRegistration[];

        this.registeredDevices = (registeredDevices || []).filter((device: DeviceRegistration) => device.id !== 0);
        this.registeredDevices.forEach(deviceReg => {
            this.deviceMap.set(deviceReg.id, deviceReg);
            console.log("adding device to map:", deviceReg.id, "with parent id:", deviceReg.parentId);
        });
        console.log("Retrieved registered devices, count:", this.registeredDevices.length);
        return registeredDevices;
    }

    // -----------------------------
    // Read / Write helpers (unchanged)
    // -----------------------------
    private async writeOpcuaValue(nodeId: string, value: any, dataType: DataType): Promise<void> {
        if (!this.opcuaSession) {
            throw new Error("OPC UA session is not initialized");
        }
        try {
            const writeValue: WriteValueOptions = {
                nodeId: nodeId,
                attributeId: AttributeIds.Value,
                value: {
                    value: {
                        dataType: dataType,
                        value: value
                    }
                }
            };
            await this.opcuaSession.write(writeValue);
            const readValue = await this.readOpcuaValue(nodeId); // verify write
            if (readValue !== value) {
                console.error(`Verification failed for node ${nodeId}: expected ${value}, got ${readValue}`);
            }
        } catch (error) {
            console.error(`Failed to write OPC UA value to ${nodeId}:`, error);
            throw error;
        }
    }

    private async readOpcuaValue(nodeId: string): Promise<any> {
        if (!this.opcuaSession) {
            throw new Error("OPC UA session is not initialized");
        }

        try {
            const readValueOptions: ReadValueIdOptions = {
                nodeId: nodeId,
                attributeId: AttributeIds.Value,
            };
            const data = await this.opcuaSession.read(readValueOptions);
            const value = this.decipherOpcuaValue(data);
            if (data.statusCode === StatusCodes.Good) {
                return value;
            } else {
                console.warn(`Failed to read OPC UA value from ${nodeId}: ${data.statusCode}`);
                return null;
            }
        } catch (error) {
            if (error instanceof Error && error.message.includes('BadNodeIdUnknown')) {
                console.warn(`Warning: BadNodeIdUnknown for ${nodeId}, the machine may not be in run mode`);
                return null;
            } else if (error instanceof Error && error.message.includes('BadConnectionClosed')) {
                console.warn(`Warning: BadConnectionClosed for ${nodeId}, the OPC UA connection may be closed`);
                this.updateOpcuaConnectionStatus(false);
                return null;
            }
            console.error(`Failed to read OPC UA value from ${nodeId}:`, error);
            throw error;
        }
    }

    private decipherOpcuaValue(data: any): any {
        const decipheredValue =
            data?.value?.arrayType === VariantArrayType.Array
                ? Array.from(data.toJSON().value.value)
                : (data?.toJSON()?.value?.value);
        return decipheredValue;
    }

    // -----------------------------
    // NodeId helpers (fixed typo)
    // -----------------------------
    private concatNodeId(namespace: string, tag: string): string {
        return `${this.nodeListPrefix}${namespace}.${tag}`;
    }

    private concatNodeIdOfArrayTag(namespace: string, tag: string, index: number): string {
        return `${this.nodeListPrefix}${namespace}.${tag}[${index}]`;
    }

    // -----------------------------
    // Bridge health & publishing (unchanged)
    // -----------------------------
    private updateBridgeHealth() {
        const newState = this.mqttBrokerIsConnected && this.opcuaServerIsConnected;
        if (newState !== this.bridgeHealthIsOk) {
            this.bridgeHealthIsOk ? console.error('❌ Bridge connection health is BAD') : console.log('✅ Bridge connection health is OK');
        }
        this.bridgeHealthIsOk = newState;
    }

    private async publishBridgeConnectionStatus() {
        if (this.mqttClient && this.mqttBrokerIsConnected) {
            let message: TopicData;
            if (this.opcuaServerIsConnected) {
                message = {
                    timestamp: Date.now(),
                    payload: "Running"
                };
            } else {
                message = {
                    timestamp: Date.now(),
                    payload: "Opcua Server Disconnected"
                };
            }
            this.mqttClient.publish(MqttTopics.BRIDGE_STATUS, JSON.stringify(message));
        }
    }

    private updateMqttConnectionStatus(isConnected: boolean) {
        if (isConnected !== this.mqttBrokerIsConnected) {
            console.log(`MQTT Broker ${isConnected ? 'connected' : 'disconnected'}`);
        }
        this.mqttBrokerIsConnected = isConnected;
        this.updateBridgeHealth();
    }

    private updateOpcuaConnectionStatus(isConnected: boolean) {
        if (this.opcuaServerIsConnected !== isConnected) {
            console.log(`OPC UA Server ${isConnected ? 'connected' : 'disconnected'}`);
        }
        this.opcuaServerIsConnected = isConnected;
        this.updateBridgeHealth();
        this.publishBridgeConnectionStatus();
    }

    // -----------------------------
    // Terminate all subscriptions & monitored groups
    // -----------------------------
    private async terminateAllSubscriptions(): Promise<void> {
        // terminate monitored item groups first
        if (this.monitoredItemGroups && this.monitoredItemGroups.length > 0) {
            for (const group of this.monitoredItemGroups) {
                try {
                    if (typeof (group as any).terminate === 'function') {
                        await (group as any).terminate();
                    } else if (typeof (group as any).dispose === 'function') {
                        (group as any).dispose();
                    }
                } catch (err) {
                    console.warn('Error terminating monitored group:', err);
                }
            }
            this.monitoredItemGroups = [];
        }

        // then terminate subscriptions
        if (this.opcuaSubscriptions && this.opcuaSubscriptions.length > 0) {
            for (const sub of this.opcuaSubscriptions) {
                try {
                    if (typeof (sub as any).terminate === 'function') {
                        await (sub as any).terminate();
                    } else if (typeof (sub as any).close === 'function') {
                        await (sub as any).close();
                    }
                } catch (err) {
                    console.warn('Error terminating subscription:', err);
                }
            }
            this.opcuaSubscriptions = [];
        }

        // legacy single ones (safety)
        if (this.monitoredItemGroup) {
            try {
                if (typeof (this.monitoredItemGroup as any).terminate === 'function') {
                    await (this.monitoredItemGroup as any).terminate();
                } else if (typeof (this.monitoredItemGroup as any).dispose === 'function') {
                    (this.monitoredItemGroup as any).dispose();
                }
            } catch (err) {
                console.warn('Error terminating legacy monitoredItemGroup:', err);
            }
            this.monitoredItemGroup = null;
        }

        if (this.opcuaSubscription0) {
            try {
                if (typeof (this.opcuaSubscription0 as any).terminate === 'function') {
                    await (this.opcuaSubscription0 as any).terminate();
                } else if (typeof (this.opcuaSubscription0 as any).close === 'function') {
                    await (this.opcuaSubscription0 as any).close();
                }
            } catch (err) {
                console.warn('Error terminating legacy subscription 0:', err);
            }
            this.opcuaSubscription0 = null;
        }

        if (this.opcuaSubscription1) {
            try {
                if (typeof (this.opcuaSubscription1 as any).terminate === 'function') {
                    await (this.opcuaSubscription1 as any).terminate();
                } else if (typeof (this.opcuaSubscription1 as any).close === 'function') {
                    await (this.opcuaSubscription1 as any).close();
                }
            } catch (err) {
                console.warn('Error terminating legacy subscription 1:', err);
            }
            this.opcuaSubscription1 = null;
        }

        console.log('All OPC UA subscriptions and monitored groups terminated');
    }

    // -----------------------------
    // Shutdown (extended to cleanup multi-sub resources)
    // -----------------------------
    public async shutdown(): Promise<void> {
        if (this.shuttingDown) {
            console.log('Shutdown already in progress/complete');
            return;
        }
        this.shuttingDown = true;
        console.log('Shutting down OpcuaMqttBridge...');

        // 1) stop the connection status timer
        try {
            if (this.timerConnectionStatusForMqtt) {
                clearInterval(this.timerConnectionStatusForMqtt);
                this.timerConnectionStatusForMqtt = null;
            }
        } catch (err) {
            console.warn('Error clearing timerConnectionStatusForMqtt', err);
        }

        // 2) stop opcua heartbeat timer
        try {
            if (this.opcuaHeartBeatConnection && this.opcuaHeartBeatConnection.timer) {
                clearInterval(this.opcuaHeartBeatConnection.timer);
                if (this.opcuaHeartBeatConnection.heartbeatInterval) {
                    clearTimeout(this.opcuaHeartBeatConnection.heartbeatInterval);
                }
                this.opcuaHeartBeatConnection = null;
            }
        } catch (err) {
            console.warn('Error clearing opcua heartbeat timers', err);
        }

        // 3) terminate all monitored groups and subscriptions
        try {
            await this.terminateAllSubscriptions();
        } catch (err) {
            console.warn('Failed to terminate subscriptions/groups', err);
        }

        // 4) close session
        try {
            if (this.opcuaSession) {
                await this.opcuaSession.close().catch((e) => {
                    console.warn('Error closing OPC UA session', e);
                });
                this.opcuaSession = null;
            }
        } catch (err) {
            console.warn('Failed to close session', err);
        }

        // 5) disconnect OPC UA client
        try {
            if (this.opcuaClient) {
                if (typeof (this.opcuaClient as any).disconnect === 'function') {
                    await (this.opcuaClient as any).disconnect().catch((e: any) => {
                        console.warn('Error disconnecting OPC UA client', e);
                    });
                }
                this.opcuaClient = null;
            }
        } catch (err) {
            console.warn('Failed to disconnect opcuaClient', err);
        }

        // 6) cleanup mqtt client
        try {
            if (this.mqttClient) {
                try {
                    this.mqttClient.unsubscribe(MqttTopics.BRIDGE_CMD, () => { /* ignore */ });
                } catch (e) { /* ignore */ }

                this.mqttClient.removeAllListeners('message');
                this.mqttClient.removeAllListeners('connect');
                this.mqttClient.removeAllListeners('error');
                this.mqttClient.removeAllListeners('close');

                await new Promise<void>((resolve) => {
                    try {
                        this.mqttClient!.end(true, {}, () => {
                            resolve();
                        });
                    } catch (e) {
                        console.warn('Error ending mqttClient', e);
                        resolve();
                    }
                });
                this.mqttClient = null;
            }
        } catch (err) {
            console.warn('Failed to cleanup mqttClient', err);
        }

        // 7) clear mqtt heartbeat connections
        try {
            if (this.mqttClientHeartBeatConnections) {
                for (const hb of this.mqttClientHeartBeatConnections.values()) {
                    if (hb.timer) clearInterval(hb.timer);
                    if (hb.heartbeatInterval) clearTimeout(hb.heartbeatInterval);
                }
                this.mqttClientHeartBeatConnections.clear();
                this.mqttClientHeartBeatConnections = null;
            }
        } catch (err) {
            console.warn('Failed clearing mqtt heartbeat connections', err);
        }

        // 8) mark flags
        this.opcuaServerIsConnected = false;
        this.mqttBrokerIsConnected = false;
        this.bridgeHealthIsOk = false;

        console.log('OpcuaMqttBridge shutdown finished');
    }
}

export default OpcuaMqttBridge;
