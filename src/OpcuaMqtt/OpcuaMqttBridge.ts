/*
# OPC-UA MQTT Bridge V2

## Key Changes from V1:
- Subscribes to top-level device nodes instead of individual sub-properties
- Publishes entire device data structure to single MQTT topic per device
- Significantly reduces number of OPC-UA subscriptions and MQTT messages
- Simplifies topic structure: machine/{devicePath} instead of machine/{devicePath}/{property}

## Architecture:
1. Connect to OPC-UA server and MQTT broker
2. Retrieve registered devices from Machine.RegisteredDevices
3. Subscribe to each device's top-level node (Machine.Devices[id])
4. On device data change, publish entire device object to MQTT topic
5. Maintain heartbeat and health monitoring

## Benefits:
- Fewer OPC-UA subscriptions (one per device vs many per device)
- Reduced MQTT traffic (one message per device update)
- Simpler topic hierarchy
- Better performance with large device counts
*/

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
    OPCUAClient,
    OPCUAClientOptions,
    ReadValueIdOptions,
    SecurityPolicy,
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
} from 'node-opcua';

import mqtt, { MqttClient } from 'mqtt';
import CodesysOpcuaDriver from './codesys-opcua-driver';
import {
    BridgeCmds,
    DeviceActionRequestData,
    DeviceId,
    MqttTopics,
    TopicData,
    buildFullTopicPath,
    initialMachine
} from '@kuriousdesign/machine-sdk';
import {
    DeviceRegistration,
    MachineTags,
    PlcNamespaces,
    nodeListString,
    nodeTypeString
} from '@kuriousdesign/machine-sdk';


interface HeartBeatConnection {
    connectionId: string;
    heartbeatValue: number;
    timer: NodeJS.Timeout;
    heartbeatInterval?: NodeJS.Timeout;
}

interface DeviceSubscription {
    deviceId: number;
    devicePath: string;
    nodeId: string;
    mqttTopic: string;
}

const connectionStrategy: ConnectionStrategyOptions = {
    initialDelay: 1000,
    maxDelay: 4000,
    maxRetry: -1
};

const opcuaOptions: OPCUAClientOptions = {
    applicationName: 'OpcuaMqttBridge',
    connectionStrategy: connectionStrategy,
    securityMode: MessageSecurityMode.None,
    securityPolicy: SecurityPolicy.None,
    endpointMustExist: true,
    keepSessionAlive: true,
};

const PUBLISHING_INTERVAL = 300; //

const subscriptionOptions: CreateSubscriptionRequestOptions = {
    maxNotificationsPerPublish: 100, // Lower than V1 since we have fewer subscriptions
    publishingEnabled: true,
    requestedLifetimeCount: 100,
    requestedMaxKeepAliveCount: 10,
    requestedPublishingInterval: PUBLISHING_INTERVAL,
};

const filter = new DataChangeFilter({
    trigger: DataChangeTrigger.StatusValueTimestamp, // Don't trigger on timestamp-only changes
    deadbandType: DeadbandType.None, // No deadband for device-level changes
    deadbandValue: 0
});

const optionsGroup: MonitoringParametersOptions = {
    discardOldest: true,
    queueSize: 1,
    samplingInterval: PUBLISHING_INTERVAL,
    filter
};

class OpcuaMqttBridge {
    private mqttClient: MqttClient | null = null;
    private opcuaClient: OPCUAClient | null = null;
    private opcuaSession: ClientSession | null = null;
    private opcuaControllerName: string;
    private codesysOpcuaDriver: CodesysOpcuaDriver | null = null;
    private opcuaHeartBeatConnection: HeartBeatConnection | null = null;
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

    // V2: Simplified subscription tracking - one subscription per device
    private nodeIdToMqttTopicMap: DeviceSubscription[] = [];
    private opcuaSubscription: ClientSubscription | null = null;
    private monitoredItemGroup: ClientMonitoredItemGroup | null = null;

    private timerConnectionStatusForMqtt: NodeJS.Timeout | null = null;
    private lastPlcHeartbeatValue: number = 0;
    private shuttingDown: boolean = false;
    private lastPublishTime: number = 0;
    private publishRate: number = 0;

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
                console.log('Building device subscription list...');
                this.buildNodeIdToMqttTopicMap();

                // Subscribe to all devices
                return this.subscribeToMonitoredItems();
            })
            .then(() => console.log('✅ Both MQTT and OPC UA clients initialized (V2)'))
            .catch((err) => {
                console.error('Initialization chain failed:', err);
            });
    }

    // -----------------------------
    // V2: Build simplified subscription list
    // -----------------------------
    private buildNodeIdToMqttTopicMap(): void {
        this.nodeIdToMqttTopicMap = [];

        // Add machine tags
        console.log('create machine entries ');
        Object.entries(initialMachine).map(([key]) => {
            const tag = key;
            const nodeId = PlcNamespaces.Machine + '.' + tag;
            const topic = PlcNamespaces.Machine.toLowerCase() + '/' + tag.toLowerCase();
            this.nodeIdToMqttTopicMap.push({
                deviceId: 0,
                devicePath: PlcNamespaces.Machine,
                nodeId: nodeId,
                mqttTopic: topic
            });
        });

        // Add write tag mqtt subscription
        this.subscribeToMqttTopicWriteTagRequest();

        // Add each device subscription
        this.registeredDevices.forEach((device) => {
            const deviceNodeId = `${PlcNamespaces.Machine}.${MachineTags.deviceStore}[${device.id}]`;
            const devicePath = buildFullTopicPath(device, this.deviceMap);
            const deviceTopic = devicePath;

            this.nodeIdToMqttTopicMap.push({
                deviceId: device.id,
                devicePath: devicePath,
                nodeId: deviceNodeId,
                mqttTopic: deviceTopic
            });

            // device log
            const deviceLogNodeId = PlcNamespaces.Machine + '.' + MachineTags.deviceLogs + '[' + device.id + ']';
            const deviceLogTopic = deviceTopic + '/log';
            this.nodeIdToMqttTopicMap.push({
                deviceId: device.id,
                devicePath: devicePath,
                nodeId: deviceLogNodeId,
                mqttTopic: deviceLogTopic
            });


            // device sts
            const deviceStsNodeId = PlcNamespaces.Machine + '.' + device.mnemonic.toLowerCase() + 'Sts';
            const deviceStsTopic = deviceTopic + '/sts';
            if (!(device.mnemonic === 'SYS' || device.mnemonic === 'CON' || device.mnemonic === 'HMI')) {
                this.nodeIdToMqttTopicMap.push({
                    deviceId: device.id,
                    devicePath: devicePath,
                    nodeId: deviceStsNodeId,
                    mqttTopic: deviceStsTopic
                });
            }

            //console.log(`📋 Added device subscription: ${deviceNodeId} -> ${deviceTopic}`);
            // Subscribe to device HMI action request topic
            this.subscribeToMqttTopicDeviceHmiActionRequest(device);
        });

        console.log(`✅ Built ${this.nodeIdToMqttTopicMap.length} device subscriptions`);
    }

    // -----------------------------
    // V2: Subscribe to all devices
    // -----------------------------
    private async subscribeToMonitoredItems(): Promise<void> {
        if (!this.opcuaSession) {
            throw new Error("OPC UA session is not initialized");
        }
        console.log(`Subscribing to ${this.nodeIdToMqttTopicMap.length} opcua nodes...`);

        // Create single subscription for all devices
        this.opcuaSubscription = await this.opcuaSession.createSubscription2(subscriptionOptions);

        // Build monitored items list
        const itemsToMonitor: { attributeId: number; nodeId: string }[] = [];

        for (const deviceSub of this.nodeIdToMqttTopicMap) {
            const fullNodeId = `${this.nodeListPrefix}${deviceSub.nodeId}`;

            // Validate node exists
            try {
                const data = await this.opcuaSession.read({
                    nodeId: fullNodeId,
                    attributeId: AttributeIds.Value,
                } as ReadValueIdOptions);

                if (data && data.statusCode && data.statusCode === StatusCodes.Good) {
                    itemsToMonitor.push({
                        attributeId: AttributeIds.Value,
                        nodeId: fullNodeId,
                    });
                    console.log(`✅ Validated nodeId: ${deviceSub.nodeId}, topic: ${deviceSub.mqttTopic}`);
                } else {
                    console.warn(`⚠️ Skipping invalid node: ${deviceSub.nodeId} (status=${data?.statusCode?.toString()})`);
                }
            } catch (err) {
                const errMsg = (err instanceof Error) ? err.message : String(err);
                console.warn(`⚠️ Read test failed for ${deviceSub.nodeId}: ${errMsg}`);
            }
        }

        if (itemsToMonitor.length === 0) {
            console.error('❌ No valid nodes found to monitor!');
            return;
        }

        console.log(`Creating monitored item group with ${itemsToMonitor.length} nodes...`);

        // Create monitored item group
        this.monitoredItemGroup = ClientMonitoredItemGroup.create(
            this.opcuaSubscription,
            itemsToMonitor,
            optionsGroup,
            TimestampsToReturn.Both
        );

        this.monitoredItemGroup.on('initialized', () => {
            console.log(`✅ Monitored group initialized with ${itemsToMonitor.length} nodes`);
        });

        this.monitoredItemGroup.on('changed', (monitoredItem: ClientMonitoredItemBase, dataValue: DataValue) => {
            this.handleDeviceChange(monitoredItem, dataValue);
        });

        // Set monitoring mode
        try {
            await this.monitoredItemGroup.setMonitoringMode(MonitoringMode.Reporting);
            console.log(`✅ Monitoring mode set to Reporting`);
        } catch (setModeErr) {
            console.warn(`⚠️ Failed to set Reporting mode:`, setModeErr);
        }

        console.log(`✅ Successfully subscribed to ${itemsToMonitor.length} device nodes`);
    }

    // -----------------------------
    // V2: Handle device-level changes
    // -----------------------------
    private async handleDeviceChange(monitoredItem: ClientMonitoredItemBase, dataValue: DataValue): Promise<void> {
        try {
            const deviceData = this.decipherOpcuaValue(dataValue);
            const fullNodeId = monitoredItem.itemToMonitor?.nodeId?.toString
                ? monitoredItem.itemToMonitor.nodeId.toString()
                : String(monitoredItem.itemToMonitor?.nodeId);

            // Strip prefix to get clean node ID
            const nodeId = fullNodeId.replace(this.nodeListPrefix, '');

            // Find matching device subscription
            const deviceSub = this.nodeIdToMqttTopicMap.find(sub => sub.nodeId === nodeId);

            if (!deviceSub) {
                console.error('❌ No device subscription found for nodeId:', nodeId);
                return;
            }

            if (this.mqttClient && this.bridgeHealthIsOk) {
                const message: TopicData = {
                    timestamp: Date.now(),
                    payload: deviceData
                };

                this.mqttClient.publish(deviceSub.mqttTopic, JSON.stringify(message));

                // want to calculate the publishing rate for Machine.Devices[0]
                if (deviceSub.deviceId === 1) {
                    const now = Date.now();
                    const timeDiff = now - this.lastPublishTime;
                    this.lastPublishTime = now;
                    //console.log(`📡 Published device ${deviceSub.deviceId} to topic ${deviceSub.mqttTopic} at rate ${timeDiff.toFixed(2)} ms`);
                }
            }
        } catch (error) {
            console.error('❌ Error processing device change:', error);
        }
    }

    // -----------------------------
    // Device action request subscription (unchanged from V1)
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
        //console.log('Subscribing to device action request topic:', topic);

        this.mqttClient.subscribe(topic, { qos: 1 }, (err) => {
            if (err) {
                console.error('Failed to subscribe to device action request topic:', err.message);
            }
        });
    }

    private async subscribeToMqttTopicWriteTagRequest(): Promise<void> {
        if (!this.opcuaSession) {
            throw new Error("OPC UA session is not initialized");
        }
        if (!this.mqttClient) {
            throw new Error("MQTT client is not initialized");
        }
        if (!this.deviceMap || this.deviceMap.size === 0) {
            throw new Error("Device map is not initialized or empty");
        }
        const topic = 'machine/write_tag';
        //console.log('Subscribing to device action request topic:', topic);

        this.mqttClient.subscribe(topic, { qos: 1 }, (err) => {
            if (err) {
                console.error('Failed to subscribe to writeTag request topic:', err.message);
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



    private async handleWriteTagRequest(message: {tag: string, value: unknown}): Promise<void> {
        console.log('Handling HMI Action Request for writeTagRequest:', message);

        if (!this.opcuaSession) {
            console.error('OPC UA session is not initialized');
            return;
        }

        let dataType = DataType.Int16;
        // if value is an object, then dataType is extension object
        if (typeof message.value === 'object') {
            dataType = DataType.ExtensionObject;
        }

        try {
            const result = await this.codesysOpcuaDriver?.writeNestedObject(message.tag, message.value);
            console.log('Write result:', result);
        } catch (error) {
            console.error('Failed to write to OPC UA nodes, Error:', error);
        }
    }

    // -----------------------------
    // MQTT init (unchanged from V1)
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
                } else if (topic.endsWith('/apiOpcua/hmiReq')) {
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
                } else if (topic === 'machine/write_tag') {
                    const writeTagData = JSON.parse(message.toString()) as {tag: string, value: unknown};
                    this.handleWriteTagRequest(writeTagData);
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
    // Bridge command handler (unchanged from V1)
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
                        //console.log("✅ Published deviceMap to bridge: ", JSON.stringify(message));
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
    // OPC UA client init (unchanged from V1)
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
                        //console.log(`OPC UA Heartbeat value: ${heartbeatValue}`);

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
    // Registered devices retrieval (unchanged from V1)
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
            // add device path to each registration
            const devicePathString = buildFullTopicPath(deviceReg, this.deviceMap);
            // create array by parsing id using / char
            const devicePathArray = devicePathString.split('/');
            console.log("devicePath:", devicePathArray);
            this.deviceMap.set(deviceReg.id, { ...deviceReg, devicePath: devicePathArray });
            //console.log("adding device to map:", deviceReg.id, "with parent id:", deviceReg.parentId);
        });

        console.log("Retrieved registered devices, count:", this.registeredDevices.length);
        return registeredDevices;
    }

    // -----------------------------
    // Read / Write helpers (unchanged from V1)
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

            const readValue = await this.readOpcuaValue(nodeId);
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
    // NodeId helpers (unchanged from V1)
    // -----------------------------
    private concatNodeId(namespace: string, tag: string): string {
        return `${this.nodeListPrefix}${namespace}.${tag}`;
    }

    // -----------------------------
    // Bridge health & publishing (unchanged from V1)
    // -----------------------------
    private updateBridgeHealth() {
        const newState = this.mqttBrokerIsConnected && this.opcuaServerIsConnected;
        if (newState !== this.bridgeHealthIsOk) {
            this.bridgeHealthIsOk
                ? console.error('❌ Bridge connection health is BAD')
                : console.log('✅ Bridge connection health is OK');
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
    // Shutdown
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

        // 3) terminate monitored item group
        try {
            if (this.monitoredItemGroup) {
                if (typeof (this.monitoredItemGroup as any).terminate === 'function') {
                    await (this.monitoredItemGroup as any).terminate();
                } else if (typeof (this.monitoredItemGroup as any).dispose === 'function') {
                    (this.monitoredItemGroup as any).dispose();
                }
                this.monitoredItemGroup = null;
            }
        } catch (err) {
            console.warn('Error terminating monitored group:', err);
        }

        // 4) terminate subscription
        try {
            if (this.opcuaSubscription) {
                if (typeof (this.opcuaSubscription as any).terminate === 'function') {
                    await (this.opcuaSubscription as any).terminate();
                } else if (typeof (this.opcuaSubscription as any).close === 'function') {
                    await (this.opcuaSubscription as any).close();
                }
                this.opcuaSubscription = null;
            }
        } catch (err) {
            console.warn('Error terminating subscription:', err);
        }

        // 5) close session
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

        // 6) disconnect OPC UA client
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

        // 7) cleanup mqtt client
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

        // 8) mark flags
        this.opcuaServerIsConnected = false;
        this.mqttBrokerIsConnected = false;
        this.bridgeHealthIsOk = false;

        console.log('OpcuaMqttBridge shutdown finished');
    }
}

export default OpcuaMqttBridge;
