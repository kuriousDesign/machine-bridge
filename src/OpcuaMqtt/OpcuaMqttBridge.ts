/*
# HOW THE BRIDGE WORKS
This bridge will manage a connection between a single machine (OpcuaServer) and a single MQTT Broker.
The bridge will publish device-based topics and some system topics to the MQTT Broker continuously.
Mqtt Clients will subscribe and unsubscripe to topics at will, which the broker will manage.

## OPCUA CONNECTION AND SUBSCRIPTION TO MONITORED NODES PROCEDURE
1. The bridge will await connection via a single session with the opcua server
2. Once connected, await a heartbeat connection (read and writing from and to two specific heartbeat nodes), and maintain this every 500ms
3. Read tag for an array tag called Machine.RegisteredDevices of type RegisteredDevice{deviceStoreIndex:UINT, deviceType:DeviceTypes enum, deviceTypeStoreIndex:UINT, deviceMqttTopicPath: string}, first device is the machine itself with its id. Each non empty entry in the registeredDeviceArray will be used to create a map of links, key = nodeId and Link{deviceIndex}. There will. the key will be the nodeId, derived from 
it will create a map of links: key will be nodeId subscription to opcua nodes to be relayed to the mqtt:

# HOW LINKS WORK
Changes to Device-based Opcua Monitored Nodes will trigger publish to mqtt topics to the broker, the topics use the devicePath plus the specific topic (e.g. status, cfg, data) to build the topic string, prepended with the machineId. Ideally this also occurs at some consistent period (250ms).

The Link will have to relay the opcua data into the corresponding json before publishing to broker. Mqtt data will contain the following structure MqttMessage{timestamp: number, payload: unknown}

MonitoredItems can immediately start publishing to the mqtt topic without waiting for mqtt clients to connect.

# MQTT CLIENT CONNECTION FLOW:
When a mqtt client sends a connection message
- store clientId in a map of mqttConnectedClients, if not already there
- if playload contains deviceRegistration, then send the device registration array.
- if payload from client contains heartbeat, then update heartbeat information (we will want to clear old clients)

# DATA STORAGE ON PLC
Machine.Devices[] for deviceStore which contains an array of device data (type Device) and Machine.<DeviceType>[] for deviceTypeStore (e.g. Machine.Axes[], Machine.Robots[])

# ON EXIT
On process shutdown (SIGINT): gracefully closes mqtt and opcua connections and stops heartbeat
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

} from 'node-opcua';
import mqtt, { MqttClient } from 'mqtt';
import CodesysOpcuaDriver from './codesys-opcua-driver';
import { BridgeCmds, DeviceActionRequestData, DeviceId, DeviceTags, MqttTopics, TopicData, buildFullTopicPath, initialMachine } from '@kuriousdesign/machine-sdk';


interface HeartBeatConnection {
    connectionId: string;
    heartbeatValue: number;
    timer: NodeJS.Timer;
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

const PUBLISHING_INTERVAL = 250; // in ms

const subscriptionOptions: CreateSubscriptionRequestOptions = {
    maxNotificationsPerPublish: 1000,
    publishingEnabled: true,
    requestedLifetimeCount: 100,
    requestedMaxKeepAliveCount: 10,
    requestedPublishingInterval: PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
};

const filter = new DataChangeFilter({
    trigger: DataChangeTrigger.StatusValueTimestamp, // Report on any of Status, Value, or Timestamp
    deadbandType: DeadbandType.None,                 // No deadband suppression
    deadbandValue: 0
});


const optionsGroup: MonitoringParametersOptions = {
    discardOldest: true,
    queueSize: 1,
    samplingInterval: PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
    filter
};

import { DeviceRegistration, MachineTags, PlcNamespaces, nodeListString, nodeTypeString } from '@kuriousdesign/machine-sdk'


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
    private opcuaSubscription: ClientSubscription | null = null;
    private monitoredItemGroup: ClientMonitoredItemGroup | null = null;
    private timerConnectionStatusForMqtt: NodeJS.Timeout | null = null;
    private lastPlcHeartbeatValue: number = 0;

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

        // need a map that uses nodeId as key and topic as value


        this.initializeMqttClient().then(() => this.initializeOpcuaClient().then(() => this.retrieveRegisteredDevices().then(() => {
            console.log('create machine entries for nodeIdToMqttTopicMap');
            // add machine topics to the map
            // If you want the property names (keys) as strings:


            // Or if you want both keys and values:
            Object.entries(initialMachine).map(([key, value]) => {
                const tag = key; // property name as string
                // value is the actual property value
                const nodeId = PlcNamespaces.Machine + '.' + tag;
                const topic = PlcNamespaces.Machine.toLowerCase() + '/' + tag.toLowerCase();
                this.nodeIdToMqttTopicMap.set(nodeId, topic);
            });

            console.log('create device entries for nodeIdToMqttTopicMap');
            // add device topics to the map
            this.registeredDevices.forEach((device) => {
                const deviceNodeId = PlcNamespaces.Machine + '.' + MachineTags.deviceStore + '[' + device.id + ']';
                const deviceTopic = buildFullTopicPath(device, this.deviceMap);

                Object.values(DeviceTags).map((tag: string) => {
                    const nodeId = deviceNodeId + '.' + tag;
                    const topic = deviceTopic + '/' + tag.toLowerCase();
                    this.nodeIdToMqttTopicMap.set(nodeId, topic);
                });

                this.subscribeToMqttTopicDeviceHmiActionRequest(device);

            });
            console.log('nodeIdToMqttTopicMap: ', this.nodeIdToMqttTopicMap);

            this.subscribeToMonitoredItems();

            console.log('Both MQTT and OPC UA clients initialized');
        })));
    }



    private async subscribeToMonitoredItems(): Promise<void> {
        if (!this.opcuaSession) {
            throw new Error("OPC UA session is not initialized");
        }
        console.log('Subscribing to monitored items...');
        this.opcuaSubscription = await this.opcuaSession.createSubscription2(
            subscriptionOptions,
        );
        // Subscribe to each nodeId in the map
        const itemToMonitor = [] as { attributeId: number; nodeId: string }[];
        for (const [nodeId, topic] of this.nodeIdToMqttTopicMap) {
            const attributeId = AttributeIds.Value;
            itemToMonitor.push({
                attributeId: AttributeIds.Value,
                nodeId: `${this.nodeListPrefix}${nodeId}`,
            });
        }


        //console.log('Creating monitored item group with items:', itemToMonitor);
        this.monitoredItemGroup = ClientMonitoredItemGroup.create(
            this.opcuaSubscription,
            itemToMonitor,
            optionsGroup,
            TimestampsToReturn.Both,
        );

        await this.monitoredItemGroup.setMonitoringMode(MonitoringMode.Reporting); // <-- forces periodic reporting

        this.monitoredItemGroup.on(
            'initialized',
            () => {
                //console.log('Monitored items initialized!!!!!');
            }
        );

        this.monitoredItemGroup.on(
            'changed',
            (monitoredItem: ClientMonitoredItemBase, dataValue: DataValue) => {

                // Decode the OPC UA value and the node id
                const newValue = this.decipherOpcuaValue(dataValue);
                const fullNodeId = monitoredItem.itemToMonitor.nodeId.value;
                // strip away Node Id Down to just namespace and tag
                const nodeId = fullNodeId.toString().replace(this.nodeListPrefix.replace('ns=4;s=', ''), '');
                const topic = this.nodeIdToMqttTopicMap.get(nodeId);

                //console.log(topic);
                if (!topic) {
                    console.error('No valid MQTT topic found for nodeId:', nodeId);
                } else if (topic && this.mqttClient && this.bridgeHealthIsOk) {
                    //console.log('nodeId changed:', nodeId, 'publishing to topic:', topic, 'with value:', newValue);
                    const message: TopicData = {
                        timestamp: Date.now(),
                        payload: newValue
                    }
                    this.mqttClient.publish(topic, JSON.stringify(message));
                    if (topic === "machine/pdmsts") {
                        //console.log('Machine.pdmSts:', newValue.parts[0].processSts);
                    }
                    if (topic === "machine/estopcircuit_ok") {
                        //console.log('Machine.estopCircuit_OK:', newValue);
                    }
                    if (topic === "machine/heartbeatplc") {
                        //console.log('Machine.heartbeatPlc:', newValue);
                    }
         

                } else if (!this.bridgeHealthIsOk) {
                    console.warn('Bridge connection health is not OK, not publishing to MQTT');
                }
            },
        );
    }

    // create device action request subscription method that creaes subscription to all device action request nodes
    // on message, write to the corresponding opcua node for the device action request
    // the device action request node is located at Machine.DeviceStore[deviceId].ApiOpcua.HmiReq
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
        const tag = 'ApiOpcua.HmiReq';
        //const nodeId = deviceNodeId + '.' + tag;
        const topic = deviceTopic + '/apiOpcua/hmiReq';
        console.log('Subscribing to device action request topic:', topic);
        //this.nodeIdToMqttTopicMap.set(nodeId, topic);
        // subscribe to topic
        this.mqttClient.subscribe(topic, { qos: 1 }, (err) => {
            if (err) {
                console.error('Failed to subscribe to device action request topic:', err.message);
            } else {
                console.log('✅ Subscribed to device action request topic');
            }
        });
    }

    private async handleHmiActionRequest(device: DeviceRegistration, message: DeviceActionRequestData): Promise<void> {
        console.log('Handling HMI Action Request for device:', device, 'with message:', message);

        if (!this.opcuaSession) {
            console.error('OPC UA session is not initialized');
            return;
        }


        //const baseNodeId = `${deviceNodeId}.apiOpcua.hmiReq.actionRequestData`;
        // const paramArrayOfValue = new Float64Array(message.ParamArray);

        // // Write values for SenderId, ActionType, and ActionId
        // const writeValues: WriteValueOptions[] = [
        //     {
        //         attributeId: AttributeIds.Value,
        //         nodeId: `${this.nodeListPrefix}${baseNodeId}.SenderId`,
        //         value: {
        //             value: {
        //                 dataType: DataType.Int16,
        //                 value: message.SenderId
        //             }
        //         }
        //     },
        //     {
        //         attributeId: AttributeIds.Value,
        //         nodeId: `${this.nodeListPrefix}${baseNodeId}.ActionType`,
        //         value: {
        //             value: {
        //                 dataType: DataType.Int16,
        //                 value: message.ActionType // Fixed the type check
        //             }
        //         }
        //     },
        //     {
        //         attributeId: AttributeIds.Value,
        //         nodeId: `${this.nodeListPrefix}${baseNodeId}.ActionId`,
        //         value: {
        //             value: {
        //                 dataType: DataType.Int16,
        //                 value: message.ActionId
        //             }
        //         }
        //     }
        // ];

        // // Separate write value for ParamArray
        // const paramArrayWriteValue: WriteValueOptions = {
        //     attributeId: AttributeIds.Value,
        //     nodeId: `${this.nodeListPrefix}${baseNodeId}.ParamArray`,
        //     value: {
        //         value: {
        //             dataType: DataType.Double, // Changed to Double to match Float32Array, adjust if needed
        //             arrayType: VariantArrayType.Array,
        //             value: paramArrayOfValue
        //         }
        //     }
        // };


        try {
            // Write SenderId, ActionType, and ActionId first
            //await this.opcuaSession.write(writeValues);
            //console.log('Successfully wrote to OPC UA nodes (excluding ParamArray):', writeValues.map(w => w.nodeId));

            // Write ParamArray separately
            //await this.opcuaSession.write(paramArrayWriteValue);
            //console.log('Successfully wrote ParamArray to OPC UA node:', paramArrayWriteValue.nodeId);
            this.codesysOpcuaDriver?.requestAction(device.id, message.ActionType, message.ActionId, message.ParamArray);
        } catch (error) {
            console.error('Failed to write to OPC UA nodes, Error:', error);
        }
    }

    private async initializeMqttClient(): Promise<void> {
        console.log('connecting to mqtt broker at: ' + this.mqttBrokerUrl);
        this.mqttClient = mqtt.connect(this.mqttBrokerUrl, this.mqttOptions as mqtt.IClientOptions);
        this.mqttClient.on('connect', () => {
            console.log('✅ MQTT client connected to broker');
            this.updateMqttConnectionStatus(true);
            // create subscription to bridge commands and handle incoming messages
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
                    console.log('Received message on topic:', topic, 'with message:', message.toString());
                    if (topic.endsWith('/apiOpcua/hmiReq')) {
                        const hmiActionReqData = JSON.parse(message.toString()) as DeviceActionRequestData;

                        // extract deviceId from topic
                        const topicParts = topic.split('/');
                        const deviceIdStr = topicParts[topicParts.length - 3];
                        const deviceId = Number(deviceIdStr);
                        if (isNaN(deviceId)) {
                            console.error('Invalid deviceId extracted from topic:', topic);
                            return;
                        }
                        //console.log('Received HMI Action Request:', hmiActionReqData, 'for deviceId:', deviceId);
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

    private async handleBridgeCommand(message: TopicData): Promise<void> {
        const cmdData = message.payload as { cmd: BridgeCmds };
        console.log('Received bridge command:', cmdData.cmd);
        // Handle the command as needed

        switch (cmdData.cmd) {
            case BridgeCmds.CONNECT:
                // publish the deviceMap if it exists, if not create a timer that keeps checking
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
                    // Create a timer to check for deviceMap availability
                    // const checkDeviceMap = setInterval(() => {
                    //     if (this.deviceMap && this.mqttClient) {
                    //         const message: TopicData = {
                    //             timestamp: Date.now(),
                    //             payload: Array.from(this.deviceMap.entries())
                    //         };
                    //         this.mqttClient.publish(MqttTopics.BRIDGE_STATUS, JSON.stringify(message));
                    //         clearInterval(checkDeviceMap);
                    //     }
                    // }, 1000);
                }
                break;
            case BridgeCmds.DISCONNECT:
                //this.handleDisconnectCommand();
                break;
            default:
                console.warn('Unknown bridge command:', cmdData.cmd);
        }
    }

    private async initializeOpcuaClient(): Promise<void> {
        this.opcuaClient = OPCUAClient.create(opcuaOptions);

        this.opcuaClient.on("backoff", (retry, delay) => {
            console.warn(
                `OPC UA retry #${retry}, next attempt in ${delay} ms`
            );
            if (this.mqttBrokerIsConnected && this.mqttClient) {
                // publish a bridge health status message
                const message: TopicData = {
                    timestamp: Date.now(),
                    payload: "OPC UA connection lost"
                };
                this.mqttClient.publish(MqttTopics.BRIDGE_STATUS, JSON.stringify(message));
                //this.mqttClient.('error', new Error(JSON.stringify(message)));
                //console.log("Published bridge error:", message);
            }
        });

        this.opcuaClient.on("connection_lost", () => {
            console.warn("OPC UA connection lost");
            this.updateOpcuaConnectionStatus(false);
        });

        this.opcuaClient.on("after_reconnection", () => {
            console.log("✅ OPC UA reconnected");
            //this.updateOpcuaConnectionStatus(true);
        });

        try {
            console.log("connecting to opcua endpoint: " + this.opcuaEndpoint);
            await this.opcuaClient.connect(this.opcuaEndpoint);
            console.log("✅ OPC UA client connected");

            this.opcuaSession = await this.opcuaClient.createSession();
            this.codesysOpcuaDriver = new CodesysOpcuaDriver(DeviceId.HMI, this.opcuaSession, this.opcuaControllerName);
            this.dataTypeManager = new ExtraDataTypeManager();

            console.log("✅ OPC UA session created");

            //this.updateOpcuaConnectionStatus(true);

            // create heartbeat with opcua tag that gets value every 1000ms
            this.opcuaHeartBeatConnection = {
                connectionId: 'opcua-heartbeat',
                heartbeatValue: 0,
                timer: setInterval(async () => {
                    if (this.opcuaSession && this.opcuaHeartBeatConnection) {
                        const heartbeatPlcNodeId = this.concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatPLC);
                        const heartbeatValue = await this.readOpcuaValue(heartbeatPlcNodeId);
                        if (heartbeatValue === null || this.lastPlcHeartbeatValue === heartbeatValue) {
                            console.warn('OPC UA Heartbeat value not changing, connection may be lost');
                            this.updateOpcuaConnectionStatus(false);
                            return;
                        }
                        this.updateOpcuaConnectionStatus(true);
                        // store the heartbeat value
                        this.opcuaHeartBeatConnection.heartbeatValue = heartbeatValue as number;
                        this.lastPlcHeartbeatValue = heartbeatValue as number;
                        console.log('OPC UA Heartbeat value:', heartbeatValue);
                        // write hmi heartbeat value back to opcua
                        const heartbeatHmiNodeId = this.concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatHMI);
                        await this.writeOpcuaValue(heartbeatHmiNodeId, heartbeatValue, DataType.Byte);
                    }
                }, 1000)
            };

            // Wait for OPC UA connection to be established
            while (!this.opcuaServerIsConnected) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
            console.log("✅ OPC UA connection established, proceeding...");

        } catch (err: any) {
            console.error("❌ Failed to connect to OPC UA:", err.message);

            // Retry after delay
            setTimeout(() => {
                this.initializeOpcuaClient();
            }, 5000);
        }
    }

    private async retrieveRegisteredDevices(): Promise<DeviceRegistration[]> {
        if (!this.opcuaServerIsConnected) {
            console.log("OPC UA server not connected, cannot retrieve registered devices");
            return [];
        }
        console.log("Retrieving registered devices from OPC UA...");
        //1. build tag
        const registeredDevicesNodeId = this.concatNodeId(PlcNamespaces.Machine, MachineTags.registeredDevices);

        //2. read 
        const heartbeatPlcNodeId = this.concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatPLC);

        //console.log('reading OPC UA value from: ' + heartbeatPlcNodeId);
        const heartbeatPlc = await this.readOpcuaValue(heartbeatPlcNodeId) as number;
        //console.log('Current PLC Heartbeat value: ' + heartbeatPlc);
        //console.log('reading OPC UA value from: ' + registeredDevicesNodeId);
        const registeredDevices = await this.readOpcuaValue(registeredDevicesNodeId) as DeviceRegistration[];
        // remove any items that have id of 0
        this.registeredDevices = registeredDevices.filter((device: DeviceRegistration) => device.id !== 0)
        // build device map
        this.registeredDevices.forEach(deviceReg => {
            this.deviceMap.set(deviceReg.id, deviceReg);
            console.log("adding device to map:", deviceReg.id, "with parent id:", deviceReg.parentId);
        });
        console.log("Retrieved registered devices, count:", this.registeredDevices.length);
        return registeredDevices;
    }

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
            //console.log(`Wrote OPC UA value to ${nodeId}:`, value);
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
            }
            const data = await this.opcuaSession.read(readValueOptions);
            const value = this.decipherOpcuaValue(data);
            //console.log(`Deciphered OPC UA value from ${nodeId}:`, value);
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
            data.value.arrayType === VariantArrayType.Array
                ? Array.from(data.toJSON().value.value)
                : (data.toJSON().value.value);

        return decipheredValue;
    }

    private concatNodeId(namespace: string, tag: string): string {
        return `${this.nodeListPrefix}${namespace}.${tag}`;
    }

    privateconcatNodeIdOfArrayTag(namespace: string, tag: string, index: number): string {
        return `${this.nodeListPrefix}.${namespace}.${tag}[${index}]`;
    }


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
            //console.log(`Published bridge connection status:`, message);
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
        // have it reset the timer and fire immediately

        this.publishBridgeConnectionStatus();
    }
}

export default OpcuaMqttBridge;