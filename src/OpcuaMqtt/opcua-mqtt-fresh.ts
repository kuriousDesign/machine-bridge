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

} from 'node-opcua';
import mqtt, { MqttClient } from 'mqtt';

import { Device } from '@kuriousdesign/machine-sdk';


interface HeartBeatConnection {
    connectionId: string;
    heartbeatValue: number;
    timer: NodeJS.Timer;
}

const connectionStrategy: ConnectionStrategyOptions = {
    initialDelay: 1000,
    maxDelay: 4000,
};

const opcuaOptions: OPCUAClientOptions = {
    applicationName: 'OpcuaMqttBridge',
    connectionStrategy: connectionStrategy,
    securityMode: MessageSecurityMode.None,
    securityPolicy: SecurityPolicy.None,
    endpointMustExist: true,
    keepSessionAlive: true,
};

const subscriptionOptions = {
    maxNotificationsPerPublish: 1000,
    publishingEnabled: true,
    requestedLifetimeCount: 100,
    requestedMaxKeepAliveCount: 10,
    requestedPublishingInterval: 50, //affects lag, decrease to reduce lag
};

const optionsGroup = {
    discardOldest: true,
    queueSize: 1,
    samplingInterval: 50, //affects lag, decrease to reduce lag
};

import { DeviceRegistration, MachineTags, PlcNamespaces, nodeListString, nodeTypeString } from '@kuriousdesign/machine-sdk'



class OpcuaMqttBridge {
    private mqttClient: MqttClient | null = null;
    private opcuaClient: OPCUAClient | null = null;
    private opcuaSession: ClientSession | null = null;
    private opcuaControllerName: string;
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
    private dataTypeManager: ExtraDataTypeManager | null = null;
    private nodeIdToMqttTopicMap = new Map<string, string>();
    private opcuaSubscription: ClientSubscription | null = null;
    private monitoredItemGroup: ClientMonitoredItemGroup | null = null;

    constructor(mqttBrokerUrl: string, mqttOptions: unknown, opcuaEndpoint: string, opcuaControllerName: string) {
        console.log('Initializing OpcuaMqttBridge...');
        this.mqttOptions = mqttOptions;
        this.opcuaEndpoint = opcuaEndpoint;
        this.mqttBrokerUrl = mqttBrokerUrl;
        this.opcuaControllerName = opcuaControllerName;
        this.nodeTypePrefix = nodeTypeString + this.opcuaControllerName + '.Application.';
        this.nodeListPrefix = nodeListString + this.opcuaControllerName + '.Application.';
        // need a map that uses nodeId as key and topic as value


        this.initializeMqttClient().then(() => this.initializeOpcuaClient().then(() => this.retrieveRegisteredDevices().then(() => {
            console.log('create device entries for nodeIdToMqttTopicMap');
            this.registeredDevices.forEach((device) => {
                const deviceNodeId = PlcNamespaces.Machine + '.' + MachineTags.deviceStore + '[' + device.id + ']';
                const deviceTopic = 'machine/' + device.parentId + '/' + device.id;
                // sts: Is
                let nodeId = deviceNodeId + '.Is';
                let topic = deviceTopic + '/is';
                this.nodeIdToMqttTopicMap.set(nodeId, topic);
                // Cfg
                nodeId = deviceNodeId + '.Cfg';
                topic = deviceTopic + '/cfg';
                this.nodeIdToMqttTopicMap.set(nodeId, topic);
            });

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

        this.monitoredItemGroup.on(
            'initialized',
            () => {
                console.log('Monitored items initialized!!!!!');
            }
        );

        this.monitoredItemGroup.on(
            'changed',
            (monitoredItem: ClientMonitoredItemBase, dataValue: DataValue) => {
                //console.log('OPC UA value changed:', dataValue);
                const newValue = this.decipherOpcuaValue(dataValue);
                const fullNodeId = monitoredItem.itemToMonitor.nodeId;
                // stripe prefix from fullNodeId
                const nodeId = fullNodeId.toString().replace(this.nodeListPrefix, '');

                //socket.emit('newValue', newValue);
                const topic = this.nodeIdToMqttTopicMap.get(nodeId);
                if (topic && this.mqttClient && this.bridgeHealthIsOk) {
                    this.mqttClient.publish(topic, newValue);
                    console.log(`Published to MQTT topic ${topic}:`, newValue);
                } else if (!this.bridgeHealthIsOk) {
                    console.warn('Bridge health is not OK, not publishing to MQTT');
                } else {
                    console.warn('No valid MQTT topic found for nodeId:', nodeId);
                }
            },
        );





    }

    private async initializeMqttClient(): Promise<void> {
        console.log('connecting to mqtt broker at: ' + this.mqttBrokerUrl);
        this.mqttClient = mqtt.connect(this.mqttBrokerUrl, this.mqttOptions as mqtt.IClientOptions);
        this.mqttClient.on('connect', () => {
            console.log('✅ MQTT client connected to broker');
            this.updateMqttConnectionStatus(true);
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
    private async initializeOpcuaClient(): Promise<void> {
        this.opcuaClient = OPCUAClient.create(opcuaOptions);

        this.opcuaClient.on("backoff", (retry, delay) => {
            console.warn(
                `OPC UA retry #${retry}, next attempt in ${delay} ms`
            );
        });

        this.opcuaClient.on("connection_lost", () => {
            console.warn("OPC UA connection lost");
            this.updateOpcuaConnectionStatus(false);
        });

        this.opcuaClient.on("after_reconnection", () => {
            console.log("✅ OPC UA reconnected");
            this.updateOpcuaConnectionStatus(true);
        });

        try {
            console.log("connecting to opcua endpoint: " + this.opcuaEndpoint);
            await this.opcuaClient.connect(this.opcuaEndpoint);
            console.log("✅ OPC UA client connected");

            this.opcuaSession = await this.opcuaClient.createSession();
            this.dataTypeManager = new ExtraDataTypeManager();

            console.log("✅ OPC UA session created");

            this.updateOpcuaConnectionStatus(true);
        } catch (err: any) {
            console.error("❌ Failed to connect to OPC UA:", err.message);

            // Retry after delay
            setTimeout(() => {
                this.initializeOpcuaClient();
            }, 5000);
        }
    }

    private async retrieveRegisteredDevices(): Promise<DeviceRegistration[]> {
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
        console.log("Retrieved registered devices, count:", this.registeredDevices.length);
        //console.log(registeredDevices[8].id);
        return registeredDevices;
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
            console.error(`Failed to read OPC UA value from ${nodeId}:`, error);
            throw error;
        }
    }

    // Add this helper method to the class (recursive for nested arrays/objects)
    private toPlainObjects(value: any): any {
        if (Array.isArray(value)) {
            return value.map(item => this.toPlainObjects(item));
        } else if (value !== null && typeof value === 'object') {
            const plainObj: any = {};
            for (const key in value) {
                if (Object.prototype.hasOwnProperty.call(value, key)) {
                    plainObj[key] = this.toPlainObjects(value[key]);
                }
            }
            return plainObj;
        }
        return value; // Primitives remain unchanged
    }

    private decipherOpcuaValue(data: any): any {
        const decipheredValue =
            data.value.arrayType === VariantArrayType.Array
                ? Array.from(data.toJSON().value.value)
                : data.value.value;

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
            console.log(`Bridge Connection Health is now: ${newState ? 'OK' : 'NOT OK'}`);
        }
        this.bridgeHealthIsOk = newState;
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
    }

}

export default OpcuaMqttBridge;