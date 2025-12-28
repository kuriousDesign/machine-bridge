import {
    AttributeIds,
    ClientSession,
    DataType,
    DataValue,
    MessageSecurityMode,
    OPCUAClient,
    OPCUAClientOptions,
    ReadValueIdOptions,
    SecurityPolicy,
    StatusCodes,
    VariantArrayType,
    WriteValueOptions,
} from 'node-opcua';

import Config from './config'; // <--- Use the central config

import { BridgeCmds, DeviceActionRequestData, DeviceId, DeviceRegistration, DeviceTags, MachineTags, MqttTopics, PlcNamespaces, TopicData, buildFullTopicPath, initialMachine, nodeListString, Device } from '@kuriousdesign/machine-sdk';
import MqttClientManager from './MqttClientManager';
import CodesysOpcuaDriver from './OpcuaMqtt/codesys-opcua-driver';





interface ReadItemInfo {
    nodeId: string;
    mqttTopic: string;
}

// --- Performance Tracking Variables ---
let minReadDurationMs: number = Infinity;
let maxReadDurationMs: number = 0;
let totalReads: number = 0;
let totalDurationMs: number = 0;
let maxTimeBetweenScansMs: number = 0;
let lastScanTime: [number, number] | null = null;

// --- State Machine Definition ---
enum OpcuaState {
    Disconnected,
    Connecting,
    Connected,
    Polling,
    Reconnecting,
    WaitingForHeartbeat,
    Disconnecting,
}

function decipherOpcuaValue(data: any): any {
    const decipheredValue =
        data?.value?.arrayType === VariantArrayType.Array
            ? Array.from(data.toJSON().value.value)
            : (data?.toJSON()?.value?.value);
    return decipheredValue;
}

function concatNodeId(namespace: string, tag: string): string {
    return `${Config.NODE_LIST_PREFIX}${namespace}.${tag}`;
}

export default class OpcuaClientManager {
    private mqttClientManager: MqttClientManager;
    private state: OpcuaState = OpcuaState.Disconnected;
    private client: OPCUAClient | null = null;
    private session: ClientSession | null = null;

    private shutdownRequested: boolean = false;

    private registeredDevices: DeviceRegistration[] = []
    private deviceMap: Map<number, DeviceRegistration> = new Map();
    private codesysOpcuaDriver: CodesysOpcuaDriver | null = null;

    //private nodeListPrefix = nodeListString + Config.OPCUA_CONTROLLER_NAME + '.Application.';

    // constructor 
    constructor() {
        if (Config.ENABLE_DIAGNOSTICS) {
            console.log(`Diagnostics ENABLED. Skipping stats for the first ${Config.DIAG_READS_TO_SKIP_AT_START} scans.`);
        } else {
            console.log(`Diagnostics DISABLED.`);
        }
        this.mqttClientManager = new MqttClientManager();
        this.mqttClientManager.manageConnectionLoop();
    }

    public requestShutdown(): void {
        this.shutdownRequested = true;
        console.log("Shutdown requested. Transitioning to Disconnecting state.");
    }

    public async manageConnectionLoop(): Promise<void> {
        let prevHeartbeatPlcValue = -1;
        let lastUpdateTime = Date.now();
        while (!this.shutdownRequested) {
            if (this.state !== this.lastPublishedState) {
                console.log(`[OPCUA] STATE: ${OpcuaState[this.state]}`);
                this.lastPublishedState = this.state;
            }
            //this.publishBridgeConnectionStatus();
            switch (this.state) {
                case OpcuaState.Disconnected:
                case OpcuaState.Reconnecting:
                    await this.handleConnection();

                    await this.updateRegisteredDevices();
                    //this.devicePollingItems = this.getDeviceReadItems();
                    //this.allPollingItems = this.machinePollingItems.concat(this.devicePollingItems);
                    //console.log("Total polling items after device update:", this.allPollingItems.length);
                    // subscribe to device HMI action request topic
                    Array.from(this.deviceMap.values()).map(async device =>
                        await this.subscribeToExtDeviceUpdateDevice(device)
                    );
                    break;

                case OpcuaState.Connected:
                case OpcuaState.Polling:
                case OpcuaState.WaitingForHeartbeat:
                    break;

                case OpcuaState.Disconnecting:
                    await this.handleDisconnect();
                    return; // Exit loop after graceful disconnect
            }
            // Small delay to prevent tight blocking loop if state transitions rapidly
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        await this.handleDisconnect();
        await this.mqttClientManager.requestShutdown();
    }

    private async handleConnection(): Promise<void> {
        this.state = OpcuaState.Connecting;
        console.log(`[OPCUA]Connecting to endpoint: ${Config.OPCUA_ENDPOINT}`);

        try {
            this.client = OPCUAClient.create(Config.OPCUA_OPTIONS);
            // Attach built-in handlers for automatic reconnection messages
            this.client.on("connection_lost", () => console.warn("[OPCUA] OPC UA connection lost (handled by internal mechanism)"));
            this.client.on("after_reconnection", () => console.log("[OPCUA] ✅ OPC UA client reconnected internally"));
            // dispose of handlers on mqttClientManager
            this.mqttClientManager.clearAllHandlers();

            await this.client.connect(Config.OPCUA_ENDPOINT);
            this.session = await this.client.createSession();
            console.log("[OPCUA] ✅ Connected to server and session created.");
            this.codesysOpcuaDriver = new CodesysOpcuaDriver(DeviceId.HMI, this.session, Config.OPCUA_CONTROLLER_NAME);

            this.state = OpcuaState.Connected;

        } catch (err) {
            console.error(`[OPCUA] ❌ Failed to connect/create session: ${err instanceof Error ? err.message : String(err)}`);
            if (this.client) {
                await this.client.disconnect();
            }
            this.client = null;
            this.session = null;
            console.log(`[OPCUA] Will retry in ${Config.RECONNECT_DELAY_MS}ms...`);
            this.state = OpcuaState.Reconnecting;
            await new Promise(resolve => setTimeout(resolve, Config.RECONNECT_DELAY_MS));
        }
    }

    private async updateRegisteredDevices(): Promise<void> {

        console.log("Retrieving registered devices from OPC UA...");
        const registeredDevicesNodeId = concatNodeId(PlcNamespaces.Machine, MachineTags.registeredDevices);
        const registeredDevices = await this.readOpcuaValue(registeredDevicesNodeId) as DeviceRegistration[];

        this.registeredDevices = (registeredDevices || []).filter((device: DeviceRegistration) => device.id !== 0);
        console.log("[OPCUA] BUILDING DEVICE MAP");
        this.registeredDevices.forEach(deviceReg => {
            const topicPath = buildFullTopicPath(deviceReg, this.deviceMap);
            const devicePath = topicPath.split('/');
            deviceReg.devicePath = devicePath;
            this.deviceMap.set(deviceReg.id, deviceReg);
            console.log("Adding", deviceReg.mnemonic, "to deviceMap, id:", deviceReg.id, "with path:", deviceReg.devicePath);
        });
        console.log("Retrieved registered devices, count:", this.registeredDevices.length);
    }

    private async subscribeToExtDeviceUpdateDevice(device: DeviceRegistration): Promise<void> {
        if (!this.session) {
            throw new Error("OPC UA session is not initialized");
        }
        if (!this.mqttClientManager) {
            throw new Error("MQTT client is not initialized");
        }
        if (!this.deviceMap || this.deviceMap.size === 0) {
            throw new Error("Device map is not initialized or empty");
        }

        //const deviceTopic = buildFullTopicPath(device, this.deviceMap);

        if (device.isExternalService) {
            //const topic = device.mnemonic.toLowerCase() + '/';
            const deviceTags = DeviceTags;
            const baseTopic = Config.BRIDGE_API_UPDATE_DEVICE + '/' + device.id.toString();

            // Subscribe to each key in deviceTags
            // Object.keys(deviceTags).forEach(async tagKey => {
            //     const topic = baseTopic + '/' + tagKey.toLowerCase();
            //     console.log('Subscribing to topic for external service device tag relay:', topic);
            //     this.mqttClientManager.subscribe(topic, async (topic: string, message: Buffer) => {
            //         this.handleExternalServiceDeviceTagRelayToPlc(topic, JSON.parse(message.toString()) as TopicData)
            //     });
            // });

            const topic = baseTopic + '/sts';
            console.log('Subscribing to topic for external service sts tag:', topic);
            this.mqttClientManager.subscribe(topic, async (topic: string, message: Buffer) => {
                this.handleExternalServiceDeviceTagRelayToPlc(topic, JSON.parse(message.toString()) as TopicData)
            });
        }
    }

    private async handleExternalServiceDeviceTagRelayToPlc(topic: string, message: TopicData): Promise<void> {
        const completeData = message.payload as unknown;
        const topicParts = topic.split('/');
        const deviceId = topicParts[topicParts.length - 2];
        const tagName = topicParts[topicParts.length - 1];
        const deviceReg = this.deviceMap.get(Number(deviceId));
        if (!deviceReg) {
            console.error('No device found for deviceId:', deviceId);
            return;
        }
        if (!message.payload || completeData === undefined || completeData === null) {
            console.error('No payload found in message for deviceId:', deviceId, ' topic:', topic);
            return;
        }
        if (tagName === 'sts') {
            const deviceTag = PlcNamespaces.Machine + '.' + deviceReg.mnemonic.toLowerCase() + 'Sts' + 'ExtService';

            //delete the iExtService.o: expected 0, got 1. Type of written value: number, Type of read value: number
            if (typeof completeData === 'object' && completeData !== null && 'iExtService' in completeData && typeof (completeData as Record<string, any>)['iExtService'] === 'object' && (completeData as Record<string, any>)['iExtService'] !== null && 'o' in (completeData as Record<string, any>)['iExtService']){
                delete ((completeData as Record<string, any>)['iExtService'] as Record<string, any>)['o'];
                //console.log('Deleted iExtService.o from sts payload for deviceId:', deviceId);
            }

            await this.codesysOpcuaDriver?.writeNestedObject(deviceTag, completeData);
        } else {
            const deviceTag = PlcNamespaces.Machine + '.' + MachineTags.deviceStore + '[' + deviceId + ']' + '.' + tagName;
            await this.codesysOpcuaDriver?.writeNestedObject(deviceTag, message.payload);
        }
    }


    private async handleHmiActionRequest(topic: string, hmiActionReqData: DeviceActionRequestData): Promise<void> {
        //const hmiActionReqData = JSON.parse(message.toString()) as DeviceActionRequestData;
        const topicParts = topic.split('/');
        const deviceIdStr = topicParts[topicParts.length - 1];
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
        console.log('Handling HMI Action Request for device:', device.mnemonic);
        await this.codesysOpcuaDriver?.requestAction(device.id, hmiActionReqData.ActionType, hmiActionReqData.ActionId, hmiActionReqData.ParamArray);

    }

    private async handleDisconnect(): Promise<void> {
        this.state = OpcuaState.Disconnecting;
        console.log("Starting graceful disconnection and cleanup...");
        if (this.session) {
            await this.session.close();
            this.session = null;
            console.log("✅ Session closed.");
        }
        if (this.client) {
            await this.client.disconnect();
            this.client = null;
            console.log("✅ Client disconnected.");
        }
        this.state = OpcuaState.Disconnected;
        console.log("✅ System fully disconnected. Exiting.");
    }

    private async readOpcuaValue(nodeId: string): Promise<any> {
        if (!this.session) {
            throw new Error("OPC UA session is not initialized");
        }


        const readValueOptions: ReadValueIdOptions = {
            nodeId: nodeId,
            attributeId: AttributeIds.Value,
        };
        const data = await this.session.read(readValueOptions);
        const value = decipherOpcuaValue(data);
        if (data.statusCode === StatusCodes.Good) {
            return value;
        } else {
            console.warn(`Failed to read OPC UA value from ${nodeId}: ${data.statusCode}`);
            return null;
        }

    }

    private async writeOpcuaValue(nodeId: string, value: any, dataType: DataType): Promise<void> {
        if (!this.session) {
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
            await this.session.write(writeValue);
            const readValue = await this.readOpcuaValue(nodeId); // verify write
            if (readValue !== value) {
                console.error(`Verification failed for node ${nodeId}: expected ${value}, got ${readValue}`);
            }
        } catch (error) {
            console.error(`Failed to write OPC UA value to ${nodeId}:`, error);
            throw error;
        }
    }

    private lastPublishedState: OpcuaState | null = null;
    private lastPublishTime: number = 0;

    private async handleBridgeCommand(message: TopicData): Promise<void> {
        const cmdData = message.payload as { cmd: BridgeCmds };
        console.log('Received bridge command:', cmdData.cmd);

        switch (cmdData.cmd) {
            case BridgeCmds.CONNECT:
                if (this.deviceMap.size > 0) {
                    //console.log("Publishing deviceMap to bridge");
                    this.mqttClientManager.publish(MqttTopics.DEVICE_MAP, Array.from(this.deviceMap.entries()));
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

    
}

async function main() {
    console.log("Application starting...");


    const manager = new OpcuaClientManager();

    // Handle graceful shutdown via Ctrl+C
    process.on('SIGINT', async () => {
        console.log("\nSIGINT received. Shutting down gracefully.");
        manager.requestShutdown();
        // Give the state machine loop time to complete the disconnect process
        // A better approach in a real app might use a promise/event listener here
        setTimeout(() => process.exit(0), 5000);
    });

    await manager.manageConnectionLoop();
    // Script finishes here after disconnection is complete.
}

//main().catch(console.error);
