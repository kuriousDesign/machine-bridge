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
    private machinePollingItems: ReadItemInfo[] = getMachineReadItems();
    private devicePollingItems: ReadItemInfo[] = [];
    private allPollingItems: ReadItemInfo[] = this.machinePollingItems;
    private allPollingValues: any[] = [];
    private shutdownRequested: boolean = false;
    private heartbeatPlcValue: number = 0;
    private heartbeatHmiValue: number = 0;
    private heartbeatHmiNodeId = concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatHMI);
    private heartbeatPlcNodeId = concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatPLC);
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
            }
            this.publishBridgeConnectionStatus();
            switch (this.state) {
                case OpcuaState.Disconnected:
                case OpcuaState.Reconnecting:
                    await this.handleConnection();

                    await this.updateRegisteredDevices();
                    this.devicePollingItems = this.getDeviceReadItems();
                    this.allPollingItems = this.machinePollingItems.concat(this.devicePollingItems);
                    console.log("Total polling items after device update:", this.allPollingItems.length);
                    // subscribe to device HMI action request topic
                    Array.from(this.deviceMap.values()).map(async device =>
                        await this.subscribeToMqttTopicDeviceHmiActionRequest(device)
                    );
                    break;

                case OpcuaState.Connected:
                case OpcuaState.Polling:
                case OpcuaState.WaitingForHeartbeat:
                    await this.handlePolling();
                    // i want to add some logic if the plc heartbeat isn't updating to go to waiting for heartbeat state
                    if (this.heartbeatPlcValue !== prevHeartbeatPlcValue) {
                        // Heartbeat is updating normally, stay in Polling state
                        lastUpdateTime = Date.now();
                        prevHeartbeatPlcValue = this.heartbeatPlcValue;
                    } else if (Date.now() - lastUpdateTime > 5000) {
                        // Heartbeat not updating, transition to waiting state
                        this.state = OpcuaState.WaitingForHeartbeat;
                        console.warn("⚠️ PLC heartbeat not updating. Waiting for heartbeat...");
                    }
                    break;

                case OpcuaState.Disconnecting:
                    await this.handleDisconnect();
                    return; // Exit loop after graceful disconnect
            }
            // Small delay to prevent tight blocking loop if state transitions rapidly
            await new Promise(resolve => setTimeout(resolve, Config.LOOP_DELAY_MS));
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

    private getDeviceReadItems(): ReadItemInfo[] {
        const readIteams: ReadItemInfo[] = [];
        this.registeredDevices.forEach((device) => {
            const deviceNodeId = PlcNamespaces.Machine + '.' + MachineTags.deviceStore + '[' + device.id + ']';
            const deviceTopic = buildFullTopicPath(device, this.deviceMap);

            if (device.isExternalService) {
                console.log(`[OPCUA] Skipping external service device for polling, Device ID: ${device.id}, Mnemonic: ${device.mnemonic}`);
                let nodeId = deviceNodeId + '.' + DeviceTags.ApiOpcuaPlcReq;
                let topic = deviceTopic + '/' + DeviceTags.ApiOpcuaPlcReq.toLowerCase().replace('.', '/');
                let readItemInfo : ReadItemInfo = {
                    nodeId: nodeId,
                    mqttTopic: topic
                };
                readIteams.push(readItemInfo);
                // registration
                nodeId = deviceNodeId + '.' + DeviceTags.Registration;
                topic = deviceTopic + '/' + DeviceTags.Registration.toLowerCase().replace('.', '/');
                readItemInfo = {
                    nodeId: nodeId,
                    mqttTopic: topic
                };
                readIteams.push(readItemInfo);
                return; // skip external service devices
            } else {
                Object.values(DeviceTags).map((tag: string) => {
                    const nodeId = deviceNodeId + '.' + tag;
                    const topic = deviceTopic + '/' + tag.toLowerCase().replace('.', '/');
                    readIteams.push({ nodeId: nodeId, mqttTopic: topic });
                    console.log(`[OPCUA] Added device tag for polling, NodeId: ${nodeId}, Topic: ${topic}`);
                });

                // device log
                const deviceLogNodeId = PlcNamespaces.Machine + '.' + MachineTags.deviceLogs + '[' + device.id + ']';
                const deviceLogTopic = deviceTopic + '/log';
                //this.nodeIdToMqttTopicMap.set(deviceLogNodeId, deviceLogTopic);
                readIteams.push({ nodeId: deviceLogNodeId, mqttTopic: deviceLogTopic });

                // device sts
                const deviceStsNodeId = PlcNamespaces.Machine + '.' + device.mnemonic.toLowerCase() + 'Sts';
                const deviceStsTopic = deviceTopic + '/sts';
                if (!(device.mnemonic === 'SYS' || device.mnemonic === 'CON' || device.mnemonic === 'HMI')) {
                    //this.nodeIdToMqttTopicMap.set(deviceStsNodeId, deviceStsTopic);
                    readIteams.push({ nodeId: deviceStsNodeId, mqttTopic: deviceStsTopic });
                }
            }
        });
        this.devicePollingItems = readIteams;
        return readIteams;
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

    private async subscribeToMqttTopicDeviceHmiActionRequest(device: DeviceRegistration): Promise<void> {
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
        const topic = MqttTopics.HMI_ACTION_REQ + '/' + device.id.toString();
        console.log('Subscribing to device action request topic:', topic);

        this.mqttClientManager.subscribe(topic, async (topic: string, message: Buffer) => {
            this.handleHmiActionRequest(topic, JSON.parse(message.toString()) as DeviceActionRequestData);
        });
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
        console.log('Handling HMI Action Request for device:', device, 'with data:', hmiActionReqData);
        this.codesysOpcuaDriver?.requestAction(device.id, hmiActionReqData.ActionType, hmiActionReqData.ActionId, hmiActionReqData.ParamArray);

    }

    private async publishTags(chunkIndex: number): Promise<void> {
        const startingIndex = chunkIndex * Config.CHUNK_SIZE;
        const endingIndex = Math.min(startingIndex + Config.CHUNK_SIZE, this.allPollingItems.length);
        await Promise.all(this.allPollingItems.slice(startingIndex, endingIndex).map((item, index) => {
            return this.mqttClientManager.publish(item.mqttTopic, this.allPollingValues[startingIndex + index]);
        }));
    }

    private async handlePolling(): Promise<void> {

        try {
            // Your polling logic from the previous script
            const timeStart = process.hrtime();
            const numChunks = Math.ceil(this.allPollingItems.length / Config.CHUNK_SIZE);
            for (let chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
                await this.pollAllTags(chunkIndex);
                await this.publishTags(chunkIndex);
            }

            await this.updateHeartbeat();
            const timeEnd = process.hrtime(timeStart);
            const durationMs = (timeEnd[0] * 1000) + (timeEnd[1] / 1e6);

            // Go back to connected state to loop again after POLLING_RATE_MS delay
            if (this.state === OpcuaState.WaitingForHeartbeat) {
                this.state = OpcuaState.Reconnecting;
            } else {
                this.state = OpcuaState.Polling;
            }
            await new Promise(resolve => setTimeout(resolve, Math.max(0, Config.POLLING_RATE_MS - durationMs - Config.LOOP_DELAY_MS)));

        } catch (error) {
            // A read error likely means the session or connection is bad.
            console.error("❌ Polling failed. Assuming connection issue, attempting reconnection:", error);
            this.state = OpcuaState.Reconnecting;
            this.session = null; // Invalidate session
        }
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


    private async publishBridgeConnectionStatus(): Promise<void> {
        const now = Date.now();
        const stateChanged = this.state !== this.lastPublishedState;
        const secondElapsed = now - this.lastPublishTime >= 3000;

        if (!stateChanged && !secondElapsed) {
            return;
        }

        let payload: string;
        if (this.state === OpcuaState.Polling) {
            payload = "Running";
        } else {
            payload = "Opcua Server Disconnected";
        }

        this.mqttClientManager.publish(MqttTopics.BRIDGE_STATUS, payload);

        if (this.deviceMap.size > 0) {
            //console.log("Publishing deviceMap to bridge");
            this.mqttClientManager.publish(MqttTopics.DEVICE_MAP, Array.from(this.deviceMap.entries()));
        } else {
            console.log("DeviceMap not yet available, cannot publish to bridge/deviceMap");
        }
        this.lastPublishedState = this.state;
        this.lastPublishTime = now;
    }

    private async updateHeartbeat(): Promise<void> {
        this.heartbeatPlcValue = await this.readOpcuaValue(this.heartbeatPlcNodeId);
        if (this.heartbeatPlcValue !== this.heartbeatHmiValue) {
            //console.log(`Heartbeat PLC Value: ${this.heartbeatPlcValue}`);
            this.heartbeatHmiValue = this.heartbeatPlcValue;
            await this.writeOpcuaValue(this.heartbeatHmiNodeId, this.heartbeatPlcValue, DataType.Byte);
            if (this.codesysOpcuaDriver){
                await this.codesysOpcuaDriver.writeCurrentTimeToCodesys();
            }
        }
    }
    /**
     * Reads all specified tags once, measuring performance if diagnostics are enabled.
     */
    private async pollAllTags(chunkIndex: number): Promise<void> {
        if (!this.session) throw new Error("Session is not active during poll operation.");
        let timeBetweenMs = 0;
        if (Config.ENABLE_DIAGNOSTICS) {
            const now = process.hrtime();
            if (lastScanTime !== null) {
                const timeBetweenHr = process.hrtime(lastScanTime);
                timeBetweenMs = (timeBetweenHr[0] * 1000) + (timeBetweenHr[1] / 1e6);

                if (totalReads >= Config.DIAG_READS_TO_SKIP_AT_START) {
                    if (timeBetweenMs > maxTimeBetweenScansMs) {
                        maxTimeBetweenScansMs = timeBetweenMs;
                        console.log(`\n⏱️ New Max Time Between Scans Recorded: ${maxTimeBetweenScansMs.toFixed(2)}ms`);
                    }
                }
            }
            lastScanTime = now;
        }
        //const fullNodeId = `${nodeListPrefix}${relativeNodeId}`;
        const nodesToRead: ReadValueIdOptions[] = this.allPollingItems.map(item => ({
            nodeId: `${Config.NODE_LIST_PREFIX}${item.nodeId}`,
            attributeId: AttributeIds.Value,
        }));

        //console.log('nodeListPrefix:', nodeListPrefix);

        const startTimeRead = process.hrtime();

        const maxIndex = nodesToRead.length;
        const startingIndex = chunkIndex * Config.CHUNK_SIZE;
        const endingIndex = Math.min(startingIndex + Config.CHUNK_SIZE, maxIndex);
        const nodeChunk = nodesToRead.slice(startingIndex, endingIndex);

        const dataValues: DataValue[] = await this.session.read(nodeChunk);
        dataValues.map((dataValue, index) => {
            this.allPollingValues[startingIndex + index] = decipherOpcuaValue(dataValue);
            // if (this.allPollingItems[startingIndex + index].nodeId.endsWith("Is")) {
            //     console.log("is tag:", JSON.stringify(decipherOpcuaValue(dataValue)));
            // }
        });


        //console.log(this.allPollingValues[startingIndex + 3]);


        const durationReadHr = process.hrtime(startTimeRead);
        const durationMs = (durationReadHr[0] * 1000) + (durationReadHr[1] / 1e6);

        if (Config.ENABLE_DIAGNOSTICS) {
            if (totalReads >= Config.DIAG_READS_TO_SKIP_AT_START) {
                totalDurationMs += durationMs;
                minReadDurationMs = Math.min(minReadDurationMs, durationMs);

                if (durationMs > maxReadDurationMs) {
                    maxReadDurationMs = durationMs;
                    console.log(`\n📈 New Max Read Duration Recorded: ${maxReadDurationMs.toFixed(2)}ms (Scan #${totalReads + 1})`);
                }
            }
            totalReads++;
            if (totalReads > Config.DIAG_READS_TO_SKIP_AT_START) {
                const avgDurationMs = totalDurationMs / (totalReads - Config.DIAG_READS_TO_SKIP_AT_START);
                console.log(`--- Read Duration: ${durationMs.toFixed(2)}ms | Avg Read Duration: ${avgDurationMs.toFixed(2)}ms |Time Between Scans: ${timeBetweenMs.toFixed(2)}ms ---`);
            }
        }
    }
}

// --- Helper Functions (remain external) ---

function getMachineReadItems(): ReadItemInfo[] {
    const itemsToRead: ReadItemInfo[] = [];
    Object.entries(initialMachine).forEach(([key, value]) => {
        const tag = key;
        const relativeNodeId = PlcNamespaces.Machine + '.' + tag;

        const topic = PlcNamespaces.Machine.toLowerCase() + '/' + tag.toLowerCase();
        itemsToRead.push({
            nodeId: relativeNodeId,
            mqttTopic: topic
        });
    });
    return itemsToRead;
}


/**
 * Main execution function
 */
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
