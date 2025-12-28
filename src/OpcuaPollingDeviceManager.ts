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
    TimestampsToReturn,
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
    update_period?: number;
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
    private lastPollTimeMs: number = 0;
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
                    // Array.from(this.deviceMap.values()).map(async device =>
                    //     //await this.subscribeToMqttTopicDeviceHmiActionRequest(device)
                    // );
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
                        console.warn(`⚠️ PLC heartbeat not updating. Waiting for heartbeat, last update was ${Date.now() - lastUpdateTime} ms ago`);
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
            let readItemInfo: ReadItemInfo = {
                nodeId: deviceNodeId,
                mqttTopic: deviceTopic
            };
            //this.nodeIdToMqttTopicMap.set(deviceNodeId, deviceTopic);
            readIteams.push(readItemInfo);
            console.log(`[OPCUA] Added device for polling, NodeId: ${deviceNodeId}, Topic: ${deviceTopic}`);

            if (false && device.isExternalService) {
                console.log(`[OPCUA] Adding external service device IExtService interface for polling, Device ID: ${device.id}, Mnemonic: ${device.mnemonic}`);
                let nodeId = deviceNodeId + '.' + DeviceTags.ApiOpcuaPlcReq;
                let topic = deviceTopic + '/' + DeviceTags.ApiOpcuaPlcReq.toLowerCase().replace('.', '/');
                let readItemInfo: ReadItemInfo = {
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
                
            } 
            // Object.values(DeviceTags).map((tag: string) => {
            //     const nodeId = deviceNodeId + '.' + tag;
            //     const topic = deviceTopic + '/' + tag.toLowerCase().replace('.', '/');
            //     const readItemInfo: ReadItemInfo = {
            //         nodeId: nodeId,
            //         mqttTopic: topic,
            //         update_period: 3
            //     };
            //     // if tag is sts or is or task, set update period to 1
             
            //     if ([DeviceTags.Is, DeviceTags.Task].includes(tag)) {
            //         readItemInfo.update_period = 1;
            //     }
            //     readIteams.push(readItemInfo);
            //     console.log(`[OPCUA] Added device tag for polling, NodeId: ${nodeId}, Topic: ${topic}, Rate: ${readItemInfo.update_period}`);
            // });


            // device log
            const deviceLogNodeId = PlcNamespaces.Machine + '.' + MachineTags.deviceLogs + '[' + device.id + ']';
            const deviceLogTopic = deviceTopic + '/log';
            readItemInfo = {
                nodeId: deviceLogNodeId,
                mqttTopic: deviceLogTopic,
                update_period: 5
            };
            //this.nodeIdToMqttTopicMap.set(deviceLogNodeId, deviceLogTopic);
            readIteams.push(readItemInfo);
            console.log(`[OPCUA] Added device tag for polling, NodeId: ${deviceLogNodeId}, Topic: ${deviceLogTopic}`);

            // device sts
            const deviceStsNodeId = PlcNamespaces.Machine + '.' + device.mnemonic.toLowerCase() + 'Sts';
            const deviceStsTopic = deviceTopic + '/sts';
            if (!(device.mnemonic === 'SYS' || device.mnemonic === 'CON' || device.mnemonic === 'HMI')) {
                //this.nodeIdToMqttTopicMap.set(deviceStsNodeId, deviceStsTopic);
                readIteams.push({ nodeId: deviceStsNodeId, mqttTopic: deviceStsTopic });
                console.log(`[OPCUA] Added device tag for polling, NodeId: ${deviceStsNodeId}, Topic: ${deviceStsTopic}`);
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

    private async publishTags(chunkIndex: number): Promise<void> {
        const startingIndex = chunkIndex * Config.CHUNK_SIZE;
        const endingIndex = Math.min(startingIndex + Config.CHUNK_SIZE, this.allPollingItems.length);
        await Promise.all(this.allPollingItems.slice(startingIndex, endingIndex).map((item, index) => {
            return this.mqttClientManager.publish(item.mqttTopic, this.allPollingValues[startingIndex + index]);
        }));
    }
    private updateCtr: number = 0;
    private async readAndPublishChunkValues(chunkIndex: number): Promise<void> {
     
        await this.pollChunkOfAllTags(chunkIndex);
        await this.publishTags(chunkIndex);
        this.updateCtr++;
    }

    private async handlePolling(): Promise<void> {
        if (!this.session) throw new Error("Session is not active during poll operation.");
     
        const readTimeStartMs = Date.now();

        try {
            // Your polling logic from the previous script
            
            const numChunks = Math.ceil(this.allPollingItems.length / Config.CHUNK_SIZE);
            await Promise.all([
                ...Array.from({ length: numChunks }, (_, chunkIndex) => 
                    this.readAndPublishChunkValues(chunkIndex)
                ),
                this.updateHeartbeat()
            ]);
   

            // Go back to connected state to loop again after POLLING_RATE_MS delay
            if (this.state === OpcuaState.WaitingForHeartbeat) {
                this.state = OpcuaState.Reconnecting;
            } else {
                this.state = OpcuaState.Polling;
            }
            const readTimeEndMs = Date.now();
            const readDurationMs = readTimeEndMs - readTimeStartMs;
            const pollScanTimeMs = readTimeStartMs - this.lastPollTimeMs;
            if (pollScanTimeMs > Config.POLLING_RATE_MS*1.10 && this.lastPollTimeMs !== 0) {
                console.warn(`⚠️ Polling delay detected! Time since last poll: ${readTimeStartMs - this.lastPollTimeMs} ms`);
            }
            this.lastPollTimeMs = readTimeStartMs;
            const timeToWaitms = Math.max(0, Config.POLLING_RATE_MS - readDurationMs - Config.LOOP_DELAY_MS);
            //await new Promise(resolve => setTimeout(resolve, timeToWaitms));

        } catch (error) {
            // A read error likely means the session or connection is bad.
            console.error("❌ Polling failed. Assuming connection issue, attempting reconnection:", error);
            this.state = OpcuaState.Reconnecting;
            this.session = null; // Invalidate session
        }
    }

   private async handleDisconnect(): Promise<void> {
    this.state = OpcuaState.Disconnecting;
    console.log("Starting graceful OPC UA disconnection and cleanup...");

    // 1. Cleanup Codesys driver first (important!)
    if (this.codesysOpcuaDriver) {
        try {
            //await this.codesysOpcuaDriver.dispose?.(); // if it has dispose/close method
            // or at least null it
            this.codesysOpcuaDriver = null;
        } catch (e) {
            console.warn("Error disposing CodesysOpcuaDriver:", e);
        }
    }

    // 2. Close session with safety
    if (this.session) {
        try {
            // Optional: force close if server is slow
            const closePromise = this.session.close();
            await Promise.race([
                closePromise,
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error("Session close timeout")), 8000)
                )
            ]);
            console.log("✅ Session closed.");
        } catch (err) {
            console.warn("Session close failed (may be already closed):", err);
        } finally {
            this.session = null;
        }
    }

    // 3. Disconnect client
    if (this.client) {
        try {
            // Remove event listeners
            this.client.removeAllListeners("connection_lost");
            this.client.removeAllListeners("after_reconnection");

            const disconnectPromise = this.client.disconnect();
            await Promise.race([
                disconnectPromise,
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error("Client disconnect timeout")), 8000)
                )
            ]);
            console.log("✅ Client disconnected.");
        } catch (err) {
            console.warn("Client disconnect failed:", err);
        } finally {
            this.client = null;
        }
    }

    this.state = OpcuaState.Disconnected;
    console.log("✅ OPC UA fully disconnected.");
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
            if (this.codesysOpcuaDriver) {
                await this.codesysOpcuaDriver.writeCurrentTimeToCodesys();
            }
        }
    }
    /**
     * Reads all specified tags once, measuring performance if diagnostics are enabled.
     */
    
    private async pollChunkOfAllTags(chunkIndex: number): Promise<void> {
        if(!this.session) throw new Error("Session is not active during poll operation.");
   
        //const fullNodeId = `${nodeListPrefix}${relativeNodeId}`;
        const nodesToRead: ReadValueIdOptions[] = this.allPollingItems.map(item => ({
            nodeId: `${Config.NODE_LIST_PREFIX}${item.nodeId}`,
            attributeId: AttributeIds.Value,
            timespampsToReturn: TimestampsToReturn.Neither
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
        });


        //console.log(this.allPollingValues[startingIndex + 3]);



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
    console.log("Application shutdown complete.");
    // Script finishes here after disconnection is complete.
}

//main().catch(console.error);
