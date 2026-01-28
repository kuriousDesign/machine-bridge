import {
    AttributeIds,
    ClientMonitoredItemBase,
    ClientMonitoredItemGroup,
    ClientSession,
    ClientSubscription,
    DataType,
    DataValue,
    MessageSecurityMode,
    MonitoringMode,
    OPCUAClient,
    OPCUAClientOptions,
    ReadValueIdOptions,
    SecurityPolicy,
    StatusCodes,
    TimestampsToReturn,
    VariantArrayType,
    WriteValueOptions,
} from 'node-opcua';

import Config from '../config'; // <--- Use the central config

import { BridgeCmds, DeviceActionRequestData, DeviceId, DeviceRegistration, DeviceTags, MachineTags, MqttTopics, PlcNamespaces, TopicData, buildFullTopicPath, initialMachine, nodeListString, Device, DeviceStatus } from '@kuriousdesign/machine-sdk';
import MqttClientManager from '../MqttClientManager';
import CodesysOpcuaDriver from '../OpcuaMqtt/codesys-opcua-driver';


interface ReadItemInfo {
    tagId: string;
    nodeId: string;
    mqttTopic: string;
    attributeId: number; //AttributeIds.Value;
    update_period: number;
    last_publish_time: number;
    value: any;
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
    private opcuaSubscriptions: ClientSubscription[] = [];
    private monitoredItemGroups: ClientMonitoredItemGroup[] = [];
    //private deviceStore: Map<number, Device> = new Map();
    //private deviceStsStore: Map<number, any> = new Map();
    private tagReadInfoMap: Map<string, ReadItemInfo> = new Map();

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
        let mode = MonitoringMode.Reporting;
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
                    this.allPollingItems.map((item) => {
                        this.tagReadInfoMap.set(item.tagId, item);
                    });

                    console.log("Total polling items after device update:", this.allPollingItems.length);
                    // subscribe to device HMI action request topic
                    // Array.from(this.deviceMap.values()).map(async device =>
                    //     //await this.subscribeToMqttTopicDeviceHmiActionRequest(device)
                    // );
                    this.terminateAllSubscriptions();
                    this.subscribeToMonitoredItems();
                    break;

                case OpcuaState.Connected:
                case OpcuaState.Polling:
                case OpcuaState.WaitingForHeartbeat:
                    //await this.handlePolling();
                    await this.updateHeartbeat();
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
                    this.checkAndPublishData();
                    break;

                case OpcuaState.Disconnecting:
                    await this.handleDisconnect();
                    return; // Exit loop after graceful disconnect
            }
            // Small delay to prevent tight blocking loop if state transitions rapidly
            await new Promise(resolve => setTimeout(resolve, 50));
        }
        await this.handleDisconnect();
        await this.mqttClientManager.requestShutdown();
    }

    private async checkAndPublishData(): Promise<void> {
        // Placeholder for any additional data checks or publications
        this.tagReadInfoMap.forEach((readInfo, tag) => {
            const now = Date.now();
            if (now - readInfo.last_publish_time >= readInfo.update_period * Config.POLLING_RATE_MS) {
                this.mqttClientManager.publish(readInfo.mqttTopic, readInfo.value);
                readInfo.last_publish_time = now;
                this.tagReadInfoMap.set(tag, readInfo);
            }
        });
    }

    private async handleConnection(): Promise<void> {
        this.state = OpcuaState.Connecting;
        console.log(`[OPCUA]Connecting to endpoint: ${Config.OPCUA_ENDPOINT}`);

        try {
            this.terminateAllSubscriptions();
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
            const deviceTag = PlcNamespaces.Machine + '.' + MachineTags.deviceStore + '[' + device.id + ']';
            const deviceTopic = buildFullTopicPath(device, this.deviceMap);
            const USE_WHOLE_DEVICE = false;
            let readItemInfo: ReadItemInfo;
            if (USE_WHOLE_DEVICE) {
                readItemInfo = {
                    tagId: deviceTag,
                    nodeId: Config.NODE_LIST_PREFIX + deviceTag,
                    mqttTopic: deviceTopic,
                    last_publish_time: 0,
                    update_period: 1,
                    value: null,
                    attributeId: AttributeIds.Value
                };
                //this.nodeIdToMqttTopicMap.set(deviceNodeId, deviceTopic);
                readIteams.push(readItemInfo);
                console.log(`[OPCUA] Added device for polling, Tag: ${deviceTag}, Topic: ${deviceTopic}`);
            } else {
                Object.values(DeviceTags).map((subTag: string) => {
                    const tag = deviceTag + '.' + subTag;
                    const topic = deviceTopic + '/' + subTag.toLowerCase().replace('.', '/');
                    readItemInfo = {
                        tagId: tag,
                        nodeId: Config.NODE_LIST_PREFIX + tag,
                        mqttTopic: topic,
                        update_period: 1,
                        last_publish_time: 0,
                        value: null,
                        attributeId: AttributeIds.Value
                    };
                    // if tag is sts or is or task, set update period to 1

                    if ([DeviceTags.Is, DeviceTags.Task].includes(tag)) {
                        readItemInfo.update_period = 1;
                    }
                    readIteams.push(readItemInfo);
                    console.log(`[OPCUA] Added device tag for polling, tag: ${tag}, Topic: ${topic}, Rate: ${readItemInfo.update_period}`);
                });
            }



            if (false && device.isExternalService) {
                console.log(`[OPCUA] Adding external service device IExtService interface for polling, Device ID: ${device.id}, Mnemonic: ${device.mnemonic}`);
                let nodeId = deviceTag + '.' + DeviceTags.ApiOpcuaPlcReq;
                let topic = deviceTopic + '/' + DeviceTags.ApiOpcuaPlcReq.toLowerCase().replace('.', '/');
                let readItemInfo: ReadItemInfo = {
                    tagId: nodeId,
                    nodeId: Config.NODE_LIST_PREFIX + nodeId,
                    mqttTopic: topic,
                    last_publish_time: 0,
                    update_period: 1,
                    value: null,
                    attributeId: AttributeIds.Value
                };
                readIteams.push(readItemInfo);
                // registration
                nodeId = deviceTag + '.' + DeviceTags.Registration;
                topic = deviceTopic + '/' + DeviceTags.Registration.toLowerCase().replace('.', '/');
                readItemInfo = {
                    tagId: nodeId,
                    nodeId: Config.NODE_LIST_PREFIX + nodeId,
                    mqttTopic: topic,
                    last_publish_time: 0,
                    update_period: 1,
                    value: null,
                    attributeId: AttributeIds.Value
                };
                readIteams.push(readItemInfo);

            }



            // whole device log
            const deviceLogTag = PlcNamespaces.Machine + '.' + MachineTags.deviceLogs + '[' + device.id + ']';
            const deviceLogTopic = deviceTopic + '/log';
            readItemInfo = {
                tagId: deviceLogTag,
                nodeId: Config.NODE_LIST_PREFIX + deviceLogTag,
                mqttTopic: deviceLogTopic,
                last_publish_time: 0,
                update_period: 5,
                value: null,
                attributeId: AttributeIds.Value
            };

            readIteams.push(readItemInfo);
            console.log(`[OPCUA] Added device tag for polling, Tag: ${deviceLogTag}, Topic: ${deviceLogTopic}`);

            // device sts
            const deviceStsTag = PlcNamespaces.Machine + '.' + device.mnemonic.toLowerCase() + 'Sts';
            const deviceStsTopic = deviceTopic + '/sts';
            if (!(device.mnemonic === 'SYS' || device.mnemonic === 'CON' || device.mnemonic === 'HMI')) {
                readItemInfo = {
                    tagId: deviceStsTag,
                    nodeId: Config.NODE_LIST_PREFIX + deviceStsTag,
                    mqttTopic: deviceStsTopic,
                    last_publish_time: 0,
                    update_period: 5,
                    value: null,
                    attributeId: AttributeIds.Value
                };
                readIteams.push(readItemInfo);
                console.log(`[OPCUA] Added device tag for polling, Tag: ${deviceStsTag}, Topic: ${deviceStsTopic}`);
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
            if (pollScanTimeMs > Config.POLLING_RATE_MS * 1.10 && this.lastPollTimeMs !== 0) {
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
                this.terminateAllSubscriptions();
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
        if (!this.session) throw new Error("Session is not active during poll operation.");

        //const fullNodeId = `${nodeListPrefix}${relativeNodeId}`;
        const nodesToRead: ReadValueIdOptions[] = this.allPollingItems.map(item => ({
            nodeId: `${Config.NODE_LIST_PREFIX}${item.tagId}`,
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

    // -----------------------------
    // Terminate all subscriptions & monitored groups
    // -----------------------------
    private async terminateAllSubscriptions(): Promise<void> {
        // terminate monitored item groups first
        if (this.monitoredItemGroups && this.monitoredItemGroups.length > 0) {
            for (const group of this.monitoredItemGroups) {
                try {
                    group.setMonitoringMode(MonitoringMode.Disabled);
                    group.terminate();
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
                    sub.terminate();
                } catch (err) {
                    console.warn('Error terminating subscription:', err);
                }
            }
            this.opcuaSubscriptions = [];
        }

        console.log('All OPC UA subscriptions and monitored groups terminated');
    }

    private async subscribeToMonitoredItems(): Promise<void> {
        if (!this.session) {
            throw new Error("OPC UA session is not initialized");
        }

        // First, clean up any existing subscriptions/groups if re-subscribing
        await this.terminateAllSubscriptions();

        //const allItems = this.createItemsToMonitor();
        console.log('Subscribing to monitored items', this.allPollingItems.length, 'items to monitor...');

        // Chunk items into groups of MAX_ITEMS_PER_GROUP
        for (let i = 0; i < this.allPollingItems.length; i += Config.CHUNK_SIZE) {
            const chunk = this.allPollingItems.slice(i, i + Config.CHUNK_SIZE);
            const groupIndex = Math.floor(i / Config.CHUNK_SIZE) + 1;
            console.log(`Creating subscription group ${groupIndex} with ${chunk.length} items...`);

            // Validate nodes in chunk (read test). Build a filtered array of valid items.
            const validatedItems: ReadItemInfo[] = [];
            for (const item of chunk) {
                try {
                    const data = await this.session.read({
                        nodeId: item.nodeId,
                        attributeId: AttributeIds.Value,
                    } as ReadValueIdOptions);

                    if (data && data.statusCode && data.statusCode === StatusCodes.Good) {
                        // good to monitor
                        validatedItems.push(item);
                    } else {
                        console.warn(`Skipping invalid/unsupported node for monitoring: ${item.tagId} (status=${data?.statusCode?.toString()})`);
                    }
                } catch (err) {
                    // If read fails with BadNotSupported or other issue, skip the item gracefully
                    const errMsg = (err instanceof Error) ? err.message : String(err);
                    console.warn(`Read test failed for ${item.tagId}: ${errMsg}. Skipping monitoring for this node.`);
                }
            }

            if (validatedItems.length === 0) {
                console.log(`No valid items in group ${groupIndex}, skipping subscription creation.`);
                continue;
            }

            try {
                const subscription = await this.session.createSubscription2(Config.SUBSCRIPTION_OPTIONS);
                this.opcuaSubscriptions.push(subscription);

                const monitoredGroup = ClientMonitoredItemGroup.create(
                    subscription,
                    validatedItems,
                    Config.OPTIONS_GROUP,
                    TimestampsToReturn.Neither
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

    private async handleMonitoredItemChange(monitoredItem: ClientMonitoredItemBase, dataValue: DataValue): Promise<void> {
        try {
            const newValue = decipherOpcuaValue(dataValue);
            const newValueType = typeof newValue;
            // monitoredItem.itemToMonitor.nodeId may be a NodeId object or string; ensure string
            const fullNodeId = monitoredItem.itemToMonitor?.nodeId?.toString ? monitoredItem.itemToMonitor.nodeId.toString() : String(monitoredItem.itemToMonitor?.nodeId);
            const tag = fullNodeId.replace(Config.NODE_LIST_PREFIX, '');
            const readInfo = this.tagReadInfoMap.get(tag);

            console.log(`Monitored item changed: tag=${tag}, monitoringMode=${monitoredItem.monitoringMode}`);

            if (!readInfo) {
                console.error('No valid MQTT topic found for tag:', tag, 'full:', fullNodeId);
                return;
            } else if (!newValue && newValueType !== 'boolean') {
                console.error('No valid new value for monitored item change:', tag, 'full:', fullNodeId);
                return;
            }
            const topic = readInfo.mqttTopic;
            readInfo.value = newValue;
            readInfo.last_publish_time = Date.now();
            // update the tagReadInfoMap
            this.tagReadInfoMap.set(tag, readInfo);

            this.mqttClientManager.publish(topic, newValue);
            if (topic === "machine/heartbeatplc" && newValue % 30 === 0) {
                console.log('Machine.heartbeatPlc:', newValue);
            }

        } catch (error) {
            console.error('Error processing monitored item change:', error);
        }
    }

}

// --- Helper Functions (remain external) ---

function getMachineReadItems(): ReadItemInfo[] {
    const itemsToRead: ReadItemInfo[] = [];
    Object.entries(initialMachine).forEach(([key, value]) => {
        const subTag = key;
        const tag = PlcNamespaces.Machine + '.' + subTag;

        const topic = PlcNamespaces.Machine.toLowerCase() + '/' + subTag.toLowerCase();
        itemsToRead.push({
            tagId: tag,
            nodeId: Config.NODE_LIST_PREFIX + tag,
            mqttTopic: topic,
            last_publish_time: 0,
            update_period: 1,
            value: null,
            attributeId: AttributeIds.Value,
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
