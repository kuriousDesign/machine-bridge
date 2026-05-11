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

import Config from '../shared/config'; // <--- Use the central config

import { DeviceId, DeviceRegistration, DeviceTags, initialKioskControlData, KioskControlData, MachineTags, PlcNamespaces, TopicData } from '@kuriousdesign/machine-sdk';
import CodesysOpcuaDriver from '../opcua/codesys-opcua-driver';
import { ReadItemInfo } from '../opcua/monitored-items';
import MqttClientManager from '../shared/MqttClientManager';
import ExternalServiceWriteManager from '../writers/ExternalServiceWriteManager';
import HmiWriteManager from '../writers/HmiWriteManager';
import { buildValidatedPollingItems, getRegisteredDevicesNodeId, loadRegisteredDevices } from './PublishBootstrap';
import { handlePublishBridgeCommand, subscribeToPublishBridgeCommands } from './PublishCommands';
import { connectPublishOpcuaSession, disconnectPublishOpcuaSession, logPublishOpcuaConnectionFailure, resetPublishOpcuaConnectionFailureTracking, teardownPublishOpcuaConnection } from './PublishConnectionLifecycle';
import { BridgeStatusSnapshot, PublishManagerStatus } from './PublishManagerContracts';
import { republishStalePollingValues } from './PublishPolling';
import { publishBridgeStatus, syncPublishHeartbeat } from './PublishStatus';
import { handlePublishMonitoredItemChange, subscribeToPublishMonitoredItems, terminatePublishSubscriptions } from './PublishSubscriptions';




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

export interface PublishManagerCallbacks {
    onStatusChange?: (status: PublishManagerStatus) => void;
    onError?: (error: Error) => void;
}

export interface PublishManagerDependencies {
    externalServiceWriteManager?: ExternalServiceWriteManager;
    getBridgeStatusSnapshot?: () => Partial<BridgeStatusSnapshot>;
    hmiWriteManager?: HmiWriteManager;
    mqttClientManager?: MqttClientManager;
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

export default class PublishManagerCore {
    private externalServiceWriteManager: ExternalServiceWriteManager;
    private getBridgeStatusSnapshot?: () => Partial<BridgeStatusSnapshot>;
    private mqttClientManager: MqttClientManager;
    private ownsMqttClientManager: boolean;
    private hmiWriteManager: HmiWriteManager;
    private readonly callbacks: PublishManagerCallbacks;
    private state: OpcuaState = OpcuaState.Disconnected;
    private publishStatus: PublishManagerStatus = PublishManagerStatus.Idle;
    private client: OPCUAClient | null = null;
    private session: ClientSession | null = null;
    private machinePollingItems: ReadItemInfo[] = [];
    private devicePollingItems: ReadItemInfo[] = [];
    private allPollingItems: ReadItemInfo[] = [];
    private allPollingValues: any[] = [];
    private shutdownRequested: boolean = false;
    private heartbeatPlcValue: number = 0;
    private heartbeatHmiValue: number = 0;
    private heartbeatHmiNodeId = concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatHMI);
    private heartbeatPlcNodeId = concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatPLC);
    private lastHeartbeatObservedAt: number = 0;
    private registeredDevices: DeviceRegistration[] = []
    private deviceMap: Map<number, DeviceRegistration> = new Map();
    private codesysOpcuaDriver: CodesysOpcuaDriver | null = null;
    private opcuaSubscriptions: ClientSubscription[] = [];
    private monitoredItemGroups: ClientMonitoredItemGroup[] = [];
    private kioskControlData: KioskControlData = { ...initialKioskControlData };
    //private deviceStore: Map<number, Device> = new Map();
    //private deviceStsStore: Map<number, any> = new Map();
    private tagReadInfoMap: Map<string, ReadItemInfo> = new Map();
    private connectionFailureCount: number = 0;
    private connectionFailureStartedAt: number | null = null;
    private lastConnectionRetryLogAt: number = 0;
    private connectionAttemptStartedAt: number | null = null;
    private lastDisconnectedStatusLogAt: number = 0;
    private connectionStatusLogTimer: NodeJS.Timeout | null = null;

    private canPerformOpcuaWrites(): boolean {
        return !!this.session
            && !!this.codesysOpcuaDriver
            && (
                this.state === OpcuaState.Connected
                || this.state === OpcuaState.Polling
                || this.state === OpcuaState.WaitingForHeartbeat
            );
    }

    private markOpcuaWritesUnavailable(reason: string): void {
        console.warn(`[OPCUA] Writes disabled: ${reason}`);
        this.codesysOpcuaDriver = null;

        if (!this.shutdownRequested && this.state !== OpcuaState.Disconnecting) {
            this.state = OpcuaState.Reconnecting;
        }
    }

    private isIgnorableOpcuaCleanupError(error: unknown): boolean {
        const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();

        return message.includes('badconnectionclosed')
            || message.includes('invalid channel')
            || message.includes('session has been closed')
            || message.includes('socket has been closed')
            || message.includes('transaction has been canceled')
            || message.includes('already been terminated')
            || message.includes('already closed');
    }

    private logOpcuaConnectionFailure(error: unknown): void {
        const nextTracking = logPublishOpcuaConnectionFailure({
            connectLogIntervalMs: Config.OPCUA_CONNECT_LOG_INTERVAL_MS,
            endpoint: Config.OPCUA_ENDPOINT,
            error,
            retryDelayMs: Config.RECONNECT_DELAY_MS,
            tracking: {
                connectionAttemptStartedAt: this.connectionAttemptStartedAt,
                connectionFailureCount: this.connectionFailureCount,
                connectionFailureStartedAt: this.connectionFailureStartedAt,
                lastConnectionRetryLogAt: this.lastConnectionRetryLogAt,
                lastDisconnectedStatusLogAt: this.lastDisconnectedStatusLogAt,
            },
        });

        this.connectionAttemptStartedAt = nextTracking.connectionAttemptStartedAt;
        this.connectionFailureCount = nextTracking.connectionFailureCount;
        this.connectionFailureStartedAt = nextTracking.connectionFailureStartedAt;
        this.lastConnectionRetryLogAt = nextTracking.lastConnectionRetryLogAt;
        this.lastDisconnectedStatusLogAt = nextTracking.lastDisconnectedStatusLogAt;
    }

    private resetOpcuaConnectionFailureTracking(): void {
        const nextTracking = resetPublishOpcuaConnectionFailureTracking({
            connectionAttemptStartedAt: this.connectionAttemptStartedAt,
            connectionFailureCount: this.connectionFailureCount,
            connectionFailureStartedAt: this.connectionFailureStartedAt,
            lastConnectionRetryLogAt: this.lastConnectionRetryLogAt,
            lastDisconnectedStatusLogAt: this.lastDisconnectedStatusLogAt,
        });

        this.connectionAttemptStartedAt = nextTracking.connectionAttemptStartedAt;
        this.connectionFailureCount = nextTracking.connectionFailureCount;
        this.connectionFailureStartedAt = nextTracking.connectionFailureStartedAt;
        this.lastConnectionRetryLogAt = nextTracking.lastConnectionRetryLogAt;
        this.lastDisconnectedStatusLogAt = nextTracking.lastDisconnectedStatusLogAt;
    }

    private async teardownExistingOpcuaConnection(): Promise<void> {
        const resources = await teardownPublishOpcuaConnection({
            isIgnorableCleanupError: (error) => this.isIgnorableOpcuaCleanupError(error),
            resources: {
                client: this.client,
                codesysOpcuaDriver: this.codesysOpcuaDriver,
                session: this.session,
            },
            terminateAllSubscriptions: () => this.terminateAllSubscriptions(),
        });

        this.client = resources.client;
        this.codesysOpcuaDriver = resources.codesysOpcuaDriver;
        this.session = resources.session;
    }

    private startConnectionStatusLogger(): void {
        if (this.connectionStatusLogTimer) {
            return;
        }

        this.connectionStatusLogTimer = setInterval(() => {
            this.logOpcuaDisconnectedStatusIfNeeded();
        }, 1000);
    }

    private stopConnectionStatusLogger(): void {
        if (this.connectionStatusLogTimer) {
            clearInterval(this.connectionStatusLogTimer);
            this.connectionStatusLogTimer = null;
        }
    }

    private logOpcuaDisconnectedStatusIfNeeded(): void {
        if (this.state !== OpcuaState.Connecting && this.state !== OpcuaState.Reconnecting) {
            return;
        }

        const startedAt = this.connectionAttemptStartedAt ?? this.connectionFailureStartedAt;
        if (startedAt === null) {
            return;
        }

        const now = Date.now();
        const elapsedMs = now - startedAt;
        if (elapsedMs < Config.OPCUA_CONNECT_LOG_INTERVAL_MS) {
            return;
        }

        if (now - this.lastDisconnectedStatusLogAt < Config.OPCUA_CONNECT_LOG_INTERVAL_MS) {
            return;
        }

        const elapsedSeconds = Math.floor(elapsedMs / 1000);
        console.warn(`[OPCUA] Still disconnected after ${elapsedSeconds}s. Current state: ${OpcuaState[this.state]}. Endpoint: ${Config.OPCUA_ENDPOINT}`);
        this.lastDisconnectedStatusLogAt = now;
    }

    //private nodeListPrefix = nodeListString + Config.OPCUA_CONTROLLER_NAME + '.Application.';

    // constructor 
    constructor(
        callbacks: PublishManagerCallbacks = {},
        dependencies: PublishManagerDependencies = {},
    ) {
        this.callbacks = callbacks;
        if (Config.ENABLE_DIAGNOSTICS) {
            console.log(`Diagnostics ENABLED. Skipping stats for the first ${Config.DIAG_READS_TO_SKIP_AT_START} scans.`);
        } else {
            console.log(`Diagnostics DISABLED.`);
        }
        this.ownsMqttClientManager = !dependencies.mqttClientManager;
        this.mqttClientManager = dependencies.mqttClientManager ?? new MqttClientManager();
        this.externalServiceWriteManager = dependencies.externalServiceWriteManager ?? new ExternalServiceWriteManager();
        this.getBridgeStatusSnapshot = dependencies.getBridgeStatusSnapshot;
        this.hmiWriteManager = dependencies.hmiWriteManager ?? new HmiWriteManager();
        this.mqttClientManager.registerOnConnect(() => {
            void this.publishBridgeConnectionStatus(true);
        });
        this.startConnectionStatusLogger();
        if (this.ownsMqttClientManager) {
            void this.mqttClientManager.manageConnectionLoop();
        }
    }

    public requestShutdown(): void {
        this.shutdownRequested = true;
        console.log("Shutdown requested. Transitioning to Disconnecting state.");
        this.setPublishStatus(PublishManagerStatus.Disconnecting);
    }

    public getPublishStatus(): PublishManagerStatus {
        return this.publishStatus;
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
                    if (!this.session) {
                        console.log("Waiting for session to be active...");
                        this.state = OpcuaState.Reconnecting;
                        this.setPublishStatus(PublishManagerStatus.Reconnecting);
                        break;
                    }
                    this.setPublishStatus(PublishManagerStatus.LoadingBootstrapData);
                    await this.executeBootstrap();
                    this.allPollingItems.map((item) => {
                        this.tagReadInfoMap.set(item.tagId, item);
                    });

                    console.log("Total all polling items:", this.allPollingItems.length);
                    this.setPublishStatus(PublishManagerStatus.CreatingSubscriptions);
                    // subscribe to device HMI action request topic
                    // Array.from(this.deviceMap.values()).map(async device =>
                    //     //await this.subscribeToMqttTopicDeviceHmiActionRequest(device)
                    // );
                    await this.terminateAllSubscriptions();
                    await this.subscribeToMonitoredItems();
                    console.log("Total validated polling items:", this.allPollingItems.length);
                    this.externalServiceWriteManager.configure({
                        mqttClientManager: this.mqttClientManager,
                        getDeviceMap: () => this.deviceMap,
                    });
                    this.hmiWriteManager.configure({
                        mqttClientManager: this.mqttClientManager,
                        getDeviceMap: () => this.deviceMap,
                    });
                    await this.externalServiceWriteManager.syncSubscriptions(this.deviceMap.values());
                    await this.hmiWriteManager.syncSubscriptions(this.deviceMap.values());
                    await this.subscribeToBridgeCommandTopic();
                    this.setPublishStatus(PublishManagerStatus.ReadyForHmiHydration);
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
                        if (this.state === OpcuaState.WaitingForHeartbeat){
                            this.state = OpcuaState.Reconnecting;
                            this.setPublishStatus(PublishManagerStatus.Reconnecting);
                        } else{
                            this.state = OpcuaState.Polling;
                            this.setPublishStatus(PublishManagerStatus.Polling);
                        }
                    }

                    const lastHeartbeatActivityTime = Math.max(lastUpdateTime, this.lastHeartbeatObservedAt);
                    if (Date.now() - lastHeartbeatActivityTime > 5000) {
                        // Heartbeat not updating, transition to waiting state
                        if (this.state !== OpcuaState.WaitingForHeartbeat)
                            console.warn(`⚠️ PLC heartbeat not updating. Waiting for heartbeat, last update was ${Date.now() - lastHeartbeatActivityTime} ms ago`);
                        this.state = OpcuaState.WaitingForHeartbeat;
                        this.setPublishStatus(PublishManagerStatus.WaitingForHeartbeat);
                    }
                    this.checkAndPublishData();
                    break;

                case OpcuaState.Disconnecting:
                    this.setPublishStatus(PublishManagerStatus.Disconnecting);
                    await this.handleDisconnect();
                    return; // Exit loop after graceful disconnect
            }
            // Small delay to prevent tight blocking loop if state transitions rapidly
            await new Promise(resolve => setTimeout(resolve, Config.LOOP_DELAY_MS));
        }
        await this.handleDisconnect();
        if (this.ownsMqttClientManager) {
            this.mqttClientManager.requestShutdown();
        }
    }

    public async syncWriterSubscriptions(): Promise<void> {
        await this.externalServiceWriteManager.syncSubscriptions(this.deviceMap.values());
        await this.hmiWriteManager.syncSubscriptions(this.deviceMap.values());
    }

    private setPublishStatus(nextStatus: PublishManagerStatus): void {
        if (this.publishStatus === nextStatus) {
            return;
        }

        this.publishStatus = nextStatus;
        console.log(`[PUBLISH_MANAGER] STATUS: ${nextStatus}`);
        this.callbacks.onStatusChange?.(nextStatus);
    }

    private reportError(error: unknown): void {
        const normalizedError = error instanceof Error ? error : new Error(String(error));
        this.callbacks.onError?.(normalizedError);
    }

    private async checkAndPublishData(): Promise<void> {
        await republishStalePollingValues({
            mqttClientManager: this.mqttClientManager,
            tagReadInfoMap: this.tagReadInfoMap,
        });
    }

    private async handleConnection(): Promise<void> {
        this.state = OpcuaState.Connecting;
        this.setPublishStatus(PublishManagerStatus.ConnectingSession);
        if (this.connectionAttemptStartedAt === null) {
            this.connectionAttemptStartedAt = Date.now();
        }
        const connectionResult = await connectPublishOpcuaSession({
            clearMqttHandlers: () => this.mqttClientManager.clearAllHandlers(),
            createDriver: (session) => new CodesysOpcuaDriver(DeviceId.HMI, session, Config.OPCUA_CONTROLLER_NAME),
            endpoint: Config.OPCUA_ENDPOINT,
            isIgnorableCleanupError: (error) => this.isIgnorableOpcuaCleanupError(error),
            onConnectionLost: () => {
                this.markOpcuaWritesUnavailable('connection lost');
            },
            opcuaOptions: Config.OPCUA_OPTIONS,
            reportError: (error) => this.reportError(error),
            resetExternalSubscriptions: () => this.externalServiceWriteManager.resetSubscriptions(),
            resetHmiSubscriptions: () => this.hmiWriteManager.resetSubscriptions(),
            resources: {
                client: this.client,
                codesysOpcuaDriver: this.codesysOpcuaDriver,
                session: this.session,
            },
            tracking: {
                connectionAttemptStartedAt: this.connectionAttemptStartedAt,
                connectionFailureCount: this.connectionFailureCount,
                connectionFailureStartedAt: this.connectionFailureStartedAt,
                lastConnectionRetryLogAt: this.lastConnectionRetryLogAt,
                lastDisconnectedStatusLogAt: this.lastDisconnectedStatusLogAt,
            },
            terminateAllSubscriptions: () => this.terminateAllSubscriptions(),
        });

        this.client = connectionResult.resources.client;
        this.codesysOpcuaDriver = connectionResult.resources.codesysOpcuaDriver;
        this.session = connectionResult.resources.session;
        this.connectionAttemptStartedAt = connectionResult.tracking.connectionAttemptStartedAt;
        this.connectionFailureCount = connectionResult.tracking.connectionFailureCount;
        this.connectionFailureStartedAt = connectionResult.tracking.connectionFailureStartedAt;
        this.lastConnectionRetryLogAt = connectionResult.tracking.lastConnectionRetryLogAt;
        this.lastDisconnectedStatusLogAt = connectionResult.tracking.lastDisconnectedStatusLogAt;

        if (this.session) {
            this.resetOpcuaConnectionFailureTracking();
            this.state = OpcuaState.Connected;
            return;
        }

        this.logOpcuaConnectionFailure(connectionResult.error ?? new Error('OPC UA connection attempt failed before session creation'));
        if (this.connectionFailureCount === 1 || Date.now() - this.lastConnectionRetryLogAt < 50) {
            console.log(`[OPCUA] Will retry in ${Config.RECONNECT_DELAY_MS}ms...`);
        }
        this.state = OpcuaState.Reconnecting;
        this.setPublishStatus(PublishManagerStatus.Reconnecting);
        await new Promise(resolve => setTimeout(resolve, Config.RECONNECT_DELAY_MS));
    }

   

    private async executeBootstrap(): Promise<void> {
        if (!this.session) {
            throw new Error('Session is not active during bootstrap');
        }

        this.registeredDevices = await loadRegisteredDevices(
            getRegisteredDevicesNodeId(concatNodeId),
            (nodeId) => this.readOpcuaValue(nodeId),
            this.deviceMap,
        );

        const bootstrapResult = await buildValidatedPollingItems(
            this.session,
            this.registeredDevices,
            this.deviceMap,
        );

        this.devicePollingItems = bootstrapResult.devicePollingItems;
        this.machinePollingItems = bootstrapResult.machinePollingItems;
        this.allPollingItems = bootstrapResult.allPollingItems;
    }

    private async subscribeToBridgeCommandTopic(): Promise<void> {
        if (!this.mqttClientManager) {
            throw new Error("MQTT client is not initialized");
        }

        await subscribeToPublishBridgeCommands({
            handleBridgeCommand: (message) => this.handleBridgeCommand(message),
            mqttClientManager: this.mqttClientManager,
        });
    }

    private async handleDisconnect(): Promise<void> {
        this.state = OpcuaState.Disconnecting;
        this.setPublishStatus(PublishManagerStatus.Disconnecting);
        const resources = await disconnectPublishOpcuaSession({
            resources: {
                client: this.client,
                codesysOpcuaDriver: this.codesysOpcuaDriver,
                session: this.session,
            },
            terminateAllSubscriptions: () => this.terminateAllSubscriptions(),
        });

        this.client = resources.client;
        this.codesysOpcuaDriver = resources.codesysOpcuaDriver;
        this.session = resources.session;

        this.state = OpcuaState.Disconnected;
        this.setPublishStatus(PublishManagerStatus.Disconnected);
        this.stopConnectionStatusLogger();
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
        this.kioskControlData = await handlePublishBridgeCommand({
            deviceMapEntries: Array.from(this.deviceMap.entries()),
            kioskControlData: this.kioskControlData,
            message,
            mqttClientManager: this.mqttClientManager,
        });
    }

    private async publishBridgeConnectionStatus(force = false): Promise<void> {
        if (!force && this.state === this.lastPublishedState && Date.now() - this.lastPublishTime < 3000) {
            return;
        }

        const nextStatusState = await publishBridgeStatus({
            currentState: this.state,
            currentStateLabel: OpcuaState[this.state],
            deviceMapEntries: Array.from(this.deviceMap.entries()),
            getBridgeStatusSnapshot: this.getBridgeStatusSnapshot,
            kioskControlData: this.kioskControlData,
            lastPublishTime: this.lastPublishTime,
            lastPublishedState: this.lastPublishedState,
            mqttClientManager: this.mqttClientManager,
            publishManagerStatus: this.publishStatus,
            registeredDeviceCount: this.deviceMap.size,
        });

        this.lastPublishedState = nextStatusState.lastPublishedState as OpcuaState | null;
        this.lastPublishTime = nextStatusState.lastPublishTime;
    }
    private timeWasSynced: boolean = false;

    private async updateHeartbeat(): Promise<void> {
        const nextHeartbeatState = await syncPublishHeartbeat({
            codesysOpcuaDriver: this.codesysOpcuaDriver,
            heartbeatHmiNodeId: this.heartbeatHmiNodeId,
            heartbeatHmiValue: this.heartbeatHmiValue,
            heartbeatPlcNodeId: this.heartbeatPlcNodeId,
            readOpcuaValue: (nodeId) => this.readOpcuaValue(nodeId),
            timeWasSynced: this.timeWasSynced,
            writeOpcuaValue: (nodeId, value, dataType) => this.writeOpcuaValue(nodeId, value, dataType),
        });

        this.heartbeatPlcValue = nextHeartbeatState.heartbeatPlcValue;
        this.heartbeatHmiValue = nextHeartbeatState.heartbeatHmiValue;
        this.timeWasSynced = nextHeartbeatState.timeWasSynced;
    }
    // -----------------------------
    // Terminate all subscriptions & monitored groups
    // -----------------------------
    private async terminateAllSubscriptions(): Promise<void> {
        await terminatePublishSubscriptions({
            monitoredItemGroups: this.monitoredItemGroups,
            opcuaSubscriptions: this.opcuaSubscriptions,
        }, (error) => this.isIgnorableOpcuaCleanupError(error));
    }

    private async subscribeToMonitoredItems(): Promise<void> {
        if (!this.session) {
            throw new Error("OPC UA session is not initialized");
        }

        const activeSession = this.session;

        await subscribeToPublishMonitoredItems({
            allPollingItems: this.allPollingItems,
            collections: {
                monitoredItemGroups: this.monitoredItemGroups,
                opcuaSubscriptions: this.opcuaSubscriptions,
            },
            onMonitoredItemChange: (monitoredItem, dataValue) => {
                void this.handleMonitoredItemChange(monitoredItem, dataValue);
            },
            session: activeSession,
            sessionIsCurrent: () => this.session === activeSession,
            terminateAllSubscriptions: () => this.terminateAllSubscriptions(),
        });
    }

    private async handleMonitoredItemChange(monitoredItem: ClientMonitoredItemBase, dataValue: DataValue): Promise<void> {
        await handlePublishMonitoredItemChange({
            dataValue,
            decipherOpcuaValue: (data) => decipherOpcuaValue(data),
            monitoredItem,
            mqttClientManager: this.mqttClientManager,
            nodeListPrefix: Config.NODE_LIST_PREFIX,
            onHeartbeatObserved: (value) => {
                this.heartbeatPlcValue = value;
                this.lastHeartbeatObservedAt = Date.now();
            },
            tagReadInfoMap: this.tagReadInfoMap,
        });
    }

}

// --- Helper Functions (remain external) ---



/**
 * Main execution function
 */
async function main() {
    console.log("Application starting...");


    const manager = new PublishManagerCore();

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
