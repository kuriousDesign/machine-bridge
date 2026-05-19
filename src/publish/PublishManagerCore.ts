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
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

import Config from '../shared/config'; // <--- Use the central config

import { BaseMachinePollingTags, DeviceId, DeviceRegistration, OptionalDevicePollingTags, initialKioskControlData, KioskControlData, MachineCfg, PlcNamespaces, TopicData } from '@kuriousdesign/machine-sdk';
import CodesysOpcuaDriver from '../opcua/codesys-opcua-driver';
import { ReadItemInfo, ReadItemValidationResult } from '../opcua/polling-items';
import MqttClientManager from '../shared/MqttClientManager';
import ExternalServiceWriteManager from '../writers/ExternalServiceWriteManager';
import HmiWriteManager from '../writers/HmiWriteManager';
import { buildValidatedPollingItems, getMachineCfgNodeId, getRegisteredDevicesNodeId, loadMachineCfg, loadRegisteredDevices } from './PublishBootstrap';
import { handlePublishBridgeCommand, subscribeToPublishBridgeCommands } from './PublishCommands';
import { connectPublishOpcuaSession, disconnectPublishOpcuaSession, logPublishOpcuaConnectionFailure, resetPublishOpcuaConnectionFailureTracking, teardownPublishOpcuaConnection } from './PublishConnectionLifecycle';
import { BootstrapCacheSnapshot, BridgeStatusSnapshot, OpcuaItemSnapshot, OpcuaItemSource, OpcuaPollStatus, OpcuaReadStatus, PublishManagerStatus } from './PublishManagerContracts';
import { readPollingChunkValues, republishStalePollingValues } from './PublishPolling';
import { publishBridgeStatus, syncPublishHeartbeat } from './PublishStatus';
import { handlePublishPollingItemChange, subscribeToPublishPollingItems, terminatePublishSubscriptions } from './PublishSubscriptions';
import { logBootstrapCacheSnapshot } from './BootstrapLogHelpers';




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
    private static readonly OPCUA_ITEM_DUMP_PATH = path.join(process.cwd(), 'TagTopics', 'bridge-opcua-items.json');

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
    private heartbeatHmiNodeId = concatNodeId(PlcNamespaces.Machine, BaseMachinePollingTags.heartbeatHMI);
    private heartbeatPlcNodeId = concatNodeId(PlcNamespaces.Machine, BaseMachinePollingTags.heartbeatPLC);
    private machineCfg: MachineCfg | null = null;
    private machineId: string = '';
    private lastHeartbeatObservedAt: number = 0;
    private lastBootstrapCompletedAt: number | null = null;
    private optionalDeviceBootstrapItems: ReadItemInfo[] = [];
    private availableOptionalDeviceBootstrapItems: ReadItemInfo[] = [];
    private registeredDevices: DeviceRegistration[] = []
    private deviceMap: Map<number, DeviceRegistration> = new Map();
    private codesysOpcuaDriver: CodesysOpcuaDriver | null = null;
    private opcuaSubscriptions: ClientSubscription[] = [];
    private pollingItemGroups: ClientMonitoredItemGroup[] = [];
    private kioskControlData: KioskControlData = { ...initialKioskControlData };
    //private deviceStore: Map<number, Device> = new Map();
    //private deviceStsStore: Map<number, any> = new Map();
    private tagReadInfoMap: Map<string, ReadItemInfo> = new Map();
    private opcuaItemSnapshotMap: Map<string, OpcuaItemSnapshot> = new Map();
    private bootstrapReplayTopicMap: Map<string, unknown> = new Map();
    private lastOpcuaItemDumpJson: string | null = null;
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

    private async forceReconnectCycle(reason: string): Promise<void> {
        console.warn(`[OPCUA] Forcing reconnect cycle: ${reason}`);
        await this.teardownExistingOpcuaConnection();

        if (!this.shutdownRequested && this.state !== OpcuaState.Disconnecting) {
            this.state = OpcuaState.Reconnecting;
            this.setPublishStatus(PublishManagerStatus.Reconnecting);
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

    // Bootstrap cache is available directly to in-process callers and is also
    // forwarded through bridge/status as bootstrapCache by BridgeSupervisor.
    public getBootstrapCacheSnapshot(): BootstrapCacheSnapshot {
        return {
            allPollingItemCount: this.allPollingItems.length,
            availableOptionalDeviceBootstrapTagCount: this.availableOptionalDeviceBootstrapItems.length,
            cachedPollingTagCount: this.tagReadInfoMap.size,
            deviceMapCount: this.deviceMap.size,
            devicePollingItemCount: this.devicePollingItems.length,
            lastBootstrapCompletedAt: this.lastBootstrapCompletedAt,
            machineId: this.machineId || null,
            machinePollingItemCount: this.machinePollingItems.length,
            opcuaItems: Array.from(this.opcuaItemSnapshotMap.values()).sort((left, right) => left.tagId.localeCompare(right.tagId)),
            optionalDeviceBootstrapTagCount: this.optionalDeviceBootstrapItems.length,
            registeredDeviceCount: this.registeredDevices.length,
        };
    }

    private getCachedTopicTimestamp(readInfo: ReadItemInfo): number {
        if (readInfo.last_publish_time > 0) {
            return readInfo.last_publish_time;
        }

        if (this.lastBootstrapCompletedAt) {
            return this.lastBootstrapCompletedAt;
        }

        return Date.now();
    }

    private async primeCachedPollingValues(): Promise<void> {
        if (!this.session || this.allPollingItems.length === 0) {
            this.allPollingValues = [];
            return;
        }

        const allPollingValues = new Array<unknown>(this.allPollingItems.length);
        const chunkCount = Math.ceil(this.allPollingItems.length / Config.CHUNK_SIZE);

        for (let chunkIndex = 0; chunkIndex < chunkCount; chunkIndex += 1) {
            try {
                await readPollingChunkValues({
                    allPollingItems: this.allPollingItems,
                    allPollingValues,
                    chunkIndex,
                    decipherOpcuaValue: (data) => decipherOpcuaValue(data),
                    session: this.session,
                });
            } catch (error) {
                const startingIndex = chunkIndex * Config.CHUNK_SIZE;
                const endingIndex = Math.min(startingIndex + Config.CHUNK_SIZE, this.allPollingItems.length);
                const errorMessage = error instanceof Error ? error.message : String(error);

                console.warn(`[BOOTSTRAP] Bulk polling read failed for items ${startingIndex}-${endingIndex - 1}. Falling back to per-node reads. ${errorMessage}`);

                for (let index = startingIndex; index < endingIndex; index += 1) {
                    const item = this.allPollingItems[index];

                    try {
                        allPollingValues[index] = await this.readOpcuaValue(item.nodeId);
                    } catch (itemError) {
                        const itemErrorMessage = itemError instanceof Error ? itemError.message : String(itemError);
                        console.warn(`[BOOTSTRAP] Skipping cached polling prime for ${item.tagId}: ${itemErrorMessage}`);
                        allPollingValues[index] = null;
                    }
                }
            }
        }

        this.allPollingValues = allPollingValues;
        const primedAt = Date.now();
        const publishPromises: Promise<void>[] = [];

        this.allPollingItems.forEach((item, index) => {
            item.value = allPollingValues[index] ?? null;
            if (item.value !== null && typeof item.value !== 'undefined') {
                item.last_publish_time = primedAt;
                publishPromises.push(this.mqttClientManager.publish(item.mqttTopic, item.value));
            }
            this.tagReadInfoMap.set(item.tagId, item);
        });

        if (publishPromises.length > 0) {
            await Promise.all(publishPromises);
        }
    }

    private setBootstrapReplayTopic(item: Pick<ReadItemInfo, 'mqttTopic'>, payload: unknown): void {
        if (payload === null || typeof payload === 'undefined') {
            return;
        }

        this.bootstrapReplayTopicMap.set(item.mqttTopic, payload);
    }

    private isBootstrapReplayReadable(item: Pick<ReadItemInfo, 'mqttTopic'>): boolean {
        return !item.mqttTopic.endsWith('/cfg');
    }

    private async primeBootstrapReplayTopics(items: ReadItemInfo[]): Promise<void> {
        for (const item of items) {
            if (!this.isBootstrapReplayReadable(item)) {
                continue;
            }

            try {
                const value = await this.readOpcuaValue(item.nodeId);

                item.value = value;
                this.setBootstrapReplayTopic(item, value);
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : String(error);
                console.warn(`[BOOTSTRAP] Skipping bootstrap replay topic ${item.mqttTopic}: ${errorMessage}`);
            }
        }
    }

    private getBridgeCachePayload(): {
        bootstrapCache: BootstrapCacheSnapshot;
        cachedTopics: Array<{
            payload: unknown;
            timestamp: number;
            topic: string;
        }>;
        machineId: string | null;
    } {
        const bootstrapCache = this.getBootstrapCacheSnapshot();
        const cachedTopicMap = new Map<string, {
            payload: unknown;
            timestamp: number;
            topic: string;
        }>();

        this.bootstrapReplayTopicMap.forEach((payload, topic) => {
            cachedTopicMap.set(topic, {
                payload,
                timestamp: this.lastBootstrapCompletedAt ?? Date.now(),
                topic,
            });
        });

        Array.from(this.tagReadInfoMap.values())
            .filter((readInfo) => {
                return readInfo.value !== null && typeof readInfo.value !== 'undefined';
            })
            .forEach((readInfo) => {
                cachedTopicMap.set(readInfo.mqttTopic, {
                    payload: readInfo.value,
                    timestamp: this.getCachedTopicTimestamp(readInfo),
                    topic: readInfo.mqttTopic,
                });
            });

        const cachedTopics = Array.from(cachedTopicMap.values()).sort((left, right) => left.topic.localeCompare(right.topic));

        return {
            bootstrapCache,
            cachedTopics,
            machineId: bootstrapCache.machineId,
        };
    }

    private async publishCachedTopics(): Promise<void> {
        const cachedTopics = Array.from(this.tagReadInfoMap.values()).filter((readInfo) => {
            return readInfo.last_publish_time > 0 && readInfo.value !== null && typeof readInfo.value !== 'undefined';
        });

        await Promise.all(cachedTopics.map((readInfo) => {
            return this.mqttClientManager.publish(readInfo.mqttTopic, readInfo.value);
        }));
    }

    private upsertOpcuaItemSnapshot(params: {
        item: Pick<OpcuaItemSnapshot, 'mqttTopic' | 'nodeId' | 'tagId'>;
        pollDetail?: string | null;
        pollStatus?: OpcuaPollStatus;
        readDetail?: string | null;
        readStatus?: OpcuaReadStatus;
        source: OpcuaItemSource;
        updatePollTimestamp?: boolean;
        updateReadTimestamp?: boolean;
    }): void {
        const {
            item,
            pollDetail,
            pollStatus,
            readDetail,
            readStatus,
            source,
            updatePollTimestamp = false,
            updateReadTimestamp = false,
        } = params;

        const existing = this.opcuaItemSnapshotMap.get(item.tagId);
        const nextSnapshot: OpcuaItemSnapshot = {
            lastPolledAt: updatePollTimestamp ? Date.now() : existing?.lastPolledAt ?? null,
            lastReadAt: updateReadTimestamp ? Date.now() : existing?.lastReadAt ?? null,
            mqttTopic: item.mqttTopic,
            nodeId: item.nodeId,
            pollDetail: pollDetail ?? existing?.pollDetail ?? null,
            pollStatus: pollStatus ?? existing?.pollStatus ?? (source === 'devicePolling' || source === 'machinePolling' ? 'pending' : 'notApplicable'),
            readDetail: readDetail ?? existing?.readDetail ?? null,
            readStatus: readStatus ?? existing?.readStatus ?? 'pending',
            source,
            tagId: item.tagId,
        };

        this.opcuaItemSnapshotMap.set(item.tagId, nextSnapshot);
    }

    private async writeOpcuaItemDumpFile(): Promise<void> {
        const snapshot = this.getBootstrapCacheSnapshot();
        const payload = {
            generatedAt: Date.now(),
            machineId: snapshot.machineId,
            lastBootstrapCompletedAt: snapshot.lastBootstrapCompletedAt,
            counts: {
                allPollingItemCount: snapshot.allPollingItemCount,
                availableOptionalDeviceBootstrapTagCount: snapshot.availableOptionalDeviceBootstrapTagCount,
                cachedPollingTagCount: snapshot.cachedPollingTagCount,
                deviceMapCount: snapshot.deviceMapCount,
                devicePollingItemCount: snapshot.devicePollingItemCount,
                machinePollingItemCount: snapshot.machinePollingItemCount,
                optionalDeviceBootstrapTagCount: snapshot.optionalDeviceBootstrapTagCount,
                registeredDeviceCount: snapshot.registeredDeviceCount,
            },
            items: snapshot.opcuaItems,
        };
        const nextJson = `${JSON.stringify(payload, null, 2)}\n`;

        if (this.lastOpcuaItemDumpJson === nextJson) {
            return;
        }

        try {
            await mkdir(path.dirname(PublishManagerCore.OPCUA_ITEM_DUMP_PATH), { recursive: true });
            await writeFile(PublishManagerCore.OPCUA_ITEM_DUMP_PATH, nextJson, 'utf8');
            this.lastOpcuaItemDumpJson = nextJson;
        } catch (error) {
            console.warn(`[BOOTSTRAP][CACHE] Failed to write OPC UA item dump to ${PublishManagerCore.OPCUA_ITEM_DUMP_PATH}:`, error);
        }
    }

    private recordBootstrapItemResult(item: ReadItemInfo, source: OpcuaItemSource, success: boolean, detail: string | null): void {
        this.upsertOpcuaItemSnapshot({
            item,
            readDetail: detail,
            readStatus: success ? 'success' : 'failed',
            source,
            updateReadTimestamp: true,
        });
    }

    private recordPollingValidationResults(results: ReadItemValidationResult[], source?: OpcuaItemSource): void {
        results.forEach((result) => {
            const existing = this.opcuaItemSnapshotMap.get(result.item.tagId);
            this.upsertOpcuaItemSnapshot({
                item: result.item,
                pollDetail: result.success ? 'subscription eligible' : result.detail,
                pollStatus: result.success ? 'subscribed' : 'notSubscribed',
                readDetail: result.detail,
                readStatus: result.success ? 'success' : 'skipped',
                source: source ?? existing?.source ?? 'machinePolling',
                updatePollTimestamp: false,
                updateReadTimestamp: result.success,
            });
        });
    }

    private recordPollingItemResult(tagId: string, success: boolean, detail: string | null = null): void {
        const readInfo = this.tagReadInfoMap.get(tagId);
        if (!readInfo) {
            return;
        }

        const existing = this.opcuaItemSnapshotMap.get(tagId);
        this.upsertOpcuaItemSnapshot({
            item: {
                mqttTopic: readInfo.mqttTopic,
                nodeId: readInfo.nodeId,
                tagId,
            },
            pollDetail: detail,
            pollStatus: success ? 'received' : 'failed',
            source: existing?.source ?? 'machinePolling',
            updatePollTimestamp: true,
        });
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
                    try {
                        await this.executeBootstrap();
                    } catch (error) {
                        const errorMessage = error instanceof Error ? error.message : String(error);
                        await this.forceReconnectCycle(`bootstrap failed: ${errorMessage}`);
                        break;
                    }
                    this.tagReadInfoMap.clear();
                    this.allPollingItems.map((item) => {
                        this.tagReadInfoMap.set(item.tagId, item);
                    });
                    await this.primeCachedPollingValues();

                    console.log("Total all polling items:", this.allPollingItems.length);
                    this.setPublishStatus(PublishManagerStatus.CreatingSubscriptions);
                    // subscribe to device HMI action request topic
                    // Array.from(this.deviceMap.values()).map(async device =>
                    //     //await this.subscribeToMqttTopicDeviceHmiActionRequest(device)
                    // );
                    await this.terminateAllSubscriptions();
                    await this.subscribeToPollingItems();
                    console.log("Total validated polling items:", this.allPollingItems.length);
                    this.externalServiceWriteManager.configure({
                        getMachineId: () => this.machineId || null,
                        getKnownMachineTagRoots: () => this.machinePollingItems.map((item) => item.tagId),
                        mqttClientManager: this.mqttClientManager,
                        getDeviceMap: () => this.deviceMap,
                    });
                    this.hmiWriteManager.configure({
                        getMachineId: () => this.machineId || null,
                        getKnownMachineTagRoots: () => this.machinePollingItems.map((item) => item.tagId),
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
        void this.publishBridgeConnectionStatus(true).catch((error) => {
            console.error('[PUBLISH_MANAGER] Failed to publish bridge status after status change:', error instanceof Error ? error.message : error);
        });
    }

    private reportError(error: unknown): void {
        const normalizedError = error instanceof Error ? error : new Error(String(error));
        this.callbacks.onError?.(normalizedError);
    }

    private isExternalServiceStatusReadInfo(readInfo: ReadItemInfo): boolean {
        if (!this.machineId) {
            return false;
        }

        return Array.from(this.deviceMap.values()).some((device) => (
            device.isExternalService
            && OptionalDevicePollingTags(device, this.machineId).Sts === readInfo.tagId
        ));
    }

    private async checkAndPublishData(): Promise<void> {
        await republishStalePollingValues({
            mqttClientManager: this.mqttClientManager,
            shouldRepublishReadInfo: (readInfo) => !this.isExternalServiceStatusReadInfo(readInfo),
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

        this.opcuaItemSnapshotMap.clear();
        this.bootstrapReplayTopicMap.clear();

        const machineCfgItem: ReadItemInfo = {
            attributeId: AttributeIds.Value,
            last_publish_time: 0,
            mqttTopic: 'machine/cfg',
            nodeId: getMachineCfgNodeId(concatNodeId),
            tagId: `${PlcNamespaces.Machine}.Cfg`,
            update_period: 1,
            value: null,
        };

        let machineCfg: MachineCfg;
        try {
            machineCfg = await loadMachineCfg(
                machineCfgItem.nodeId,
                (nodeId) => this.readOpcuaValue(nodeId),
            );
            machineCfgItem.value = machineCfg;
            this.setBootstrapReplayTopic(machineCfgItem, machineCfg);
            this.recordBootstrapItemResult(machineCfgItem, 'machineCfg', true, 'loaded machine cfg');
        } catch (error) {
            this.recordBootstrapItemResult(machineCfgItem, 'machineCfg', false, error instanceof Error ? error.message : String(error));
            throw error;
        }
        this.machineCfg = machineCfg;
        this.machineId = machineCfg.machineId.trim();

        const registeredDevicesItem: ReadItemInfo = {
            attributeId: AttributeIds.Value,
            last_publish_time: 0,
            mqttTopic: 'machine/registereddevices',
            nodeId: getRegisteredDevicesNodeId(concatNodeId),
            tagId: `${PlcNamespaces.Machine}.RegisteredDevices`,
            update_period: 1,
            value: null,
        };

        try {
            this.registeredDevices = await loadRegisteredDevices(
                registeredDevicesItem.nodeId,
                (nodeId) => this.readOpcuaValue(nodeId),
                this.deviceMap,
            );
            registeredDevicesItem.value = this.registeredDevices;
            this.setBootstrapReplayTopic(registeredDevicesItem, this.registeredDevices);
            this.recordBootstrapItemResult(registeredDevicesItem, 'registeredDevices', true, `loaded ${this.registeredDevices.length} registered device(s)`);
        } catch (error) {
            this.recordBootstrapItemResult(registeredDevicesItem, 'registeredDevices', false, error instanceof Error ? error.message : String(error));
            throw error;
        }

        const bootstrapResult = await buildValidatedPollingItems(
            this.session,
            machineCfg,
            this.registeredDevices,
            this.deviceMap,
        );

        bootstrapResult.optionalDeviceBootstrapAvailabilityResults.forEach((result) => {
            this.recordBootstrapItemResult(result.item, 'optionalDeviceBootstrap', result.success, result.detail);
        });
        this.recordPollingValidationResults(bootstrapResult.devicePollingValidationResults, 'devicePolling');
        this.recordPollingValidationResults(bootstrapResult.machinePollingValidationResults, 'machinePolling');

        this.devicePollingItems = bootstrapResult.devicePollingItems;
        this.machinePollingItems = bootstrapResult.machinePollingItems;
        this.allPollingItems = bootstrapResult.allPollingItems;
        this.optionalDeviceBootstrapItems = bootstrapResult.optionalDeviceBootstrapItems;
        this.availableOptionalDeviceBootstrapItems = bootstrapResult.availableOptionalDeviceBootstrapItems;
        await this.primeBootstrapReplayTopics(this.availableOptionalDeviceBootstrapItems);
        this.lastBootstrapCompletedAt = Date.now();
        logBootstrapCacheSnapshot(this.getBootstrapCacheSnapshot());
        await this.writeOpcuaItemDumpFile();
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

    private async readOpcuaValue(nodeId: string, options?: { throwOnBadNodeIdUnknown?: boolean }): Promise<any> {
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
            if (options?.throwOnBadNodeIdUnknown && data.statusCode === StatusCodes.BadNodeIdUnknown) {
                throw new Error(`Failed to read OPC UA value from ${nodeId}: ${data.statusCode}`);
            }
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
            getBridgeCachePayload: () => this.getBridgeCachePayload(),
            kioskControlData: this.kioskControlData,
            message,
            mqttClientManager: this.mqttClientManager,
            publishCachedTopics: () => this.publishCachedTopics(),
        });
    }

    private async publishBridgeConnectionStatus(force = false): Promise<void> {
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

    public requestBridgeStatusRefresh(): void {
        void this.publishBridgeConnectionStatus(true).catch((error) => {
            console.error('[PUBLISH_MANAGER] Failed to publish bridge status refresh:', error instanceof Error ? error.message : error);
        });
    }
    private timeWasSynced: boolean = false;

    private async updateHeartbeat(): Promise<void> {
        let nextHeartbeatState;

        try {
            nextHeartbeatState = await syncPublishHeartbeat({
                codesysOpcuaDriver: this.codesysOpcuaDriver,
                heartbeatHmiNodeId: this.heartbeatHmiNodeId,
                heartbeatHmiValue: this.heartbeatHmiValue,
                heartbeatPlcNodeId: this.heartbeatPlcNodeId,
                readOpcuaValue: (nodeId) => this.readOpcuaValue(nodeId, { throwOnBadNodeIdUnknown: true }),
                timeWasSynced: this.timeWasSynced,
                writeOpcuaValue: (nodeId, value, dataType) => this.writeOpcuaValue(nodeId, value, dataType),
            });
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            await this.forceReconnectCycle(`heartbeat sync failed: ${errorMessage}`);
            return;
        }

        this.heartbeatPlcValue = nextHeartbeatState.heartbeatPlcValue;
        this.heartbeatHmiValue = nextHeartbeatState.heartbeatHmiValue;
        this.timeWasSynced = nextHeartbeatState.timeWasSynced;
    }
    // -----------------------------
    // Terminate all subscriptions & polling groups
    // -----------------------------
    private async terminateAllSubscriptions(): Promise<void> {
        await terminatePublishSubscriptions({
            pollingItemGroups: this.pollingItemGroups,
            opcuaSubscriptions: this.opcuaSubscriptions,
        }, (error) => this.isIgnorableOpcuaCleanupError(error));
    }

    private async subscribeToPollingItems(): Promise<void> {
        if (!this.session) {
            throw new Error("OPC UA session is not initialized");
        }

        const activeSession = this.session;

        await subscribeToPublishPollingItems({
            allPollingItems: this.allPollingItems,
            collections: {
                pollingItemGroups: this.pollingItemGroups,
                opcuaSubscriptions: this.opcuaSubscriptions,
            },
            onPollingItemChange: (pollingItem, dataValue) => {
                setImmediate(() => {
                    void this.handlePollingItemChange(pollingItem, dataValue);
                });
            },
            onPollingValidationResults: (results) => {
                this.recordPollingValidationResults(results);
            },
            session: activeSession,
            sessionIsCurrent: () => this.session === activeSession,
            terminateAllSubscriptions: () => this.terminateAllSubscriptions(),
        });
    }

    private async handlePollingItemChange(pollingItem: ClientMonitoredItemBase, dataValue: DataValue): Promise<void> {
        await handlePublishPollingItemChange({
            dataValue,
            decipherOpcuaValue: (data) => decipherOpcuaValue(data),
            pollingItem,
            mqttClientManager: this.mqttClientManager,
            nodeListPrefix: Config.NODE_LIST_PREFIX,
            onHeartbeatObserved: (value) => {
                this.heartbeatPlcValue = value;
                this.lastHeartbeatObservedAt = Date.now();
            },
            onPollingItemResult: (tagId, success, detail) => {
                this.recordPollingItemResult(tagId, success, detail ?? null);
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
