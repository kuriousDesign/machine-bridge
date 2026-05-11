export enum PublishManagerStatus {
    Idle = 'idle',
    ConnectingSession = 'connectingSession',
    LoadingBootstrapData = 'loadingBootstrapData',
    CreatingSubscriptions = 'creatingSubscriptions',
    ReadyForHmiHydration = 'readyForHmiHydration',
    Polling = 'polling',
    WaitingForHeartbeat = 'waitingForHeartbeat',
    Reconnecting = 'reconnecting',
    Disconnecting = 'disconnecting',
    Disconnected = 'disconnected',
}

export interface WriterHealthSnapshot {
    lastError: string | null;
    lastResetAt: number | null;
    lastResetReason: string | null;
    resetCount: number;
    state: string;
}

export type OpcuaItemSource =
    | 'machineCfg'
    | 'registeredDevices'
    | 'optionalDeviceBootstrap'
    | 'devicePolling'
    | 'machinePolling';

export type OpcuaReadStatus = 'pending' | 'success' | 'failed' | 'skipped';

export type OpcuaPollStatus = 'notApplicable' | 'pending' | 'subscribed' | 'received' | 'failed' | 'notSubscribed';

export interface OpcuaItemSnapshot {
    lastPolledAt: number | null;
    lastReadAt: number | null;
    mqttTopic: string;
    nodeId: string;
    pollDetail: string | null;
    pollStatus: OpcuaPollStatus;
    readDetail: string | null;
    readStatus: OpcuaReadStatus;
    source: OpcuaItemSource;
    tagId: string;
}

export interface BootstrapCacheSnapshot {
    allPollingItemCount: number;
    availableOptionalDeviceBootstrapTagCount: number;
    cachedPollingTagCount: number;
    deviceMapCount: number;
    devicePollingItemCount: number;
    lastBootstrapCompletedAt: number | null;
    machineId: string | null;
    machinePollingItemCount: number;
    opcuaItems: OpcuaItemSnapshot[];
    optionalDeviceBootstrapTagCount: number;
    registeredDeviceCount: number;
}

export interface BridgeStatusSnapshot {
    // Accessed directly from PublishManagerCore via getBootstrapCacheSnapshot()
    // and propagated externally on the bridge/status payload as bootstrapCache.
    bootstrapCache?: BootstrapCacheSnapshot;
    machineId?: string | null;
    mqttConnected: boolean;
    opcuaState: number;
    opcuaStateLabel: string;
    publishManagerStatus: PublishManagerStatus;
    registeredDeviceCount: number;
    supervisorState?: string;
    writeManagers?: {
        externalService: WriterHealthSnapshot;
        hmi: WriterHealthSnapshot;
    };
}