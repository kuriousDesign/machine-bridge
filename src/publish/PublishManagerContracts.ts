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

export interface BridgeStatusSnapshot {
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