import ExternalServiceWriteManager, { ExternalServiceWriteManagerState } from './ExternalServiceWriteManager';
import HmiWriteManager, { HmiWriteManagerState } from './HmiWriteManager';
import MqttClientManager from './MqttClientManager';
import PublishManager, { BridgeStatusSnapshot, PublishManagerStatus } from './PublishManager';

interface WriterHealth {
    state: string;
    resetCount: number;
    lastResetReason: string | null;
    lastResetAt: number | null;
    lastError: string | null;
}

type WriterName = 'hmi' | 'externalService';

export enum BridgeSupervisorState {
    Startup = 'startup',
    StartingPublishManager = 'startingPublishManager',
    PublishManagerRecovering = 'publishManagerRecovering',
    StartingWriters = 'startingWriters',
    Healthy = 'healthy',
    Draining = 'draining',
    Shutdown = 'shutdown',
}

export default class BridgeSupervisor {
    private state: BridgeSupervisorState = BridgeSupervisorState.Startup;
    private mqttClientManager: MqttClientManager | null = null;
    private mqttLoopPromise: Promise<void> | null = null;
    private publishManager: PublishManager | null = null;
    private hmiWriteManager: HmiWriteManager | null = null;
    private externalServiceWriteManager: ExternalServiceWriteManager | null = null;
    private publishManagerStatus: PublishManagerStatus = PublishManagerStatus.Idle;
    private shutdownPromise: Promise<void> | null = null;
    private writeManagersStarted = false;
    private writerHealth: Record<WriterName, WriterHealth> = {
        hmi: {
            state: HmiWriteManagerState.Idle,
            resetCount: 0,
            lastResetReason: null,
            lastResetAt: null,
            lastError: null,
        },
        externalService: {
            state: ExternalServiceWriteManagerState.Idle,
            resetCount: 0,
            lastResetReason: null,
            lastResetAt: null,
            lastError: null,
        },
    };

    public async start(): Promise<void> {
        this.transitionTo(BridgeSupervisorState.StartingPublishManager);
        if (!this.mqttClientManager) {
            this.mqttClientManager = new MqttClientManager();
        }
        if (!this.externalServiceWriteManager) {
            this.externalServiceWriteManager = new ExternalServiceWriteManager({
                onError: (error) => {
                    this.recordWriterError('externalService', error);
                },
                onSessionReset: (reason, error, resetCount) => {
                    this.recordWriterReset('externalService', reason, error, resetCount);
                },
                onStateChange: (state) => {
                    this.recordWriterState('externalService', state);
                },
            });
        }
        if (!this.hmiWriteManager) {
            this.hmiWriteManager = new HmiWriteManager({
                onError: (error) => {
                    this.recordWriterError('hmi', error);
                },
                onSessionReset: (reason, error, resetCount) => {
                    this.recordWriterReset('hmi', reason, error, resetCount);
                },
                onStateChange: (state) => {
                    this.recordWriterState('hmi', state);
                },
            });
        }
        this.mqttLoopPromise = this.mqttClientManager.manageConnectionLoop();
        await this.mqttClientManager.waitUntilConnected();
        this.publishManager = new PublishManager({
            onStatusChange: (status) => {
                this.publishManagerStatus = status;
                console.log(`[SUPERVISOR] Publish manager status: ${status}`);

                if (status === PublishManagerStatus.Polling) {
                    void this.startWriteManagers();
                } else if (status === PublishManagerStatus.Reconnecting || status === PublishManagerStatus.WaitingForHeartbeat) {
                    this.transitionTo(BridgeSupervisorState.PublishManagerRecovering);
                    this.writeManagersStarted = false;
                }
            },
            onError: (error) => {
                console.error(`[SUPERVISOR] Publish manager error: ${error.message}`);
                if (this.state !== BridgeSupervisorState.Draining && this.state !== BridgeSupervisorState.Shutdown) {
                    this.transitionTo(BridgeSupervisorState.PublishManagerRecovering);
                }
            },
        }, {
            externalServiceWriteManager: this.externalServiceWriteManager,
            getBridgeStatusSnapshot: () => this.buildBridgeStatusSnapshot(),
            hmiWriteManager: this.hmiWriteManager,
            mqttClientManager: this.mqttClientManager,
        });
        await this.publishManager.manageConnectionLoop();
        await this.mqttLoopPromise;
        this.transitionTo(BridgeSupervisorState.Shutdown);
    }

    public async requestShutdown(): Promise<void> {
        if (this.shutdownPromise) {
            await this.shutdownPromise;
            return;
        }

        this.shutdownPromise = this.performShutdown();
        await this.shutdownPromise;
    }

    private async performShutdown(): Promise<void> {
        if (!this.publishManager) {
            this.transitionTo(BridgeSupervisorState.Shutdown);
            return;
        }

        this.transitionTo(BridgeSupervisorState.Draining);
        this.mqttClientManager?.requestShutdown();
        await Promise.all([
            this.hmiWriteManager?.requestShutdown(),
            this.externalServiceWriteManager?.requestShutdown(),
        ]);
        this.publishManager.requestShutdown();

        if (this.mqttLoopPromise) {
            await this.mqttLoopPromise;
        }
    }

    public getState(): BridgeSupervisorState {
        return this.state;
    }

    public getPublishManagerStatus(): PublishManagerStatus {
        return this.publishManagerStatus;
    }

    public getWriterHealth(): Record<WriterName, WriterHealth> {
        return {
            hmi: { ...this.writerHealth.hmi },
            externalService: { ...this.writerHealth.externalService },
        };
    }

    private transitionTo(nextState: BridgeSupervisorState): void {
        if (this.state === nextState) {
            return;
        }

        console.log(`[SUPERVISOR] STATE: ${this.state} -> ${nextState}`);
        this.state = nextState;
    }

    private async startWriteManagers(): Promise<void> {
        if (this.writeManagersStarted) {
            this.transitionTo(BridgeSupervisorState.Healthy);
            return;
        }

        this.transitionTo(BridgeSupervisorState.StartingWriters);

        await this.hmiWriteManager?.start();
        await this.externalServiceWriteManager?.start();
        await this.publishManager?.syncWriterSubscriptions();

        this.writeManagersStarted = true;
        this.transitionTo(BridgeSupervisorState.Healthy);
    }

    private recordWriterState(name: WriterName, state: string): void {
        this.writerHealth[name] = {
            ...this.writerHealth[name],
            state,
        };

        console.log(`[SUPERVISOR] ${name} writer state: ${state}`);
    }

    private recordWriterError(name: WriterName, error: Error): void {
        this.writerHealth[name] = {
            ...this.writerHealth[name],
            lastError: error.message,
        };

        console.error(`[SUPERVISOR] ${name} writer error: ${error.message}`);
    }

    private recordWriterReset(name: WriterName, reason: string, error: Error, resetCount: number): void {
        this.writerHealth[name] = {
            ...this.writerHealth[name],
            lastError: error.message,
            lastResetAt: Date.now(),
            lastResetReason: reason,
            resetCount,
        };

        const logMethod = resetCount >= 3 ? console.error : console.warn;
        logMethod(`[SUPERVISOR] ${name} writer session reset #${resetCount}: ${reason}. Last error: ${error.message}`);
    }

    private buildBridgeStatusSnapshot(): Partial<BridgeStatusSnapshot> {
        return {
            supervisorState: this.state,
            writeManagers: this.getWriterHealth(),
        };
    }
}