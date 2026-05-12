import PublishManager from '../publish/PublishManager';
import { BridgeStatusSnapshot, PublishManagerStatus } from '../publish/PublishManagerContracts';
import MqttClientManager from '../shared/MqttClientManager';
import ExternalServiceWriteManager, { ExternalServiceWriteManagerState } from '../writers/ExternalServiceWriteManager';
import HmiWriteManager, { HmiWriteManagerState } from '../writers/HmiWriteManager';

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
    private publishManagerLoopPromise: Promise<void> | null = null;
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

        this.publishManagerLoopPromise = this.publishManager.manageConnectionLoop();
        await this.publishManagerLoopPromise;
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
        console.log('[SUPERVISOR] Shutdown: requesting publish manager stop');
        this.publishManager.requestShutdown();
        console.log('[SUPERVISOR] Shutdown: stopping writer managers');
        await Promise.all([
            this.hmiWriteManager?.requestShutdown(),
            this.externalServiceWriteManager?.requestShutdown(),
        ]);
        console.log('[SUPERVISOR] Shutdown: writer managers stopped');

        if (this.publishManagerLoopPromise) {
            console.log('[SUPERVISOR] Shutdown: awaiting publish manager loop');
            await this.publishManagerLoopPromise;
            console.log('[SUPERVISOR] Shutdown: publish manager loop resolved');
        }

        console.log('[SUPERVISOR] Shutdown: requesting MQTT stop');
        this.mqttClientManager?.requestShutdown();

        if (this.mqttLoopPromise) {
            console.log('[SUPERVISOR] Shutdown: awaiting MQTT loop');
            await this.mqttLoopPromise;
            console.log('[SUPERVISOR] Shutdown: MQTT loop resolved');
        }

        this.transitionTo(BridgeSupervisorState.Shutdown);
        console.log('[SUPERVISOR] Shutdown: complete');
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
        this.publishManager?.requestBridgeStatusRefresh();
    }

    private async startWriteManagers(): Promise<void> {
        if (this.writeManagersStarted) {
            this.transitionTo(BridgeSupervisorState.Healthy);
            return;
        }

        if (!this.mqttClientManager) {
            throw new Error('MQTT client manager is not initialized');
        }

        this.transitionTo(BridgeSupervisorState.StartingWriters);
        console.log('[SUPERVISOR] Waiting for MQTT connection before starting writer managers');
        await this.mqttClientManager.waitUntilConnected();

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
        this.publishManager?.requestBridgeStatusRefresh();
    }

    private recordWriterError(name: WriterName, error: Error): void {
        this.writerHealth[name] = {
            ...this.writerHealth[name],
            lastError: error.message,
        };

        console.error(`[SUPERVISOR] ${name} writer error: ${error.message}`);
        this.publishManager?.requestBridgeStatusRefresh();
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
        this.publishManager?.requestBridgeStatusRefresh();
    }

    private buildBridgeStatusSnapshot(): Partial<BridgeStatusSnapshot> {
        return {
            machineId: this.publishManager?.getBootstrapCacheSnapshot().machineId ?? null,
            supervisorState: this.state,
            writeManagers: this.getWriterHealth(),
        };
    }
}