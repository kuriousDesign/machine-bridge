import { DeviceId, DeviceRegistration, TopicData, PlcNamespaces } from '@kuriousdesign/machine-sdk';

import Config from './config';
import MqttClientManager from './MqttClientManager';
import OpcuaWriteSession from './OpcuaWriteSession';

export enum ExternalServiceWriteManagerState {
    Idle = 'idle',
    Starting = 'starting',
    Running = 'running',
    Stopping = 'stopping',
    Stopped = 'stopped',
}

export interface ExternalServiceWriteManagerDependencies {
    mqttClientManager: MqttClientManager;
    getDeviceMap: () => Map<number, DeviceRegistration>;
}

export interface ExternalServiceWriteManagerCallbacks {
    onStateChange?: (state: ExternalServiceWriteManagerState) => void;
    onError?: (error: Error) => void;
    onSessionReset?: (reason: string, error: Error, resetCount: number) => void;
}

export default class ExternalServiceWriteManager {
    private state: ExternalServiceWriteManagerState = ExternalServiceWriteManagerState.Idle;
    private dependencies: ExternalServiceWriteManagerDependencies | null = null;
    private opcuaWriteSession = new OpcuaWriteSession(DeviceId.HMI, 'EXT_SERVICE_MANAGER');
    private writeQueue: Promise<void> = Promise.resolve();
    private subscribedTopics = new Set<string>();
    private sessionResetCount = 0;

    constructor(
        private readonly callbacks: ExternalServiceWriteManagerCallbacks = {},
    ) {}

    public configure(dependencies: ExternalServiceWriteManagerDependencies): void {
        this.dependencies = dependencies;
    }

    public async start(): Promise<void> {
        if (this.state === ExternalServiceWriteManagerState.Starting || this.state === ExternalServiceWriteManagerState.Running) {
            return;
        }

        this.setState(ExternalServiceWriteManagerState.Starting);

        try {
            await this.opcuaWriteSession.ensureConnected();
            this.setState(ExternalServiceWriteManagerState.Running);
        } catch (error) {
            this.reportError(error, '[EXT_SERVICE_MANAGER] Failed to start external service write manager');
            throw error;
        }
    }

    public async syncSubscriptions(devices: Iterable<DeviceRegistration>): Promise<void> {
        if (!this.dependencies) {
            throw new Error('External service manager dependencies are not configured');
        }

        if (this.state !== ExternalServiceWriteManagerState.Running) {
            return;
        }

        for (const device of devices) {
            if (!device.isExternalService) {
                continue;
            }

            const topic = `${Config.BRIDGE_API_UPDATE_DEVICE}/${device.id}/sts`;
            if (this.subscribedTopics.has(topic)) {
                continue;
            }

            console.log('[EXT_SERVICE_MANAGER] Subscribing to external service sts tag:', topic);
            await this.dependencies.mqttClientManager.subscribe(topic, (recvTopic: string, message: Buffer) => {
                void this.enqueueWrite(recvTopic, message);
            });
            this.subscribedTopics.add(topic);
        }
    }

    public resetSubscriptions(): void {
        this.subscribedTopics.clear();
    }

    public async requestShutdown(): Promise<void> {
        if (this.state === ExternalServiceWriteManagerState.Stopping || this.state === ExternalServiceWriteManagerState.Stopped) {
            return;
        }

        this.setState(ExternalServiceWriteManagerState.Stopping);
        this.resetSubscriptions();
        await this.opcuaWriteSession.disconnect();
        this.setState(ExternalServiceWriteManagerState.Stopped);
    }

    public getState(): ExternalServiceWriteManagerState {
        return this.state;
    }

    private async enqueueWrite(topic: string, message: Buffer): Promise<void> {
        this.writeQueue = this.writeQueue
            .then(async () => {
                const request = JSON.parse(message.toString()) as TopicData;
                await this.handleDeviceUpdate(topic, request);
            })
            .catch((error) => {
                console.error('[EXT_SERVICE_MANAGER] Failed to process external service write:', error);
            });

        await this.writeQueue;
    }

    private async handleDeviceUpdate(topic: string, message: TopicData): Promise<void> {
        if (!this.dependencies) {
            throw new Error('External service manager dependencies are not configured');
        }

        const topicParts = topic.split('/');
        const deviceId = Number(topicParts[topicParts.length - 2]);
        if (Number.isNaN(deviceId)) {
            console.error('[EXT_SERVICE_MANAGER] Invalid deviceId extracted from topic:', topic);
            return;
        }

        const device = this.dependencies.getDeviceMap().get(deviceId);
        if (!device) {
            console.error('[EXT_SERVICE_MANAGER] No device found for deviceId:', deviceId);
            return;
        }

        const payload = message.payload as unknown;
        if (payload === undefined || payload === null) {
            console.error('[EXT_SERVICE_MANAGER] No payload found in message for deviceId:', deviceId, ' topic:', topic);
            return;
        }

        try {
            await this.opcuaWriteSession.ensureConnected();
            const driver = this.opcuaWriteSession.getDriver();
            if (!driver) {
                console.warn(`[EXT_SERVICE_MANAGER] Skipping external service write while OPC UA is unavailable: ${topic}`);
                return;
            }

            const completeData = payload as Record<string, unknown>;
            const deviceTag = `${PlcNamespaces.Machine}.${device.mnemonic.toLowerCase()}Sts`;

            if (
                typeof completeData === 'object'
                && completeData !== null
                && 'iExtService' in completeData
                && typeof completeData.iExtService === 'object'
                && completeData.iExtService !== null
                && 'o' in (completeData.iExtService as Record<string, unknown>)
            ) {
                delete (completeData.iExtService as Record<string, unknown>).o;
            }

            const result = await driver.writeNestedObject(deviceTag, completeData, true);
            if (!result.success) {
                console.warn(`[EXT_SERVICE_MANAGER] External service write failed for ${deviceTag}: ${result.message}`);
            }
        } catch (error) {
            await this.handleSessionFailure(`external service write failed for topic ${topic}`, error);
            throw error;
        }
    }

    private setState(nextState: ExternalServiceWriteManagerState): void {
        if (this.state === nextState) {
            return;
        }

        this.state = nextState;
        console.log(`[EXT_SERVICE_MANAGER] STATE: ${nextState}`);
        this.callbacks.onStateChange?.(nextState);
    }

    private async handleSessionFailure(reason: string, error: unknown): Promise<void> {
        const normalizedError = this.normalizeError(error);
        this.sessionResetCount += 1;
        this.callbacks.onSessionReset?.(reason, normalizedError, this.sessionResetCount);
        this.reportError(normalizedError, '[EXT_SERVICE_MANAGER] Session failure detected');
        await this.opcuaWriteSession.reset(reason, normalizedError);
    }

    private reportError(error: unknown, prefix: string): void {
        const normalizedError = this.normalizeError(error);
        console.error(prefix, normalizedError);
        this.callbacks.onError?.(normalizedError);
    }

    private normalizeError(error: unknown): Error {
        return error instanceof Error ? error : new Error(String(error));
    }
}