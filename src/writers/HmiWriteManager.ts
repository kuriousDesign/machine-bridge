import { DeviceActionRequestData, DeviceId, DeviceRegistration, MqttTopics } from '@kuriousdesign/machine-sdk';

import Config from '../shared/config';
import MqttClientManager from '../shared/MqttClientManager';
import OpcuaWriteSession from './OpcuaWriteSession';

export enum HmiWriteManagerState {
    Idle = 'idle',
    Starting = 'starting',
    Running = 'running',
    Stopping = 'stopping',
    Stopped = 'stopped',
}

export interface HmiWriteManagerDependencies {
    mqttClientManager: MqttClientManager;
    getDeviceMap: () => Map<number, DeviceRegistration>;
}

export interface HmiWriteManagerCallbacks {
    onStateChange?: (state: HmiWriteManagerState) => void;
    onError?: (error: Error) => void;
    onSessionReset?: (reason: string, error: Error, resetCount: number) => void;
}

export default class HmiWriteManager {
    private state: HmiWriteManagerState = HmiWriteManagerState.Idle;
    private dependencies: HmiWriteManagerDependencies | null = null;
    private actionQueue: Promise<void> = Promise.resolve();
    private opcuaWriteSession = new OpcuaWriteSession(DeviceId.HMI, 'HMI_MANAGER');
    private subscribedTopics = new Set<string>();
    private sessionResetCount = 0;

    constructor(
        private readonly callbacks: HmiWriteManagerCallbacks = {},
    ) {}

    public configure(dependencies: HmiWriteManagerDependencies): void {
        this.dependencies = dependencies;
    }

    public async start(): Promise<void> {
        if (this.state === HmiWriteManagerState.Starting || this.state === HmiWriteManagerState.Running) {
            return;
        }

        this.setState(HmiWriteManagerState.Starting);

        try {
            await this.opcuaWriteSession.ensureConnected();
            this.setState(HmiWriteManagerState.Running);
        } catch (error) {
            this.reportError(error, '[HMI_MANAGER] Failed to start HMI write manager');
            throw error;
        }
    }

    public async syncSubscriptions(devices: Iterable<DeviceRegistration>): Promise<void> {
        if (!this.dependencies) {
            throw new Error('HMI manager dependencies are not configured');
        }

        if (this.state !== HmiWriteManagerState.Running) {
            return;
        }

        await this.syncWriteTagSubscription();

        for (const device of devices) {
            const topic = `${MqttTopics.HMI_ACTION_REQ}/${device.id}`;
            if (this.subscribedTopics.has(topic)) {
                continue;
            }

            console.log('[HMI_MANAGER] Subscribing to device action request topic:', topic);
            await this.dependencies.mqttClientManager.subscribe(topic, (recvTopic: string, message: Buffer) => {
                void this.enqueueAction(recvTopic, message);
            });
            this.subscribedTopics.add(topic);
        }
    }

    public resetSubscriptions(): void {
        this.subscribedTopics.clear();
    }

    public async requestShutdown(): Promise<void> {
        if (this.state === HmiWriteManagerState.Stopping || this.state === HmiWriteManagerState.Stopped) {
            return;
        }

        this.setState(HmiWriteManagerState.Stopping);
        this.resetSubscriptions();
        await this.opcuaWriteSession.disconnect();
        this.setState(HmiWriteManagerState.Stopped);
    }

    public getState(): HmiWriteManagerState {
        return this.state;
    }

    private async enqueueAction(topic: string, message: Buffer): Promise<void> {
        this.actionQueue = this.actionQueue
            .then(async () => {
                const request = JSON.parse(message.toString()) as DeviceActionRequestData;
                await this.handleActionRequest(topic, request);
            })
            .catch((error) => {
                console.error('[HMI_MANAGER] Failed to process HMI action request:', error);
            });

        await this.actionQueue;
    }

    private async syncWriteTagSubscription(): Promise<void> {
        if (!this.dependencies) {
            throw new Error('HMI manager dependencies are not configured');
        }

        if (this.subscribedTopics.has(Config.BRIDGE_API_WRITE_TAG)) {
            return;
        }

        console.log('[HMI_MANAGER] Subscribing to bridge api write_tag topic:', Config.BRIDGE_API_WRITE_TAG);
        await this.dependencies.mqttClientManager.subscribe(Config.BRIDGE_API_WRITE_TAG, (recvTopic: string, message: Buffer) => {
            void this.enqueueWriteTag(recvTopic, message);
        });
        this.subscribedTopics.add(Config.BRIDGE_API_WRITE_TAG);
    }

    private async enqueueWriteTag(topic: string, message: Buffer): Promise<void> {
        this.actionQueue = this.actionQueue
            .then(async () => {
                const writeTagData = JSON.parse(message.toString()) as { tag: string; value: any };
                await this.handleWriteTag(topic, writeTagData);
            })
            .catch((error) => {
                console.error('[HMI_MANAGER] Failed to process write_tag request:', error);
            });

        await this.actionQueue;
    }

    private async handleActionRequest(topic: string, request: DeviceActionRequestData): Promise<void> {
        if (!this.dependencies) {
            throw new Error('HMI manager dependencies are not configured');
        }

        const deviceId = Number(topic.split('/').pop());
        if (Number.isNaN(deviceId)) {
            console.error('[HMI_MANAGER] Invalid deviceId extracted from topic:', topic);
            return;
        }

        const device = this.dependencies.getDeviceMap().get(deviceId);
        if (!device) {
            console.error('[HMI_MANAGER] No device found for deviceId:', deviceId);
            return;
        }

        console.log('[HMI_MANAGER] Handling HMI Action Request for device:', device.mnemonic);
        try {
            await this.opcuaWriteSession.ensureConnected();
            const driver = this.opcuaWriteSession.getDriver();
            if (!driver) {
                console.warn(`[HMI_MANAGER] Skipping HMI action request while OPC UA is unavailable: ${topic}`);
                return;
            }

            await driver.requestAction(deviceId, request.ActionType, request.ActionId, request.ParamArray);
        } catch (error) {
            await this.handleSessionFailure(`HMI action request failed for topic ${topic}`, error);
            throw error;
        }
    }

    private async handleWriteTag(topic: string, writeTagData: { tag: string; value: any }): Promise<void> {
        console.log('[HMI_MANAGER] Handling write tag request for tag:', writeTagData.tag);

        try {
            await this.opcuaWriteSession.ensureConnected();
            const driver = this.opcuaWriteSession.getDriver();
            if (!driver) {
                console.warn(`[HMI_MANAGER] Skipping write_tag for ${writeTagData.tag} while OPC UA is unavailable.`);
                return;
            }

            const result = await driver.writeNestedObject(writeTagData.tag, writeTagData.value, true);
            if (!result.success) {
                console.warn(`[HMI_MANAGER] Write tag request failed for ${writeTagData.tag}: ${result.message}`);
            }
        } catch (error) {
            await this.handleSessionFailure(`write_tag failed for topic ${topic}`, error);
            throw error;
        }
    }

    private setState(nextState: HmiWriteManagerState): void {
        if (this.state === nextState) {
            return;
        }

        this.state = nextState;
        console.log(`[HMI_MANAGER] STATE: ${nextState}`);
        this.callbacks.onStateChange?.(nextState);
    }

    private async handleSessionFailure(reason: string, error: unknown): Promise<void> {
        const normalizedError = this.normalizeError(error);
        this.sessionResetCount += 1;
        this.callbacks.onSessionReset?.(reason, normalizedError, this.sessionResetCount);
        this.reportError(normalizedError, '[HMI_MANAGER] Session failure detected');
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