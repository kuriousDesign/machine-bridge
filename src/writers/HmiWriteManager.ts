import {
    DeviceActionRequestData as SdkDeviceActionRequestData,
    DeviceId,
    DeviceRegistration,
    actionTypeToString,
    getBridgeApiWriteTagTopic,
    getMqttTopics,
    MqttTopics,
    TopicData,
} from '@kuriousdesign/machine-sdk';

import Config from '../shared/config';
import MqttClientManager from '../shared/MqttClientManager';
import { getProjectMachineTag, PlcNamespaces } from '../opcua/plc-tags';
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
    getMachineId: () => string | null;
    getKnownMachineTagRoots: () => string[];
}

export interface HmiWriteManagerCallbacks {
    onStateChange?: (state: HmiWriteManagerState) => void;
    onError?: (error: Error) => void;
    onSessionReset?: (reason: string, error: Error, resetCount: number) => void;
}

type WriteTagRequest = {
    tag: string;
    value: any;
};

type WriteRecipeRequest = {
    index: number;
    recipe: any;
};

type WriteJobRequest = {
    job: any;
};

type WriteActiveRecipeIndexRequest = {
    index: number;
};

function unwrapTopicPayload<T>(message: Buffer): T {
    const envelope = JSON.parse(message.toString()) as Partial<TopicData>;
    return (envelope?.payload ?? envelope) as T;
}

type LowerCamelActionRequest = {
    actionId?: number;
    actionType?: number;
    paramArray?: number[];
    senderId?: number;
    uniqueActionRequestId?: number;
};

function normalizeActionRequest(
    request: Partial<SdkDeviceActionRequestData> | LowerCamelActionRequest | null | undefined,
): SdkDeviceActionRequestData | null {
    if (!request) {
        return null;
    }

    const pascalRequest = request as Partial<SdkDeviceActionRequestData>;
    const lowerCamelRequest = request as LowerCamelActionRequest;

    const actionType = pascalRequest.ActionType ?? lowerCamelRequest.actionType;
    const actionId = pascalRequest.ActionId ?? lowerCamelRequest.actionId;
    const paramArray = pascalRequest.ParamArray ?? lowerCamelRequest.paramArray;
    const senderId = pascalRequest.SenderId ?? lowerCamelRequest.senderId ?? DeviceId.HMI;
    const uniqueActionRequestId = pascalRequest.UniqueActionRequestId ?? lowerCamelRequest.uniqueActionRequestId ?? 0;

    if (!Number.isFinite(actionType) || !Number.isFinite(actionId) || !Array.isArray(paramArray)) {
        return null;
    }

    const normalizedActionType = actionType as SdkDeviceActionRequestData['ActionType'];
    const normalizedActionId = actionId as number;

    return {
        UniqueActionRequestId: uniqueActionRequestId,
        SenderId: senderId,
        ActionType: normalizedActionType,
        ActionId: normalizedActionId,
        ParamArray: paramArray,
    };
}

function getDeviceLabel(deviceMap: Map<number, DeviceRegistration>, deviceId: number): string {
    const mnemonic = deviceMap.get(deviceId)?.mnemonic?.trim();
    if (mnemonic) {
        return mnemonic;
    }

    // if (deviceId === DeviceId.HMI) {
    //     return 'HMI';
    // }

    return `device-${deviceId}`;
}

function formatDeviceLabel(deviceMap: Map<number, DeviceRegistration>, deviceId: number): string {
    return `${getDeviceLabel(deviceMap, deviceId)}(${deviceId})`;
}

export default class HmiWriteManager {
    private state: HmiWriteManagerState = HmiWriteManagerState.Idle;
    private dependencies: HmiWriteManagerDependencies | null = null;
    private actionQueue: Promise<void> = Promise.resolve();
    private opcuaWriteSession = new OpcuaWriteSession(
        DeviceId.HMI,
        'HMI_MANAGER',
        () => this.dependencies?.getMachineId() ?? null,
        () => this.dependencies?.getKnownMachineTagRoots() ?? [],
        (deviceId) => getDeviceLabel(this.dependencies?.getDeviceMap() ?? new Map<number, DeviceRegistration>(), deviceId),
    );
    private subscribedTopics = new Set<string>();
    private sessionResetCount = 0;
    private pendingWriteTags: WriteTagRequest[] = [];

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
        await this.syncWriteRecipeSubscription();
        await this.syncWriteJobSubscription();
        await this.syncWriteActiveRecipeIndexSubscription();

        const topics = this.getTopics();

        for (const device of devices) {
            const topic = `${topics.HMI_ACTION_REQ}/${device.id}`;
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

    private getMachineWriteRootTag(): string {
        const machineId = this.dependencies?.getMachineId()?.trim();
        return machineId ? getProjectMachineTag(machineId) : PlcNamespaces.Machine;
    }

    private getTopics() {
        const machineId = this.dependencies?.getMachineId()?.trim();
        return machineId ? getMqttTopics(machineId) : MqttTopics;
    }

    private getWriteTagTopic(): string {
        const machineId = this.dependencies?.getMachineId()?.trim();
        return machineId ? getBridgeApiWriteTagTopic(machineId) : Config.BRIDGE_API_WRITE_TAG;
    }

    private async enqueueAction(topic: string, message: Buffer): Promise<void> {
        this.actionQueue = this.actionQueue
            .then(async () => {
                const envelope = JSON.parse(message.toString()) as Partial<TopicData>;
                const rawRequest = (envelope?.payload ?? envelope) as Partial<SdkDeviceActionRequestData> | LowerCamelActionRequest;
                const request = normalizeActionRequest(rawRequest);

                if (!request) {
                    console.warn('[HMI_MANAGER] Ignoring invalid HMI action request payload:', envelope);
                    return;
                }

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

        const writeTagTopic = this.getWriteTagTopic();

        if (this.subscribedTopics.has(writeTagTopic)) {
            return;
        }

        console.log('[HMI_MANAGER] Subscribing to bridge api write_tag topic:', writeTagTopic);
        await this.dependencies.mqttClientManager.subscribe(writeTagTopic, (recvTopic: string, message: Buffer) => {
            void this.enqueueWriteTag(recvTopic, message);
        });
        this.subscribedTopics.add(writeTagTopic);
    }

    private async syncWriteRecipeSubscription(): Promise<void> {
        if (!this.dependencies) {
            throw new Error('HMI manager dependencies are not configured');
        }

        const recipeTopic = this.getTopics().HMI_WRITE_RECIPE;

        if (this.subscribedTopics.has(recipeTopic)) {
            return;
        }

        console.log('[HMI_MANAGER] Subscribing to recipe write topic:', recipeTopic);
        await this.dependencies.mqttClientManager.subscribe(recipeTopic, (recvTopic: string, message: Buffer) => {
            void this.enqueueWriteRecipe(recvTopic, message);
        });
        this.subscribedTopics.add(recipeTopic);
    }

    private async syncWriteJobSubscription(): Promise<void> {
        if (!this.dependencies) {
            throw new Error('HMI manager dependencies are not configured');
        }

        const jobTopic = this.getTopics().HMI_WRITE_JOB;

        if (this.subscribedTopics.has(jobTopic)) {
            return;
        }

        console.log('[HMI_MANAGER] Subscribing to job write topic:', jobTopic);
        await this.dependencies.mqttClientManager.subscribe(jobTopic, (recvTopic: string, message: Buffer) => {
            void this.enqueueWriteJob(recvTopic, message);
        });
        this.subscribedTopics.add(jobTopic);
    }

    private async syncWriteActiveRecipeIndexSubscription(): Promise<void> {
        if (!this.dependencies) {
            throw new Error('HMI manager dependencies are not configured');
        }

        const activeRecipeIndexTopic = this.getTopics().HMI_WRITE_ACTIVE_RECIPE_INDEX;

        if (this.subscribedTopics.has(activeRecipeIndexTopic)) {
            return;
        }

        console.log('[HMI_MANAGER] Subscribing to active recipe index write topic:', activeRecipeIndexTopic);
        await this.dependencies.mqttClientManager.subscribe(activeRecipeIndexTopic, (recvTopic: string, message: Buffer) => {
            void this.enqueueWriteActiveRecipeIndex(recvTopic, message);
        });
        this.subscribedTopics.add(activeRecipeIndexTopic);
    }

    private async enqueueWriteTag(topic: string, message: Buffer): Promise<void> {
        const writeTagRequests = this.parseWriteTagRequests(message);
        if (writeTagRequests.length === 0) {
            return;
        }

        this.pendingWriteTags.push(...writeTagRequests);

        this.actionQueue = this.actionQueue
            .then(async () => {
                const batchedWriteTags = this.drainPendingWriteTags();
                if (batchedWriteTags.length === 0) {
                    return;
                }

                await this.handleWriteTagBatch(topic, batchedWriteTags);
            })
            .catch((error) => {
                console.error('[HMI_MANAGER] Failed to process write_tag request:', error);
            });

        await this.actionQueue;
    }

    private async enqueueWriteRecipe(topic: string, message: Buffer): Promise<void> {
        this.actionQueue = this.actionQueue
            .then(async () => {
                const writeRecipeRequest = this.parseWriteRecipeRequest(message);
                if (!writeRecipeRequest) {
                    return;
                }

                await this.handleWriteRecipe(topic, writeRecipeRequest);
            })
            .catch((error) => {
                console.error('[HMI_MANAGER] Failed to process recipe write request:', error);
            });

        await this.actionQueue;
    }

    private async enqueueWriteJob(topic: string, message: Buffer): Promise<void> {
        this.actionQueue = this.actionQueue
            .then(async () => {
                const writeJobRequest = this.parseWriteJobRequest(message);
                if (!writeJobRequest) {
                    return;
                }

                await this.handleWriteJob(topic, writeJobRequest);
            })
            .catch((error) => {
                console.error('[HMI_MANAGER] Failed to process job write request:', error);
            });

        await this.actionQueue;
    }

    private async enqueueWriteActiveRecipeIndex(topic: string, message: Buffer): Promise<void> {
        this.actionQueue = this.actionQueue
            .then(async () => {
                const writeActiveRecipeIndexRequest = this.parseWriteActiveRecipeIndexRequest(message);
                if (!writeActiveRecipeIndexRequest) {
                    return;
                }

                await this.handleWriteActiveRecipeIndex(topic, writeActiveRecipeIndexRequest);
            })
            .catch((error) => {
                console.error('[HMI_MANAGER] Failed to process active recipe index write request:', error);
            });

        await this.actionQueue;
    }

    private async handleActionRequest(topic: string, request: SdkDeviceActionRequestData): Promise<void> {
        if (!this.dependencies) {
            throw new Error('HMI manager dependencies are not configured');
        }

        const deviceId = Number(topic.split('/').pop());
        if (Number.isNaN(deviceId)) {
            console.error('[HMI_MANAGER] Invalid deviceId extracted from topic:', topic);
            return;
        }

        const deviceMap = this.dependencies.getDeviceMap();
        const device = deviceMap.get(deviceId);
        if (!device) {
            console.error('[HMI_MANAGER] No device found for deviceId:', deviceId);
            return;
        }

        const senderLabel = formatDeviceLabel(deviceMap, request.SenderId);
        const targetLabel = formatDeviceLabel(deviceMap, deviceId);
        const actionTypeLabel = actionTypeToString(request.ActionType);

        console.log(
            `[HMI_MANAGER] Handling HMI Action Request ${senderLabel} -> ${targetLabel} type=${actionTypeLabel}(${request.ActionType}) actionId=${request.ActionId}`,
        );
        try {
            await this.opcuaWriteSession.ensureConnected();
            const driver = this.opcuaWriteSession.getDriver();
            if (!driver) {
                console.warn(`[HMI_MANAGER] Skipping HMI action request while OPC UA is unavailable: ${topic}`);
                return;
            }

            await driver.requestAction(deviceId, request.ActionType, request.ActionId, request.ParamArray, {
                senderId: request.SenderId,
                senderLabel,
                targetLabel,
            });
        } catch (error) {
            await this.handleSessionFailure(`HMI action request failed for topic ${topic}`, error);
            throw error;
        }
    }

    private parseWriteTagRequests(message: Buffer): WriteTagRequest[] {
        const payload = unwrapTopicPayload<WriteTagRequest | WriteTagRequest[]>(message);
        const writeTagRequests = Array.isArray(payload) ? payload : [payload];

        const validWriteTagRequests = writeTagRequests.filter((writeTagRequest) => {
            if (!writeTagRequest || typeof writeTagRequest.tag !== 'string' || writeTagRequest.tag.trim().length === 0) {
                console.warn('[HMI_MANAGER] Ignoring invalid write_tag payload:', writeTagRequest);
                return false;
            }

            return true;
        });

        if (validWriteTagRequests.length > 0) {
            const requestedTags = validWriteTagRequests.map((writeTagRequest) => writeTagRequest.tag).join(', ');
            console.log(`[HMI_MANAGER] Received write_tag request for ${validWriteTagRequests.length} tag(s): ${requestedTags}`);
        }

        return validWriteTagRequests;
    }

    private drainPendingWriteTags(): WriteTagRequest[] {
        const pendingWriteTags = this.pendingWriteTags;
        this.pendingWriteTags = [];
        return pendingWriteTags;
    }

    private parseWriteRecipeRequest(message: Buffer): WriteRecipeRequest | null {
        const payload = unwrapTopicPayload<Partial<WriteRecipeRequest>>(message);
        if (typeof payload.index !== 'number' || !Number.isInteger(payload.index) || payload.index < 0) {
            console.warn('[HMI_MANAGER] Ignoring invalid recipe write index:', payload);
            return null;
        }

        if (payload.recipe === null || typeof payload.recipe !== 'object') {
            console.warn('[HMI_MANAGER] Ignoring invalid recipe write payload:', payload);
            return null;
        }

        return {
            index: payload.index,
            recipe: payload.recipe,
        };
    }

    private parseWriteJobRequest(message: Buffer): WriteJobRequest | null {
        const payload = unwrapTopicPayload<Partial<WriteJobRequest>>(message);
        if (payload.job === null || typeof payload.job !== 'object') {
            console.warn('[HMI_MANAGER] Ignoring invalid job write payload:', payload);
            return null;
        }

        return {
            job: payload.job,
        };
    }

    private parseWriteActiveRecipeIndexRequest(message: Buffer): WriteActiveRecipeIndexRequest | null {
        const payload = unwrapTopicPayload<Partial<WriteActiveRecipeIndexRequest>>(message);
        if (typeof payload.index !== 'number' || !Number.isInteger(payload.index) || payload.index < 0) {
            console.warn('[HMI_MANAGER] Ignoring invalid active recipe index write payload:', payload);
            return null;
        }

        return {
            index: payload.index,
        };
    }

    private async handleWriteTagBatch(topic: string, writeTagRequests: WriteTagRequest[]): Promise<void> {
        const writeTagSummary = writeTagRequests.map((writeTagRequest) => writeTagRequest.tag).join(', ');
        console.log(`[HMI_MANAGER] Processing write_tag request from ${topic} for ${writeTagRequests.length} tag(s): ${writeTagSummary}`);

        try {
            await this.opcuaWriteSession.ensureConnected();
            const driver = this.opcuaWriteSession.getDriver();
            if (!driver) {
                console.warn('[HMI_MANAGER] Skipping write_tag batch while OPC UA is unavailable.');
                return;
            }

            const result = await driver.writeTagList(writeTagRequests, true);
            if (!result.success) {
                console.warn(`[HMI_MANAGER] Write tag batch failed for ${writeTagSummary}: ${result.message}`);
            }
        } catch (error) {
            await this.handleSessionFailure(`write_tag failed for topic ${topic}`, error);
            throw error;
        }
    }

    private async handleWriteRecipe(topic: string, writeRecipeRequest: WriteRecipeRequest): Promise<void> {
        console.log('[HMI_MANAGER] Handling recipe write request for index:', writeRecipeRequest.index);

        try {
            await this.opcuaWriteSession.ensureConnected();
            const driver = this.opcuaWriteSession.getDriver();
            if (!driver) {
                console.warn(`[HMI_MANAGER] Skipping recipe write for index ${writeRecipeRequest.index} while OPC UA is unavailable.`);
                return;
            }

            const machineWriteRootTag = this.getMachineWriteRootTag();
            const recipeTag = `${machineWriteRootTag}.recipeStore.recipes[${writeRecipeRequest.index}]`;
            const result = await driver.writeTagList([
                {
                    tag: recipeTag,
                    value: writeRecipeRequest.recipe,
                },
            ], true);

            if (!result.success) {
                console.warn(`[HMI_MANAGER] Recipe write request failed for index ${writeRecipeRequest.index}: ${result.message}`);
                return;
            }
        } catch (error) {
            await this.handleSessionFailure(`recipe write failed for topic ${topic}`, error);
            throw error;
        }
    }

    private async handleWriteJob(topic: string, writeJobRequest: WriteJobRequest): Promise<void> {
        console.log('[HMI_MANAGER] Handling job write request');

        try {
            await this.opcuaWriteSession.ensureConnected();
            const driver = this.opcuaWriteSession.getDriver();
            if (!driver) {
                console.warn('[HMI_MANAGER] Skipping job write while OPC UA is unavailable.');
                return;
            }

            const machineWriteRootTag = this.getMachineWriteRootTag();
            const result = await driver.writeTagList([
                {
                    tag: `${machineWriteRootTag}.job`,
                    value: writeJobRequest.job,
                },
            ], true);

            if (!result.success) {
                console.warn(`[HMI_MANAGER] Job write request failed: ${result.message}`);
            }
        } catch (error) {
            await this.handleSessionFailure(`job write failed for topic ${topic}`, error);
            throw error;
        }
    }

    private async handleWriteActiveRecipeIndex(topic: string, writeActiveRecipeIndexRequest: WriteActiveRecipeIndexRequest): Promise<void> {
        console.log('[HMI_MANAGER] Handling active recipe index write request:', writeActiveRecipeIndexRequest.index);

        try {
            await this.opcuaWriteSession.ensureConnected();
            const driver = this.opcuaWriteSession.getDriver();
            if (!driver) {
                console.warn(`[HMI_MANAGER] Skipping active recipe index write for index ${writeActiveRecipeIndexRequest.index} while OPC UA is unavailable.`);
                return;
            }

            const machineWriteRootTag = this.getMachineWriteRootTag();
            const result = await driver.writeTagList([
                {
                    tag: `${machineWriteRootTag}.job.activeRecipeIndex`,
                    value: writeActiveRecipeIndexRequest.index,
                },
            ], true);

            if (!result.success) {
                console.warn(`[HMI_MANAGER] Active recipe index write failed for index ${writeActiveRecipeIndexRequest.index}: ${result.message}`);
            }
        } catch (error) {
            await this.handleSessionFailure(`active recipe index write failed for topic ${topic}`, error);
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