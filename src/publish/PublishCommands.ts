import {
    BridgeCmds,
    getMqttTopics,
    KioskControlData,
    MqttTopics,
    TopicData,
} from '@kuriousdesign/machine-sdk';

import MqttClientManager from '../shared/MqttClientManager';
import { publishKioskControlStatus } from './PublishStatus';

export function normalizeAllowedKioskIds(ids: string[] | undefined): string[] {
    if (!Array.isArray(ids)) {
        return [];
    }

    return Array.from(new Set(ids.map(id => id.trim()).filter(Boolean)));
}

export function patchKioskControlData(
    current: KioskControlData,
    patch: Partial<KioskControlData>,
): KioskControlData {
    const allowedKioskIds = patch.allowedKioskIds !== undefined
        ? normalizeAllowedKioskIds(patch.allowedKioskIds)
        : current.allowedKioskIds;

    return {
        ...current,
        ...patch,
        allowedKioskIds,
    };
}

export async function handlePublishBridgeCommand(params: {
    deviceMapEntries: [number, unknown][];
    getBridgeCachePayload: () => unknown;
    getMachineId: () => string | null;
    kioskControlData: KioskControlData;
    message: TopicData;
    mqttClientManager: MqttClientManager;
    publishCachedTopics: () => Promise<void>;
}): Promise<KioskControlData> {
    const {
        deviceMapEntries,
        getBridgeCachePayload,
        getMachineId,
        kioskControlData,
        message,
        mqttClientManager,
        publishCachedTopics,
    } = params;
    const topics = getMachineId() ? getMqttTopics(getMachineId() as string) : MqttTopics;

    const cmdData = message.payload as {
        allowedKioskIds?: string[];
        cmd: BridgeCmds;
        kioskId?: string;
        releaseControl?: boolean;
        requestControl?: boolean;
    };
    console.log('Received bridge command:', cmdData.cmd);

    let nextKioskControlData = kioskControlData;
    const kioskId = typeof cmdData.kioskId === 'string' ? cmdData.kioskId.trim() : '';

    if (Array.isArray(cmdData.allowedKioskIds)) {
        const normalizedAllowed = normalizeAllowedKioskIds(cmdData.allowedKioskIds);
        nextKioskControlData = patchKioskControlData(nextKioskControlData, {
            controlMode: 'kiosk',
            allowedKioskIds: normalizedAllowed,
            isControlled: normalizedAllowed.length > 0,
        });
    }

    if (cmdData.requestControl && kioskId) {
        nextKioskControlData = patchKioskControlData(nextKioskControlData, {
            controlMode: 'kiosk',
            isControlled: true,
            allowedKioskIds: [...(nextKioskControlData.allowedKioskIds || []), kioskId],
        });
    }

    if (cmdData.releaseControl && kioskId) {
        const remainingKioskIds = (nextKioskControlData.allowedKioskIds || []).filter(id => id !== kioskId);
        nextKioskControlData = patchKioskControlData(nextKioskControlData, {
            controlMode: 'kiosk',
            isControlled: remainingKioskIds.length > 0,
            allowedKioskIds: remainingKioskIds,
        });
    }

    switch (cmdData.cmd) {
        case BridgeCmds.CONNECT:
            if (deviceMapEntries.length > 0) {
                await mqttClientManager.publish(topics.DEVICE_MAP, deviceMapEntries);
            } else {
                console.log('DeviceMap not yet available, cannot publish to bridge/deviceMap');
            }
            break;
        case BridgeCmds.GET_CACHE:
            await publishCachedTopics();
            await mqttClientManager.publish(topics.BRIDGE_CACHE, getBridgeCachePayload());
            break;
        case BridgeCmds.DISCONNECT:
            break;
        default:
            console.warn('Unknown bridge command:', cmdData.cmd);
    }

    await publishKioskControlStatus(mqttClientManager, nextKioskControlData, getMachineId());
    return nextKioskControlData;
}

export async function subscribeToPublishBridgeCommands(params: {
    handleBridgeCommand: (message: TopicData) => Promise<void>;
    machineId: string | null;
    mqttClientManager: MqttClientManager;
}): Promise<void> {
    const { handleBridgeCommand, machineId, mqttClientManager } = params;
    const topics = machineId ? getMqttTopics(machineId) : MqttTopics;

    console.log('Subscribing to bridge command topic:', topics.BRIDGE_CMD);
    mqttClientManager.subscribe(topics.BRIDGE_CMD, async (_topic: string, message: Buffer) => {
        await handleBridgeCommand(JSON.parse(message.toString()) as TopicData);
    });
}
