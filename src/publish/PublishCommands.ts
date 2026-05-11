import {
    BridgeCmds,
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
    kioskControlData: KioskControlData;
    message: TopicData;
    mqttClientManager: MqttClientManager;
}): Promise<KioskControlData> {
    const { deviceMapEntries, kioskControlData, message, mqttClientManager } = params;

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
                await mqttClientManager.publish(MqttTopics.DEVICE_MAP, deviceMapEntries);
            } else {
                console.log('DeviceMap not yet available, cannot publish to bridge/deviceMap');
            }
            break;
        case BridgeCmds.DISCONNECT:
            break;
        default:
            console.warn('Unknown bridge command:', cmdData.cmd);
    }

    await publishKioskControlStatus(mqttClientManager, nextKioskControlData);
    return nextKioskControlData;
}

export async function subscribeToPublishBridgeCommands(params: {
    handleBridgeCommand: (message: TopicData) => Promise<void>;
    mqttClientManager: MqttClientManager;
}): Promise<void> {
    const { handleBridgeCommand, mqttClientManager } = params;

    console.log('Subscribing to bridge command topic:', MqttTopics.BRIDGE_CMD);
    mqttClientManager.subscribe(MqttTopics.BRIDGE_CMD, async (_topic: string, message: Buffer) => {
        await handleBridgeCommand(JSON.parse(message.toString()) as TopicData);
    });
}
