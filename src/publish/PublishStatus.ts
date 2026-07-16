import { DataType } from 'node-opcua';

import { KioskControlData, MqttTopics } from '@kuriousdesign/machine-sdk';

import CodesysOpcuaDriver from '../opcua/codesys-opcua-driver';
import MqttClientManager from '../shared/MqttClientManager';
import Config from '../shared/config';
import { BridgeStatusSnapshot, PublishManagerStatus } from './PublishManagerContracts';

export async function publishKioskControlStatus(
    mqttClientManager: MqttClientManager,
    controlData: KioskControlData,
): Promise<void> {
    await mqttClientManager.publish(MqttTopics.KIOSK_CONTROL, controlData);
}

export async function publishBridgeStatus(params: {
    currentState: number;
    currentStateLabel: string;
    deviceMapEntries: [number, unknown][];
    getBridgeStatusSnapshot?: () => Partial<BridgeStatusSnapshot>;
    kioskControlData: KioskControlData;
    lastPublishTime: number;
    lastPublishedState: number | null;
    mqttClientManager: MqttClientManager;
    now?: number;
    publishManagerStatus: PublishManagerStatus;
    registeredDeviceCount: number;
}): Promise<{ lastPublishTime: number; lastPublishedState: number | null }> {
    const {
        currentState,
        currentStateLabel,
        deviceMapEntries,
        getBridgeStatusSnapshot,
        kioskControlData,
        lastPublishTime,
        lastPublishedState,
        mqttClientManager,
        now = Date.now(),
        publishManagerStatus,
        registeredDeviceCount,
    } = params;

    const stateChanged = currentState !== lastPublishedState;
    const publishIntervalElapsed = now - lastPublishTime >= Config.BRIDGE_STATUS_PUBLISH_INTERVAL_MS;

    if (!stateChanged && !publishIntervalElapsed) {
        return { lastPublishTime, lastPublishedState };
    }

    const payload: BridgeStatusSnapshot = {
        ...getBridgeStatusSnapshot?.(),
        mqttConnected: mqttClientManager.isConnected(),
        opcuaState: currentState,
        opcuaStateLabel: currentStateLabel,
        publishManagerStatus,
        registeredDeviceCount,
    };

    await mqttClientManager.publish(MqttTopics.BRIDGE_STATUS, payload, true);
    await publishKioskControlStatus(mqttClientManager, kioskControlData);

    return {
        lastPublishTime: now,
        lastPublishedState: currentState,
    };
}

export async function syncPublishHeartbeat(params: {
    codesysOpcuaDriver: CodesysOpcuaDriver | null;
    heartbeatHmiNodeId: string;
    heartbeatHmiValue: number;
    heartbeatPlcNodeId: string;
    readOpcuaValue: (nodeId: string) => Promise<any>;
    timeWasSynced: boolean;
    writeOpcuaValue: (nodeId: string, value: any, dataType: DataType) => Promise<void>;
}): Promise<{ heartbeatHmiValue: number; heartbeatPlcValue: number; timeWasSynced: boolean }> {
    const {
        codesysOpcuaDriver,
        heartbeatHmiNodeId,
        heartbeatHmiValue,
        heartbeatPlcNodeId,
        readOpcuaValue,
        timeWasSynced,
        writeOpcuaValue,
    } = params;

    const heartbeatPlcValue = await readOpcuaValue(heartbeatPlcNodeId);
    let nextHeartbeatHmiValue = heartbeatHmiValue;
    let nextTimeWasSynced = timeWasSynced;

    if (heartbeatPlcValue !== nextHeartbeatHmiValue) {
        nextHeartbeatHmiValue = heartbeatPlcValue;
        await writeOpcuaValue(heartbeatHmiNodeId, heartbeatPlcValue, DataType.Byte);
        if (codesysOpcuaDriver && !nextTimeWasSynced) {
            await codesysOpcuaDriver.writeCurrentTimeToCodesys();
            nextTimeWasSynced = true;
        }
    }

    return {
        heartbeatHmiValue: nextHeartbeatHmiValue,
        heartbeatPlcValue,
        timeWasSynced: nextTimeWasSynced,
    };
}
