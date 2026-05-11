import {
    AttributeIds,
    ClientSession,
    DataValue,
    ReadValueIdOptions,
    TimestampsToReturn,
} from 'node-opcua';

import { ReadItemInfo } from '../opcua/polling-items';
import MqttClientManager from '../shared/MqttClientManager';
import Config from '../shared/config';

export async function republishStalePollingValues(params: {
    mqttClientManager: MqttClientManager;
    tagReadInfoMap: Map<string, ReadItemInfo>;
}): Promise<void> {
    const { mqttClientManager, tagReadInfoMap } = params;

    if (!Config.ENABLE_STALE_POLLING_REPUBLISH) {
        return;
    }

    tagReadInfoMap.forEach((readInfo, tag) => {
        if (readInfo.last_publish_time <= 0) {
            return;
        }

        const now = Date.now();
        if (now - readInfo.last_publish_time >= readInfo.update_period * Config.REPUBLISH_RATE_MS) {
            void mqttClientManager.publish(readInfo.mqttTopic, readInfo.value);
            readInfo.last_publish_time = now;
            tagReadInfoMap.set(tag, readInfo);
        }
    });
}

export async function readPollingChunkValues(params: {
    allPollingItems: ReadItemInfo[];
    allPollingValues: unknown[];
    chunkIndex: number;
    decipherOpcuaValue: (dataValue: DataValue) => unknown;
    session: ClientSession;
}): Promise<void> {
    const { allPollingItems, allPollingValues, chunkIndex, decipherOpcuaValue, session } = params;

    const nodesToRead: ReadValueIdOptions[] = allPollingItems.map(item => ({
        nodeId: `${Config.NODE_LIST_PREFIX}${item.tagId}`,
        attributeId: AttributeIds.Value,
        timespampsToReturn: TimestampsToReturn.Neither,
    }));

    const maxIndex = nodesToRead.length;
    const startingIndex = chunkIndex * Config.CHUNK_SIZE;
    const endingIndex = Math.min(startingIndex + Config.CHUNK_SIZE, maxIndex);
    const nodeChunk = nodesToRead.slice(startingIndex, endingIndex);

    const dataValues = await session.read(nodeChunk);
    dataValues.forEach((dataValue, index) => {
        allPollingValues[startingIndex + index] = decipherOpcuaValue(dataValue);
    });
}

export async function publishPollingChunkValues(params: {
    allPollingItems: ReadItemInfo[];
    allPollingValues: unknown[];
    chunkIndex: number;
    mqttClientManager: MqttClientManager;
}): Promise<void> {
    const { allPollingItems, allPollingValues, chunkIndex, mqttClientManager } = params;

    const startingIndex = chunkIndex * Config.CHUNK_SIZE;
    const endingIndex = Math.min(startingIndex + Config.CHUNK_SIZE, allPollingItems.length);
    await Promise.all(allPollingItems.slice(startingIndex, endingIndex).map((item, index) => {
        return mqttClientManager.publish(item.mqttTopic, allPollingValues[startingIndex + index]);
    }));
}

export async function readAndPublishPollingChunkValues(params: {
    allPollingItems: ReadItemInfo[];
    allPollingValues: unknown[];
    chunkIndex: number;
    decipherOpcuaValue: (dataValue: DataValue) => unknown;
    mqttClientManager: MqttClientManager;
    session: ClientSession;
}): Promise<void> {
    const {
        allPollingItems,
        allPollingValues,
        chunkIndex,
        decipherOpcuaValue,
        mqttClientManager,
        session,
    } = params;

    await readPollingChunkValues({
        allPollingItems,
        allPollingValues,
        chunkIndex,
        decipherOpcuaValue,
        session,
    });
    await publishPollingChunkValues({
        allPollingItems,
        allPollingValues,
        chunkIndex,
        mqttClientManager,
    });
}
