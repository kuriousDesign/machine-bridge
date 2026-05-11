import {
    ClientMonitoredItemBase,
    ClientMonitoredItemGroup,
    ClientSession,
    ClientSubscription,
    DataValue,
    TimestampsToReturn,
} from 'node-opcua';

import { MachineTags, PlcNamespaces } from '@kuriousdesign/machine-sdk';

import { ReadItemInfo, validateReadItems } from '../opcua/monitored-items';
import MqttClientManager from '../shared/MqttClientManager';
import Config from '../shared/config';

export interface PublishSubscriptionCollections {
    monitoredItemGroups: ClientMonitoredItemGroup[];
    opcuaSubscriptions: ClientSubscription[];
}

export async function terminatePublishSubscriptions(
    collections: PublishSubscriptionCollections,
    isIgnorableCleanupError: (error: unknown) => boolean,
): Promise<void> {
    const monitoredGroups = collections.monitoredItemGroups.splice(0);
    const subscriptions = collections.opcuaSubscriptions.splice(0);

    if (monitoredGroups.length > 0) {
        for (const group of monitoredGroups) {
            try {
                await group.terminate();
            } catch (error) {
                if (isIgnorableCleanupError(error)) {
                    console.warn('[OPCUA] Ignoring monitored group termination error after connection loss:', error instanceof Error ? error.message : error);
                } else {
                    console.warn('Error terminating monitored group:', error);
                }
            }
        }
    }

    if (subscriptions.length > 0) {
        for (const subscription of subscriptions) {
            try {
                await subscription.terminate();
            } catch (error) {
                if (isIgnorableCleanupError(error)) {
                    console.warn('[OPCUA] Ignoring subscription termination error after connection loss:', error instanceof Error ? error.message : error);
                } else {
                    console.warn('Error terminating subscription:', error);
                }
            }
        }
    }

    console.log('All OPC UA subscriptions and monitored groups terminated');
}

export async function subscribeToPublishMonitoredItems(params: {
    allPollingItems: ReadItemInfo[];
    collections: PublishSubscriptionCollections;
    onMonitoredItemChange: (monitoredItem: ClientMonitoredItemBase, dataValue: DataValue) => void;
    session: ClientSession;
    sessionIsCurrent: () => boolean;
    terminateAllSubscriptions: () => Promise<void>;
}): Promise<void> {
    const {
        allPollingItems,
        collections,
        onMonitoredItemChange,
        session,
        sessionIsCurrent,
        terminateAllSubscriptions,
    } = params;

    await terminateAllSubscriptions();

    console.log('Subscribing to monitored items', allPollingItems.length, 'items to monitor...');
    const chunks: ReadItemInfo[][] = [];
    for (let i = 0; i < allPollingItems.length; i += Config.CHUNK_SIZE) {
        chunks.push(allPollingItems.slice(i, i + Config.CHUNK_SIZE));
    }

    await Promise.all(chunks.map(async (chunk, chunkIdx) => {
        const groupIndex = chunkIdx + 1;
        console.log(`Creating subscription group ${groupIndex} with ${chunk.length} items...`);

        if (!sessionIsCurrent()) {
            console.warn(`Skipping subscription group ${groupIndex} because the OPC UA session changed.`);
            return;
        }

        const validatedItems = await validateReadItems(session, chunk);
        if (validatedItems.length === 0) {
            console.warn(`No valid items in group ${groupIndex}, skipping subscription creation.`);
            return;
        }

        if (!sessionIsCurrent()) {
            console.warn(`Skipping subscription group ${groupIndex} after validation because the OPC UA session changed.`);
            return;
        }

        try {
            const subscription = await session.createSubscription2(Config.SUBSCRIPTION_OPTIONS);
            if (!subscription) {
                console.error(`Failed to create subscription for group ${groupIndex}`);
                return;
            }
            collections.opcuaSubscriptions.push(subscription);

            const monitoredGroup = ClientMonitoredItemGroup.create(
                subscription,
                validatedItems,
                Config.OPTIONS_GROUP,
                TimestampsToReturn.Neither,
            );

            collections.monitoredItemGroups.push(monitoredGroup);
            monitoredGroup.on('changed', onMonitoredItemChange);
        } catch (error) {
            console.error(`Failed to create subscription/monitored group ${groupIndex}:`, error);
        }
    }));

    console.log(`✅ Subscribed via ${collections.opcuaSubscriptions.length} subscriptions and ${collections.monitoredItemGroups.length} monitored groups`);
}

export async function handlePublishMonitoredItemChange(params: {
    dataValue: DataValue;
    decipherOpcuaValue: (data: DataValue) => unknown;
    monitoredItem: ClientMonitoredItemBase;
    mqttClientManager: MqttClientManager;
    nodeListPrefix: string;
    onHeartbeatObserved: (value: number) => void;
    tagReadInfoMap: Map<string, ReadItemInfo>;
}): Promise<void> {
    const {
        dataValue,
        decipherOpcuaValue,
        monitoredItem,
        mqttClientManager,
        nodeListPrefix,
        onHeartbeatObserved,
        tagReadInfoMap,
    } = params;

    try {
        const newValue = decipherOpcuaValue(dataValue);
        const newValueType = typeof newValue;
        const fullNodeId = monitoredItem.itemToMonitor?.nodeId?.toString ? monitoredItem.itemToMonitor.nodeId.toString() : String(monitoredItem.itemToMonitor?.nodeId);
        const tag = fullNodeId.replace(nodeListPrefix, '');
        const readInfo = tagReadInfoMap.get(tag);

        if (!readInfo) {
            console.error('No valid MQTT topic found for tag:', tag, 'full:', fullNodeId);
            return;
        }

        if (newValue === null && newValueType === 'undefined') {
            console.error('No valid new value for monitored item:', tag, ', value:', newValue);
            return;
        }

        readInfo.value = newValue;
        readInfo.last_publish_time = Date.now();
        tagReadInfoMap.set(tag, readInfo);

        if (tag === `${PlcNamespaces.Machine}.${MachineTags.HeartbeatPLC}`) {
            onHeartbeatObserved(newValue as number);
        }

        mqttClientManager.publish(readInfo.mqttTopic, newValue);
        if (readInfo.mqttTopic === 'machine/heartbeatplc' && typeof newValue === 'number' && newValue % 30 === 0) {
            console.log('Machine.heartbeatPlc:', newValue);
        }
    } catch (error) {
        console.error('Error processing monitored item change:', error);
    }
}
