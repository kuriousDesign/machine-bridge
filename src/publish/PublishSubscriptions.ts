import {
    ClientMonitoredItemBase,
    ClientMonitoredItemGroup,
    ClientSession,
    ClientSubscription,
    DataValue,
    TimestampsToReturn,
} from 'node-opcua';

import { BaseMachinePollingTags, PlcNamespaces } from '../opcua/plc-tags';

import { ReadItemInfo, ReadItemValidationResult, validateReadItemsDetailed } from '../opcua/polling-items';
import MqttClientManager from '../shared/MqttClientManager';
import Config from '../shared/config';

export interface PublishSubscriptionCollections {
    pollingItemGroups: ClientMonitoredItemGroup[];
    opcuaSubscriptions: ClientSubscription[];
}

export async function terminatePublishSubscriptions(
    collections: PublishSubscriptionCollections,
    isIgnorableCleanupError: (error: unknown) => boolean,
): Promise<void> {
    const pollingGroups = collections.pollingItemGroups.splice(0);
    const subscriptions = collections.opcuaSubscriptions.splice(0);

    if (pollingGroups.length > 0) {
        for (const group of pollingGroups) {
            try {
                await group.terminate();
            } catch (error) {
                if (isIgnorableCleanupError(error)) {
                    console.warn('[OPCUA] Ignoring polling group termination error after connection loss:', error instanceof Error ? error.message : error);
                } else {
                    console.warn('Error terminating polling group:', error);
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

    console.log('All OPC UA subscriptions and polling groups terminated');
}

export async function subscribeToPublishPollingItems(params: {
    allPollingItems: ReadItemInfo[];
    collections: PublishSubscriptionCollections;
    onPollingItemChange: (pollingItem: ClientMonitoredItemBase, dataValue: DataValue) => void;
    onPollingValidationResults?: (results: ReadItemValidationResult[]) => void;
    session: ClientSession;
    sessionIsCurrent: () => boolean;
    terminateAllSubscriptions: () => Promise<void>;
}): Promise<void> {
    const {
        allPollingItems,
        collections,
        onPollingItemChange,
        onPollingValidationResults,
        session,
        sessionIsCurrent,
        terminateAllSubscriptions,
    } = params;

    await terminateAllSubscriptions();

    console.log('Subscribing to polling items', allPollingItems.length, 'items to monitor...');
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

        const validationResults = await validateReadItemsDetailed(session, chunk);
        onPollingValidationResults?.(validationResults);
        const validatedItems = validationResults.filter((result) => result.success).map((result) => result.item);
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

            const pollingGroup = ClientMonitoredItemGroup.create(
                subscription,
                validatedItems,
                Config.OPTIONS_GROUP,
                TimestampsToReturn.Neither,
            );

            collections.pollingItemGroups.push(pollingGroup);
            pollingGroup.on('changed', onPollingItemChange);
        } catch (error) {
            console.error(`Failed to create subscription/polling group ${groupIndex}:`, error);
        }
    }));

    console.log(`✅ Subscribed via ${collections.opcuaSubscriptions.length} subscriptions and ${collections.pollingItemGroups.length} polling groups`);
}

export async function handlePublishPollingItemChange(params: {
    dataValue: DataValue;
    decipherOpcuaValue: (data: DataValue) => unknown;
    pollingItem: ClientMonitoredItemBase;
    mqttClientManager: MqttClientManager;
    nodeListPrefix: string;
    onHeartbeatObserved: (value: number) => void;
    onPollingItemResult?: (tagId: string, success: boolean, detail?: string | null) => void;
    tagReadInfoMap: Map<string, ReadItemInfo>;
}): Promise<void> {
    const {
        dataValue,
        decipherOpcuaValue,
        pollingItem,
        mqttClientManager,
        nodeListPrefix,
        onHeartbeatObserved,
        onPollingItemResult,
        tagReadInfoMap,
    } = params;

    const fullNodeId = pollingItem.itemToMonitor?.nodeId?.toString ? pollingItem.itemToMonitor.nodeId.toString() : String(pollingItem.itemToMonitor?.nodeId);
    const tag = fullNodeId.replace(nodeListPrefix, '');

    try {
        const newValue = decipherOpcuaValue(dataValue);
        const newValueType = typeof newValue;
        const readInfo = tagReadInfoMap.get(tag);

        if (!readInfo) {
            console.error('No valid MQTT topic found for tag:', tag, 'full:', fullNodeId);
            onPollingItemResult?.(tag, false, 'missing MQTT topic mapping');
            return;
        }

        if (newValue === null && newValueType === 'undefined') {
            console.error('No valid new value for polling item:', tag, ', value:', newValue);
            onPollingItemResult?.(tag, false, 'undefined polling value');
            return;
        }

        readInfo.value = newValue;
        readInfo.last_publish_time = Date.now();
        tagReadInfoMap.set(tag, readInfo);

        if (tag === `${PlcNamespaces.Machine}.${BaseMachinePollingTags.heartbeatPLC}`) {
            onHeartbeatObserved(newValue as number);
        }

        mqttClientManager.publish(readInfo.mqttTopic, newValue);
        onPollingItemResult?.(tag, true, 'published');
        if (readInfo.mqttTopic.endsWith('/estopcircuit_ok') || readInfo.mqttTopic.endsWith('/estopcircuitdelayed_ok')) {
            console.log(`[MQTT][ESTOP] Published ${readInfo.mqttTopic}:`, newValue);
        }
        if (readInfo.mqttTopic.endsWith('/heartbeatplc') && typeof newValue === 'number' && newValue % 30 === 0) {
            console.log('Machine.heartbeatPlc:', newValue);
        }
    } catch (error) {
        console.error('Error processing polling item change:', error);
        onPollingItemResult?.(tag, false, error instanceof Error ? error.message : String(error));
    }
}
