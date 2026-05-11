import { ReadItemInfo } from '../opcua/polling-items';
import { BootstrapCacheSnapshot } from './PublishManagerContracts';

type BootstrapItemStage = 'bootstrap' | 'polling';

function logItemBatch(stage: BootstrapItemStage, label: string, items: ReadItemInfo[]): void {
    const stageLabel = stage.toUpperCase();
    console.log(`[BOOTSTRAP][${stageLabel}] ${label}: ${items.length} tag(s)`);
    items.forEach((item, index) => {
        console.log(`[BOOTSTRAP][${stageLabel}]   [${index + 1}/${items.length}] tag=${item.tagId} node=${item.nodeId} topic=${item.mqttTopic}`);
    });
}

export function logBootstrapStep(step: string, description: string): void {
    console.log(`[BOOTSTRAP] ${step} ${description}`);
}

export function logBootstrapReadItemBatch(label: string, items: ReadItemInfo[]): void {
    logItemBatch('bootstrap', label, items);
}

export function logPollingReadItemBatch(label: string, items: ReadItemInfo[]): void {
    logItemBatch('polling', label, items);
}

export function logValidatedPollingItemBatch(label: string, requestedItems: ReadItemInfo[], validatedItems: ReadItemInfo[]): void {
    const validatedTagIds = new Set(validatedItems.map((item) => item.tagId));
    const skippedItems = requestedItems.filter((item) => !validatedTagIds.has(item.tagId));

    console.log(`[BOOTSTRAP][POLLING] ${label}: validated ${validatedItems.length}/${requestedItems.length}`);
    validatedItems.forEach((item, index) => {
        console.log(`[BOOTSTRAP][POLLING]   [OK ${index + 1}/${validatedItems.length}] tag=${item.tagId} node=${item.nodeId} topic=${item.mqttTopic}`);
    });

    if (skippedItems.length > 0) {
        console.warn(`[BOOTSTRAP][POLLING] ${label}: skipped ${skippedItems.length} invalid tag(s)`);
        skippedItems.forEach((item, index) => {
            console.warn(`[BOOTSTRAP][POLLING]   [SKIP ${index + 1}/${skippedItems.length}] tag=${item.tagId} node=${item.nodeId} topic=${item.mqttTopic}`);
        });
    }
}

export function logBootstrapCacheSnapshot(snapshot: BootstrapCacheSnapshot): void {
    console.log(
        `[BOOTSTRAP][CACHE] machineId=${snapshot.machineId ?? 'unknown'} registeredDevices=${snapshot.registeredDeviceCount} ` +
        `deviceMap=${snapshot.deviceMapCount} bootstrapOptional=${snapshot.availableOptionalDeviceBootstrapTagCount}/${snapshot.optionalDeviceBootstrapTagCount} ` +
        `machinePolling=${snapshot.machinePollingItemCount} devicePolling=${snapshot.devicePollingItemCount} ` +
        `allPolling=${snapshot.allPollingItemCount} cachedPolling=${snapshot.cachedPollingTagCount} opcuaItems=${snapshot.opcuaItems.length}`
    );
    console.log('[BOOTSTRAP][CACHE] access via publishManager.getBootstrapCacheSnapshot() or bridge/status.bootstrapCache');
}
