import { ReadItemInfo } from '../opcua/polling-items';
import Config from '../shared/config';
import { BootstrapCacheSnapshot } from './PublishManagerContracts';

type BootstrapItemStage = 'bootstrap' | 'polling';
const WARNING_ICON = process.stderr.isTTY ? '\x1b[33m⚠\x1b[0m' : '⚠';

function logItemBatch(stage: BootstrapItemStage, label: string, items: ReadItemInfo[]): void {
    const stageLabel = stage.toUpperCase();
    console.log(`[BOOTSTRAP][${stageLabel}] ${label}: ${items.length} tag(s)`);
    if (!Config.SHOW_SUCCESSFUL_TAG_SUBSCRIPTION_LOGS) {
        return;
    }

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

export function logValidatedPollingItemBatch(
    label: string,
    requestedItems: ReadItemInfo[],
    validatedItems: ReadItemInfo[],
    detailsByTagId: Map<string, string | null> = new Map(),
): void {
    const validatedTagIds = new Set(validatedItems.map((item) => item.tagId));
    const skippedItems = requestedItems.filter((item) => !validatedTagIds.has(item.tagId));

    console.log(`[BOOTSTRAP][POLLING] ${label}: validated ${validatedItems.length}/${requestedItems.length}`);
    if (Config.SHOW_SUCCESSFUL_TAG_SUBSCRIPTION_LOGS) {
        validatedItems.forEach((item, index) => {
            console.log(`[BOOTSTRAP][POLLING]   [OK ${index + 1}/${validatedItems.length}] tag=${item.tagId} node=${item.nodeId} topic=${item.mqttTopic}`);
        });
    }

    if (skippedItems.length > 0) {
        console.warn(`${WARNING_ICON} [BOOTSTRAP][POLLING] ${label}: skipped ${skippedItems.length} invalid tag(s)`);
        skippedItems.forEach((item, index) => {
            const detail = detailsByTagId.get(item.tagId);
            const detailSuffix = detail ? ` reason=${detail}` : '';
            console.warn(`${WARNING_ICON} [BOOTSTRAP][POLLING]   [SKIP ${index + 1}/${skippedItems.length}] tag=${item.tagId} node=${item.nodeId} topic=${item.mqttTopic}${detailSuffix}`);
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
