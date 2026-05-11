import { ReadItemInfo } from '../opcua/monitored-items';

export function logBootstrapStep(step: string, description: string): void {
    console.log(`[BOOTSTRAP] ${step} ${description}`);
}

export function logReadItemBatch(label: string, items: ReadItemInfo[]): void {
    console.log(`[BOOTSTRAP] ${label}: ${items.length} tag(s)`);
    items.forEach((item, index) => {
        console.log(`[BOOTSTRAP]   [${index + 1}/${items.length}] tag=${item.tagId} node=${item.nodeId} topic=${item.mqttTopic}`);
    });
}

export function logValidatedReadItemBatch(label: string, requestedItems: ReadItemInfo[], validatedItems: ReadItemInfo[]): void {
    const validatedTagIds = new Set(validatedItems.map((item) => item.tagId));
    const skippedItems = requestedItems.filter((item) => !validatedTagIds.has(item.tagId));

    console.log(`[BOOTSTRAP] ${label}: validated ${validatedItems.length}/${requestedItems.length}`);
    validatedItems.forEach((item, index) => {
        console.log(`[BOOTSTRAP]   [OK ${index + 1}/${validatedItems.length}] tag=${item.tagId} node=${item.nodeId} topic=${item.mqttTopic}`);
    });

    if (skippedItems.length > 0) {
        console.warn(`[BOOTSTRAP] ${label}: skipped ${skippedItems.length} invalid tag(s)`);
        skippedItems.forEach((item, index) => {
            console.warn(`[BOOTSTRAP]   [SKIP ${index + 1}/${skippedItems.length}] tag=${item.tagId} node=${item.nodeId} topic=${item.mqttTopic}`);
        });
    }
}
