import {
    buildFullTopicPath,
    DeviceRegistration,
} from "@kuriousdesign/machine-sdk";
import { ClientSession, ReadValueIdOptions, AttributeIds, StatusCodes } from "node-opcua-client";
import Config from "../shared/config";
import {
    BaseDevicePollingTags,
    BaseMachinePollingTags,
    OptionalDeviceBootstrapTags,
    OptionalDevicePollingTags,
    PlcNamespaces,
    ProjectMachinePollingTags,
} from "./plc-tags";

export interface ReadItemInfo {
    tagId: string;
    nodeId: string;
    mqttTopic: string;
    attributeId: number; //AttributeIds.Value;
    update_period: number;
    last_publish_time: number;
    value: any;
}

export interface ReadItemValidationResult {
    detail: string | null;
    item: ReadItemInfo;
    success: boolean;
}

const DEVICE_STORE_TAG = 'Devices';
const WARNING_ICON = process.stderr.isTTY ? '\x1b[33m⚠\x1b[0m' : '⚠';

function isUnsupportedPollingTag(tagId: string): boolean {
    return false;
}

function makeReadItem(tagId: string, mqttTopic: string, updatePeriod: number = 1): ReadItemInfo {
    return {
        tagId,
        nodeId: Config.NODE_LIST_PREFIX + tagId,
        mqttTopic,
        attributeId: AttributeIds.Value,
        last_publish_time: 0,
        update_period: updatePeriod,
        value: null,
    };
}

function pushReadItem(items: ReadItemInfo[], item: ReadItemInfo): void {
    if (!items.some((existingItem) => existingItem.tagId === item.tagId)) {
        items.push(item);
    }
}

function toTopicSegment(tag: string): string {
    return tag.toLowerCase().replace(/\./g, '/');
}


export async function validateReadItem(session: ClientSession, item: ReadItemInfo, showWarning: boolean = true): Promise<boolean> {
    const result = await validateReadItemDetailed(session, item, showWarning);
    return result.success;
}

export async function validateReadItemDetailed(session: ClientSession, item: ReadItemInfo, showWarning: boolean = true): Promise<ReadItemValidationResult> {
    if (isUnsupportedPollingTag(item.tagId)) {
        const detail = 'Skipped polling for extension object that does not decode reliably in subscriptions';
        if (showWarning) {
            console.warn(`${WARNING_ICON} [OPCUA][VALIDATE] Skipping unsupported polling tag: ${item.tagId} (${detail})`);
        }
        return {
            detail,
            item,
            success: false,
        };
    }

    try {
        const data = await session.read({
            nodeId: item.nodeId,
            attributeId: AttributeIds.Value,
        } as ReadValueIdOptions);

        if (data && data.statusCode && data.statusCode === StatusCodes.Good) {
            return {
                detail: data.statusCode.toString(),
                item,
                success: true,
            };
        } else {
            if (showWarning){
                console.warn(`${WARNING_ICON} [OPCUA][VALIDATE] Invalid tag to read from opcua server: ${item.tagId} (status=${data?.statusCode?.toString()})`);
            }
            return {
                detail: data?.statusCode?.toString() ?? 'unknown status',
                item,
                success: false,
            };
        }
    } catch (err) {
        const errMsg = (err instanceof Error) ? err.message : String(err);
        console.warn(`${WARNING_ICON} [OPCUA][VALIDATE] Read test failed for ${item.tagId}: ${errMsg}. Skipping polling for this node.`);
        return {
            detail: errMsg,
            item,
            success: false,
        };
    }
}

export async function validateReadItemsDetailed(session: ClientSession, items: ReadItemInfo[]): Promise<ReadItemValidationResult[]> {
    return Promise.all(items.map((item) => validateReadItemDetailed(session, item, false)));
}

export async function validateReadItems(session: ClientSession, items: ReadItemInfo[]): Promise<ReadItemInfo[]> {
    const results = await validateReadItemsDetailed(session, items);
    return results
        .map((result) => {
            if (result.success) {
                return result.item;
            }

            const detailSuffix = result.detail ? ` (${result.detail})` : '';
            console.warn(`${WARNING_ICON} [OPCUA][VALIDATE] Skipping invalid/unsupported node for polling: ${result.item.tagId}${detailSuffix}`);
            return null;
        })
        .filter((item): item is ReadItemInfo => item !== null);
}


export function getOptionalDeviceBootstrapReadItems(
    registeredDevices: DeviceRegistration[],
    deviceMap: Map<number, DeviceRegistration>,
    machineId: string,
): ReadItemInfo[] {
    const readItems: ReadItemInfo[] = [];

    registeredDevices.forEach((device) => {
        const deviceTopic = buildFullTopicPath(device, deviceMap);
        Object.entries(OptionalDeviceBootstrapTags(device, machineId)).forEach(([key, tagId]) => {
            pushReadItem(readItems, makeReadItem(tagId, `${deviceTopic}/${key.toLowerCase()}`));
        });
    });

    readItems.forEach((item) => {
        if (Config.SHOW_SUCCESSFUL_TAG_SUBSCRIPTION_LOGS) {
            console.log(`[OPCUA] Added Optional Device Bootstrap Item - TagId: ${item.tagId}, MqttTopic: ${item.mqttTopic}`);
        }
    });

    return readItems;
}

export async function getDeviceReadItems(
    registeredDevices: DeviceRegistration[],
    deviceMap: Map<number, DeviceRegistration>,
    machineId: string,
): Promise<ReadItemInfo[]> {
    const readIteams: ReadItemInfo[] = [];
    registeredDevices.forEach((device) => {
        const deviceTag = `${PlcNamespaces.Machine}.${DEVICE_STORE_TAG}[${device.id}]`;
        const deviceTopic = buildFullTopicPath(device, deviceMap);
        Object.values(BaseDevicePollingTags).forEach((subTag: string) => {
            const tag = `${deviceTag}.${subTag}`;
            const topic = `${deviceTopic}/${toTopicSegment(subTag)}`;
            pushReadItem(readIteams, makeReadItem(tag, topic));
        });

        Object.entries(OptionalDevicePollingTags(device, machineId)).forEach(([key, tag]) => {
            const updatePeriod = key === 'Log' ? 5 : 1;
            const topic = `${deviceTopic}/${key.toLowerCase()}`;
            pushReadItem(readIteams, makeReadItem(tag, topic, updatePeriod));
        });

    });
    if (Config.SHOW_SUCCESSFUL_TAG_SUBSCRIPTION_LOGS) {
        readIteams.map((item) => {
            console.log(`[OPCUA] Added Device Polling Item - TagId: ${item.tagId}, MqttTopic: ${item.mqttTopic}, UpdatePeriod: ${item.update_period}`);
        });
    }
    return readIteams;
}


export function getMachineReadItems(machineId: string): ReadItemInfo[] {
    const itemsToRead: ReadItemInfo[] = [];
    Object.entries(BaseMachinePollingTags).forEach(([key, subTag]) => {
        const tag = `${PlcNamespaces.Machine}.${subTag}`;
        const topic = `${PlcNamespaces.Machine.toLowerCase()}/${key.toLowerCase()}`;
        pushReadItem(itemsToRead, makeReadItem(tag, topic));
    });

    Object.entries(ProjectMachinePollingTags(machineId)).forEach(([key, tag]) => {
        const topic = `${PlcNamespaces.Machine.toLowerCase()}/${key.toLowerCase()}`;
        pushReadItem(itemsToRead, makeReadItem(tag, topic));
    });
    if (Config.SHOW_SUCCESSFUL_TAG_SUBSCRIPTION_LOGS) {
        itemsToRead.map((item) => {
            console.log(`[OPCUA] Added Machine Polling Item - TagId: ${item.tagId}, MqttTopic: ${item.mqttTopic}, UpdatePeriod: ${item.update_period}`);
        });
    }

    return itemsToRead;
}