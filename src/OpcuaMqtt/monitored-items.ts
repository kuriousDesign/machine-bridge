import { buildFullTopicPath, DeviceRegistration, DeviceTags, initialMachine, MachineTags, PlcNamespaces } from "@kuriousdesign/machine-sdk";
import { ClientSession, ReadValueIdOptions, AttributeIds, StatusCodes } from "node-opcua-client";
import { Config } from "../config";

export interface ReadItemInfo {
    tagId: string;
    nodeId: string;
    mqttTopic: string;
    attributeId: number; //AttributeIds.Value;
    update_period: number;
    last_publish_time: number;
    value: any;
}


export async function validateReadItem(session: ClientSession, item: ReadItemInfo, showWarning: boolean = true): Promise<boolean> {
    try {
        const data = await session.read({
            nodeId: item.nodeId,
            attributeId: AttributeIds.Value,
        } as ReadValueIdOptions);

        if (data && data.statusCode && data.statusCode === StatusCodes.Good) {
            // good
            return true;
        } else {
            if (showWarning){
                console.warn(`Invalid tag to read from opcua server: ${item.tagId} (status=${data?.statusCode?.toString()})`);
            }
            return false;
        }
    } catch (err) {
        // If read fails with BadNotSupported or other issue, skip the item gracefully
        const errMsg = (err instanceof Error) ? err.message : String(err);
        console.warn(`Read test failed for ${item.tagId}: ${errMsg}. Skipping monitoring for this node.`);
        return false;
    }
}

export async function validateReadItems(session: ClientSession, items: ReadItemInfo[]): Promise<ReadItemInfo[]> {
    const results = await Promise.all(
        items.map(async (item) => {
            if (await validateReadItem(session, item, false)) {
                return item;
            } else {
                console.warn(`Skipping invalid/unsupported node for monitoring: ${item.tagId}`);
                return null;
            }
        })
    );
    return results.filter((item): item is ReadItemInfo => item !== null);
}


export async function getDeviceReadItems(registeredDevices: DeviceRegistration[], deviceMap: Map<number, DeviceRegistration>): Promise<ReadItemInfo[]> {
    const readIteams: ReadItemInfo[] = [];
    registeredDevices.forEach((device) => {
        const deviceTag = PlcNamespaces.Machine + '.' + MachineTags.deviceStore + '[' + device.id + ']';
        const deviceTopic = buildFullTopicPath(device, deviceMap);
        const USE_WHOLE_DEVICE = false;
        let readItemInfo: ReadItemInfo;
        if (USE_WHOLE_DEVICE) {
            readItemInfo = {
                tagId: deviceTag,
                nodeId: Config.NODE_LIST_PREFIX + deviceTag,
                mqttTopic: deviceTopic,
                last_publish_time: 0,
                update_period: 1,
                value: null,
                attributeId: AttributeIds.Value
            };
            //this.nodeIdToMqttTopicMap.set(deviceNodeId, deviceTopic);
            readIteams.push(readItemInfo);
            //console.log(`[OPCUA] Added device for polling, Tag: ${deviceTag}, Topic: ${deviceTopic}`);
        } else {
            Object.values(DeviceTags).map((subTag: string) => {
                const tag = deviceTag + '.' + subTag;
                const topic = deviceTopic + '/' + subTag.toLowerCase().replace('.', '/');
                readItemInfo = {
                    tagId: tag,
                    nodeId: Config.NODE_LIST_PREFIX + tag,
                    mqttTopic: topic,
                    update_period: 1,
                    last_publish_time: 0,
                    value: null,
                    attributeId: AttributeIds.Value
                };
                // if tag is sts or is or task, set update period to 1

                if ([DeviceTags.Is, DeviceTags.Task].includes(tag)) {
                    readItemInfo.update_period = 1;
                }
                readIteams.push(readItemInfo);
                //console.log(`[OPCUA] Added device tag for polling, tag: ${tag}, Topic: ${topic}, Rate: ${readItemInfo.update_period}`);
            });
        }



        if (false && device.isExternalService) {
            console.log(`[OPCUA] Adding external service device IExtService interface for polling, Device ID: ${device.id}, Mnemonic: ${device.mnemonic}`);
            let nodeId = deviceTag + '.' + DeviceTags.ApiOpcuaPlcReq;
            let topic = deviceTopic + '/' + DeviceTags.ApiOpcuaPlcReq.toLowerCase().replace('.', '/');
            let readItemInfo: ReadItemInfo = {
                tagId: nodeId,
                nodeId: Config.NODE_LIST_PREFIX + nodeId,
                mqttTopic: topic,
                last_publish_time: 0,
                update_period: 1,
                value: null,
                attributeId: AttributeIds.Value
            };
            readIteams.push(readItemInfo);
            // registration
            nodeId = deviceTag + '.' + DeviceTags.Registration;
            topic = deviceTopic + '/' + DeviceTags.Registration.toLowerCase().replace('.', '/');
            readItemInfo = {
                tagId: nodeId,
                nodeId: Config.NODE_LIST_PREFIX + nodeId,
                mqttTopic: topic,
                last_publish_time: 0,
                update_period: 1,
                value: null,
                attributeId: AttributeIds.Value
            };
            readIteams.push(readItemInfo);

        }


        // device log
        const deviceLogTag = PlcNamespaces.Machine + '.' + MachineTags.deviceLogs + '[' + device.id + ']';
        const deviceLogTopic = deviceTopic + '/log';
        readItemInfo = {
            tagId: deviceLogTag,
            nodeId: Config.NODE_LIST_PREFIX + deviceLogTag,
            mqttTopic: deviceLogTopic,
            last_publish_time: 0,
            update_period: 5,
            value: null,
            attributeId: AttributeIds.Value
        };

        readIteams.push(readItemInfo);
        //console.log(`[OPCUA] Added device tag for polling, Tag: ${deviceLogTag}, Topic: ${deviceLogTopic}`);

        // device sts
        const deviceStsTag = PlcNamespaces.Machine + '.' + device.mnemonic.toLowerCase() + 'Sts';
        const deviceStsTopic = deviceTopic + '/sts';
        if (true) {//!(device.mnemonic === 'SYS' || device.mnemonic === 'CON' || device.mnemonic === 'HMI')) {
            readItemInfo = {
                tagId: deviceStsTag,
                nodeId: Config.NODE_LIST_PREFIX + deviceStsTag,
                mqttTopic: deviceStsTopic,
                last_publish_time: 0,
                update_period: 1,
                value: null,
                attributeId: AttributeIds.Value
            };
            readIteams.push(readItemInfo);
            //console.log(`[OPCUA] Added device tag for polling, Tag: ${deviceStsTag}, Topic: ${deviceStsTopic}`);
        }

        const deviceInputsTag = 'inputs' + '.' + device.mnemonic.toLowerCase();
        const deviceInputsTopic = deviceTopic + '/inputs';
        readItemInfo = {
            tagId: deviceInputsTag,
            nodeId: Config.NODE_LIST_PREFIX + deviceInputsTag,
            mqttTopic: deviceInputsTopic,
            last_publish_time: 0,
            update_period: 1,
            value: null,
            attributeId: AttributeIds.Value
        };
        readIteams.push(readItemInfo);
        //console.log(`[OPCUA] Added device tag for polling, Tag: ${deviceInputsTag}, Topic: ${deviceInputsTopic}`);

        const deviceOutputsTag = 'outputs' + '.' + device.mnemonic.toLowerCase();
        const deviceOutputsTopic = deviceTopic + '/outputs';
        readItemInfo = {
            tagId: deviceOutputsTag,
            nodeId: Config.NODE_LIST_PREFIX + deviceOutputsTag,
            mqttTopic: deviceOutputsTopic,
            last_publish_time: 0,
            update_period: 1,
            value: null,
            attributeId: AttributeIds.Value
        };
        readIteams.push(readItemInfo);

    });
    readIteams.map((item) => {
        console.log(`[OPCUA] Added Device Polling Item - TagId: ${item.tagId}, MqttTopic: ${item.mqttTopic}, UpdatePeriod: ${item.update_period}`);
    });
    //this.devicePollingItems = readIteams;
    return readIteams;
}


export const MachineHwTagsApolloTubeLiner00251 = {
    WeidmullerPlcIoRack: 'weidmullerPlcIoRack',
};

export function getMachineHwReadItems(tags: object): ReadItemInfo[] {
    const itemsToRead: ReadItemInfo[] = [];
    const baseTag = 'MachineHw';
    const baseTopic = 'MachineHw'.toLowerCase() + '/';
    Object.entries(tags).forEach(([key, subTag]) => {
        const tag = baseTag + '.' + subTag;
        const topic = baseTopic + subTag.toLowerCase();
        itemsToRead.push({
            tagId: tag,
            nodeId: Config.NODE_LIST_PREFIX + tag,
            mqttTopic: topic,
            last_publish_time: 0,
            update_period: 1,
            value: null,
            attributeId: AttributeIds.Value,
        });
    });
    return itemsToRead;
}


export function getMachineReadItems(): ReadItemInfo[] {
    const itemsToRead: ReadItemInfo[] = [];
    Object.entries(initialMachine).forEach(([key, value]) => {
        const subTag = key;
        const tag = PlcNamespaces.Machine + '.' + subTag;

        const topic = PlcNamespaces.Machine.toLowerCase() + '/' + subTag.toLowerCase();
        itemsToRead.push({
            tagId: tag,
            nodeId: Config.NODE_LIST_PREFIX + tag,
            mqttTopic: topic,
            last_publish_time: 0,
            update_period: 1,
            value: null,
            attributeId: AttributeIds.Value,
        });
    });
    const hwItems = getMachineHwReadItems(MachineHwTagsApolloTubeLiner00251);
    hwItems.forEach((item) => itemsToRead.push(item));
    return itemsToRead;
}


