import { AttributeIds, ClientSession, StatusCodes } from 'node-opcua';
import { DeviceRegistration, DeviceTypes, MachineCfg, buildFullTopicPath } from '@kuriousdesign/machine-sdk';

import { getDeviceReadItems, getMachineReadItems, getOptionalDeviceBootstrapReadItems, ReadItemInfo, ReadItemValidationResult, validateReadItemsDetailed } from '../opcua/polling-items';
import { BaseMachineBootstrapTags, PlcNamespaces } from '../opcua/plc-tags';
import Config from '../shared/config';
import { logBootstrapReadItemBatch, logBootstrapStep, logPollingReadItemBatch, logValidatedPollingItemBatch } from './BootstrapLogHelpers';

const WARNING_ICON = process.stderr.isTTY ? '\x1b[33m⚠\x1b[0m' : '⚠';

function createBootstrapReadItem(nodeId: string, mqttTopic: string): ReadItemInfo {
    return {
        tagId: nodeId.replace(/^.*\.Application\./, ''),
        nodeId,
        mqttTopic,
        attributeId: AttributeIds.Value,
        last_publish_time: 0,
        update_period: 1,
        value: null,
    };
}

export interface PublishBootstrapResult {
    availableOptionalDeviceBootstrapItems: ReadItemInfo[];
    devicePollingValidationResults: ReadItemValidationResult[];
    machineCfg: MachineCfg;
    machinePollingValidationResults: ReadItemValidationResult[];
    optionalDeviceBootstrapAvailabilityResults: ReadItemValidationResult[];
    registeredDevices: DeviceRegistration[];
    devicePollingItems: ReadItemInfo[];
    machinePollingItems: ReadItemInfo[];
    allPollingItems: ReadItemInfo[];
    optionalDeviceBootstrapItems: ReadItemInfo[];
}

export async function loadMachineCfg(
    machineCfgNodeId: string,
    readOpcuaValue: (nodeId: string) => Promise<unknown>,
): Promise<MachineCfg> {
    console.log('[BOOTSTRAP] Retrieving machine cfg from OPC UA...');
    logBootstrapReadItemBatch('Base machine bootstrap reads', [
        createBootstrapReadItem(machineCfgNodeId, 'machine/cfg'),
    ]);

    const machineCfg = await readOpcuaValue(machineCfgNodeId) as MachineCfg | null;

    if (!machineCfg) {
        throw new Error(`Machine cfg at ${machineCfgNodeId} was not available`);
    }

    const machineId = machineCfg.machineId?.trim();

    if (!machineId) {
        throw new Error(`Machine cfg at ${machineCfgNodeId} did not provide a valid machineId`);
    }

    console.log(`[BOOTSTRAP] Machine cfg loaded for machineId=${machineId}`);
    return machineCfg;
}

export async function loadRegisteredDevices(
    registeredDevicesNodeId: string,
    readOpcuaValue: (nodeId: string) => Promise<unknown>,
    deviceMap: Map<number, DeviceRegistration>,
): Promise<DeviceRegistration[]> {
    console.log('[BOOTSTRAP] Retrieving registered devices from OPC UA...');
    logBootstrapReadItemBatch('Base machine bootstrap reads', [
        createBootstrapReadItem(registeredDevicesNodeId, 'machine/registereddevices'),
    ]);

    const registeredDevices = await readOpcuaValue(registeredDevicesNodeId) as DeviceRegistration[];

    console.log('[BOOTSTRAP] Raw OPC UA registeredDevices count:', registeredDevices?.length ?? 0,
        '| ids:', (registeredDevices || []).map((device) => device.id).join(','));

    const filteredDevices = (registeredDevices || []).filter((device) => device.id !== 0);

    console.log('[BOOTSTRAP] Building device map (clearing previous map)');
    deviceMap.clear();
    filteredDevices.forEach((deviceReg) => {
        deviceReg.isExternalService = deviceReg.isExternalService || deviceReg.deviceType === DeviceTypes.ExtService;
        const topicPath = buildFullTopicPath(deviceReg, deviceMap);
        const devicePath = topicPath.split('/');
        deviceReg.devicePath = devicePath;
        deviceMap.set(deviceReg.id, deviceReg);
        console.log(`[BOOTSTRAP] Device map entry: id=${deviceReg.id} mnemonic=${deviceReg.mnemonic} parentId=${deviceReg.parentId} topic=${topicPath}`);
    });

    console.log('[BOOTSTRAP] Registered devices ready, count:', filteredDevices.length,
        '| deviceMap ids:', Array.from(deviceMap.keys()).sort((left, right) => left - right).join(','));

    return filteredDevices;
}

export async function buildValidatedPollingItems(
    session: ClientSession,
    machineCfg: MachineCfg,
    registeredDevices: DeviceRegistration[],
    deviceMap: Map<number, DeviceRegistration>,
): Promise<PublishBootstrapResult> {
    const optionalDeviceBootstrapItems = getOptionalDeviceBootstrapReadItems(
        registeredDevices,
        deviceMap,
        machineCfg.machineId,
    );
    const availableOptionalDeviceBootstrapItems: ReadItemInfo[] = [];
    const optionalDeviceBootstrapAvailabilityResults: ReadItemValidationResult[] = [];
    logBootstrapStep('3/8', `Reading optional device bootstrap tags for machineId=${machineCfg.machineId}`);
    logBootstrapReadItemBatch('Optional device bootstrap candidates', optionalDeviceBootstrapItems);

    for (const item of optionalDeviceBootstrapItems) {
        try {
            const data = await session.read({
                nodeId: item.nodeId,
                attributeId: AttributeIds.DataType,
            });

            if (data.statusCode === StatusCodes.Good) {
                availableOptionalDeviceBootstrapItems.push(item);
                optionalDeviceBootstrapAvailabilityResults.push({
                    detail: data.statusCode.toString(),
                    item,
                    success: true,
                });
                if (Config.SHOW_SUCCESSFUL_TAG_SUBSCRIPTION_LOGS) {
                    console.log(`[BOOTSTRAP] Optional device bootstrap tag available: ${item.tagId}`);
                }
            } else {
                optionalDeviceBootstrapAvailabilityResults.push({
                    detail: data.statusCode.toString(),
                    item,
                    success: false,
                });
                console.warn(`${WARNING_ICON} [BOOTSTRAP] Optional device bootstrap tag unavailable: ${item.tagId} (status=${data.statusCode.toString()})`);
            }
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            optionalDeviceBootstrapAvailabilityResults.push({
                detail: errorMessage,
                item,
                success: false,
            });
            console.warn(`${WARNING_ICON} [BOOTSTRAP] Optional device bootstrap tag read failed: ${item.tagId} (${errorMessage})`);
        }
    }

    logBootstrapReadItemBatch('Optional device bootstrap tags available', availableOptionalDeviceBootstrapItems);

    logBootstrapStep('4/8', `Building unvalidated polling tag list for machineId=${machineCfg.machineId}`);
    const unvalidatedDeviceReadItems = await getDeviceReadItems(registeredDevices, deviceMap, machineCfg.machineId);
    logPollingReadItemBatch('Device polling candidates', unvalidatedDeviceReadItems);

    logBootstrapStep('5/8', 'Validating device polling tags against live OPC UA session');
    const devicePollingValidationResults = await validateReadItemsDetailed(session, unvalidatedDeviceReadItems);
    const devicePollingItems = devicePollingValidationResults.filter((result) => result.success).map((result) => result.item);
    logValidatedPollingItemBatch(
        'Device polling tags',
        unvalidatedDeviceReadItems,
        devicePollingItems,
        new Map(devicePollingValidationResults.map((result) => [result.item.tagId, result.detail])),
    );

    logBootstrapStep('6/8', 'Building unvalidated machine polling tag list');
    const unvalidatedMachineReadItems = await getMachineReadItems(machineCfg.machineId);
    logPollingReadItemBatch('Machine polling candidates', unvalidatedMachineReadItems);

    logBootstrapStep('7/8', 'Validating machine polling tags against live OPC UA session');
    const machinePollingValidationResults = await validateReadItemsDetailed(session, unvalidatedMachineReadItems);
    const machinePollingItems = machinePollingValidationResults.filter((result) => result.success).map((result) => result.item);
    logValidatedPollingItemBatch(
        'Machine polling tags',
        unvalidatedMachineReadItems,
        machinePollingItems,
        new Map(machinePollingValidationResults.map((result) => [result.item.tagId, result.detail])),
    );

    const allPollingItems = machinePollingItems.concat(devicePollingItems);
    logBootstrapStep('8/8', `Caching validated polling tags (${allPollingItems.length} total)`);

    return {
        allPollingItems,
        availableOptionalDeviceBootstrapItems,
        devicePollingValidationResults,
        devicePollingItems,
        machineCfg,
        machinePollingValidationResults,
        machinePollingItems,
        optionalDeviceBootstrapAvailabilityResults,
        optionalDeviceBootstrapItems,
        registeredDevices,
    };
}

export function getMachineCfgNodeId(concatNodeId: (namespace: string, tag: string) => string): string {
    return concatNodeId(PlcNamespaces.Machine, BaseMachineBootstrapTags.cfg);
}

export function getRegisteredDevicesNodeId(concatNodeId: (namespace: string, tag: string) => string): string {
    return concatNodeId(PlcNamespaces.Machine, BaseMachineBootstrapTags.registeredDevices);
}