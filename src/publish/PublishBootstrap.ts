import { DeviceRegistration, MachineTags, PlcNamespaces, buildFullTopicPath } from '@kuriousdesign/machine-sdk';
import { ClientSession } from 'node-opcua';

import { getDeviceReadItems, getMachineReadItems, ReadItemInfo, validateReadItems } from '../opcua/monitored-items';
import { logBootstrapStep, logReadItemBatch, logValidatedReadItemBatch } from './BootstrapLogHelpers';

export interface PublishBootstrapResult {
    registeredDevices: DeviceRegistration[];
    devicePollingItems: ReadItemInfo[];
    machinePollingItems: ReadItemInfo[];
    allPollingItems: ReadItemInfo[];
}

export async function loadRegisteredDevices(
    registeredDevicesNodeId: string,
    readOpcuaValue: (nodeId: string) => Promise<unknown>,
    deviceMap: Map<number, DeviceRegistration>,
): Promise<DeviceRegistration[]> {
    console.log('[BOOTSTRAP] Retrieving registered devices from OPC UA...');
    console.log(`[BOOTSTRAP] Reading node: ${registeredDevicesNodeId}`);

    const registeredDevices = await readOpcuaValue(registeredDevicesNodeId) as DeviceRegistration[];

    console.log('[BOOTSTRAP] Raw OPC UA registeredDevices count:', registeredDevices?.length ?? 0,
        '| ids:', (registeredDevices || []).map((device) => device.id).join(','));

    const filteredDevices = (registeredDevices || []).filter((device) => device.id !== 0);

    console.log('[BOOTSTRAP] Building device map (clearing previous map)');
    deviceMap.clear();
    filteredDevices.forEach((deviceReg) => {
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
    registeredDevices: DeviceRegistration[],
    deviceMap: Map<number, DeviceRegistration>,
): Promise<PublishBootstrapResult> {
    logBootstrapStep('2/6', 'Building unvalidated device polling tag list');
    const unvalidatedDeviceReadItems = await getDeviceReadItems(registeredDevices, deviceMap);
    logReadItemBatch('Device polling candidates', unvalidatedDeviceReadItems);

    logBootstrapStep('3/6', 'Validating device polling tags against live OPC UA session');
    const devicePollingItems = await validateReadItems(session, unvalidatedDeviceReadItems);
    logValidatedReadItemBatch('Device polling tags', unvalidatedDeviceReadItems, devicePollingItems);

    logBootstrapStep('4/6', 'Building unvalidated machine polling tag list');
    const unvalidatedMachineReadItems = await getMachineReadItems();
    logReadItemBatch('Machine polling candidates', unvalidatedMachineReadItems);

    logBootstrapStep('5/6', 'Validating machine polling tags against live OPC UA session');
    const machinePollingItems = await validateReadItems(session, unvalidatedMachineReadItems);
    logValidatedReadItemBatch('Machine polling tags', unvalidatedMachineReadItems, machinePollingItems);

    const allPollingItems = machinePollingItems.concat(devicePollingItems);
    logBootstrapStep('6/6', `Caching validated polling tags (${allPollingItems.length} total)`);

    return {
        allPollingItems,
        devicePollingItems,
        machinePollingItems,
        registeredDevices,
    };
}

export function getRegisteredDevicesNodeId(concatNodeId: (namespace: string, tag: string) => string): string {
    return concatNodeId(PlcNamespaces.Machine, MachineTags.registeredDevices);
}