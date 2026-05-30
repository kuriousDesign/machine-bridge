import type { DeviceRegistration } from '@kuriousdesign/machine-sdk';

export const PlcNamespaces = {
    Machine: 'Machine',
} as const;

export const BaseMachineBootstrapTags = {
    cfg: 'Cfg',
    registeredDevices: 'RegisteredDevices',
} as const;

export const BaseMachinePollingTags = {
    heartbeatPLC: 'HeartbeatPLC',
    heartbeatHMI: 'HeartbeatHMI',
    machineLog: 'MachineLog',
    errors: 'Errors',
    warnings: 'Warnings',
    estopCircuit_OK: 'EstopCircuit_OK',
    estopCircuitDelayed_OK: 'EstopCircuitDelayed_OK',
    fenceCircuit_OK: 'FenceCircuit_OK',
    guardDoors_LOCKED: 'GuardDoors_LOCKED',
    manualMode: 'ManualMode',
    networkHealth_OK: 'NetworkHealth_OK',
    ethercatMaster_OK: 'EthercatMaster_OK',
    ethercatSlaves_OK: 'EthercatSlaves_OK',
    currentTimeMs: 'CurrentTimeMs',
    settings: 'Settings',
    user: 'User',
} as const;

export function getProjectMachineTag(projectId: string): string {
    return `Machine_${projectId}`;
}

export const ProjectMachinePollingTags = (projectId: string) => ({
    TaskQueue: `${getProjectMachineTag(projectId)}.TaskQueue`,
    RecipeStore: `${getProjectMachineTag(projectId)}.RecipeStore`,
    ActiveRecipe: `${getProjectMachineTag(projectId)}.ActiveRecipe`,
    Job: `${getProjectMachineTag(projectId)}.Job`,
    PdmSts: `${getProjectMachineTag(projectId)}.PdmSts`,
});

export const BaseDevicePollingTags = {
    Is: 'Is',
    Errors: 'Errors',
    Warnings: 'Warnings',
    ExecMethod: 'ExecMethod',
    Task: 'Task',
    Process: 'Process',
    Script: 'Script',
    MutedChildrenArray: 'MutedChildrenArray',
    ApiOpcuaHmiReq: 'ApiOpcua.HmiReq',
    ApiOpcuaHmiResp: 'ApiOpcua.HmiResp',
    ApiOpcuaPlcReq: 'ApiOpcua.InternalReq',
    ApiOpcuaPlcResp: 'ApiOpcua.InternalResp',
} as const;

export const BaseDeviceBootstrapTags = {
    Cfg: 'Cfg',
    Registration: 'Registration',
} as const;

export const OptionalDeviceBootstrapTags = (
    deviceRegistration: DeviceRegistration,
    projectId: string,
) => ({
    Cfg: `${getProjectMachineTag(projectId)}.${deviceRegistration.mnemonic}Cfg`,
});

export const OptionalDevicePollingTags = (
    deviceRegistration: DeviceRegistration,
    projectId: string,
) => ({
    Log: `Machine.DeviceLogs[${deviceRegistration.id}]`,
    Sts: `${getProjectMachineTag(projectId)}.${deviceRegistration.mnemonic}Sts`,
    Meta: `${getProjectMachineTag(projectId)}.${deviceRegistration.mnemonic}Meta`,
    Inputs: `${getProjectMachineTag(projectId)}.inputs.${deviceRegistration.mnemonic}`,
    Outputs: `${getProjectMachineTag(projectId)}.outputs.${deviceRegistration.mnemonic}`,
});