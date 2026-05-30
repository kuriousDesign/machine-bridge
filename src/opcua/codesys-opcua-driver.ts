import { ClientSession, Variant, AttributeIds, DataType, VariantArrayType, ReadValueIdOptions, StatusCodes, DataValue } from "node-opcua";
import { ActionTypes, initialApiOpcuaReqData, DeviceCmds, States, ApiOpcuaReqData, DeviceActionRequestData, ApiReqRespStates, AxisProcesses, DeviceConstants, actionTypeToString, apiReqRespStateToString, deviceIdToString, Device, initialDevice, initialDeviceActionRequestData } from "@kuriousdesign/machine-sdk";
import { read, write } from "fs";
import { BaseMachineBootstrapTags, BaseMachinePollingTags, getProjectMachineTag, PlcNamespaces } from "./plc-tags";
import { writeExtensionObject } from "./opcua-helpers";

// Debug: Log the imported ApiReqRespStates to verify its structure
//console.log('ApiReqRespStates:', ApiReqRespStates);

type NestedWriteValue = {
    tag: string;
    value: any;
};

type RuntimeWritePlanItem = {
    tag: string;
    dataType: DataType;
    value: any;
};

type EnumDefinitionLike = {
    fields?: Array<{ value?: unknown }>;
};

type ActionRequestLogContext = {
    senderId?: number;
    senderLabel?: string;
    targetLabel?: string;
};


export default class CodesysOpcuaDriver {
    private id: number;
    private session: ClientSession;
    private nodePrefix: string;
    private apiReqTag: string = "apiOpcua.hmiReq";
    private apiRespTag: string = "apiOpcua.hmiResp";
    private request: ApiOpcuaReqData;
    private response: ApiOpcuaReqData;
    //private machineStatus: Machine;
    private logMsg: string = "";
    private lastLogTimeStamp: number = 0;
    private lastLogMsgId: number = 0;
    private lastReadLogIndex: number = 255;
    private devicesNodeId = `${PlcNamespaces.Machine}.Devices`;
    private machineId: string | null = null;
    private loggedTagNormalizations = new Set<string>();
    private uniqueActionRequestCtr: number = 0;
    private cachedTagDataTypes = new Map<string, DataType>();
    private baseMachineRootSegments = new Map<string, string>();
    private projectMachineRootSegments = new Map<string, string>();

    private static readonly projectSpecificNestedSegmentAliases = new Map<string, Map<string, string>>([
        ['Job', new Map<string, string>([
            ['activebatchnumber', 'ActiveBatchNumber'],
            ['activerecipeindex', 'ActiveRecipeIndex'],
            ['assemblyname', 'AssemblyName'],
            ['assemblynumber', 'AssemblyNumber'],
            ['batchid', 'BatchId'],
            ['goodcnt', 'GoodCnt'],
            ['jobcompleted', 'JobCompleted'],
            ['jobendtime', 'JobEndTime'],
            ['jobname', 'JobName'],
            ['jobstarttime', 'JobStartTime'],
            ['lotid', 'LotId'],
            ['lotqty', 'LotQty'],
            ['operationnumber', 'OperationNumber'],
            ['operatorid', 'OperatorId'],
            ['salesorderid', 'SalesOrderId'],
            ['scrapcnt', 'ScrapCnt'],
            ['setupcompleted', 'SetupCompleted'],
            ['setupendtime', 'SetupEndTime'],
            ['setupstarttime', 'SetupStartTime'],
            ['tubetypestring', 'TubeTypeString'],
            ['workinstruction', 'WorkInstruction'],
            ['workorderid', 'WorkOrderId'],
        ])],
        ['PdmSts', new Map<string, string>([
            ['activecnt', 'ActiveCnt'],
            ['allfixturesareempty', 'AllFixturesAreEmpty'],
            ['allstationsareempty', 'AllStationsAreEmpty'],
            ['batchcntflag', 'BatchCntFlag'],
            ['doneshelfisempty', 'DoneShelfIsEmpty'],
            ['doneshelfisfull', 'DoneShelfIsFull'],
            ['doneshelfspacesleftcnt', 'DoneShelfSpacesLeftCnt'],
            ['finishedcnt', 'FinishedCnt'],
            ['fixturelocationwhenloaded', 'FixtureLocationWhenLoaded'],
            ['infixture', 'InFixture'],
            ['loadedbadsensor', 'LoadedBadSensor'],
            ['oneormorerejectpartsincell', 'OneOrMoreRejectPartsInCell'],
            ['oneormorerejectpartsinrobot', 'OneOrMoreRejectPartsInRobot'],
            ['parts', 'Parts'],
            ['processsts', 'ProcessSts'],
            ['validation', 'Validation'],
            ['linerweight_g', 'LinerWeight_g'],
            ['postweight_g', 'PostWeight_g'],
            ['preweight_g', 'PreWeight_g'],
            ['serialnumber', 'SerialNumber'],
            ['statusmsg', 'StatusMsg'],
            ['timestampvision_sec', 'TimestampVision_sec'],
            ['visionsts', 'VisionSts'],
            ['weightsts', 'WeightSts'],
        ])],
    ]);

    public setMachineId(machineId: string | null): void {
        const normalizedMachineId = machineId?.trim() || null;
        if (this.machineId === normalizedMachineId) {
            return;
        }

        this.machineId = normalizedMachineId;
        this.cachedNestedTagDataTypesMap.clear();
        this.cachedTagDataTypes.clear();
        this.loggedTagNormalizations.clear();

        if (this.machineId) {
            console.log(`[OPCUA] Using machineId-aware write tag resolution for ${this.machineId}`);
        }
    }

    public setKnownMachineTagRoots(tagIds: string[]): void {
        this.baseMachineRootSegments.clear();
        this.projectMachineRootSegments.clear();

        const projectPrefix = this.machineId ? `${getProjectMachineTag(this.machineId)}.` : null;

        for (const tagId of tagIds) {
            if (tagId.startsWith(`${PlcNamespaces.Machine}.`)) {
                const rootSegment = tagId.slice(`${PlcNamespaces.Machine}.`.length).split(/[.[]/, 1)[0];
                if (rootSegment) {
                    this.baseMachineRootSegments.set(rootSegment.toLowerCase(), rootSegment);
                }
                continue;
            }

            if (projectPrefix && tagId.startsWith(projectPrefix)) {
                const rootSegment = tagId.slice(projectPrefix.length).split(/[.[]/, 1)[0];
                if (rootSegment) {
                    this.projectMachineRootSegments.set(rootSegment.toLowerCase(), rootSegment);
                }
            }
        }
    }

    private isOpcuaConnectionClosedError(message: string): boolean {
        const normalizedMessage = message.toLowerCase();

        return normalizedMessage.includes("badconnectionclosed")
            || normalizedMessage.includes("invalid channel")
            || normalizedMessage.includes("session has been closed")
            || normalizedMessage.includes("socket has been closed")
            || normalizedMessage.includes("transaction has been canceled");
    }

    constructor(id: number, session: ClientSession, opcuaControllerName: string = "CODESYS Control for Linux SL") {
        this.id = id;
        this.session = session;
        this.nodePrefix = `ns=4;s=|var|${opcuaControllerName}.Application.`;
        this.request = initialApiOpcuaReqData;
        console.log(`CodesysOpcuaDriver initialized for device ID ${this.id} with controller ${opcuaControllerName}`);
        this.response = { ...this.request };
    }

    private addNodePrefix(tag: string): string {
        return `${this.nodePrefix}${tag}`;
    }

    private normalizeProjectSpecificLeafSegments(rootSegment: string, remainder: string): string {
        const aliasMap = CodesysOpcuaDriver.projectSpecificNestedSegmentAliases.get(rootSegment);
        if (!aliasMap || !remainder) {
            return remainder;
        }

        return remainder.replace(/(^|\.|\[)([A-Za-z_][A-Za-z0-9_]*)(?=\.|\[|$)/g, (match, prefix: string, segment: string) => {
            const aliasedSegment = aliasMap.get(segment.toLowerCase());
            if (!aliasedSegment) {
                return match;
            }

            return `${prefix}${aliasedSegment}`;
        });
    }

    private normalizeProjectSpecificMachineTag(tag: string): string {
        const normalizedInputTag = tag.startsWith('machine.')
            ? `${PlcNamespaces.Machine}.${tag.slice('machine.'.length)}`
            : tag;

        if (!this.machineId || !normalizedInputTag.startsWith(`${PlcNamespaces.Machine}.`)) {
            return normalizedInputTag;
        }

        const remainder = normalizedInputTag.slice(`${PlcNamespaces.Machine}.`.length);
        const [rawFirstSegment = ''] = remainder.split(/[.[]/, 1);
        const firstSegmentKey = rawFirstSegment.toLowerCase();
        const canonicalBaseRoot = this.baseMachineRootSegments.get(firstSegmentKey);
        const canonicalProjectRoot = this.projectMachineRootSegments.get(firstSegmentKey);
        const tail = remainder.slice(rawFirstSegment.length);

        let normalizedTag = normalizedInputTag;
        if (canonicalBaseRoot) {
            normalizedTag = `${PlcNamespaces.Machine}.${canonicalBaseRoot}${tail}`;
        } else if (canonicalProjectRoot) {
            const normalizedTail = this.normalizeProjectSpecificLeafSegments(canonicalProjectRoot, tail);
            normalizedTag = `${getProjectMachineTag(this.machineId)}.${canonicalProjectRoot}${normalizedTail}`;
        }

        if (normalizedTag !== normalizedInputTag && !this.loggedTagNormalizations.has(tag)) {
            this.loggedTagNormalizations.add(tag);
            console.log(`[OPCUA] Resolved project-specific tag ${tag} -> ${normalizedTag}`);
        }
        return normalizedTag;
    }

    private getDeviceNodeId(deviceId: number): string {
        return `${this.devicesNodeId}[${deviceId}]`;
    }

    private decipherOpcuaValue(data: any): any {
        const decipheredValue =
            data.value.arrayType === VariantArrayType.Array
                ? Array.from(data.toJSON().value.value)
                : (data.toJSON().value.value);

        return decipheredValue;
    }

    private async readOpcuaValue(tag: string): Promise<any> {
        const nodeId = this.addNodePrefix(tag);
        if (!this.session) {
            throw new Error("OPC UA session is not initialized");
        }

        try {
            const readValueOptions: ReadValueIdOptions = {
                nodeId: nodeId,
                attributeId: AttributeIds.Value,
            }
            const data = await this.session.read(readValueOptions);
            const value = this.decipherOpcuaValue(data);
            //console.log(`Deciphered OPC UA value from ${nodeId}:`, value);
            if (data.statusCode === StatusCodes.Good) {
                return value;
            } else {
                console.warn(`Failed to read OPC UA value from ${tag}: ${data.statusCode}`);
                return null;
            }
        } catch (error) {
            console.error(`Failed to read OPC UA value from ${nodeId}:`, error);
            throw error;
        }
    }

    public async writeTagV2(tag: string, value: any, dataType?: DataType): Promise<void> {
        if (!this.session) {
            throw new Error("OPC UA session is not initialized");
        }
        tag = this.normalizeProjectSpecificMachineTag(tag);
        let dType: DataType | null = null;
        if (dataType === undefined) {
            dType = await this.readTagDataType(tag);
            console.log(`Determined data type for tag ${tag}: ${dType}`);
        } else {
            dType = dataType;
        }
        if (dType === null) {
            throw new Error(`Cannot determine data type for tag ${tag}`);
        } else if (true || dType === DataType.ExtensionObject) {
            console.log('writing extension object tag');
            writeExtensionObject(this.session, this.addNodePrefix(tag), value);
        } else {
            console.log('writing simple tag');
            //this.writeTag(tag, value, dType);
        }
    }


    public async readTagDataType(tag: string): Promise<DataType | null> {
        if (!this.session) {
            console.error('OPC UA session is not initialized');
            return null;
        }
        try {
            tag = this.normalizeProjectSpecificMachineTag(tag);
            const nodeId = this.addNodePrefix(tag);
            const readValueOptions: ReadValueIdOptions = {
                nodeId: nodeId,
                attributeId: AttributeIds.DataType
            };
            const dataValue: DataValue = await this.session.read(readValueOptions);

            if (dataValue.statusCode === StatusCodes.Good) {
                const rawDataType = dataValue.value.value;

                if (typeof rawDataType === 'number') {
                    if (rawDataType === 3013) {
                        return DataType.String;
                    }

                    return rawDataType as DataType;
                }

                const nodeIdValue = rawDataType as { namespace?: number; value?: unknown; toString?: () => string } | null;
                const rawNodeValue = nodeIdValue?.value;

                if (typeof rawNodeValue === 'number') {
                    if (nodeIdValue?.namespace === 0) {
                        return rawNodeValue as DataType;
                    }

                    if (rawNodeValue === 3013) {
                        return DataType.String;
                    }
                }

                const dataTypeNodeId = rawDataType && typeof (rawDataType as { toString?: () => string }).toString === 'function'
                    ? (rawDataType as { toString: () => string }).toString()
                    : null;

                if (dataTypeNodeId) {
                    const definitionValue = await this.session.read({
                        nodeId: dataTypeNodeId,
                        attributeId: AttributeIds.DataTypeDefinition,
                    });

                    if (definitionValue.statusCode === StatusCodes.Good) {
                        const definition = definitionValue.value.value as EnumDefinitionLike | null;
                        if (definition && Array.isArray(definition.fields) && definition.fields.every((field) => field && typeof field === 'object' && 'value' in field)) {
                            return DataType.Int32;
                        }
                    }
                }

                const valueTypeValue = await this.session.read({
                    nodeId,
                    attributeId: AttributeIds.Value,
                });

                if (valueTypeValue.statusCode === StatusCodes.Good && valueTypeValue.value) {
                    const variantDataType = valueTypeValue.value.dataType;
                    if (variantDataType !== DataType.Null) {
                        console.debug(`[OPCUA] Resolved writable data type for ${tag} via live Value variant: ${DataType[variantDataType]} (${variantDataType})`);
                        return variantDataType;
                    }
                }

                console.warn(`Unsupported OPC UA data type for ${tag}: ${dataTypeNodeId ?? String(rawDataType)}`);
                return null;
            } else {
                console.warn(`Failed to read OPC UA data type from ${tag}: ${dataValue.statusCode}`);
                return null;
            }
        } catch (error) {
            console.error(`Failed to read OPC UA data type from ${tag}:`, error);
            return null;
        }
    }

    async readTag(tag: string, dataType: DataType = DataType.Int16): Promise<any> {
        if (!this.session) {
            console.error('OPC UA session is not initialized');
            return null;
        }
        //console.log(`Reading tag ${tag} with dataType ${DataType[dataType]}`);
        try {
            tag = this.normalizeProjectSpecificMachineTag(tag);
            const nodeId = this.addNodePrefix(tag);
            const readValueOptions: ReadValueIdOptions = {
                nodeId: nodeId,
                attributeId: AttributeIds.Value
            };
            const dataValue: DataValue = await this.session.read(readValueOptions);
            const dataType = dataValue.value.dataType

            if (!dataValue || !dataValue.value) {
                console.warn(`No value returned for node ${tag}`);
                return null;
            }

            // Handle ULINT (UInt64) values
            if (dataType === DataType.UInt64) {
                const variant = dataValue.value;
                //console.log(`Read UInt64 variant for node ${tag}:`, variant);

                // Handle case where ULINT is returned as array of two 32-bit UInt32 values [high, low]
                if (Array.isArray(variant.value) && variant.value.length === 2) {
                    const [high, low] = variant.value; // Adjusted to [high, low] order
                    if (typeof high === 'number' && typeof low === 'number' &&
                        high >= 0 && low >= 0 && high <= 0xFFFFFFFF && low <= 0xFFFFFFFF) {
                        return Number(BigInt(high) * BigInt(0x100000000) + BigInt(low));
                    } else {
                        console.error(`Invalid ULINT array format for node ${tag}:`, variant.value);
                        return null;
                    }
                }
                console.error(`Unexpected UInt64 format for node ${tag}:`, variant.value);
                return null;
            }

            // Return the value based on the specified dataType
            const value = dataValue.value.value;
            if (value === null || value === undefined) {
                console.warn(`Null or undefined value for node ${tag}`);
                return null;
            }
            //console.log(`Read value for node ${tag}:`, value);
            return value as number; // Cast to number for non-UInt64 types
        } catch (error) {
            console.error(`Failed to read node ${tag}:`, error);
            return null;
        }
    }

    async writeDeviceDataToPlc(deviceId: number, data: Partial<Device>): Promise<{ success: boolean; message: string; details?: any }> {
        const baseTag = this.getDeviceNodeId(deviceId);
        return await this.writeNestedObject(baseTag, data);
    }

    // 1. Flatten nested object/array → list of { nodeId, value }
    private traverseAndFlatten = (currentTag: string, currentValue: any, writeItems: Array<{ nodeId: string; value: any, dataType: any }>) => {
        if (currentValue === null || typeof currentValue !== "object") {
            const writeItem = { nodeId: currentTag, value: currentValue, dataType: null };
            writeItems.push(writeItem);
            return;
        }

        if (Array.isArray(currentValue)) {
            for (let i = 0; i < currentValue.length; i++) {
                this.traverseAndFlatten(`${currentTag}[${i}]`, currentValue[i], writeItems);
            }
        } else {
            for (const key in currentValue) {
                if (Object.prototype.hasOwnProperty.call(currentValue, key)) {
                    this.traverseAndFlatten(`${currentTag}.${key}`, currentValue[key], writeItems);
                }
            }
        }
    };

    private cachedNestedTagDataTypesMap: Map<string, any> = new Map();

    private buildFlattenedWriteItems(baseTag: string, value: any): Array<{ nodeId: string; value: any; dataType: DataType | null }> {
        const normalizedBaseTag = this.normalizeProjectSpecificMachineTag(baseTag);
        const writeItems: Array<{ nodeId: string; value: any; dataType: DataType | null }> = [];
        this.traverseAndFlatten(normalizedBaseTag, value, writeItems);
        return writeItems;
    }

    private async getCachedTagDataType(tag: string): Promise<DataType | null> {
        const normalizedTag = this.normalizeProjectSpecificMachineTag(tag);
        const cachedType = this.cachedTagDataTypes.get(normalizedTag);
        if (cachedType !== undefined) {
            return cachedType;
        }

        const dataType = await this.readTagDataType(normalizedTag);
        if (dataType !== null) {
            this.cachedTagDataTypes.set(normalizedTag, dataType);
            console.debug(`[OPCUA] Cached writable data type for ${normalizedTag}: ${DataType[dataType]} (${dataType})`);
        }

        return dataType;
    }

    private async buildRuntimeWritePlan(writeValues: NestedWriteValue[]): Promise<RuntimeWritePlanItem[]> {
        const mergedWriteValues = new Map<string, any>();

        for (const writeValue of writeValues) {
            const flattenedWriteItems = this.buildFlattenedWriteItems(writeValue.tag, writeValue.value);
            for (const flattenedWriteItem of flattenedWriteItems) {
                mergedWriteValues.set(flattenedWriteItem.nodeId, flattenedWriteItem.value);
            }
        }

        if (mergedWriteValues.size === 0) {
            return [];
        }

        const runtimeWritePlan: RuntimeWritePlanItem[] = [];
        for (const [tag, value] of mergedWriteValues.entries()) {
            const dataType = await this.getCachedTagDataType(tag);
            if (dataType === null) {
                console.warn(`Skipping runtime write plan item for ${tag} because its OPC UA data type is unavailable.`);
                continue;
            }

            runtimeWritePlan.push({
                tag,
                dataType,
                value,
            });
        }

        return runtimeWritePlan;
    }

    private async executeRuntimeWritePlan(runtimeWritePlan: RuntimeWritePlanItem[], skipValidation: boolean): Promise<{ success: boolean; message: string; details?: any }> {
        if (runtimeWritePlan.length === 0) {
            return { success: false, message: "No cached data types to write" };
        }

        try {
            const writeResults: Array<{ nodeId: string; success: boolean; error?: string }> = [];

            for (const runtimeWritePlanItem of runtimeWritePlan) {
                const result = await this.writeTag(
                    runtimeWritePlanItem.tag,
                    runtimeWritePlanItem.value,
                    runtimeWritePlanItem.dataType,
                    skipValidation,
                );

                if (!result.success) {
                    writeResults.push({
                        nodeId: runtimeWritePlanItem.tag,
                        success: false,
                        error: result.message,
                    });

                    if (this.isOpcuaConnectionClosedError(result.message)) {
                        console.warn(`Stopping runtime write plan because the OPC UA connection is unavailable at ${runtimeWritePlanItem.tag}.`);
                        break;
                    }

                    continue;
                }

                writeResults.push({ nodeId: runtimeWritePlanItem.tag, success: true });
            }

            const successCount = writeResults.filter((result) => result.success).length;
            const failed = writeResults.filter((result) => !result.success);
            const message = `Wrote ${successCount}/${writeResults.length} tags`;
            const fullSuccess = failed.length === 0;

            if (!fullSuccess) {
                console.warn("Some writes failed:", failed);
            }

            return {
                success: fullSuccess,
                message,
                details: {
                    total: writeResults.length,
                    success: successCount,
                    failed: failed.length,
                    errors: failed,
                },
            };
        } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            console.error("Runtime write plan execution failed:", msg);
            return { success: false, message: `Critical failure: ${msg}` };
        }
    }

    async createAndCacheNestedTagDataTypes(
        baseTag: string,
        value: any
    ): Promise<any> {
        baseTag = this.normalizeProjectSpecificMachineTag(baseTag);
        const writeItems = this.buildFlattenedWriteItems(baseTag, value);
        const dataTypePromises = writeItems.map(item =>
            this.getCachedTagDataType(item.nodeId).then(dataType => {
            item.dataType = dataType;
            })
        );
        await Promise.all(dataTypePromises);
        this.cachedNestedTagDataTypesMap.set(baseTag, writeItems);
        console.log(`Cached writes for nested tag ${baseTag}`);
        return writeItems;
    }

    async writeTagList(writeValues: NestedWriteValue[], skipValidation: boolean = false): Promise<{ success: boolean; message: string; details?: any }> {
        if (writeValues.length === 0) {
            return { success: false, message: "No values to write" };
        }

        const runtimeWritePlan = await this.buildRuntimeWritePlan(writeValues);
        return this.executeRuntimeWritePlan(runtimeWritePlan, skipValidation);
    }

    async writeNestedObject(baseTag: string, value: any, skipValidation: boolean = false): Promise<{ success: boolean; message: string; details?: any }> {
        return this.writeTagList([{ tag: baseTag, value }], skipValidation);
    }

    async writeTag(tag: string, value: any, dataType: DataType = DataType.Int16, skipValidation: boolean = false): Promise<{ success: boolean; message: string }> {
        try {
            tag = this.normalizeProjectSpecificMachineTag(tag);
            const nodeId = this.addNodePrefix(tag);
            const variant = new Variant({ dataType, value });

            await this.session.write({
                nodeId,
                attributeId: AttributeIds.Value,
                value: { value: variant }
            });

            if (!skipValidation) {
                const readValue = await this.readTag(tag, dataType);
                const isNumber = (val: any): val is number => typeof val === 'number';
                const tolerance = 0.0001;
                const valuesMatch = isNumber(readValue) && isNumber(value)
                    ? Math.abs(readValue - value) < tolerance
                    : readValue === value;

                if (!valuesMatch) {
                    console.error(`Verification failed for tag ${tag}: expected ${value}, got ${readValue}. Type of written value: ${typeof value}, Type of read value: ${typeof readValue}`);
                    return {
                        success: false,
                        message: `Failed to verify write to tag ${tag}: expected ${value}, got ${readValue}`
                    };
                }
            }

            return {
                success: true,
                message: `Wrote ${value} to node ${tag}`
            };
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);

            if (this.isOpcuaConnectionClosedError(errorMessage)) {
                console.warn(`Skipped write to node ${tag} because the OPC UA connection is closed.`);
            } else {
                console.error(`Failed to write to node ${tag}:`, error);
            }

            return {
                success: false,
                message: `Failed to write to node ${tag}: ${errorMessage}`
            };
        }
    }

    async requestAction(
        targetDeviceId: number,
        actionType: ActionTypes,
        actionId: number,
        paramArray: number[] = Array(DeviceConstants.MAX_NUM_PARAMS).fill(0.0),
        logContext: ActionRequestLogContext = {},
    ): Promise<{ success: boolean; message: string }> {
        const senderId = logContext.senderId ?? this.id;
        const senderLabel = logContext.senderLabel ?? `${deviceIdToString(senderId)}(${senderId})`;
        const targetLabel = logContext.targetLabel ?? `${deviceIdToString(targetDeviceId)}(${targetDeviceId})`;
        const actionTypeLabel = actionTypeToString(actionType);

        console.log(`Requesting ${actionTypeLabel}(${actionType}) actionId=${actionId} ${senderLabel} -> ${targetLabel}`);

        // Check if we have control of target device
        const commanderTag = `${this.getDeviceNodeId(targetDeviceId)}.Is.CommanderId`;
        const commanderId = await this.readTag(commanderTag);

        if (commanderId !== this.id) {
            if (!process.env.IGNORE_TAKE_CONTROL && !(actionType === ActionTypes.CMD && actionId === DeviceCmds.TAKE_CONTROL)) {
                console.warn(`Requesting ${actionTypeLabel}(${actionType}) actionId=${actionId} ${senderLabel} -> ${targetLabel} but commander is ${commanderId}`);
                return {
                    success: false,
                    message: `We don't have control of target device ${targetDeviceId}, current commander is ${commanderId}`
                };
            }
        }

        //console.log(`We have control of device ${targetDeviceId}, proceeding with action request`);

        //console.log(`Writing sts to WRITING`);
        const apiReqBaseTag = `${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}`;

        await this.writeTagList([
            {
                tag: apiReqBaseTag,
                value: {
                    Sts: ApiReqRespStates.WRITING,
                },
            },
        ]);
        //console.log(`Set ${stsTag} to WRITING`);

        //await this.writeTagV2(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.ActionRequestData`, initialDeviceActionRequestData);
        //console.log("successfully wrote writeTagV2 for ActionRequestData");
        // 3. Fill action request data

        const paddedParamArray = Array(DeviceConstants.MAX_NUM_PARAMS).fill(0.0);
        for (let i = 0; i < Math.min(paramArray.length, DeviceConstants.MAX_NUM_PARAMS); i++) {
            paddedParamArray[i] = paramArray[i] || 0.0;
        }
        this.uniqueActionRequestCtr += 1;
        this.uniqueActionRequestCtr %= 255;
        const uniqueActionRequestId = this.id * 1000 + this.uniqueActionRequestCtr;
        const deviceActionRequestData: DeviceActionRequestData = {
            UniqueActionRequestId: uniqueActionRequestId,
            SenderId: this.id,
            ActionType: actionType,
            ActionId: actionId,
            ParamArray: paddedParamArray
        };

        const plcActionRequestData = {
            UniqueActionRequestId: deviceActionRequestData.UniqueActionRequestId,
            SenderId: deviceActionRequestData.SenderId,
            ActionType: deviceActionRequestData.ActionType,
            ActionId: deviceActionRequestData.ActionId,
            ParamArray: deviceActionRequestData.ParamArray,
        };

        // 4. Write action request data using runtime data type resolution
        await this.writeTagList([
            {
                tag: `${apiReqBaseTag}.ActionRequestData`,
                value: plcActionRequestData,
            },
        ]);

        // 5. Fill API data
        this.request = {
            id: uniqueActionRequestId,
            checkSum: 0, // Simplified checksum
            actionRequestData: deviceActionRequestData,
            sts: ApiReqRespStates.REQUEST_READY
        };

        const plcApiRequestState = {
            id: this.request.id,
            checkSum: this.request.checkSum,
            Sts: this.request.sts,
        };

        // 6. Write API data using runtime data type resolution
        await this.writeTagList([
            {
                tag: apiReqBaseTag,
                value: plcApiRequestState,
            },
        ]);

        // 7. Wait for response
        return await this.awaitApiResponse(targetDeviceId, this.request.id);
    }

    private async awaitApiResponse(targetDeviceId: number, requestId: number): Promise<{ success: boolean; message: string }> {
        const startTime = Date.now();
        console.log(`Waiting for API response for request ID: ${requestId}`);

        // Poll for response status change
        while (Date.now() - startTime < 1000) {
            const responseSts = await this.readTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiRespTag}.Sts`);
            const responseId = await this.readTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiRespTag}.id`, DataType.Int32);

            if (responseId === requestId &&
                responseSts !== ApiReqRespStates.REQUEST_READY &&
                responseSts !== ApiReqRespStates.INACTIVE) {
                console.log(`Received API response for request ID: ${requestId} with status ${apiReqRespStateToString(responseSts)}`);

                switch (responseSts) {
                    case ApiReqRespStates.ACCEPTED:
                        return { success: true, message: "Action request accepted" };
                    case ApiReqRespStates.REJECTED_INVALID_CHECKSUM:
                        return { success: false, message: "Action request rejected: Invalid checksum" };
                    case ApiReqRespStates.REJECTED_ACTION_NOT_ACCEPTED:
                        return { success: false, message: "Action request rejected: Action not accepted" };
                    case ApiReqRespStates.REJECTED_INVALID_SENDERID:
                        return { success: false, message: "Action request rejected: Invalid Sender ID" };
                    default:
                        return { success: false, message: "Action request rejected: Unknown reason" };
                }
            }

            await this.sleep(15);
        }
        console.warn(`Timeout waiting for API response for request ID: ${requestId}`);

        return { success: false, message: "Timeout waiting for PLC to respond" };
    }

    async requestCmd(targetDeviceId: number, cmdId: DeviceCmds): Promise<{ success: boolean; message: string }> {
        return await this.requestAction(targetDeviceId, ActionTypes.CMD, cmdId);
    }

    async requestTakeControlCmd(targetDeviceId: number): Promise<{ success: boolean; message: string }> {
        return await this.requestCmd(targetDeviceId, DeviceCmds.TAKE_CONTROL);
    }

    async requestReleaseControlCmd(targetDeviceId: number): Promise<{ success: boolean; message: string }> {
        return await this.requestCmd(targetDeviceId, DeviceCmds.RELEASE_CONTROL);
    }

    async requestProcess(targetDeviceId: number, processId: number, param0: number = 0.0, param1: number = 0.0, param2: number = 0.0): Promise<{ success: boolean; message: string }> {
        return await this.requestAction(targetDeviceId, ActionTypes.PROCESS, processId, [param0, param1, param2]);
    }

    async executeProcess(targetDeviceId: number, processId: number): Promise<{ success: boolean; logMsg: string; errorMsg: string }> {
        return await this.executeAction(targetDeviceId, ActionTypes.PROCESS, processId);
    }

    async executeMasteringProcess(targetDeviceId: number): Promise<{ success: boolean; logMsg: string; errorMsg: string }> {
        return await this.executeProcess(targetDeviceId, AxisProcesses.PERFORM_MASTERING);
    }

    private async executeAction(targetDeviceId: number, actionType: ActionTypes, actionId: number): Promise<{ success: boolean; logMsg: string; errorMsg: string }> {
        const SCAN_TIME_WHILE_EXECUTING = 1; // milliseconds
        let errorMsg = "";

        this.addLogMsg(this.id, `Executing action ${actionId} of type ${actionType} on device ${targetDeviceId}`, 0, 0, true);

        // Start recording logs
        const isLogging = await this.startRecordingLogs(targetDeviceId);
        if (!isLogging) {
            return { success: false, logMsg: this.logMsg, errorMsg: "Failed to start recording logs" };
        }

        // Request the action
        const { success, message } = await this.requestAction(targetDeviceId, actionType, actionId);

        if (!success) {
            const { present, message: errMsg } = await this.getDeviceErrorMessages(targetDeviceId);
            errorMsg = present ? errMsg : "";
            this.addLogMsg(this.id, `Failed to start action: ${message}`, 0, 0);
            return { success: false, logMsg: this.logMsg, errorMsg };
        }

        this.addLogMsg(this.id, `Action ${actionId} of type ${actionType} started executing on device ${targetDeviceId}`, 0, 0);

        // Monitor the activity
        const startTime = Date.now();
        while (Date.now() - startTime < 300000) { // 5 minute timeout
            this.checkLogRecord();
            const { activeId, activityStepNum, deviceStepNum } = await this.getActivityStatus(targetDeviceId, actionType);

            if (activityStepNum === States.DONE) {
                this.addLogMsg(this.id, `Action ${actionId} of type ${actionType} finished executing on device ${targetDeviceId}`, 0, 0);
                break;
            }

            if (activityStepNum === States.ERROR || deviceStepNum === States.ERROR) {
                this.addLogMsg(this.id, `Action ${actionId} of type ${actionType} failed on device ${targetDeviceId}`, 0, 0);
                const { present, message: errMsg } = await this.getDeviceErrorMessages(targetDeviceId);
                errorMsg = present ? errMsg : "";
                return { success: false, logMsg: this.logMsg, errorMsg };
            }

            if (activeId !== actionId && deviceStepNum === States.ERROR) {
                this.addLogMsg(this.id, `Error: ActiveId ${activeId} does not match requested ActionId ${actionId}`, 0, 0);
                return { success: false, logMsg: this.logMsg, errorMsg: "ActiveId mismatch" };
            }

            await this.sleep(SCAN_TIME_WHILE_EXECUTING);
        }

        this.checkLogRecord();
        await this.stopRecordingLogs(targetDeviceId);

        return { success: true, logMsg: this.logMsg, errorMsg };
    }

    private async getActivityStatus(targetDeviceId: number, activityType: ActionTypes): Promise<{ activeId: number; activityStepNum: number; deviceStepNum: number }> {
        let activityTypeName: string;

        switch (activityType) {
            case ActionTypes.SCRIPT:
                activityTypeName = "Script";
                break;
            case ActionTypes.PROCESS:
                activityTypeName = "Process";
                break;
            case ActionTypes.TASK:
                activityTypeName = "Task";
                break;
            default:
                throw new Error("Invalid activity type for monitoring status");
        }

        const activeId = await this.readTag(`Machine.Devices[${targetDeviceId}].${activityTypeName}.ActiveId`);
        const activityStepNum = await this.readTag(`Machine.Devices[${targetDeviceId}].${activityTypeName}.isStepNum`);
        const deviceStepNum = await this.readTag(`Machine.Devices[${targetDeviceId}].Is.StepNum`);

        return { activeId, activityStepNum, deviceStepNum };
    }

    private async getDeviceErrorMessages(targetDeviceId: number): Promise<{ present: boolean; message: string }> {
        const errorsPresent = await this.readTag(`Machine.Devices[${targetDeviceId}].Errors.Present`);

        if (!errorsPresent) {
            return { present: false, message: "" };
        }

        let errorMsg = "";
        for (let index = 0; index < DeviceConstants.DEVICE_FAULTCODEARRAY_LEN; index++) {
            const msg = await this.readTag(`Machine.Devices[${targetDeviceId}].Errors.List[${index}].Msg`);
            if (msg && msg !== "") {
                errorMsg += `Device ${targetDeviceId} ERROR: ${msg}\n`;
            } else {
                break;
            }
        }

        return { present: true, message: errorMsg };
    }

    private async startRecordingLogs(targetDeviceId: number): Promise<boolean> {
        const { success } = await this.requestCmd(targetDeviceId, DeviceCmds.START_RECORDING_LOGS);
        if (!success) {
            return false;
        }

        const startTime = Date.now();
        while (Date.now() - startTime < 3000) {
            const isRecording = await this.readTag(`Machine.Devices[${targetDeviceId}].Is.RecordingLogs`);
            if (isRecording) {
                this.lastReadLogIndex = 255;
                return true;
            }
            await this.sleep(10);
        }

        return false;
    }

    private async stopRecordingLogs(targetDeviceId: number): Promise<boolean> {
        const { success } = await this.requestCmd(targetDeviceId, DeviceCmds.STOP_RECORDING_LOGS);
        if (!success) {
            return false;
        }

        const startTime = Date.now();
        while (Date.now() - startTime < 3000) {
            const isRecording = await this.readTag(`Machine.Devices[${targetDeviceId}].Is.RecordingLogs`);
            if (!isRecording) {
                await this.sleep(1000);
                this.checkLogRecord();
                return true;
            }
            await this.sleep(10);
        }

        return false;
    }

    private addLogMsg(sourceId: number, msg: string, timeStamp: number, logId: number, reset: boolean = false): void {
        if (reset) {
            this.logMsg = "";
        }

        if (!msg) return;

        this.logMsg += `Device ${sourceId} - ${msg} - timeStamp: ${timeStamp}\n`;
    }

    private async checkLogRecord(): Promise<void> {
        const latestLogIndex = await this.readTag("Machine.LogRecord.LastIndex");

        if (this.lastReadLogIndex === latestLogIndex) {
            return;
        }

        while (true) {
            let readIndex = this.lastReadLogIndex + 1;
            if (readIndex >= 256) {
                readIndex = 0;
            }

            if (readIndex > latestLogIndex) {
                this.lastReadLogIndex = latestLogIndex;
                break;
            }

            const log = await this.readLogRecord(readIndex);
            if (!log) break;

            this.addLogMsg(log.Id, log.Msg, log.TimeStamp, log.Id);
            this.lastReadLogIndex = readIndex;
        }
    }

    private async readLogRecord(index: number): Promise<any> {
        return await this.readTag(`Machine.LogRecord.List[${index}]`);
    }

    private sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    // Getter for log messages
    getLogMsg(): string {
        return this.logMsg;
    }

    clearLogMsg(): void {
        this.logMsg = "";
    }

    private timeSyncWasPerformed: boolean = false;

    async writeCurrentTimeToCodesys(): Promise<void> {
        // 1. Get the current time in milliseconds since the Unix epoch (Jan 1, 1970 UTC).
        // JavaScript Date.now() provides time in milliseconds UTC by default.

        const plcTimeMsTag = "machine.utilities.currentTimeMs";
        const reqTag = "machine.syncClockReq";
        const timeTag = "machine.syncClockTime";
        const respTag = "machine.syncClockDone";
        const currentTimeMs = Date.now();
        const currentPlcTimeMs = await this.readTag(plcTimeMsTag, DataType.UInt64);


        if (Math.abs(currentTimeMs - currentPlcTimeMs) > 7 && !this.timeSyncWasPerformed) {


            await this.writeTag(reqTag, false, DataType.Boolean);
            // 2. Convert milliseconds to seconds and ensure it's an integer value, as required by the DWORD type.
            const millisecondsSinceEpoch = Date.now();
            const latencyOffsetMs: number = 10; // Estimated latency offset in milliseconds
            const setTimeMs = millisecondsSinceEpoch + latencyOffsetMs;
            //secondsSinceEpoch = Math.floor(millisecondsSinceEpoch / 1000);
            await this.writeTag(timeTag, setTimeMs, DataType.UInt64);
            await this.writeTag(reqTag, true, DataType.Boolean);
            if (!this.timeSyncWasPerformed) {
                console.log("[DRIVER] Performing initial PLC time sync");
            } else {
                console.log("[DRIVER] Writing current time to PLC:", setTimeMs, " discrepancy is ", currentTimeMs - currentPlcTimeMs, "ms");
            }

            let respReceived = false;
            const timeoutTime = Date.now() + 5000; // 5 second timeout
            while (!respReceived) {
                const resp = await this.readTag(respTag, DataType.Boolean);
                if (resp) {
                    respReceived = true;
                    await this.writeTag(reqTag, false, DataType.Boolean);
                    this.timeSyncWasPerformed = true;
                    console.log("[DRIVER] PLC time sync completed");
                } else {
                    if (Date.now() > timeoutTime) {
                        console.error("[DRIVER] Timed out waiting for PLC time sync response");
                        break;
                    }
                    await this.sleep(5);
                }
            }
        }
    }
}
