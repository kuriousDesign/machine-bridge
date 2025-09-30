import { ClientSession, Variant, AttributeIds, DataType, VariantArrayType, ReadValueIdOptions, StatusCodes, DataValue } from "node-opcua";
import { ActionTypes, DeviceCmds, AxisMethods, DeviceId, States, ApiOpcuaReqData, Machine, DeviceActionRequestData, ApiReqRespStates, AxisProcesses, DeviceConstants, PlcNamespaces, MachineTags, apiReqRespStateToString } from "@kuriousdesign/machine-sdk";


export default class CodesysOpcuaDriver {
    private id: number;
    private session: ClientSession;
    private nodePrefix: string;
    private apiReqTag: string = "apiOpcua.hmiReq";
    private apiRespTag: string = "apiOpcua.hmiResp";
    private request: ApiOpcuaReqData;
    private response: ApiOpcuaReqData;
    private machineStatus: Machine;
    private logMsg: string = "";
    private lastLogTimeStamp: number = 0;
    private lastLogMsgId: number = 0;
    private lastReadLogIndex: number = 255;
    private devicesNodeId = `${PlcNamespaces.Machine}.${MachineTags.deviceStore}`;

    constructor(id: number, session: ClientSession, opcuaControllerName: string = "CODESYS Control for Linux SL") {
        this.id = id;
        this.session = session;
        this.nodePrefix = `ns=4;s=|var|${opcuaControllerName}.Application.`;

        this.request = {
            id: 0,
            checkSum: 0,
            actionRequestData: {
                SenderId: DeviceId.NONE,
                ActionType: 0,
                ActionId: 0,
                ParamArray: [0.0, 0.0, 0.0]
            },
            sts: ApiReqRespStates.INACTIVE
        };

        this.response = { ...this.request };

        this.machineStatus = {
            estopCircuit_OK: false,
            estopCircuitDelayed_OK: false,
            fenceCircuit_OK: false,
            guardDoors_LOCKED: false,
            networkHealth_OK: false,
            ethercatMaster_OK: false,
            ethercatSlaves_OK: false,
            supplyAir_OK: false
        };
    }

    private addNodePrefix(tag: string): string {
        return `${this.nodePrefix}${tag}`;
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
                console.warn(`Failed to read OPC UA value from ${nodeId}: ${data.statusCode}`);
                return null;
            }
        } catch (error) {
            console.error(`Failed to read OPC UA value from ${nodeId}:`, error);
            throw error;
        }
    }


    async readTag(tag: string, dataType: DataType = DataType.Int16): Promise<any> {
        if (!this.session) {
            console.error('OPC UA session is not initialized');
            return null;
        }

        try {
            const nodeId = this.addNodePrefix(tag);
            const readValueOptions: ReadValueIdOptions = {
                nodeId: nodeId,
                attributeId: AttributeIds.Value
            };
            const dataValue: DataValue = await this.session.read(readValueOptions);

            if (!dataValue || !dataValue.value) {
                console.warn(`No value returned for node ${tag}`);
                return null;
            }

            // Handle ULINT (UInt64) values
            if (dataType === DataType.UInt64) {
                const variant = dataValue.value;
                console.log(`Read UInt64 variant for node ${tag}:`, variant);

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
            return value as number; // Cast to number for non-UInt64 types
        } catch (error) {
            console.error(`Failed to read node ${tag}:`, error);
            return null;
        }
    }

    async writeTag(tag: string, value: any, dataType: DataType = DataType.Int16): Promise<{ success: boolean; message: string }> {
        try {
            const nodeId = this.addNodePrefix(tag);
            const variant = new Variant({ dataType, value });

            await this.session.write({
                nodeId,
                attributeId: AttributeIds.Value,
                value: { value: variant }
            });

            // Verify write
            const readValue = await this.readTag(tag, dataType);
            if (readValue !== value) {
                console.error(`Verification failed for node ${tag}: expected ${value}, got ${readValue}`);
                // print the types of each
                console.log(`Type of written value: ${typeof value}, Type of read value: ${typeof readValue}`);
                return {
                    success: false,
                    message: `Failed to verify write to node ${tag}: expected ${value}, got ${readValue}`
                };
            }

            console.log(`Wrote ${value} to node ${tag}`);

            return {

                success: true,
                message: `Wrote ${value} to node ${tag}`
            };
        } catch (error) {
            console.error(`Failed to write to node ${tag}:`, error);
            return {
                success: false,
                message: `Failed to write to node ${tag}: ${error}`
            };
        }
    }

    async requestAction(
        targetDeviceId: number,
        actionType: ActionTypes,
        actionId: number,
        paramArray: number[] = Array(DeviceConstants.MAX_NUM_PARAMS).fill(0.0),
    ): Promise<{ success: boolean; message: string }> {
        console.log(`Requesting action ${actionType} ${actionId} on device ${targetDeviceId}`);

        // Check if we have control of target device
        const commanderTag = `${this.getDeviceNodeId(targetDeviceId)}.Is.CommanderId`;
        const commanderId = await this.readTag(commanderTag);

        if (commanderId !== this.id) {
            if (!process.env.IGNORE_TAKE_CONTROL && !(actionType === ActionTypes.CMD && actionId === DeviceCmds.TAKE_CONTROL)) {
                console.warn(`Requesting action ${actionType} ${actionId} on device ${targetDeviceId} but we don't have control`);
                return {
                    success: false,
                    message: `We don't have control of target device ${targetDeviceId}, current commander is ${commanderId}`
                };
            }
        }

        // 1. Set Request Sts to INACTIVE
        const stsTag = `${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.Sts`;
        await this.writeTag(stsTag, ApiReqRespStates.INACTIVE);
        console.log(`Set ${stsTag} to INACTIVE`);

        // 2. Wait for status to update
        const startTime = Date.now();
        while (Date.now() - startTime < 3000) {
            const currentSts = await this.readTag(stsTag);
            if (currentSts === ApiReqRespStates.INACTIVE) break;
            await this.sleep(10);
        }
        console.log(`Confirmed ${stsTag} is INACTIVE`);


        // 3. Fill action request data

        const paddedParamArray = Array(DeviceConstants.MAX_NUM_PARAMS).fill(0.0);
        for (let i = 0; i < Math.min(paramArray.length, DeviceConstants.MAX_NUM_PARAMS); i++) {
            paddedParamArray[i] = paramArray[i] || 0.0;
        }


        const DeviceActionRequestData: DeviceActionRequestData = {
            SenderId: this.id,
            ActionType: actionType,
            ActionId: actionId,
            ParamArray: paddedParamArray
        };


        // 4. Write action request data
        await this.writeTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.ActionRequestData.SenderId`, DeviceActionRequestData.SenderId);
        await this.writeTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.ActionRequestData.ActionType`, DeviceActionRequestData.ActionType);
        await this.writeTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.ActionRequestData.ActionId`, DeviceActionRequestData.ActionId);

        // Write all parameters using the constant
        for (let i = 0; i < DeviceConstants.MAX_NUM_PARAMS; i++) {
            await this.writeTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.ActionRequestData.ParamArray[${i}]`, DeviceActionRequestData.ParamArray[i], DataType.Double);
        }

        // 5. Fill API data
        this.request = {
            id: Date.now(),
            checkSum: 0, // Simplified checksum
            actionRequestData: DeviceActionRequestData,
            sts: ApiReqRespStates.REQUEST_READY
        };

        // 6. Write API data
        //console.log(`Writing action request to device ${targetDeviceId}:`, this.request);
        await this.writeTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.id`, this.request.id, DataType.UInt64);
        //console.log(`Wrote ${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.id = ${this.request.id}`);
        await this.writeTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.checkSum`, this.request.checkSum);
        await this.writeTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiReqTag}.Sts`, this.request.sts);

        // 7. Wait for response
        return await this.awaitApiResponse(targetDeviceId, this.request.id);
    }

    private async awaitApiResponse(targetDeviceId: number, requestId: number): Promise<{ success: boolean; message: string }> {
        const startTime = Date.now();
        console.log(`Waiting for API response for request ID: ${requestId}`);

        while (Date.now() - startTime < 1000) {
            const responseSts = await this.readTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiRespTag}.Sts`);
            const responseId = await this.readTag(`${this.getDeviceNodeId(targetDeviceId)}.${this.apiRespTag}.id`, DataType.UInt64);

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

            await this.sleep(5);
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
}
