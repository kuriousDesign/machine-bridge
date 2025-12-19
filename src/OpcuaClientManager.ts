import {
    AttributeIds,
    ClientSession,
    DataType,
    DataValue,
    MessageSecurityMode,
    OPCUAClient,
    OPCUAClientOptions,
    ReadValueIdOptions,
    SecurityPolicy,
    StatusCodes,
    VariantArrayType,
    WriteValueOptions,
} from 'node-opcua';

import "dotenv/config"; // auto-loads .env

import { MachineTags, PlcNamespaces, initialMachine, nodeListString } from '@kuriousdesign/machine-sdk';

// --- Feature Flag & Configuration ---
const ENABLE_DIAGNOSTICS = process.env.ENABLE_DIAGNOSTICS === 'true' || false;
const opcuaControllerName = process.env.OPCUA_CONTROLLER_NAME || "DefaultController";
const opcuaEndpoint = `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;
const nodeListPrefix = nodeListString + opcuaControllerName + '.Application.';
const POLLING_RATE_MS = 200;
const DIAG_READS_TO_SKIP_AT_START = 10;
const RECONNECT_DELAY_MS = 3000; // Time to wait before attempting reconnection

const opcuaOptions: OPCUAClientOptions = {
    applicationName: 'OpcuaMqttBridge',
    securityMode: MessageSecurityMode.None,
    securityPolicy: SecurityPolicy.None,
    endpointMustExist: true,
    keepSessionAlive: true, // Let node-opcua handle internal session heartbeat
};

interface ReadItemInfo {
    nodeId: string;
    mqttTopic: string;
}

// --- Performance Tracking Variables ---
let minReadDurationMs: number = Infinity;
let maxReadDurationMs: number = 0;
let totalReads: number = 0;
let totalDurationMs: number = 0;
let maxTimeBetweenScansMs: number = 0;
let lastScanTime: [number, number] | null = null;

// --- State Machine Definition ---
enum OpcuaState {
    Disconnected,
    Connecting,
    Connected,
    Polling,
    Reconnecting,
    WaitingForHeartbeat,
    Disconnecting,
}

function decipherOpcuaValue(data: any): any {
    const decipheredValue =
        data?.value?.arrayType === VariantArrayType.Array
            ? Array.from(data.toJSON().value.value)
            : (data?.toJSON()?.value?.value);
    return decipheredValue;
}

function concatNodeId(namespace: string, tag: string): string {
    return `${nodeListPrefix}${namespace}.${tag}`;
}

class OpcuaClientManager {
    private state: OpcuaState = OpcuaState.Disconnected;
    private client: OPCUAClient | null = null;
    private session: ClientSession | null = null;
    private itemsToRead: ReadItemInfo[] = getMachineReadItems();
    private shutdownRequested: boolean = false;
    private heartbeatPlcValue: number = 0;

    public requestShutdown(): void {
        this.shutdownRequested = true;
        console.log("Shutdown requested. Transitioning to Disconnecting state.");
    }

    public async manageConnectionLoop(): Promise<void> {
        let prevHeartbeatPlcValue = -1;
        let lastUpdateTime = Date.now();
        while (!this.shutdownRequested) {
            console.log(`\nSTATE: ${OpcuaState[this.state]}`);
            switch (this.state) {
                case OpcuaState.Disconnected:
                case OpcuaState.Reconnecting:
                    await this.handleConnection();
                    break;

                case OpcuaState.Connected:
                case OpcuaState.Polling:
                case OpcuaState.WaitingForHeartbeat:
                    await this.handlePolling();
                    // i want to add some logic if the plc heartbeat isn't updating to go to waiting for heartbeat state
                    if (this.heartbeatPlcValue !== prevHeartbeatPlcValue) {
                        // Heartbeat is updating normally, stay in Polling state
                        lastUpdateTime = Date.now();
                        prevHeartbeatPlcValue = this.heartbeatPlcValue;
                    } else if (Date.now() - lastUpdateTime > 5000) {
                        // Heartbeat not updating, transition to waiting state
                        this.state = OpcuaState.WaitingForHeartbeat;
                        console.warn("⚠️ PLC heartbeat not updating. Waiting for heartbeat...");
                    }
                    break;

                case OpcuaState.Disconnecting:
                    await this.handleDisconnect();
                    return; // Exit loop after graceful disconnect
            }
            // Small delay to prevent tight blocking loop if state transitions rapidly
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        await this.handleDisconnect();
    }

    private async handleConnection(): Promise<void> {
        this.state = OpcuaState.Connecting;
        console.log(`Connecting to endpoint: ${opcuaEndpoint}`);

        try {
            this.client = OPCUAClient.create(opcuaOptions);
            // Attach built-in handlers for automatic reconnection messages
            this.client.on("connection_lost", () => console.warn("OPC UA connection lost (handled by internal mechanism)"));
            this.client.on("after_reconnection", () => console.log("✅ OPC UA client reconnected internally"));

            await this.client.connect(opcuaEndpoint);
            this.session = await this.client.createSession();
            console.log("✅ Connection successful. Session created.");
            this.state = OpcuaState.Connected;

        } catch (err) {
            console.error(`❌ Failed to connect/create session: ${err instanceof Error ? err.message : String(err)}`);
            if (this.client) {
                await this.client.disconnect();
            }
            this.client = null;
            this.session = null;
            console.log(`Will retry in ${RECONNECT_DELAY_MS}ms...`);
            this.state = OpcuaState.Reconnecting;
            await new Promise(resolve => setTimeout(resolve, RECONNECT_DELAY_MS));
        }
    }

    private async handlePolling(): Promise<void> {
        this.state = OpcuaState.Polling;

        try {
            // Your polling logic from the previous script
            await this.pollAllTags();
            // Go back to connected state to loop again after POLLING_RATE_MS delay
            this.state = OpcuaState.Polling;
            await new Promise(resolve => setTimeout(resolve, POLLING_RATE_MS));

        } catch (error) {
            // A read error likely means the session or connection is bad.
            console.error("❌ Polling failed. Assuming connection issue, attempting reconnection:", error);
            this.state = OpcuaState.Reconnecting;
            this.session = null; // Invalidate session
        }
    }

    private async handleDisconnect(): Promise<void> {
        this.state = OpcuaState.Disconnecting;
        console.log("Starting graceful disconnection and cleanup...");
        if (this.session) {
            await this.session.close();
            this.session = null;
            console.log("✅ Session closed.");
        }
        if (this.client) {
            await this.client.disconnect();
            this.client = null;
            console.log("✅ Client disconnected.");
        }
        this.state = OpcuaState.Disconnected;
        console.log("✅ System fully disconnected. Exiting.");
    }

    private async readOpcuaValue(nodeId: string): Promise<any> {
        if (!this.session) {
            throw new Error("OPC UA session is not initialized");
        }


        const readValueOptions: ReadValueIdOptions = {
            nodeId: nodeId,
            attributeId: AttributeIds.Value,
        };
        const data = await this.session.read(readValueOptions);
        const value = decipherOpcuaValue(data);
        if (data.statusCode === StatusCodes.Good) {
            return value;
        } else {
            console.warn(`Failed to read OPC UA value from ${nodeId}: ${data.statusCode}`);
            return null;
        }

    }

    private async writeOpcuaValue(nodeId: string, value: any, dataType: DataType): Promise<void> {
        if (!this.session) {
            throw new Error("OPC UA session is not initialized");
        }
        try {
            const writeValue: WriteValueOptions = {
                nodeId: nodeId,
                attributeId: AttributeIds.Value,
                value: {
                    value: {
                        dataType: dataType,
                        value: value
                    }
                }
            };
            await this.session.write(writeValue);
            const readValue = await this.readOpcuaValue(nodeId); // verify write
            if (readValue !== value) {
                console.error(`Verification failed for node ${nodeId}: expected ${value}, got ${readValue}`);
            }
        } catch (error) {
            console.error(`Failed to write OPC UA value to ${nodeId}:`, error);
            throw error;
        }
    }

    /**
     * Reads all specified tags once, measuring performance if diagnostics are enabled.
     */
    private async pollAllTags(): Promise<void> {
        if (!this.session) throw new Error("Session is not active during poll operation.");

        if (ENABLE_DIAGNOSTICS) {
            const now = process.hrtime();
            if (lastScanTime !== null) {
                const timeBetweenHr = process.hrtime(lastScanTime);
                const timeBetweenMs = (timeBetweenHr[0] * 1000) + (timeBetweenHr[1] / 1e6);

                if (totalReads >= DIAG_READS_TO_SKIP_AT_START) {
                    if (timeBetweenMs > maxTimeBetweenScansMs) {
                        maxTimeBetweenScansMs = timeBetweenMs;
                        // console.log(`\n⏱️ New Max Time Between Scans Recorded: ${maxTimeBetweenScansMs.toFixed(2)}ms`);
                    }
                }
            }
            lastScanTime = now;
        }

        const nodesToRead: ReadValueIdOptions[] = this.itemsToRead.map(item => ({
            nodeId: item.nodeId,
            attributeId: AttributeIds.Value,
        }));

        const startTimeRead = process.hrtime();

        const dataValues: DataValue[] = await this.session.read(nodesToRead);
        // get index to print the node that ends in heartbeatPlc
        const heartbeatPlcIndex = this.itemsToRead.findIndex(item => item.nodeId.endsWith("heartbeatPlc"));
        this.heartbeatPlcValue = decipherOpcuaValue(dataValues[heartbeatPlcIndex]);
        console.log(`Heartbeat PLC Value: ${this.heartbeatPlcValue}`);

        const durationReadHr = process.hrtime(startTimeRead);
        const durationMs = (durationReadHr[0] * 1000) + (durationReadHr[1] / 1e6);

        if (ENABLE_DIAGNOSTICS) {
            if (totalReads >= DIAG_READS_TO_SKIP_AT_START) {
                totalDurationMs += durationMs;
                minReadDurationMs = Math.min(minReadDurationMs, durationMs);

                if (durationMs > maxReadDurationMs) {
                    maxReadDurationMs = durationMs;
                    console.log(`\n📈 New Max Read Duration Recorded: ${maxReadDurationMs.toFixed(2)}ms (Scan #${totalReads + 1})`);
                }
            }
            totalReads++;
            if (totalReads > DIAG_READS_TO_SKIP_AT_START) {
                const avgDurationMs = totalDurationMs / (totalReads - DIAG_READS_TO_SKIP_AT_START);
                console.log(`--- Read Duration: ${durationMs.toFixed(2)}ms | Avg Read Duration: ${avgDurationMs.toFixed(2)}ms | Max Time Between Scans: ${maxTimeBetweenScansMs.toFixed(2)}ms ---`);
            }
        }
        const heartbeatHmiIndex = this.itemsToRead.findIndex(item => item.nodeId.endsWith("heartbeatHmi"));
        const heartbeatHmiNodeId = concatNodeId(PlcNamespaces.Machine, MachineTags.HeartbeatHMI);
    
        this.writeOpcuaValue(heartbeatHmiNodeId, this.heartbeatPlcValue, DataType.Byte);
  

        // Process and display the results (MQTT logic goes here)
        // Example:
        // dataValues.forEach((dataValue, index) => {
        //     if (dataValue.statusCode === StatusCodes.Good) {
        //         // console.log(`${this.itemsToRead[index].mqttTopic}: ${dataValue.value.value}`);
        //     }
        // });
    }
}

// --- Helper Functions (remain external) ---

function getMachineReadItems(): ReadItemInfo[] {
    const itemsToRead: ReadItemInfo[] = [];
    Object.entries(initialMachine).forEach(([key, value]) => {
        const tag = key;
        const relativeNodeId = PlcNamespaces.Machine + '.' + tag;
        const fullNodeId = `${nodeListPrefix}${relativeNodeId}`;
        const topic = PlcNamespaces.Machine.toLowerCase() + '/' + tag.toLowerCase();
        itemsToRead.push({
            nodeId: fullNodeId,
            mqttTopic: topic
        });
    });
    return itemsToRead;
}


/**
 * Main execution function
 */
async function main() {
    console.log("Application starting...");
    if (ENABLE_DIAGNOSTICS) {
        console.log(`Diagnostics ENABLED. Skipping stats for the first ${DIAG_READS_TO_SKIP_AT_START} scans.`);
    } else {
        console.log(`Diagnostics DISABLED.`);
    }

    const manager = new OpcuaClientManager();

    // Handle graceful shutdown via Ctrl+C
    process.on('SIGINT', async () => {
        console.log("\nSIGINT received. Shutting down gracefully.");
        manager.requestShutdown();
        // Give the state machine loop time to complete the disconnect process
        // A better approach in a real app might use a promise/event listener here
        setTimeout(() => process.exit(0), 5000);
    });

    await manager.manageConnectionLoop();
    // Script finishes here after disconnection is complete.
}

main().catch(console.error);
