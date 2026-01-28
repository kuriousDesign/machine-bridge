import {
    AttributeIds,
    ClientSession,
    DataValue,
    MessageSecurityMode,
    OPCUAClient,
    OPCUAClientOptions,
    ReadValueIdOptions,
    SecurityPolicy,
} from 'node-opcua';

import "dotenv/config"; // auto-loads .env

import { PlcNamespaces, initialMachine, nodeListString } from '@kuriousdesign/machine-sdk';

// --- Feature Flag & Configuration ---
const ENABLE_DIAGNOSTICS = process.env.ENABLE_DIAGNOSTICS === 'true' || false;

const opcuaControllerName = process.env.OPCUA_CONTROLLER_NAME || "DefaultController";
const opcuaEndpoint = `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;
const nodeListPrefix = nodeListString + opcuaControllerName + '.Application.';

const POLLING_RATE_MS = 200; 
const DIAG_READS_TO_SKIP_AT_START = 10;

const opcuaOptions: OPCUAClientOptions = {
    applicationName: 'OpcuaMqttBridge',
    securityMode: MessageSecurityMode.None,
    securityPolicy: SecurityPolicy.None,
    endpointMustExist: true,
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

// --- Helper Functions ---
async function initializeOpcuaClient(opcuaEndpoint: string): Promise<ClientSession | null> {
    const opcuaClient = OPCUAClient.create(opcuaOptions);
    opcuaClient.on("connection_lost", () => { console.warn("OPC UA connection lost"); });
    opcuaClient.on("after_reconnection", () => { console.log("✅ OPC UA reconnected"); });

    try {
        console.log("Connecting to OPC UA endpoint: " + opcuaEndpoint);
        await opcuaClient.connect(opcuaEndpoint);
        console.log("✅ OPC UA client connected");

        const opcuaSession = await opcuaClient.createSession();
        console.log("✅ OPC UA session created");
        return opcuaSession;
    }
    catch (err: any) {
        console.error("❌ Failed to create OPC UA session:", err?.message ?? err);
        await opcuaClient.disconnect();
        return null;
    }
}

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
 * Reads all specified tags once, measuring performance if diagnostics are enabled.
 */
async function pollAllTags(opcuaSession: ClientSession, itemsToRead: ReadItemInfo[]): Promise<void> {
    if (!opcuaSession) {
        console.error("Session lost during polling interval.");
        return;
    }

    if (ENABLE_DIAGNOSTICS) {
        // --- Measure Time Between Scans ---
        const now = process.hrtime();
        if (lastScanTime !== null) {
            const timeBetweenHr = process.hrtime(lastScanTime);
            // FIX: Destructure the hrtime tuple correctly to perform arithmetic
            const secondsBetween = timeBetweenHr[0];
            const nanosecondsBetween = timeBetweenHr[1];
            const timeBetweenMs = (secondsBetween * 1000) + (nanosecondsBetween / 1e6);

            if (totalReads >= DIAG_READS_TO_SKIP_AT_START) {
                if (timeBetweenMs > maxTimeBetweenScansMs) {
                    maxTimeBetweenScansMs = timeBetweenMs;
                    console.log(`\n⏱️ New Max Time Between Scans Recorded: ${maxTimeBetweenScansMs.toFixed(2)}ms`);
                }
            }
        }
        lastScanTime = now;
    }


    const nodesToRead: ReadValueIdOptions[] = itemsToRead.map(item => ({
        nodeId: item.nodeId,
        attributeId: AttributeIds.Value,
    }));

    const startTimeRead = process.hrtime();

    try {
        const dataValues: DataValue[] = await opcuaSession.read(nodesToRead);
        
        const durationReadHr = process.hrtime(startTimeRead);
        // FIX: Destructure the hrtime tuple correctly to perform arithmetic
        const secondsRead = durationReadHr[0];
        const nanosecondsRead = durationReadHr[1];
        const durationMs = (secondsRead * 1000) + (nanosecondsRead / 1e6);

        if (ENABLE_DIAGNOSTICS) {
            // Update statistics only after the initial skips
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
                console.log(`\n--- Polling Cycle Complete --- Read Duration: ${durationMs.toFixed(2)}ms | Avg Read Duration: ${avgDurationMs.toFixed(2)}ms`);
            } else {
                console.log(`\n--- Polling Cycle Complete --- (Skipping stats for initial read #${totalReads})`);
            }
        }

        // Process and display the results (MQTT logic goes here)

    } catch (error) {
        console.error("❌ An error occurred during the OPC UA read operation, session might be invalid:", error);
    }
}


/**
 * Main execution function
 */
async function main() {
    const session = await initializeOpcuaClient(opcuaEndpoint);
    const itemsToRead = getMachineReadItems();

    if (session) {
        console.log(`Starting continuous polling every ${POLLING_RATE_MS}ms for ${itemsToRead.length} items.`);
        if (ENABLE_DIAGNOSTICS) {
            console.log(`Diagnostics ENABLED. Skipping statistics for the first ${DIAG_READS_TO_SKIP_AT_START} scans.`);
        } else {
            console.log(`Diagnostics DISABLED.`);
        }
        
        setInterval(() => {
            pollAllTags(session, itemsToRead).catch(console.error);
        }, POLLING_RATE_MS);
    }
}

main().catch(console.error);
