import {
    AttributeIds,
    ClientMonitoredItemBase,
    ClientMonitoredItemGroup,
    ClientSession,
    ClientSubscription,
    ConnectionStrategyOptions,
    DataType,
    ExtraDataTypeManager,
    DataValue,
    MessageSecurityMode,
    NodeId,
    OPCUAClient,
    OPCUAClientOptions,
    ReadValueIdOptions,
    resolveNodeId,
    SecurityPolicy,
    StatusCode,
    StatusCodes,
    TimestampsToReturn,
    VariantArrayType,
    WriteValueOptions,
    CreateSubscriptionRequestOptions,
    MonitoringParametersOptions,
    MonitoringMode,
    DataChangeFilter,
    DataChangeTrigger,
    DeadbandType,
} from 'node-opcua';

import "dotenv/config"; // auto-loads .env

import { DeviceRegistration, MachineTags, PlcNamespaces, initialMachine, nodeListString, nodeTypeString } from '@kuriousdesign/machine-sdk';


const opcuaControllerName = process.env.OPCUA_CONTROLLER_NAME || "DefaultController";
const opcuaEndpoint = `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;
const nodeTypePrefix = nodeTypeString + opcuaControllerName + '.Application.';
const nodeListPrefix = nodeListString + opcuaControllerName + '.Application.';

interface HeartBeatConnection {
    connectionId: string;
    heartbeatValue: number;
    timer: NodeJS.Timeout;
    heartbeatInterval?: NodeJS.Timeout;
}

const connectionStrategy: ConnectionStrategyOptions = {
    initialDelay: 1000,
    maxDelay: 4000,
    maxRetry: -1  // -1 means infinite retries
};

interface MonitoredItemToMonitor {
    attributeId: number;
    nodeId: string;
}

const opcuaOptions: OPCUAClientOptions = {
    applicationName: 'OpcuaMqttBridge',
    connectionStrategy: connectionStrategy,
    securityMode: MessageSecurityMode.None,
    securityPolicy: SecurityPolicy.None,
    endpointMustExist: true,
    keepSessionAlive: true,
    //requestedSessionTimeout: 300000, 
};

const PUBLISHING_INTERVAL = 1000; // in ms

const subscriptionOptions: CreateSubscriptionRequestOptions = {
    maxNotificationsPerPublish: 2000,
    publishingEnabled: true,
    requestedLifetimeCount: 100,
    requestedMaxKeepAliveCount: 47,
    requestedPublishingInterval: PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
};

const filter = new DataChangeFilter({
    trigger: DataChangeTrigger.StatusValue, // Report on any of Status, Value, or Timestamp
    deadbandType: DeadbandType.None,                 // No deadband suppression
    deadbandValue: 0
});

const optionsGroup: MonitoringParametersOptions = {
    discardOldest: true,
    queueSize: 1,
    samplingInterval: PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
    filter: filter
};

const MAX_ITEMS_PER_GROUP = 100; // you found 100 as the practical limit; keep here for easy adjust


async function initializeOpcuaClient(opcuaEndpoint: string): Promise<ClientSession | null> {
    const opcuaClient = OPCUAClient.create(opcuaOptions);

    opcuaClient.on("backoff", (retry, delay) => {
        console.log(`OPC UA connection failed, retrying #${retry} in ${delay}ms`);
    });

    opcuaClient.on("connection_lost", () => {
        console.warn("OPC UA connection lost");
    });

    opcuaClient.on("after_reconnection", () => {
        console.log("✅ OPC UA reconnected");
    });

    try {
        console.log("connecting to opcua endpoint: " + opcuaEndpoint);
        await opcuaClient.connect(opcuaEndpoint);
        console.log("✅ OPC UA client connected");

        const opcuaSession = await opcuaClient.createSession();
        console.log("✅ OPC UA session created");

        const dataTypeManager = new ExtraDataTypeManager();
        console.log("✅ OPC UA connection established, proceeding...");
        return opcuaSession;
    }
    catch (err: any) {
        console.error("❌ Failed to create OPC UA session:", err?.message ?? err);
        await opcuaClient.disconnect();
        console.log("✅ OPC UA client disconnected due to session creation failure");
        return null;
    }
}

function getMachineMonitoredItems(): Map<string, string> {
    const nodeIdToMqttTopicMap: Map<string, string> = new Map<string, string>();
    Object.entries(initialMachine).map(([key, value]) => {
        const tag = key;
        const nodeId = PlcNamespaces.Machine + '.' + tag;
        const topic = PlcNamespaces.Machine.toLowerCase() + '/' + tag.toLowerCase();
        nodeIdToMqttTopicMap.set(nodeId, topic);
    });
    return nodeIdToMqttTopicMap;
}

function createItemsToMonitor(nodeIdToMqttTopicMap: Map<string, string>): MonitoredItemToMonitor[] {
    const items: MonitoredItemToMonitor[] = [];
    for (const [nodeId] of nodeIdToMqttTopicMap) {
        items.push({
            attributeId: AttributeIds.Value,
            nodeId: `${nodeListPrefix}${nodeId}`,
        });
    }
    items.push({
        attributeId: AttributeIds.Value,
        nodeId: `${nodeListPrefix}Machine.Devices[13]`,
    });

    return items;
}

// NEW: Diagnostic function to read important server capabilities
async function printServerCapabilities(session: ClientSession): Promise<void> {
    console.log("\n=== OPC UA Server Capabilities & Status ===");

    const capabilitiesToRead = [
        { nodeId: "ns=0;i=11578", name: "MaxSubscriptions" },
        { nodeId: "ns=0;i=11581", name: "MaxMonitoredItemsPerSubscription" },
        { nodeId: "ns=0;i=11579", name: "MaxMonitoredItemsPerCall" },
        { nodeId: "ns=0;i=2272", name: "MinSupportedSamplingInterval (ms)" },
    ];

    for (const item of capabilitiesToRead) {
        try {
            const dataValue = await session.read({
                nodeId: item.nodeId,
                attributeId: AttributeIds.Value,
            });

            if (dataValue.statusCode === StatusCodes.Good) {
                let value = dataValue.value.value;

                // Convert microseconds to ms for sampling interval
                if (item.name.includes("SamplingInterval")) {
                    value = value / 1000;
                }

                console.log(`${item.name.padEnd(40)}: ${value ?? "null"}`);

                // Add helpful warnings for Weidmüller/embedded constraints
                if (item.name === "MaxSubscriptions" && value <= 5) {
                    console.warn("   → WARNING: Very low MaxSubscriptions (common on embedded PLCs)");
                }
                if (item.name === "MaxMonitoredItemsPerSubscription" && value <= 100) {
                    console.warn("   → WARNING: Low per-subscription limit – may explain why subs fail");
                }
                if (item.name === "MaxMonitoredItemsPerCall" && value < MAX_ITEMS_PER_GROUP) {
                    console.warn(`   → WARNING: MaxMonitoredItemsPerCall (${value}) < chunk size (${MAX_ITEMS_PER_GROUP}) → reduce MAX_ITEMS_PER_GROUP`);
                }
            } else {
                console.warn(`${item.name.padEnd(40)}: NOT AVAILABLE (status: ${dataValue.statusCode.toString()})`);
            }
        } catch (err) {
            console.warn(`${item.name.padEnd(40)}: Read failed → ${err instanceof Error ? err.message : String(err)}`);
        }
    }

    // Bonus: Try to read current subscription count
    try {
        const currSubs = await session.read({
            nodeId: "ns=0;i=2260", // ServerStatus.CurrentSubscriptionCount
            attributeId: AttributeIds.Value,
        });
        if (currSubs.statusCode === StatusCodes.Good) {
            console.log(`Current active subscriptions:     ${currSubs.value.value}`);
        }
    } catch (e) {
        // Many servers don't expose this – silent fail
    }

    console.log("=======================================\n");
}

async function subscribeToMonitoredItems(opcuaSession: ClientSession): Promise<void> {
    if (!opcuaSession) {
        throw new Error("OPC UA session is not initialized");
    }
    const nodeIdToMqttTopicMap = getMachineMonitoredItems();
    const monitoredItems = createItemsToMonitor(nodeIdToMqttTopicMap);
    console.log('Subscribing to monitored items', monitoredItems.length, 'items to monitor...');

    // Chunk items into groups of MAX_ITEMS_PER_GROUP
    for (let i = 0; i < monitoredItems.length; i += MAX_ITEMS_PER_GROUP) {
        const chunk = monitoredItems.slice(i, i + MAX_ITEMS_PER_GROUP);
        const groupIndex = Math.floor(i / MAX_ITEMS_PER_GROUP) + 1;
        console.log(`Creating subscription group ${groupIndex} with ${chunk.length} items...`);

        // Validate nodes in chunk (read test)
        const validatedItems: { attributeId: number; nodeId: string }[] = [];
        for (const item of chunk) {
            try {
                const data = await opcuaSession.read({
                    nodeId: item.nodeId,
                    attributeId: AttributeIds.Value,
                } as ReadValueIdOptions);

                if (data && data.statusCode && data.statusCode === StatusCodes.Good) {
                    validatedItems.push(item);
                    console.log(`Node validated for monitoring: ${item.nodeId}`);
                } else {
                    console.warn(`Skipping invalid/unsupported node: ${item.nodeId} (status=${data?.statusCode?.toString()})`);
                }
            } catch (err) {
                const errMsg = (err instanceof Error) ? err.message : String(err);
                console.warn(`Read test failed for ${item.nodeId}: ${errMsg}. Skipping.`);
            }
        }

        if (validatedItems.length === 0) {
            console.log(`No valid items in group ${groupIndex}, skipping subscription creation.`);
            continue;
        }

        try {
            const subscription = await opcuaSession.createSubscription2(subscriptionOptions);

            const monitoredGroup = ClientMonitoredItemGroup.create(
                subscription,
                validatedItems,
                optionsGroup,
                TimestampsToReturn.Neither
            );

            monitoredGroup.on('initialized', () => {
                console.log(`Monitored group ${groupIndex} initialized with ${validatedItems.length} items`);
            });

            monitoredGroup.on('changed', (monitoredItem: ClientMonitoredItemBase, dataValue: DataValue) => {
                handleMonitoredItemChange(monitoredItem, dataValue);
            });

            // Force sampling mode (helps some servers)
            try {
                await monitoredGroup.setMonitoringMode(MonitoringMode.Sampling);
                console.log(`Monitored group ${groupIndex} set to Sampling mode`);
            } catch (setModeErr) {
                console.warn(`Failed to set Monitoring mode for group ${groupIndex}:`, setModeErr);
            }

        } catch (err) {
            console.error(`Failed to create subscription/monitored group ${groupIndex}:`, err);
        }
    }
}

function decipherOpcuaValue(data: any): any {
    const decipheredValue =
        data?.value?.arrayType === VariantArrayType.Array
            ? Array.from(data.toJSON().value.value)
            : (data?.toJSON()?.value?.value);
    return decipheredValue;
}

async function handleMonitoredItemChange(monitoredItem: ClientMonitoredItemBase, dataValue: DataValue): Promise<void> {
    try {
        const newValue = decipherOpcuaValue(dataValue);
        const fullNodeId = monitoredItem.itemToMonitor?.nodeId?.toString
            ? monitoredItem.itemToMonitor.nodeId.toString()
            : String(monitoredItem.itemToMonitor?.nodeId);

        const nodeId = fullNodeId.replace(nodeListPrefix, '');
        console.log(`Monitored item changed - NodeId: ${nodeId}, New Value:`, newValue);

    } catch (error) {
        console.error('Error processing monitored item change:', error);
    }
}

async function main() {
    const session = await initializeOpcuaClient(opcuaEndpoint);

    if (session) {
        // Print server capabilities to help debug subscription issues
        await printServerCapabilities(session);

        console.log("Starting subscription setup...");
        await subscribeToMonitoredItems(session);

        // Keep-alive logging
        setInterval(() => {
            console.log(`OPC UA Session active: ${session.lastResponseReceivedTime ? session.lastResponseReceivedTime.toISOString() : 'N/A'}`);
            console.log(`Current subscriptions in client: ${session.subscriptionCount || 0}`);
        }, 5000);
    } else {
        console.error("Main execution stopped because OPC UA session could not be established.");
    }
}

// Execute the main function
main().catch(console.error);