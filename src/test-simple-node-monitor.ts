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
    BrowseDescriptionLike,
    BrowseDirection,
    MonitoredItem,
    ClientMonitoredItem,
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
};

const PUBLISHING_INTERVAL = 1000; // in ms

const subscriptionOptions: CreateSubscriptionRequestOptions = {
    maxNotificationsPerPublish: 2000,
    publishingEnabled: true,
    requestedLifetimeCount: 100,
    requestedMaxKeepAliveCount: 10,
    requestedPublishingInterval: PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
};

const filter = new DataChangeFilter({
    trigger: DataChangeTrigger.StatusValueTimestamp, // Report on any of Status, Value, or Timestamp
    deadbandType: DeadbandType.None,                 // No deadband suppression
    deadbandValue: 0
});

const optionsGroup: MonitoringParametersOptions = {
    discardOldest: true,
    queueSize: 1,
    samplingInterval: PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
    filter
};

const MAX_ITEMS_PER_GROUP = 100; // you found 100 as the practical limit; keep here for easy adjust


async function initializeOpcuaClient(opcuaEndpoint: string): Promise<ClientSession | null> {
    const opcuaClient = OPCUAClient.create(opcuaOptions);

    opcuaClient.on("backoff", (retry, delay) => {
        console.log(`OPC UA connection failed, retrying #${retry} in ${delay}ms`);
    });

    opcuaClient.on("connection_lost", () => {
        console.warn("OPC UA connection lost");
        //this.updateOpcuaConnectionStatus(false);
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
        //this.codesysOpcuaDriver = new CodesysOpcuaDriver(DeviceId.HMI, this.opcuaSession, this.opcuaControllerName);
        // console.log("✅ CODESYS OPC UA driver created");
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
    return items;
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

        // Validate nodes in chunk (read test). Build a filtered array of valid items.
        const validatedItems: { attributeId: number; nodeId: string }[] = [];
        for (const item of chunk) {
            try {
                const data = await opcuaSession.read({
                    nodeId: item.nodeId,
                    attributeId: AttributeIds.Value,
                } as ReadValueIdOptions);

                if (data && data.statusCode && data.statusCode === StatusCodes.Good) {
                    // good to monitor
                    validatedItems.push(item);
                } else {
                    console.warn(`Skipping invalid/unsupported node for monitoring: ${item.nodeId} (status=${data?.statusCode?.toString()})`);
                }
            } catch (err) {
                // If read fails with BadNotSupported or other issue, skip the item gracefully
                const errMsg = (err instanceof Error) ? err.message : String(err);
                console.warn(`Read test failed for ${item.nodeId}: ${errMsg}. Skipping monitoring for this node.`);
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
                TimestampsToReturn.Both
            );

            monitoredGroup.on('initialized', () => {
                console.log(`Monitored group ${groupIndex} initialized with ${validatedItems.length} items`);
            });

            monitoredGroup.on('changed', (monitoredItem: ClientMonitoredItemBase, dataValue: DataValue) => {
                handleMonitoredItemChange(monitoredItem, dataValue);
            });

            // ensure reporting mode is set (forces notifications)
            try {
                await monitoredGroup.setMonitoringMode(MonitoringMode.Reporting);
                console.log(`Monitored group ${groupIndex} set to Reporting mode`);
            } catch (setModeErr) {
                console.warn(`Failed to set Reporting mode for group ${groupIndex}:`, setModeErr);
            }

        } catch (err) {
            console.error(`Failed to create subscription/monitored group ${groupIndex}:`, err);
        }
    }

    //console.log(`✅ Subscribed via ${this.opcuaSubscriptions.length} subscriptions and ${this.monitoredItemGroups.length} monitored groups`);
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
        // monitoredItem.itemToMonitor.nodeId may be a NodeId object or string; ensure string
        const fullNodeId = monitoredItem.itemToMonitor?.nodeId?.toString ? monitoredItem.itemToMonitor.nodeId.toString() : String(monitoredItem.itemToMonitor?.nodeId);
        // strip away Node Id Down to just namespace and tag
        const strippedPrefix = nodeListPrefix.replace('ns=4;s=', '');
        const nodeId = fullNodeId.replace(nodeListPrefix, '');
        //const topic = this.nodeIdToMqttTopicMap.get(nodeId);
        console.log(`Monitored item changed - NodeId: ${nodeId}, New Value:`, newValue);

    } catch (error) {
        console.error('Error processing monitored item change:', error);
    }
}
// --- ADDED CODE TO KEEP ALIVE & RUN MAIN FUNCTION ---

// Add a simple heartbeat logger to confirm the script is active
setInterval(() => {
    // console.log(`[HEARTBEAT] Script is alive at ${new Date().toISOString()}`);
    process.stdout.write('.'); // Print a dot to keep terminal active without spamming lines
}, 1000);


async function main() {
    // 1. Get the session, and WAIT for the promise to resolve
    const session = await initializeOpcuaClient(opcuaEndpoint);

    // 2. Check if a valid session was returned (in case of connection failure)
    if (session) {
        // 3. NOW you can call the subscription function with the valid session object
        await subscribeToMonitoredItems(session);
        // The script will now stay alive due to the open session/subscriptions
    } else {
        console.error("Main execution stopped because OPC UA session could not be established.");
        // If the session fails to initialize, the script will naturally exit here.
    }
}

// Execute the main function
main().catch(console.error);