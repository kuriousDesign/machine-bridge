import {
    AttributeIds,
    BrowseDescriptionLike,
    BrowseDirection,
    ClientMonitoredItemGroup,
    ClientSession,
    DataValue,
    MessageSecurityMode,
    NodeClass,
    OPCUAClient,
    OPCUAClientOptions,
    ReadValueIdOptions,
    ReferenceDescription,
    SecurityPolicy,
    StatusCodes,
    TimestampsToReturn,
    VariantArrayType,
    CreateSubscriptionRequestOptions,
    MonitoringParametersOptions,
    DataChangeFilter,
    DataChangeTrigger,
    DeadbandType,
} from 'node-opcua';

import 'dotenv/config';

import { nodeListString } from '@kuriousdesign/machine-sdk';

const OPCUA_CONTROLLER_NAME = process.env.OPCUA_CONTROLLER_NAME || 'DefaultController';
const OPCUA_ENDPOINT = `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;
const NODE_LIST_PREFIX = `${nodeListString}${OPCUA_CONTROLLER_NAME}.Application.`;
const TARGET_TAG = process.env.TEST_TAG_ID || 'Machine.Devices[1].MutedChildrenArray';
const TARGET_NODE_ID = process.env.TEST_NODE_ID || `${NODE_LIST_PREFIX}${TARGET_TAG}`;
const MONITOR_DURATION_MS = Number(process.env.TEST_MONITOR_DURATION_MS || 15000);
const TARGET_PARENT_TAG = TARGET_TAG.includes('.') ? TARGET_TAG.slice(0, TARGET_TAG.lastIndexOf('.')) : TARGET_TAG;
const TARGET_PARENT_NODE_ID = `${NODE_LIST_PREFIX}${TARGET_PARENT_TAG}`;

const opcuaOptions: OPCUAClientOptions = {
    applicationName: 'MutedChildrenArrayProbe',
    endpointMustExist: true,
    keepSessionAlive: true,
    securityMode: MessageSecurityMode.None,
    securityPolicy: SecurityPolicy.None,
};

const subscriptionOptions: CreateSubscriptionRequestOptions = {
    maxNotificationsPerPublish: 100,
    publishingEnabled: true,
    requestedLifetimeCount: 100,
    requestedMaxKeepAliveCount: 10,
    requestedPublishingInterval: 250,
};

const monitoringParameters: MonitoringParametersOptions = {
    discardOldest: true,
    filter: new DataChangeFilter({
        trigger: DataChangeTrigger.StatusValue,
        deadbandType: DeadbandType.None,
        deadbandValue: 0,
    }),
    queueSize: 10,
    samplingInterval: 250,
};

function formatValue(dataValue: DataValue): unknown {
    if (dataValue.value.arrayType === VariantArrayType.Array) {
        return Array.from((dataValue.value.value as Iterable<unknown>) ?? []);
    }

    return dataValue.value.value;
}

function logReadResult(label: string, dataValue: DataValue): void {
    console.log(`\n[${label}] status=${dataValue.statusCode.toString()}`);

    if (dataValue.statusCode !== StatusCodes.Good) {
        return;
    }

    console.log(`[${label}] dataType=${dataValue.value.dataType}`);
    console.log(`[${label}] arrayType=${dataValue.value.arrayType}`);
    console.log(`[${label}] value=`, formatValue(dataValue));
}

async function browseChildren(session: ClientSession, nodeId: string): Promise<ReferenceDescription[]> {
    const browseDescription: BrowseDescriptionLike = {
        browseDirection: BrowseDirection.Forward,
        includeSubtypes: true,
        nodeClassMask: NodeClass.Object | NodeClass.Variable,
        nodeId,
        referenceTypeId: 'HierarchicalReferences',
        resultMask: 0x3f,
    };

    const result = await session.browse(browseDescription);
    return result.references ?? [];
}

function printReferences(label: string, references: ReferenceDescription[]): void {
    console.log(`\n[BROWSE] ${label}: ${references.length} child reference(s)`);

    references.forEach((reference, index) => {
        const browseName = typeof reference.browseName === 'string'
            ? reference.browseName
            : (reference.browseName?.name ?? 'unknown');
        console.log(
            `[BROWSE]   [${index + 1}/${references.length}] browseName=${browseName} ` +
            `nodeClass=${reference.nodeClass?.toString() ?? 'unknown'} nodeId=${reference.nodeId.toString()}`,
        );
    });
}

async function browseTagContext(session: ClientSession): Promise<void> {
    console.log(`\n[BROWSE] parentTag=${TARGET_PARENT_TAG}`);
    console.log(`[BROWSE] parentNodeId=${TARGET_PARENT_NODE_ID}`);

    const parentReferences = await browseChildren(session, TARGET_PARENT_NODE_ID);
    printReferences('Parent children', parentReferences);

    const mutedMatches = parentReferences.filter((reference) => {
        const browseName = typeof reference.browseName === 'string'
            ? reference.browseName
            : (reference.browseName?.name ?? '');
        return browseName.toLowerCase().includes('muted');
    });

    if (mutedMatches.length > 0) {
        printReferences('Muted-related children', mutedMatches);
    } else {
        console.log('[BROWSE] no muted-related children found beneath parent node');
    }
}

async function connectSession(): Promise<{ client: OPCUAClient; session: ClientSession; }> {
    const client = OPCUAClient.create(opcuaOptions);

    client.on('backoff', (retry, delay) => {
        console.log(`[OPCUA] reconnect retry=${retry} delayMs=${delay}`);
    });

    console.log(`[OPCUA] connecting to ${OPCUA_ENDPOINT}`);
    await client.connect(OPCUA_ENDPOINT);
    console.log('[OPCUA] client connected');

    const session = await client.createSession();
    console.log('[OPCUA] session created');

    return { client, session };
}

async function readNodeDiagnostics(session: ClientSession): Promise<void> {
    const requests: ReadValueIdOptions[] = [
        { nodeId: TARGET_NODE_ID, attributeId: AttributeIds.Value },
        { nodeId: TARGET_NODE_ID, attributeId: AttributeIds.DataType },
        { nodeId: TARGET_NODE_ID, attributeId: AttributeIds.ValueRank },
        { nodeId: TARGET_NODE_ID, attributeId: AttributeIds.ArrayDimensions },
        { nodeId: TARGET_NODE_ID, attributeId: AttributeIds.DisplayName },
    ];

    const [value, dataType, valueRank, arrayDimensions, displayName] = await session.read(requests);

    logReadResult('VALUE', value);
    logReadResult('DATA_TYPE', dataType);
    logReadResult('VALUE_RANK', valueRank);
    logReadResult('ARRAY_DIMENSIONS', arrayDimensions);
    logReadResult('DISPLAY_NAME', displayName);
}

async function monitorNode(session: ClientSession): Promise<void> {
    console.log(`\n[MONITOR] creating subscription for ${TARGET_NODE_ID}`);
    const subscription = await session.createSubscription2(subscriptionOptions);

    const monitoredGroup = ClientMonitoredItemGroup.create(
        subscription,
        [{ nodeId: TARGET_NODE_ID, attributeId: AttributeIds.Value }],
        monitoringParameters,
        TimestampsToReturn.Both,
    );

    monitoredGroup.on('initialized', () => {
        console.log('[MONITOR] monitored group initialized');
    });

    monitoredGroup.on('changed', (_monitoredItem, dataValue) => {
        console.log('[MONITOR] change received');
        logReadResult('MONITOR_VALUE', dataValue);
    });

    monitoredGroup.on('err', (errorMessage: string) => {
        console.error(`[MONITOR] monitored group error: ${errorMessage}`);
    });

    subscription.on('started', () => {
        console.log(`[MONITOR] subscription started id=${subscription.subscriptionId}`);
    });

    subscription.on('keepalive', () => {
        console.log('[MONITOR] keepalive');
    });

    subscription.on('internal_error', (error) => {
        console.error('[MONITOR] subscription internal error:', error instanceof Error ? error.message : error);
    });

    console.log(`[MONITOR] waiting ${MONITOR_DURATION_MS}ms for notifications`);
    await new Promise((resolve) => setTimeout(resolve, MONITOR_DURATION_MS));

    await subscription.terminate();
    console.log('[MONITOR] subscription terminated');
}

async function main(): Promise<void> {
    console.log(`[TEST] targetTag=${TARGET_TAG}`);
    console.log(`[TEST] targetNodeId=${TARGET_NODE_ID}`);

    const { client, session } = await connectSession();

    try {
        await readNodeDiagnostics(session);
        await browseTagContext(session);
        await monitorNode(session);
    } finally {
        await session.close();
        await client.disconnect();
        console.log('[OPCUA] session closed and client disconnected');
    }
}

main().catch((error) => {
    console.error('[TEST] failed:', error instanceof Error ? error.stack ?? error.message : error);
    process.exitCode = 1;
});