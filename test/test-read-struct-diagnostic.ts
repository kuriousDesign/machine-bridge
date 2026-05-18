import {
    AttributeIds,
    BrowseDirection,
    ClientSession,
    NodeClass,
    OPCUAClient,
    OPCUAClientOptions,
    QualifiedNameLike,
    ReadValueIdOptions,
    ReferenceDescription,
    StatusCodes,
} from 'node-opcua';

import 'dotenv/config';

import Config, { createSharedOpcuaClientOptions } from '../src/shared/config';

const variableNodeId = process.argv[2] ?? '';
const opcuaEndpoint = Config.OPCUA_ENDPOINT || `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;
const opcuaOptions: OPCUAClientOptions = createSharedOpcuaClientOptions();

function printUsage(): void {
    console.log([
        'Usage: npm run test-read-struct-diagnostic -- <node-id>',
        '',
        'Example:',
        '  npm run test-read-struct-diagnostic -- "ns=4;s=|var|CODESYS Control for Linux SL.Application.Machine.Devices[1].Is"',
    ].join('\n'));
}

function toQualifiedNameText(value: QualifiedNameLike | undefined): string {
    if (typeof value === 'string') {
        return value;
    }

    return value?.name ?? '';
}

async function initializeOpcuaClient(endpoint: string): Promise<{ client: OPCUAClient; session: ClientSession } | null> {
    const client = OPCUAClient.create(opcuaOptions);
    client.on('connection_lost', () => console.warn('OPC UA connection lost'));
    client.on('after_reconnection', () => console.log('OPC UA reconnected'));

    try {
        console.log(`Connecting to OPC UA endpoint: ${endpoint}`);
        await client.connect(endpoint);
        const session = await client.createSession();
        console.log('OPC UA session created');
        return { client, session };
    } catch (error) {
        console.error('Failed to create OPC UA session:', error);
        await client.disconnect();
        return null;
    }
}

async function readSingle(session: ClientSession, nodeId: string, attributeId: number) {
    return session.read({
        attributeId,
        nodeId,
    } as ReadValueIdOptions);
}

async function browseChildren(session: ClientSession, nodeId: string): Promise<ReferenceDescription[]> {
    const result = await session.browse({
        browseDirection: BrowseDirection.Forward,
        includeSubtypes: true,
        nodeClassMask: NodeClass.Object | NodeClass.Variable,
        nodeId,
        referenceTypeId: 'HierarchicalReferences',
        resultMask: 0x3f,
    });

    return result.references ?? [];
}

async function main(): Promise<void> {
    if (!variableNodeId) {
        printUsage();
        process.exitCode = 1;
        return;
    }

    const connection = await initializeOpcuaClient(opcuaEndpoint);
    if (!connection) {
        process.exitCode = 1;
        return;
    }

    const { client, session } = connection;

    try {
        const [valueResult, dataTypeResult] = await session.read([
            { attributeId: AttributeIds.Value, nodeId: variableNodeId } as ReadValueIdOptions,
            { attributeId: AttributeIds.DataType, nodeId: variableNodeId } as ReadValueIdOptions,
        ]);

        console.log(`Variable node: ${variableNodeId}`);
        console.log(`Value status: ${valueResult.statusCode.toString()}`);
        console.log(`DataType status: ${dataTypeResult.statusCode.toString()}`);

        const dataTypeNodeId = dataTypeResult.statusCode === StatusCodes.Good
            ? dataTypeResult.value.value?.toString() ?? null
            : null;

        console.log(`DataType node: ${dataTypeNodeId ?? '<none>'}`);

        if (dataTypeNodeId) {
            const [browseNameResult, definitionResult] = await session.read([
                { attributeId: AttributeIds.BrowseName, nodeId: dataTypeNodeId } as ReadValueIdOptions,
                { attributeId: AttributeIds.DataTypeDefinition, nodeId: dataTypeNodeId } as ReadValueIdOptions,
            ]);

            const dataTypeName = browseNameResult.statusCode === StatusCodes.Good
                ? toQualifiedNameText(browseNameResult.value.value)
                : '<unknown>';

            console.log(`DataType name: ${dataTypeName}`);
            console.log(`DataTypeDefinition status: ${definitionResult.statusCode.toString()}`);

            const typeChildren = await browseChildren(session, dataTypeNodeId);
            const typeChildNames = typeChildren.map((reference) => toQualifiedNameText(reference.browseName)).filter(Boolean);
            console.log(`DataType children: ${typeChildNames.length > 0 ? typeChildNames.join(', ') : '<none>'}`);
        }

        const children = await browseChildren(session, variableNodeId);
        const childNames = children.map((reference) => toQualifiedNameText(reference.browseName)).filter(Boolean);
        console.log(`Variable children (${childNames.length}): ${childNames.length > 0 ? childNames.join(', ') : '<none>'}`);

        const sampleChildren = children.slice(0, 5);
        for (const child of sampleChildren) {
            const childNodeId = child.nodeId.toString();
            const childName = toQualifiedNameText(child.browseName);
            const childValueResult = await readSingle(session, childNodeId, AttributeIds.Value);
            console.log('---');
            console.log(`Child: ${childName}`);
            console.log(`Child node: ${childNodeId}`);
            console.log(`Child value status: ${childValueResult.statusCode.toString()}`);
            if (childValueResult.statusCode === StatusCodes.Good) {
                console.log(`Child value: ${JSON.stringify(childValueResult.value.value)}`);
            }
        }
    } finally {
        await session.close();
        await client.disconnect();
    }
}

void main().catch((error) => {
    console.error('Struct diagnostic failed:', error);
    process.exitCode = 1;
});