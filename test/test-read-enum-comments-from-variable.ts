import {
    AttributeIds,
    BrowseDirection,
    ClientSession,
    NodeClass,
    OPCUAClient,
    OPCUAClientOptions,
    StatusCodes,
} from 'node-opcua';

import 'dotenv/config';

import Config, { createSharedOpcuaClientOptions } from '../src/shared/config';

interface EnumMetadataEntry {
    description: string | null;
    label: string;
    value: number | null;
}

const variableNodeId = process.argv[2] ?? '';
const opcuaEndpoint = Config.OPCUA_ENDPOINT || `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;

const opcuaOptions: OPCUAClientOptions = createSharedOpcuaClientOptions();

function printUsage(): void {
    console.log([
        'Usage: npm run test-read-enum-comments-from-variable -- <node-id>',
        '',
        'Example:',
        '  npm run test-read-enum-comments-from-variable -- "ns=4;s=|var|CODESYS Control for Linux SL.Application.SomeProgram.myEnumVar"',
    ].join('\n'));
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

function toEnumValue(rawValue: number | bigint | [number, number] | undefined): number | null {
    if (Array.isArray(rawValue)) {
        return Number(rawValue[1]);
    }

    if (typeof rawValue === 'bigint') {
        return Number(rawValue);
    }

    return typeof rawValue === 'number' ? rawValue : null;
}

function parseEnumValues(rawValue: unknown): EnumMetadataEntry[] {
    if (!Array.isArray(rawValue)) {
        return [];
    }

    return rawValue.map((entry) => {
        const candidate = entry as {
            description?: { text?: string } | string;
            displayName?: { text?: string } | string;
            value?: number | bigint | [number, number];
        } | null;

        return {
            description: typeof candidate?.description === 'string'
                ? candidate.description
                : candidate?.description?.text ?? null,
            label: typeof candidate?.displayName === 'string'
                ? candidate.displayName
                : candidate?.displayName?.text ?? 'UNKNOWN',
            value: toEnumValue(candidate?.value),
        };
    });
}

function parseEnumStrings(rawValue: unknown): EnumMetadataEntry[] {
    if (!Array.isArray(rawValue)) {
        return [];
    }

    return rawValue.map((entry, index) => ({
        description: null,
        label: typeof entry === 'string'
            ? entry
            : (entry as { text?: string } | null)?.text ?? String(entry),
        value: index,
    }));
}

async function readEnumEntriesForVariable(session: ClientSession, nodeId: string): Promise<{
    currentValue: unknown;
    dataTypeNodeId: string;
    entries: EnumMetadataEntry[];
    source: 'EnumValues' | 'EnumStrings';
} | null> {
    const [valueDataValue, dataTypeDataValue] = await session.read([
        {
            attributeId: AttributeIds.Value,
            nodeId,
        },
        {
            attributeId: AttributeIds.DataType,
            nodeId,
        },
    ]);

    if (valueDataValue.statusCode !== StatusCodes.Good) {
        throw new Error(`Failed to read Value for ${nodeId}: ${valueDataValue.statusCode.toString()}`);
    }

    if (dataTypeDataValue.statusCode !== StatusCodes.Good) {
        throw new Error(`Failed to read DataType for ${nodeId}: ${dataTypeDataValue.statusCode.toString()}`);
    }

    const dataTypeNodeId = dataTypeDataValue.value.value?.toString();
    if (!dataTypeNodeId) {
        throw new Error(`Variable ${nodeId} did not expose a DataType node id.`);
    }

    const browseResult = await session.browse({
        browseDirection: BrowseDirection.Forward,
        includeSubtypes: true,
        nodeClassMask: NodeClass.Variable,
        nodeId: dataTypeNodeId,
        referenceTypeId: 'HasProperty',
        resultMask: 0x3f,
    });

    const references = browseResult.references ?? [];
    const enumValuesNode = references.find((reference) => reference.browseName.name === 'EnumValues');
    const enumStringsNode = references.find((reference) => reference.browseName.name === 'EnumStrings');

    const metadataNode = enumValuesNode ?? enumStringsNode;
    if (!metadataNode) {
        return null;
    }

    const metadataValue = await session.read({
        attributeId: AttributeIds.Value,
        nodeId: metadataNode.nodeId.toString(),
    });

    if (metadataValue.statusCode !== StatusCodes.Good) {
        throw new Error(`Failed to read ${metadataNode.browseName.name} for ${dataTypeNodeId}: ${metadataValue.statusCode.toString()}`);
    }

    const source = enumValuesNode ? 'EnumValues' : 'EnumStrings';
    return {
        currentValue: valueDataValue.value.value,
        dataTypeNodeId,
        entries: source === 'EnumValues'
            ? parseEnumValues(metadataValue.value.value)
            : parseEnumStrings(metadataValue.value.value),
        source,
    };
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
        const metadata = await readEnumEntriesForVariable(session, variableNodeId);
        if (!metadata) {
            console.error(`Could not find EnumValues or EnumStrings on the DataType for ${variableNodeId}.`);
            process.exitCode = 1;
            return;
        }

        console.log(`Variable node: ${variableNodeId}`);
        console.log(`Current raw value: ${String(metadata.currentValue)}`);
        console.log(`DataType node: ${metadata.dataTypeNodeId}`);
        console.log(`Source: ${metadata.source}`);

        const normalizedCurrentValue = typeof metadata.currentValue === 'bigint'
            ? Number(metadata.currentValue)
            : typeof metadata.currentValue === 'number'
                ? metadata.currentValue
                : Number(metadata.currentValue);

        const activeEntry = metadata.entries.find((entry) => entry.value === normalizedCurrentValue) ?? null;

        for (const entry of metadata.entries) {
            console.log('---');
            console.log(`value: ${entry.value}`);
            console.log(`label: ${entry.label}`);
            console.log(`description: ${entry.description ?? '<none>'}`);
        }

        console.log('---');
        console.log(`entries with descriptions: ${metadata.entries.filter((entry) => !!entry.description?.trim()).length}/${metadata.entries.length}`);

        if (activeEntry) {
            console.log('--- active ---');
            console.log(`value: ${activeEntry.value}`);
            console.log(`label: ${activeEntry.label}`);
            console.log(`description: ${activeEntry.description ?? '<none>'}`);
        } else {
            console.warn('Active value did not match any enum metadata entry.');
        }
    } finally {
        await session.close();
        await client.disconnect();
    }
}

void main().catch((error) => {
    console.error('Enum variable comment inspection failed:', error);
    process.exitCode = 1;
});