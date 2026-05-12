import {
    AttributeIds,
    BrowseDescriptionLike,
    BrowseDirection,
    ClientSession,
    DataValue,
    MessageSecurityMode,
    NodeClass,
    OPCUAClient,
    OPCUAClientOptions,
    ObjectIds,
    QualifiedNameLike,
    ReferenceDescription,
    SecurityPolicy,
    StatusCodes,
} from 'node-opcua';

import 'dotenv/config';

const enumLabel = process.argv[2] || 'DeviceIds';
const opcuaEndpoint = `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;

const opcuaOptions: OPCUAClientOptions = {
    applicationName: 'OpcuaEnumInspector',
    securityMode: MessageSecurityMode.None,
    securityPolicy: SecurityPolicy.None,
    endpointMustExist: true,
    keepSessionAlive: true,
};

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

async function browseChildren(session: ClientSession, nodeId: string): Promise<ReferenceDescription[]> {
    const browseDescription: BrowseDescriptionLike = {
        browseDirection: BrowseDirection.Forward,
        includeSubtypes: true,
        nodeClassMask: NodeClass.Object | NodeClass.DataType | NodeClass.Variable,
        nodeId,
        referenceTypeId: 'HierarchicalReferences',
        resultMask: 0x3f,
    };

    const result = await session.browse(browseDescription);
    return result.references ?? [];
}

async function findEnumDataTypeNode(session: ClientSession, label: string): Promise<ReferenceDescription | null> {
    const visited = new Set<string>();
    const queue: string[] = [`ns=0;i=${ObjectIds.DataTypesFolder}`];

    while (queue.length > 0) {
        const currentNodeId = queue.shift();
        if (!currentNodeId || visited.has(currentNodeId)) {
            continue;
        }

        visited.add(currentNodeId);
        const references = await browseChildren(session, currentNodeId);

        for (const reference of references) {
            const browseName = reference.browseName as QualifiedNameLike | undefined;
            const browseNameText = typeof browseName === "string" ? browseName : (browseName?.name || "");
            const referenceNodeId = reference.nodeId.toString();

            if (reference.nodeClass === NodeClass.DataType && browseNameText === label) {
                return reference;
            }

            if (
                reference.nodeClass === NodeClass.Object
                || reference.nodeClass === NodeClass.DataType
            ) {
                queue.push(referenceNodeId);
            }
        }
    }

    return null;
}

async function readNodeValue(session: ClientSession, nodeId: string): Promise<DataValue> {
    return session.read({
        attributeId: AttributeIds.Value,
        nodeId,
    });
}

function printEnumStrings(enumStringsValue: unknown): boolean {
    if (!Array.isArray(enumStringsValue)) {
        return false;
    }

    console.log('EnumStrings:');
    enumStringsValue.forEach((entry, index) => {
        const text = typeof entry === 'string'
            ? entry
            : (entry as { text?: string } | null)?.text ?? String(entry);
        console.log(`${index}: ${text}`);
    });
    return true;
}

function printEnumValues(enumValuesValue: unknown): boolean {
    if (!Array.isArray(enumValuesValue)) {
        return false;
    }

    console.log('EnumValues:');
    enumValuesValue.forEach((entry) => {
        const candidate = entry as {
            displayName?: { text?: string } | string;
            value?: number | bigint | [number, number];
        } | null;

        const displayName = typeof candidate?.displayName === 'string'
            ? candidate.displayName
            : candidate?.displayName?.text ?? 'UNKNOWN';

        const rawValue = candidate?.value;
        const numericValue = Array.isArray(rawValue)
            ? Number(rawValue[1])
            : typeof rawValue === 'bigint'
                ? Number(rawValue)
                : rawValue;

        console.log(`${numericValue}: ${displayName}`);
    });
    return true;
}

async function printEnumMetadata(session: ClientSession, enumReference: ReferenceDescription): Promise<void> {
    const enumNodeId = enumReference.nodeId.toString();
    console.log(`Found enum DataType ${enumLabel} at ${enumNodeId}`);

    const children = await browseChildren(session, enumNodeId);
    const enumValuesNode = children.find((child) => child.browseName.name === 'EnumValues');
    const enumStringsNode = children.find((child) => child.browseName.name === 'EnumStrings');

    let foundMetadata = false;

    if (enumValuesNode) {
        const dataValue = await readNodeValue(session, enumValuesNode.nodeId.toString());
        if (dataValue.statusCode === StatusCodes.Good) {
            foundMetadata = printEnumValues(dataValue.value.value) || foundMetadata;
        }
    }

    if (enumStringsNode) {
        const dataValue = await readNodeValue(session, enumStringsNode.nodeId.toString());
        if (dataValue.statusCode === StatusCodes.Good) {
            foundMetadata = printEnumStrings(dataValue.value.value) || foundMetadata;
        }
    }

    if (!foundMetadata) {
        console.warn(`No EnumValues or EnumStrings metadata was exposed for ${enumLabel}.`);
    }
}

async function main(): Promise<void> {
    const connection = await initializeOpcuaClient(opcuaEndpoint);
    if (!connection) {
        process.exitCode = 1;
        return;
    }

    const { client, session } = connection;

    try {
        const enumReference = await findEnumDataTypeNode(session, enumLabel);
        if (!enumReference) {
            console.error(`Could not find enum DataType named ${enumLabel} under the OPC UA DataTypes tree.`);
            process.exitCode = 1;
            return;
        }

        await printEnumMetadata(session, enumReference);
    } finally {
        await session.close();
        await client.disconnect();
    }
}

void main().catch((error) => {
    console.error('Enum inspection failed:', error);
    process.exitCode = 1;
});