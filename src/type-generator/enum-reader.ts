import {
    AttributeIds,
    BrowseDescriptionLike,
    BrowseDirection,
    ClientSession,
    DataValue,
    NodeClass,
    ObjectIds,
    QualifiedNameLike,
    ReferenceDescription,
    StatusCodes,
} from 'node-opcua';

export interface OpcuaEnumEntry {
    description?: string | null;
    label: string;
    value: number | null;
}

export interface OpcuaEnumMetadata {
    entries: OpcuaEnumEntry[];
    nodeId: string;
    source: 'EnumStrings' | 'EnumValues';
}

function toQualifiedNameText(value: QualifiedNameLike | undefined): string {
    if (typeof value === 'string') {
        return value;
    }

    return value?.name ?? '';
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

async function readNodeValue(session: ClientSession, nodeId: string): Promise<DataValue> {
    return session.read({
        attributeId: AttributeIds.Value,
        nodeId,
    });
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

function parseEnumValues(enumValuesValue: unknown): OpcuaEnumEntry[] {
    if (!Array.isArray(enumValuesValue)) {
        return [];
    }

    return enumValuesValue.map((entry) => {
        const candidate = entry as {
            description?: { text?: string } | string;
            displayName?: { text?: string } | string;
            value?: number | bigint | [number, number];
        } | null;

        const label = typeof candidate?.displayName === 'string'
            ? candidate.displayName
            : candidate?.displayName?.text ?? 'UNKNOWN';
        const description = typeof candidate?.description === 'string'
            ? candidate.description
            : candidate?.description?.text ?? null;

        return {
            description,
            label,
            value: toEnumValue(candidate?.value),
        };
    });
}

function parseEnumStrings(enumStringsValue: unknown): OpcuaEnumEntry[] {
    if (!Array.isArray(enumStringsValue)) {
        return [];
    }

    return enumStringsValue.map((entry, index) => {
        const label = typeof entry === 'string'
            ? entry
            : (entry as { text?: string } | null)?.text ?? String(entry);

        return {
            description: null,
            label,
            value: index,
        };
    });
}

export async function findEnumDataTypeNode(session: ClientSession, label: string): Promise<ReferenceDescription | null> {
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
            const browseNameText = toQualifiedNameText(reference.browseName);
            const referenceNodeId = reference.nodeId.toString();

            if (reference.nodeClass === NodeClass.DataType && browseNameText === label) {
                return reference;
            }

            if (reference.nodeClass === NodeClass.Object || reference.nodeClass === NodeClass.DataType) {
                queue.push(referenceNodeId);
            }
        }
    }

    return null;
}

export async function readEnumMetadata(session: ClientSession, enumLabel: string): Promise<OpcuaEnumMetadata | null> {
    const enumReference = await findEnumDataTypeNode(session, enumLabel);
    if (!enumReference) {
        return null;
    }

    const enumNodeId = enumReference.nodeId.toString();
    const children = await browseChildren(session, enumNodeId);
    const enumValuesNode = children.find((child) => toQualifiedNameText(child.browseName) === 'EnumValues');
    const enumStringsNode = children.find((child) => toQualifiedNameText(child.browseName) === 'EnumStrings');

    if (enumValuesNode) {
        const dataValue = await readNodeValue(session, enumValuesNode.nodeId.toString());
        if (dataValue.statusCode === StatusCodes.Good) {
            const entries = parseEnumValues(dataValue.value.value);
            if (entries.length > 0) {
                return {
                    entries,
                    nodeId: enumNodeId,
                    source: 'EnumValues',
                };
            }
        }
    }

    if (enumStringsNode) {
        const dataValue = await readNodeValue(session, enumStringsNode.nodeId.toString());
        if (dataValue.statusCode === StatusCodes.Good) {
            const entries = parseEnumStrings(dataValue.value.value);
            if (entries.length > 0) {
                return {
                    entries,
                    nodeId: enumNodeId,
                    source: 'EnumStrings',
                };
            }
        }
    }

    return {
        entries: [],
        nodeId: enumNodeId,
        source: enumValuesNode ? 'EnumValues' : 'EnumStrings',
    };
}