import { promises as fs } from 'fs';
import path from 'path';

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
    QualifiedNameLike,
    ReadValueIdOptions,
    ReferenceDescription,
    SecurityPolicy,
    StatusCodes,
} from 'node-opcua';

import 'dotenv/config';

import { nodeListString } from '@kuriousdesign/machine-sdk';
import Config, { createSharedOpcuaClientOptions } from '../shared/config';
import { OpcuaEnumEntry, readEnumMetadata, readEnumMetadataByNodeId } from './enum-reader';

type SchemaNode = ArraySchemaNode | LeafSchemaNode | ObjectSchemaNode;

interface CliOptions {
    controllerName: string;
    endpoint: string;
    machineId: string | null;
    outputPath: string;
    tagsOutputPath: string;
    rootNodeId: string;
    rootTag: string;
    rootTypeName: string;
}

interface GenerationTarget {
    outputPath: string;
    rootNodeId: string;
    rootTag: string;
    rootTypeName: string;
    tagsOutputPath: string;
}

interface BaseSchemaNode {
    browseName: string;
    dataTypeName: string | null;
    dataTypeNodeId: string | null;
    enumEntries?: OpcuaEnumEntry[];
    nodeId: string;
}

interface ArraySchemaNode extends BaseSchemaNode {
    element: SchemaNode;
    kind: 'array';
}

interface LeafSchemaNode extends BaseSchemaNode {
    isArray: boolean;
    kind: 'leaf';
    tsType: string;
}

interface ObjectSchemaNode extends BaseSchemaNode {
    children: SchemaNode[];
    isArray: boolean;
    kind: 'object';
}

interface ChildReferenceInfo {
    browseName: string;
    nodeClass: NodeClass | undefined;
    nodeId: string;
}

interface EnumFieldLike {
    name?: string | null;
    value?: unknown;
}

interface EnumDefinitionLike {
    fields?: EnumFieldLike[] | null;
}

interface StructureFieldLike {
    arrayDimensions?: number[] | null;
    dataType?: { toString(): string } | null;
    name?: string | null;
    valueRank?: number;
}

interface StructureDefinitionLike {
    baseDataType?: unknown;
    fields?: StructureFieldLike[] | null;
    structureType?: unknown;
}

interface GeneratedTagNode {
    children?: Record<string, GeneratedTagNode>;
    items?: GeneratedTagNode[];
    tag: string;
}

const DEFAULT_ROOT_TAG = process.env.MACHINE_TYPE_ROOT_TAG ?? process.env.MACHINE_DEVICE_TAG ?? 'Machine';
const DEFAULT_ROOT_TYPE_NAME = 'Machine';
const DEFAULT_OUTPUT_PATH = path.resolve(__dirname, '../../generated-types/shared/types.ts');
const DEFAULT_TAGS_OUTPUT_PATH = path.resolve(__dirname, '../../generated-types/shared/tags.ts');
const ARRAY_HELPER_BROWSE_NAMES = new Set(['Dimensions', 'IndexMin', 'IndexMax']);
const MACHINE_RUNTIME_EXCLUDED_KEYS = [
    'SyncClockReq',
    'ResetEthercat_REQ',
    'DeviceLogs',
    'RegisteredInputs',
    'Utilities',
    'RegisteredDevices',
    'Devices',
    'DeviceRegisteredActions',
    'SysFb',
] as const;
let activeGeneratedMachineId: string | null = null;

const opcuaOptions: OPCUAClientOptions = createSharedOpcuaClientOptions();

function printUsage(): void {
    console.log([
        'Usage: ts-node src/type-generator/generate-machine-types.ts [options]',
        '       ts-node src/type-generator/generate-machine-types.ts [machine-id] [options]',
        '',
        'Options:',
        '  --endpoint=<opc.tcp://host:port>   Override OPC UA endpoint',
        '  --controller=<name>               Override OPCUA_CONTROLLER_NAME',
        '  --tag=<Machine>                   Root tag to inspect',
        '  --node-id=<ns=4;s=...>            Root OPC UA node id to inspect',
        '  --output=<path>                   Type file to generate',
        '  --tags-output=<path>              Tag tree file to generate',
        '  --type-name=<Machine>             Exported root interface name',
        '  --machine-id=<00225>              Also generate Machine_<id> artifacts',
        '  00225                             Positional shorthand for --machine-id=00225',
        '  --help                            Show this message',
        '',
        `Default tag: ${DEFAULT_ROOT_TAG}`,
        `Default output: ${DEFAULT_OUTPUT_PATH}`,
        `Default tags output: ${DEFAULT_TAGS_OUTPUT_PATH}`,
    ].join('\n'));
}

function parseArgValue(argument: string, name: string): string | null {
    const prefix = `${name}=`;
    return argument.startsWith(prefix) ? argument.slice(prefix.length) : null;
}

function parseMachineId(argv: string[]): string | null {
    const explicitMachineId = argv
        .map((argument) => parseArgValue(argument, '--machine-id'))
        .find((value): value is string => !!value);

    if (explicitMachineId) {
        return explicitMachineId;
    }

    return argv.find((argument) => !argument.startsWith('--')) ?? null;
}

function buildEndpoint(): string {
    const ip = process.env.OPCUA_SERVER_IP_ADDRESS;
    const port = process.env.OPCUA_PORT;

    if (!ip || !port) {
        throw new Error('Missing OPCUA_SERVER_IP_ADDRESS or OPCUA_PORT environment variable');
    }

    return `opc.tcp://${ip}:${port}`;
}

function toNodeId(rootTag: string, controllerName: string): string {
    return `${nodeListString}${controllerName}.Application.${rootTag}`;
}

function toMachineSpecificRootTag(machineId: string): string {
    return `Machine_${machineId}`;
}

function toMachineSpecificTypeName(machineId: string): string {
    return sanitizeTypeName(toMachineSpecificRootTag(machineId));
}

function toMachineSpecificOutputPath(machineId: string): string {
    return path.resolve(__dirname, `../../generated-types/${machineId}/types.ts`);
}

function toMachineSpecificTagsOutputPath(machineId: string): string {
    return path.resolve(__dirname, `../../generated-types/${machineId}/tags.ts`);
}

function parseOptions(argv: string[]): CliOptions | null {
    if (argv.includes('--help')) {
        printUsage();
        return null;
    }

    const controllerName = argv
        .map((argument) => parseArgValue(argument, '--controller'))
        .find((value): value is string => !!value)
        ?? process.env.OPCUA_CONTROLLER_NAME
        ?? 'DefaultController';

    const rootTag = argv
        .map((argument) => parseArgValue(argument, '--tag'))
        .find((value): value is string => !!value)
        ?? DEFAULT_ROOT_TAG;

    const rootNodeId = argv
        .map((argument) => parseArgValue(argument, '--node-id'))
        .find((value): value is string => !!value)
        ?? toNodeId(rootTag, controllerName);

    const endpoint = argv
        .map((argument) => parseArgValue(argument, '--endpoint'))
        .find((value): value is string => !!value)
        ?? Config.OPCUA_ENDPOINT
        ?? buildEndpoint();

    const machineId = parseMachineId(argv);

    const outputPath = path.resolve(
        argv
            .map((argument) => parseArgValue(argument, '--output'))
            .find((value): value is string => !!value)
            ?? DEFAULT_OUTPUT_PATH,
    );

    const tagsOutputPath = path.resolve(
        argv
            .map((argument) => parseArgValue(argument, '--tags-output'))
            .find((value): value is string => !!value)
            ?? DEFAULT_TAGS_OUTPUT_PATH,
    );

    const rootTypeName = argv
        .map((argument) => parseArgValue(argument, '--type-name'))
        .find((value): value is string => !!value)
        ?? DEFAULT_ROOT_TYPE_NAME;

    return {
        controllerName,
        endpoint,
        machineId,
        outputPath,
        tagsOutputPath,
        rootNodeId,
        rootTag,
        rootTypeName,
    };
}

function buildGenerationTargets(options: CliOptions): GenerationTarget[] {
    if (options.machineId) {
        const machineRootTag = toMachineSpecificRootTag(options.machineId);

        return [{
        outputPath: toMachineSpecificOutputPath(options.machineId),
        rootNodeId: toNodeId(machineRootTag, options.controllerName),
        rootTag: machineRootTag,
        rootTypeName: toMachineSpecificTypeName(options.machineId),
        tagsOutputPath: toMachineSpecificTagsOutputPath(options.machineId),
        }];
    }

    return [{
        outputPath: options.outputPath,
        rootNodeId: options.rootNodeId,
        rootTag: options.rootTag,
        rootTypeName: options.rootTypeName,
        tagsOutputPath: options.tagsOutputPath,
    }];
}

function toQualifiedNameText(value: unknown): string {
    if (typeof value === 'string') {
        return value;
    }

    if (!value || typeof value !== 'object') {
        return '';
    }

    const qualifiedName = value as Exclude<QualifiedNameLike, string>;
    return qualifiedName.name ?? '';
}

function isArrayIndexName(name: string): boolean {
    return /^\d+$/.test(name) || /^\[\d+\]$/.test(name) || /^.+\[\d+\]$/.test(name);
}

function isValidTypeName(name: string): boolean {
    return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name);
}

function stripGeneratedTypePrefix(name: string): string {
    return name
        .replace(/^Application\./, '')
    .replace(/^Application(?=[A-Z_])/, '')
    .replace(new RegExp(`_${activeGeneratedMachineId}$`), '');
}

function sanitizeTypeName(name: string): string {
    const normalizedName = stripGeneratedTypePrefix(name.trim());
    return isValidTypeName(normalizedName) ? normalizedName : toPascalCase(normalizedName);
}

function toPascalCase(value: string): string {
    const parts = value
        .replace(/[^a-zA-Z0-9]+/g, ' ')
        .split(' ')
        .filter(Boolean);

    const pascal = parts
        .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
        .join('');

    return pascal || 'GeneratedType';
}

function renderPropertyName(name: string): string {
    return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name) ? name : JSON.stringify(name);
}

function countUpperCase(value: string): number {
    return Array.from(value).filter((character) => /[A-Z]/.test(character)).length;
}

function countAlpha(value: string): number {
    return Array.from(value).filter((character) => /[A-Za-z]/.test(character)).length;
}

function isUpperCaseChar(character: string | undefined): boolean {
    return typeof character === 'string' && /^[A-Z]$/.test(character);
}

// Mirror node-opcua's lowerFirstLetter behavior so generated types match runtime payload keys.
function toRuntimePropertyName(name: string): string {
    if (!name) {
        return name;
    }

    if (name.length >= 2 && countUpperCase(name) === countAlpha(name)) {
        return name;
    }

    if (name.includes('_')) {
        return name.split('_').map(toRuntimePropertyName).join('_');
    }

    let result = name.substring(0, 1).toLowerCase() + name.substring(1);
    if (result.length > 3 && isUpperCaseChar(name[1]) && isUpperCaseChar(name[2])) {
        result = name.substring(0, 2).toLowerCase() + name.substring(2);
    }

    return result;
}

function indent(level: number): string {
    return '    '.repeat(level);
}

function mapKnownOpcuaTypeToTs(dataTypeName: string | null): string | null {
    const normalizedTypeName = dataTypeName?.trim() ?? null;

    switch (normalizedTypeName) {
        case 'Boolean':
        case 'BOOL':
            return 'boolean';
        case 'SByte':
        case 'SINT':
        case 'Byte':
        case 'USINT':
        case 'Int8':
        case 'UInt8':
        case 'Int16':
        case 'INT':
        case 'UInt16':
        case 'UINT':
        case 'Int32':
        case 'DINT':
        case 'UInt32':
        case 'UDINT':
        case 'Float':
        case 'REAL':
        case 'Double':
        case 'LREAL':
        case 'Decimal':
        case 'WORD':
        case 'DWORD':
            return 'number';
        case 'Int64':
        case 'LINT':
        case 'UInt64':
        case 'ULINT':
        case 'LWORD':
            return 'bigint | number';
        case 'String':
        case 'STRING':
        case 'WSTRING':
        case 'CharArray':
        case 'CHAR':
        case 'WCHAR':
        case 'Guid':
        case 'XmlElement':
            return 'string';
        case 'ByteString':
            return 'Uint8Array | string';
        case 'DateTime':
        case 'DATE':
        case 'DATE_AND_TIME':
        case 'DT':
        case 'TIME':
        case 'TIME_OF_DAY':
        case 'TOD':
        case 'LTIME':
            return 'Date | string';
        case 'LocalizedText':
            return '{ locale?: string | null; text?: string | null; }';
        case 'QualifiedName':
            return '{ namespaceIndex?: number; name?: string; }';
        case 'NodeId':
        case 'ExpandedNodeId':
            return 'string';
        case 'ExtensionObject':
            return 'Record<string, unknown>';
        default:
            return null;
    }
}

function mapCustomLeafTypeToTs(dataTypeName: string | null): string {
    const normalizedTypeName = dataTypeName?.trim() ?? null;

    if (!normalizedTypeName) {
        return 'unknown';
    }

    if (/^[A-Z0-9_]+$/.test(normalizedTypeName)) {
        return 'number | string | unknown';
    }

    return 'unknown';
}

function getNamedDataType(dataTypeName: string | null): string | null {
    if (!dataTypeName) {
        return null;
    }

    if (mapKnownOpcuaTypeToTs(dataTypeName)) {
        return null;
    }

    return sanitizeTypeName(dataTypeName.trim());
}

async function readEnumMetadataByTypeName(
    session: ClientSession,
    dataTypeName: string | null,
    dataTypeNodeId: string | null = null,
): Promise<OpcuaEnumEntry[] | null> {
    if (dataTypeNodeId && getNamedDataType(dataTypeName)) {
        const definition = await readDataTypeDefinition(session, dataTypeNodeId);
        if (definition) {
            if (!isEnumDefinition(definition)) {
                return null;
            }

            const metadata = await readEnumMetadataByNodeId(session, dataTypeNodeId);
            return metadata && metadata.entries.length > 0 ? metadata.entries : null;
        }
    }

    if (!dataTypeName) {
        return null;
    }

    const candidates = Array.from(new Set([
        dataTypeName.trim(),
        stripGeneratedTypePrefix(dataTypeName.trim()),
    ].filter((value) => value.length > 0)));

    for (const candidate of candidates) {
        const metadata = await readEnumMetadata(session, candidate);
        if (metadata && metadata.entries.length > 0) {
            return metadata.entries;
        }
    }

    return null;
}

function toEnumMemberName(label: string, value: number | null): string {
    const normalizedLabel = label
        .trim()
        .replace(/[^A-Za-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '')
        .toUpperCase();

    const fallback = value === null ? 'UNKNOWN' : `VALUE_${value}`;
    const candidate = normalizedLabel || fallback;
    return /^[A-Z_]/.test(candidate) ? candidate : `_${candidate}`;
}

function renderEnumDeclaration(typeName: string, entries: OpcuaEnumEntry[]): string {
    const lines = [`export enum ${typeName} {`];
    const usedMemberNames = new Set<string>();

    for (const entry of entries) {
        let memberName = toEnumMemberName(entry.label, entry.value);
        if (usedMemberNames.has(memberName)) {
            const valueSuffix = entry.value === null ? 'UNKNOWN' : String(entry.value).replace(/[^A-Za-z0-9]+/g, '_');
            memberName = `${memberName}_${valueSuffix}`;
        }
        usedMemberNames.add(memberName);

        if (entry.value === null) {
            lines.push(`${indent(1)}${memberName} = ${JSON.stringify(entry.label)},`);
        } else {
            lines.push(`${indent(1)}${memberName} = ${entry.value},`);
        }
    }

    lines.push('}');
    return lines.join('\n');
}

function toChildReferenceInfo(reference: ReferenceDescription): ChildReferenceInfo {
    return {
        browseName: toQualifiedNameText(reference.browseName),
        nodeClass: reference.nodeClass,
        nodeId: reference.nodeId.toString(),
    };
}

function dedupeChildReferences(references: ChildReferenceInfo[]): ChildReferenceInfo[] {
    const seenBrowseNames = new Set<string>();

    return references.filter((reference) => {
        if (seenBrowseNames.has(reference.browseName)) {
            return false;
        }

        seenBrowseNames.add(reference.browseName);
        return true;
    });
}

function splitIndexedChildren(references: ChildReferenceInfo[]): {
    indexedChildren: ChildReferenceInfo[];
    structuralChildren: ChildReferenceInfo[];
} {
    const indexedChildren = references.filter((reference) => isArrayIndexName(reference.browseName));
    const structuralChildren = references.filter((reference) => {
        if (isArrayIndexName(reference.browseName)) {
            return false;
        }

        if (ARRAY_HELPER_BROWSE_NAMES.has(reference.browseName)) {
            return false;
        }

        return true;
    });

    return {
        indexedChildren,
        structuralChildren,
    };
}

function isHelperFieldName(name: string): boolean {
    return ARRAY_HELPER_BROWSE_NAMES.has(name);
}

function shouldExcludeBrowseName(name: string): boolean {
    return name.toLowerCase().endsWith('fb');
}

function toIndexedSuffix(name: string): string | null {
    const bracketMatch = name.match(/\[(\d+)\]$/);
    if (bracketMatch) {
        return `[${bracketMatch[1]}]`;
    }

    if (/^\d+$/.test(name)) {
        return `[${name}]`;
    }

    return null;
}

function joinTagPath(parentTag: string, parentBrowseName: string, childBrowseName: string): string {
    const indexedSuffix = toIndexedSuffix(childBrowseName);

    if (indexedSuffix) {
        return `${parentTag}${indexedSuffix}`;
    }

    const prefixPattern = new RegExp(`^${parentBrowseName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\[(\\d+)\\]$`);
    const prefixedMatch = childBrowseName.match(prefixPattern);
    if (prefixedMatch) {
        return `${parentTag}[${prefixedMatch[1]}]`;
    }

    return `${parentTag}.${childBrowseName}`;
}

async function connectSession(endpoint: string): Promise<{ client: OPCUAClient; session: ClientSession; }> {
    const client = OPCUAClient.create(opcuaOptions);

    client.on('backoff', (retry, delay) => {
        console.log(`[OPCUA] reconnect retry=${retry} delayMs=${delay}`);
    });

    console.log(`[OPCUA] connecting to ${endpoint}`);
    await client.connect(endpoint);
    console.log('[OPCUA] client connected');

    const session = await client.createSession();
    console.log('[OPCUA] session created');

    return { client, session };
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

async function readSingleAttribute(session: ClientSession, nodeId: string, attributeId: number): Promise<DataValue> {
    return session.read({
        attributeId,
        nodeId,
    } as ReadValueIdOptions);
}

async function readBrowseName(session: ClientSession, nodeId: string): Promise<string> {
    const result = await readSingleAttribute(session, nodeId, AttributeIds.BrowseName);

    if (result.statusCode !== StatusCodes.Good) {
        return '';
    }

    return toQualifiedNameText(result.value.value);
}

async function readDataTypeInfo(
    session: ClientSession,
    nodeId: string,
): Promise<{ dataTypeName: string | null; dataTypeNodeId: string | null; isArray: boolean; }> {
    const [dataType, valueRank] = await session.read([
        { attributeId: AttributeIds.DataType, nodeId } as ReadValueIdOptions,
        { attributeId: AttributeIds.ValueRank, nodeId } as ReadValueIdOptions,
    ]);

    const dataTypeNodeId = dataType.statusCode === StatusCodes.Good
        ? dataType.value.value?.toString() ?? null
        : null;

    const dataTypeName = dataTypeNodeId ? await readBrowseName(session, dataTypeNodeId) : null;
    const valueRankValue = valueRank.statusCode === StatusCodes.Good ? Number(valueRank.value.value) : -1;

    return {
        dataTypeName,
        dataTypeNodeId,
        isArray: valueRankValue >= 1,
    };
}

async function readDataTypeDefinition(session: ClientSession, dataTypeNodeId: string): Promise<unknown | null> {
    const definition = await session.read({
        attributeId: AttributeIds.DataTypeDefinition,
        nodeId: dataTypeNodeId,
    } as ReadValueIdOptions);

    if (definition.statusCode !== StatusCodes.Good) {
        return null;
    }

    return definition.value.value ?? null;
}

function isStructureDefinition(definition: unknown): definition is StructureDefinitionLike {
    return !!definition
        && typeof definition === 'object'
        && ('structureType' in definition || 'baseDataType' in definition)
        && Array.isArray((definition as StructureDefinitionLike).fields);
}

function isEnumDefinition(definition: unknown): definition is EnumDefinitionLike {
    if (!definition || typeof definition !== 'object' || !Array.isArray((definition as EnumDefinitionLike).fields)) {
        return false;
    }

    const fields = (definition as EnumDefinitionLike).fields;
    if (!fields || fields.length === 0) {
        return true;
    }

    return fields.every((field) => field && typeof field === 'object' && 'value' in field && !('dataType' in field));
}

async function buildSchemaFromDataTypeDefinition(
    session: ClientSession,
    dataTypeNodeId: string,
    browseName: string,
    isArray: boolean,
    visited: Set<string>,
): Promise<SchemaNode | null> {
    const definitionNodeKey = `datatype:${dataTypeNodeId}`;
    const dataTypeName = await readBrowseName(session, dataTypeNodeId) || null;
    const definition = await readDataTypeDefinition(session, dataTypeNodeId);

    if (!definition) {
        return null;
    }

    if (isEnumDefinition(definition)) {
        const enumEntries = await readEnumMetadataByTypeName(session, dataTypeName, dataTypeNodeId);
        return {
            browseName,
            dataTypeName,
            dataTypeNodeId,
            enumEntries: enumEntries ?? undefined,
            isArray,
            kind: 'leaf',
            nodeId: definitionNodeKey,
            tsType: 'number',
        };
    }

    if (!isStructureDefinition(definition)) {
        return null;
    }

    const usefulFields = (definition.fields ?? []).filter((field) => {
        const fieldName = field.name ?? '';
        return fieldName.length > 0 && !isHelperFieldName(fieldName) && !shouldExcludeBrowseName(fieldName);
    });

    if (usefulFields.length === 0) {
        return null;
    }

    if (visited.has(definitionNodeKey)) {
        return {
            browseName,
            dataTypeName,
            dataTypeNodeId,
            isArray,
            kind: 'leaf',
            nodeId: definitionNodeKey,
            tsType: getNamedDataType(dataTypeName) ?? 'unknown',
        };
    }

    visited.add(definitionNodeKey);

    const children: SchemaNode[] = [];
    for (const field of usefulFields) {
        const fieldName = field.name ?? '';
        if (!fieldName) {
            continue;
        }

        const fieldDataTypeNodeId = field.dataType?.toString() ?? null;
        const fieldIsArray = typeof field.valueRank === 'number'
            ? field.valueRank >= 1
            : Array.isArray(field.arrayDimensions) && field.arrayDimensions.length > 0;

        if (fieldDataTypeNodeId) {
            const childFromDefinition = await buildSchemaFromDataTypeDefinition(
                session,
                fieldDataTypeNodeId,
                fieldName,
                fieldIsArray,
                visited,
            );

            if (childFromDefinition) {
                if (childFromDefinition.kind === 'array') {
                    children.push({
                        ...childFromDefinition,
                        browseName: fieldName,
                    });
                } else {
                    children.push({
                        ...childFromDefinition,
                        browseName: fieldName,
                        isArray: fieldIsArray,
                    });
                }
                continue;
            }

            const fieldDataTypeName = await readBrowseName(session, fieldDataTypeNodeId) || null;
            children.push({
                browseName: fieldName,
                dataTypeName: fieldDataTypeName,
                dataTypeNodeId: fieldDataTypeNodeId,
                isArray: fieldIsArray,
                kind: 'leaf',
                nodeId: `${definitionNodeKey}:${fieldName}`,
                tsType: mapKnownOpcuaTypeToTs(fieldDataTypeName) ?? mapCustomLeafTypeToTs(fieldDataTypeName),
            });
            continue;
        }

        children.push({
            browseName: fieldName,
            dataTypeName: null,
            dataTypeNodeId: null,
            isArray: fieldIsArray,
            kind: 'leaf',
            nodeId: `${definitionNodeKey}:${fieldName}`,
            tsType: 'unknown',
        });
    }

    return {
        browseName,
        children,
        dataTypeName,
        dataTypeNodeId,
        isArray,
        kind: 'object',
        nodeId: definitionNodeKey,
    };
}

async function buildSchemaTree(
    session: ClientSession,
    nodeId: string,
    fallbackBrowseName: string,
    visited: Set<string>,
): Promise<SchemaNode> {
    const browseName = await readBrowseName(session, nodeId) || fallbackBrowseName;
    const { dataTypeName, dataTypeNodeId, isArray } = await readDataTypeInfo(session, nodeId);

    if (dataTypeNodeId && getNamedDataType(dataTypeName)) {
        const schemaFromDefinition = await buildSchemaFromDataTypeDefinition(
            session,
            dataTypeNodeId,
            browseName,
            isArray,
            visited,
        );

        if (schemaFromDefinition) {
            return schemaFromDefinition;
        }
    }

    if (visited.has(nodeId)) {
        return {
            browseName,
            dataTypeName,
            dataTypeNodeId,
            isArray,
            kind: 'leaf',
            nodeId,
            tsType: 'unknown',
        };
    }

    visited.add(nodeId);

    const childReferences = dedupeChildReferences((await browseChildren(session, nodeId))
        .map((reference) => toChildReferenceInfo(reference))
        .filter((reference) => reference.browseName.length > 0)
        .filter((reference) => !shouldExcludeBrowseName(reference.browseName))
        .filter((reference) => reference.nodeClass === NodeClass.Object || reference.nodeClass === NodeClass.Variable)
        .sort((left, right) => left.browseName.localeCompare(right.browseName)));

    if (childReferences.length === 0) {
        return {
            browseName,
            dataTypeName,
            dataTypeNodeId,
            enumEntries: await readEnumMetadataByTypeName(session, dataTypeName, dataTypeNodeId) ?? undefined,
            isArray,
            kind: 'leaf',
            nodeId,
            tsType: mapKnownOpcuaTypeToTs(dataTypeName) ?? mapCustomLeafTypeToTs(dataTypeName),
        };
    }

    const { indexedChildren, structuralChildren } = splitIndexedChildren(childReferences);
    if (indexedChildren.length > 0 && structuralChildren.length === 0) {
        const firstChild = indexedChildren[0];
        const element = await buildSchemaTree(session, firstChild.nodeId, firstChild.browseName, visited);
        return {
            browseName,
            dataTypeName,
            dataTypeNodeId,
            element,
            kind: 'array',
            nodeId,
        };
    }

    if (indexedChildren.length === 0 && structuralChildren.length === 0 && childReferences.length > 0) {
        const namedType = getNamedDataType(dataTypeName);

        if (namedType) {
            return {
                browseName,
                children: [],
                dataTypeName,
                dataTypeNodeId,
                isArray,
                kind: 'object',
                nodeId,
            };
        }

        return {
            browseName,
            dataTypeName,
            dataTypeNodeId,
            enumEntries: await readEnumMetadataByTypeName(session, dataTypeName, dataTypeNodeId) ?? undefined,
            isArray,
            kind: 'leaf',
            nodeId,
            tsType: mapKnownOpcuaTypeToTs(dataTypeName) ?? mapCustomLeafTypeToTs(dataTypeName),
        };
    }

    const children: SchemaNode[] = [];
    for (const childReference of structuralChildren) {
        children.push(await buildSchemaTree(session, childReference.nodeId, childReference.browseName, visited));
    }

    return {
        browseName,
        children,
        dataTypeName,
        dataTypeNodeId,
        isArray,
        kind: 'object',
        nodeId,
    };
}

interface RenderContext {
    declarations: Map<string, string>;
    order: string[];
}

function addDeclaration(context: RenderContext, typeName: string, declaration: string): void {
    if (context.declarations.has(typeName)) {
        return;
    }

    context.declarations.set(typeName, declaration);
    context.order.push(typeName);
}

function renderInlineAnonymousObject(node: ObjectSchemaNode, context: RenderContext, level: number): string {
    if (node.children.length === 0) {
        return 'Record<string, unknown>';
    }

    const lines = ['{'];

    for (const child of node.children) {
        lines.push(`${indent(level + 1)}${renderPropertyName(toRuntimePropertyName(child.browseName))}: ${renderTypeReference(child, context, level + 1)};`);
    }

    lines.push(`${indent(level)}}`);
    return lines.join('\n');
}

function ensureDeclaration(node: SchemaNode, context: RenderContext): string | null {
    const typeName = getNamedDataType(node.dataTypeName);
    if (!typeName) {
        return null;
    }

    if (context.declarations.has(typeName)) {
        return typeName;
    }

    if (node.kind === 'leaf') {
        if (node.enumEntries && node.enumEntries.length > 0) {
            addDeclaration(context, typeName, renderEnumDeclaration(typeName, node.enumEntries));
            return typeName;
        }

        addDeclaration(context, typeName, `export type ${typeName} = ${mapCustomLeafTypeToTs(node.dataTypeName)};`);
        return typeName;
    }

    if (node.kind === 'array') {
        const elementType = renderTypeReference(node.element, context, 0, false);
        addDeclaration(context, typeName, `export type ${typeName} = Array<${elementType}>;`);
        return typeName;
    }

    const lines = [`export interface ${typeName} {`];
    for (const child of node.children) {
        lines.push(`${indent(1)}${renderPropertyName(toRuntimePropertyName(child.browseName))}: ${renderTypeReference(child, context, 1)};`);
    }
    lines.push('}');
    addDeclaration(context, typeName, lines.join('\n'));
    return typeName;
}

function renderTypeReference(node: SchemaNode, context: RenderContext, level: number, wrapArray: boolean = true): string {
    if (node.kind === 'array') {
        const namedType = ensureDeclaration(node, context);
        if (namedType) {
            return wrapArray ? `Array<${namedType}>` : namedType;
        }

        return `Array<${renderTypeReference(node.element, context, level, false)}>`;
    }

    if (node.kind === 'leaf') {
        const namedType = ensureDeclaration(node, context);
        const baseType = namedType ?? node.tsType;
        return node.isArray && wrapArray ? `Array<${baseType}>` : baseType;
    }

    const namedType = ensureDeclaration(node, context);
    const baseType = namedType ?? renderInlineAnonymousObject(node, context, level);
    return node.isArray && wrapArray ? `Array<${baseType}>` : baseType;
}

function renderGeneratedModule(options: CliOptions, schema: SchemaNode): string {
    const generatedAt = new Date().toISOString();
    const context: RenderContext = {
        declarations: new Map<string, string>(),
        order: [],
    };
    const rootTypeName = renderTypeReference(schema, context, 0, false);
    const declarationBlocks = context.order.map((typeName) => context.declarations.get(typeName) ?? '').filter(Boolean);
    const aliasLines: string[] = [];
    const runtimeAliasName = `${options.rootTypeName}Runtime`;
    const runtimeExcludedKeysTypeName = `${options.rootTypeName}RuntimeExcludedKeys`;
    const runtimeExcludedKeysUnion = MACHINE_RUNTIME_EXCLUDED_KEYS.map((key) => `'${key}'`).join(' | ');

    if (options.rootTypeName !== rootTypeName) {
        aliasLines.push(`export type ${options.rootTypeName} = ${rootTypeName};`);
    }

    aliasLines.push(`export type MachineTypeRoot = ${rootTypeName};`);
    aliasLines.push(`export type ${runtimeExcludedKeysTypeName} = ${runtimeExcludedKeysUnion};`);
    aliasLines.push(`export type ${runtimeAliasName} = Omit<${options.rootTypeName}, ${runtimeExcludedKeysTypeName}>;`);
    aliasLines.push(`export type MachineRuntimeRoot = Omit<MachineTypeRoot, ${runtimeExcludedKeysTypeName}>;`);

    return [
        '// This file is generated by machine-bridge/src/type-generator/generate-machine-types.ts',
        '// Output location defaults to machine-bridge/generated-types/shared/types.ts.',
        '// Do not edit by hand; rerun the generator against a live OPC UA server.',
        '',
        `export const machineTypeSource = ${JSON.stringify({
            controllerName: options.controllerName,
            endpoint: options.endpoint,
            generatedAt,
            rootNodeId: options.rootNodeId,
            rootTag: options.rootTag,
        }, null, 4)} as const;`,
        '',
        ...declarationBlocks,
        '',
        ...aliasLines,
        '',
    ].join('\n');
}

async function buildTagTree(
    session: ClientSession,
    nodeId: string,
    browseName: string,
    tagPath: string,
    visited: Set<string>,
): Promise<GeneratedTagNode> {
    const visitKey = `tags:${nodeId}`;
    if (visited.has(visitKey)) {
        return { tag: tagPath };
    }

    visited.add(visitKey);

    const childReferences = dedupeChildReferences((await browseChildren(session, nodeId))
        .map((reference) => toChildReferenceInfo(reference))
        .filter((reference) => reference.browseName.length > 0)
        .filter((reference) => !shouldExcludeBrowseName(reference.browseName))
        .filter((reference) => reference.nodeClass === NodeClass.Object || reference.nodeClass === NodeClass.Variable)
        .sort((left, right) => left.browseName.localeCompare(right.browseName)));

    const { indexedChildren, structuralChildren } = splitIndexedChildren(childReferences);
    const node: GeneratedTagNode = { tag: tagPath };

    if (structuralChildren.length > 0) {
        const children: Record<string, GeneratedTagNode> = {};
        for (const childReference of structuralChildren) {
            const childTagPath = joinTagPath(tagPath, browseName, childReference.browseName);
            children[childReference.browseName] = await buildTagTree(
                session,
                childReference.nodeId,
                childReference.browseName,
                childTagPath,
                visited,
            );
        }
        node.children = children;
    }

    if (indexedChildren.length > 0) {
        node.items = [];
        for (const childReference of indexedChildren) {
            const childTagPath = joinTagPath(tagPath, browseName, childReference.browseName);
            node.items.push(await buildTagTree(
                session,
                childReference.nodeId,
                childReference.browseName,
                childTagPath,
                visited,
            ));
        }
    }

    return node;
}

function renderTagNode(node: GeneratedTagNode, level: number): string {
    const lines = ['{'];
    lines.push(`${indent(level + 1)}tag: ${JSON.stringify(node.tag)},`);

    if (node.children && Object.keys(node.children).length > 0) {
        lines.push(`${indent(level + 1)}children: {`);
        for (const [key, childNode] of Object.entries(node.children)) {
            lines.push(`${indent(level + 2)}${renderPropertyName(key)}: ${renderTagNode(childNode, level + 2)},`);
        }
        lines.push(`${indent(level + 1)}} as const,`);
    }

    if (node.items && node.items.length > 0) {
        lines.push(`${indent(level + 1)}items: [`);
        for (const childNode of node.items) {
            lines.push(`${indent(level + 2)}${renderTagNode(childNode, level + 2)},`);
        }
        lines.push(`${indent(level + 1)}] as const,`);
    }

    lines.push(`${indent(level)}}`);
    return lines.join('\n');
}

function renderTagModule(options: CliOptions, tagTree: GeneratedTagNode): string {
    return [
        '// This file is generated by machine-bridge/src/type-generator/generate-machine-types.ts',
        '// It captures the browsed Machine tag structure for later bridge-side iteration.',
        '// Do not edit by hand; rerun the generator against a live OPC UA server.',
        '',
        'export interface MachineTagNode {',
        '    tag: string;',
        '    children?: Record<string, MachineTagNode>;',
        '    items?: readonly MachineTagNode[];',
        '}',
        '',
        `export const machineTagSource = ${JSON.stringify({
            controllerName: options.controllerName,
            endpoint: options.endpoint,
            rootNodeId: options.rootNodeId,
            rootTag: options.rootTag,
        }, null, 4)} as const;`,
        '',
        `export const machineTags: MachineTagNode = ${renderTagNode(tagTree, 0)};`,
        '',
        'export function* iterateMachineTags(node: MachineTagNode = machineTags): Generator<string> {',
        '    yield node.tag;',
        '',
        '    if (node.children) {',
        '        for (const child of Object.values(node.children)) {',
        '            yield* iterateMachineTags(child);',
        '        }',
        '    }',
        '',
        '    if (node.items) {',
        '        for (const child of node.items) {',
        '            yield* iterateMachineTags(child);',
        '        }',
        '    }',
        '}',
        '',
        'export function collectMachineTags(node: MachineTagNode = machineTags): string[] {',
        '    return Array.from(iterateMachineTags(node));',
        '}',
        '',
    ].join('\n');
}

async function ensureOutputDirectory(filePath: string): Promise<void> {
    await fs.mkdir(path.dirname(filePath), { recursive: true });
}

async function writeOutputFile(filePath: string, content: string): Promise<void> {
    await ensureOutputDirectory(filePath);
    await fs.writeFile(filePath, content, 'utf8');
}

async function main(): Promise<void> {
    const options = parseOptions(process.argv.slice(2));
    if (!options) {
        return;
    }

    activeGeneratedMachineId = options.machineId?.trim() || null;

    const generationTargets = buildGenerationTargets(options);

    const { client, session } = await connectSession(options.endpoint);

    try {
        for (const target of generationTargets) {
            const targetOptions: CliOptions = {
                ...options,
                outputPath: target.outputPath,
                rootNodeId: target.rootNodeId,
                rootTag: target.rootTag,
                rootTypeName: target.rootTypeName,
                tagsOutputPath: target.tagsOutputPath,
            };

            console.log(`[OPCUA] inspecting tag ${targetOptions.rootTag}`);
            console.log(`[OPCUA] root node id ${targetOptions.rootNodeId}`);

            const schema = await buildSchemaTree(session, targetOptions.rootNodeId, targetOptions.rootTag, new Set<string>());
            const content = renderGeneratedModule(targetOptions, schema);
            const tagTree = await buildTagTree(session, targetOptions.rootNodeId, targetOptions.rootTag, targetOptions.rootTag, new Set<string>());
            const tagsContent = renderTagModule(targetOptions, tagTree);

            await writeOutputFile(targetOptions.outputPath, content);
            await writeOutputFile(targetOptions.tagsOutputPath, tagsContent);

            console.log(`[WRITE] generated ${targetOptions.outputPath}`);
            console.log(`[WRITE] generated ${targetOptions.tagsOutputPath}`);
        }
    } finally {
        await session.close();
        await client.disconnect();
        console.log('[OPCUA] disconnected');
    }
}

main().catch((error) => {
    console.error('[ERROR] failed to generate Machine TypeScript artifacts');
    console.error(error);
    process.exitCode = 1;
});