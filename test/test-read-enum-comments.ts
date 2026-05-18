import {
    ClientSession,
    OPCUAClient,
    OPCUAClientOptions,
} from 'node-opcua';

import 'dotenv/config';

import Config, { createSharedOpcuaClientOptions } from '../src/shared/config';
import { readEnumMetadata } from '../src/type-generator/enum-reader';

const enumLabel = process.argv[2] || 'DeviceIds';
const opcuaEndpoint = Config.OPCUA_ENDPOINT || `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;

const opcuaOptions: OPCUAClientOptions = createSharedOpcuaClientOptions();

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

async function main(): Promise<void> {
    const connection = await initializeOpcuaClient(opcuaEndpoint);
    if (!connection) {
        process.exitCode = 1;
        return;
    }

    const { client, session } = connection;

    try {
        const metadata = await readEnumMetadata(session, enumLabel);
        if (!metadata) {
            console.error(`Could not find enum DataType named ${enumLabel} under the OPC UA DataTypes tree.`);
            process.exitCode = 1;
            return;
        }

        console.log(`Found enum DataType ${enumLabel} at ${metadata.nodeId}`);
        console.log(`Source: ${metadata.source}`);

        if (metadata.entries.length === 0) {
            console.warn(`No EnumValues or EnumStrings metadata was exposed for ${enumLabel}.`);
            return;
        }

        for (const entry of metadata.entries) {
            console.log('---');
            console.log(`value: ${entry.value}`);
            console.log(`label: ${entry.label}`);
            console.log(`description: ${entry.description ?? '<none>'}`);
        }

        const describedEntries = metadata.entries.filter((entry) => !!entry.description?.trim());
        console.log('---');
        console.log(`entries with descriptions: ${describedEntries.length}/${metadata.entries.length}`);

        if (metadata.source === 'EnumStrings') {
            console.warn('EnumStrings source usually does not include per-entry descriptions.');
        }
    } finally {
        await session.close();
        await client.disconnect();
    }
}

void main().catch((error) => {
    console.error('Enum comment inspection failed:', error);
    process.exitCode = 1;
});