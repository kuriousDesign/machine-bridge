import { promises as fs } from 'fs';
import os from 'os';
import path from 'path';

import { OPCUAClient } from 'node-opcua';

import Config, { ApplicationIdentity, createSharedOpcuaClientOptions } from '../shared/config';

const ownCertDirectory = path.join(os.homedir(), '.config', 'node-opcua-default-nodejs', 'PKI', 'own');
const ownCertFile = path.join(ownCertDirectory, 'certs', 'client_certificate.pem');
const ownPrivateKeyFile = path.join(ownCertDirectory, 'private', 'private_key.pem');

async function removeExistingClientCertificate(): Promise<void> {
    await fs.rm(ownCertFile, { force: true });
    await fs.rm(ownPrivateKeyFile, { force: true });
}

async function main(): Promise<void> {
    if (!Config.OPCUA_ENDPOINT) {
        throw new Error('Missing OPCUA endpoint configuration. Set OPCUA_SERVER_IP_ADDRESS and OPCUA_PORT.');
    }

    console.log(`[CERT] applicationName=${ApplicationIdentity.applicationName}`);
    console.log(`[CERT] applicationUri=${ApplicationIdentity.applicationUri}`);
    console.log(`[CERT] endpoint=${Config.OPCUA_ENDPOINT}`);
    console.log('[CERT] removing existing local client certificate and key');

    await removeExistingClientCertificate();

    const client = OPCUAClient.create(createSharedOpcuaClientOptions());

    try {
        console.log('[CERT] connecting to OPC UA endpoint to regenerate client certificate');
        await client.connect(Config.OPCUA_ENDPOINT);
        await client.disconnect();
        console.log(`[CERT] regenerated client certificate at ${ownCertFile}`);
    } catch (error) {
        try {
            await client.disconnect();
        } catch {
            // Ignore disconnect failures during cert regeneration cleanup.
        }
        throw error;
    }
}

void main().catch((error) => {
    console.error('[CERT] failed to regenerate OPC UA client certificate');
    console.error(error);
    process.exitCode = 1;
});