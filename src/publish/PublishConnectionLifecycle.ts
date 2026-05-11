import { ClientSession, OPCUAClient, OPCUAClientOptions } from 'node-opcua';

import CodesysOpcuaDriver from '../opcua/codesys-opcua-driver';

export interface PublishConnectionTracking {
    connectionAttemptStartedAt: number | null;
    connectionFailureCount: number;
    connectionFailureStartedAt: number | null;
    lastConnectionRetryLogAt: number;
    lastDisconnectedStatusLogAt: number;
}

export interface PublishConnectionResources {
    client: OPCUAClient | null;
    codesysOpcuaDriver: CodesysOpcuaDriver | null;
    session: ClientSession | null;
}

export function logPublishOpcuaConnectionFailure(params: {
    connectLogIntervalMs: number;
    endpoint: string;
    error: unknown;
    retryDelayMs: number;
    tracking: PublishConnectionTracking;
}): PublishConnectionTracking {
    const { connectLogIntervalMs, endpoint, error, retryDelayMs, tracking } = params;
    const now = Date.now();
    const errorMessage = error instanceof Error ? error.message : String(error);
    const nextTracking: PublishConnectionTracking = {
        ...tracking,
        connectionFailureCount: tracking.connectionFailureCount + 1,
        connectionFailureStartedAt: tracking.connectionFailureStartedAt ?? now,
    };

    const shouldLogDetailedFailure = nextTracking.connectionFailureCount === 1
        || now - tracking.lastConnectionRetryLogAt >= connectLogIntervalMs;

    if (!shouldLogDetailedFailure) {
        return nextTracking;
    }

    const elapsedSeconds = Math.floor((now - (nextTracking.connectionFailureStartedAt ?? now)) / 1000);
    console.error(`[OPCUA] ❌ Unable to connect to ${endpoint}. Attempt ${nextTracking.connectionFailureCount}. Last error: ${errorMessage}`);

    if (nextTracking.connectionFailureCount > 1) {
        console.warn(`[OPCUA] Still retrying OPC UA connection after ${elapsedSeconds}s. Retrying every ${retryDelayMs}ms.`);
    }

    nextTracking.lastConnectionRetryLogAt = now;
    return nextTracking;
}

export function resetPublishOpcuaConnectionFailureTracking(
    tracking: PublishConnectionTracking,
): PublishConnectionTracking {
    if (tracking.connectionFailureStartedAt !== null && tracking.connectionFailureCount > 0) {
        const elapsedSeconds = Math.floor((Date.now() - tracking.connectionFailureStartedAt) / 1000);
        console.log(`[OPCUA] ✅ Connection established after ${tracking.connectionFailureCount} failed attempt(s) over ${elapsedSeconds}s.`);
    }

    return {
        connectionAttemptStartedAt: null,
        connectionFailureCount: 0,
        connectionFailureStartedAt: null,
        lastConnectionRetryLogAt: 0,
        lastDisconnectedStatusLogAt: 0,
    };
}

export async function teardownPublishOpcuaConnection(params: {
    isIgnorableCleanupError: (error: unknown) => boolean;
    resources: PublishConnectionResources;
    terminateAllSubscriptions: () => Promise<void>;
}): Promise<PublishConnectionResources> {
    const { isIgnorableCleanupError, resources, terminateAllSubscriptions } = params;

    await terminateAllSubscriptions();

    if (resources.session) {
        try {
            await resources.session.close();
        } catch (error) {
            if (!isIgnorableCleanupError(error)) {
                console.warn('[OPCUA] Error closing existing session before reconnect:', error);
            }
        }
    }

    if (resources.client) {
        try {
            resources.client.removeAllListeners('connection_lost');
            resources.client.removeAllListeners('after_reconnection');
            await resources.client.disconnect();
        } catch (error) {
            if (!isIgnorableCleanupError(error)) {
                console.warn('[OPCUA] Error disconnecting existing client before reconnect:', error);
            }
        }
    }

    return {
        client: null,
        codesysOpcuaDriver: null,
        session: null,
    };
}

export async function connectPublishOpcuaSession(params: {
    clearMqttHandlers: () => void;
    createDriver: (session: ClientSession) => CodesysOpcuaDriver;
    endpoint: string;
    isIgnorableCleanupError: (error: unknown) => boolean;
    onConnectionLost: () => void;
    opcuaOptions: OPCUAClientOptions;
    reportError: (error: unknown) => void;
    resetExternalSubscriptions: () => void;
    resetHmiSubscriptions: () => void;
    resources: PublishConnectionResources;
    tracking: PublishConnectionTracking;
    terminateAllSubscriptions: () => Promise<void>;
}): Promise<{
    error?: unknown;
    resources: PublishConnectionResources;
    tracking: PublishConnectionTracking;
}> {
    const {
        clearMqttHandlers,
        createDriver,
        endpoint,
        isIgnorableCleanupError,
        onConnectionLost,
        opcuaOptions,
        reportError,
        resetExternalSubscriptions,
        resetHmiSubscriptions,
        resources,
        tracking,
        terminateAllSubscriptions,
    } = params;

    if (tracking.connectionFailureCount === 0) {
        console.log(`[OPCUA] Connecting to endpoint: ${endpoint}`);
    }

    clearMqttHandlers();
    resetExternalSubscriptions();
    resetHmiSubscriptions();

    let nextResources = resources;

    try {
        nextResources = await teardownPublishOpcuaConnection({
            isIgnorableCleanupError,
            resources: nextResources,
            terminateAllSubscriptions,
        });

        const client = OPCUAClient.create(opcuaOptions);
        client.on('connection_lost', onConnectionLost);
        client.on('after_reconnection', () => console.log('[OPCUA] ✅ OPC UA client reconnected internally'));

        await client.connect(endpoint);
        const session = await client.createSession();
        console.log('[OPCUA] ✅ Connected to server and session created.');

        nextResources = {
            client,
            codesysOpcuaDriver: createDriver(session),
            session,
        };

        return {
            resources: nextResources,
            tracking: resetPublishOpcuaConnectionFailureTracking(tracking),
        };
    } catch (error) {
        reportError(error);
        const cleanedResources = await teardownPublishOpcuaConnection({
            isIgnorableCleanupError,
            resources: nextResources,
            terminateAllSubscriptions,
        });

        return {
            error,
            resources: cleanedResources,
            tracking,
        };
    }
}

export async function disconnectPublishOpcuaSession(params: {
    resources: PublishConnectionResources;
    terminateAllSubscriptions: () => Promise<void>;
}): Promise<PublishConnectionResources> {
    const { resources, terminateAllSubscriptions } = params;

    console.log('Starting graceful OPC UA disconnection and cleanup...');

    if (resources.session) {
        try {
            await terminateAllSubscriptions();
            const closePromise = resources.session.close();
            await Promise.race([
                closePromise,
                new Promise((_, reject) => setTimeout(() => reject(new Error('Session close timeout')), 8000)),
            ]);
            console.log('✅ Session closed.');
        } catch (error) {
            console.warn('Session close failed (may be already closed):', error);
        }
    }

    if (resources.client) {
        try {
            resources.client.removeAllListeners('connection_lost');
            resources.client.removeAllListeners('after_reconnection');

            const disconnectPromise = resources.client.disconnect();
            await Promise.race([
                disconnectPromise,
                new Promise((_, reject) => setTimeout(() => reject(new Error('Client disconnect timeout')), 8000)),
            ]);
            console.log('✅ Client disconnected.');
        } catch (error) {
            console.warn('Client disconnect failed:', error);
        }
    }

    return {
        client: null,
        codesysOpcuaDriver: null,
        session: null,
    };
}
