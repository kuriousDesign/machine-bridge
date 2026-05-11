//import "dotenv/config"; // auto-loads .env
import BridgeSupervisor from './bridge/BridgeSupervisor';

type ProcessWithDebugHandles = NodeJS.Process & {
    _getActiveHandles?: () => unknown[];
    _getActiveRequests?: () => unknown[];
};

function logActiveProcessState(): void {
    const debugProcess = process as ProcessWithDebugHandles;
    const activeHandles = debugProcess._getActiveHandles?.() ?? [];
    const activeRequests = debugProcess._getActiveRequests?.() ?? [];

    console.error('[SHUTDOWN] Active handles:', activeHandles.map((handle) => handle?.constructor?.name ?? typeof handle));
    console.error('[SHUTDOWN] Active requests:', activeRequests.map((request) => request?.constructor?.name ?? typeof request));
}

async function main() {
    console.log('🚀 Starting OPC UA ↔ MQTT Bridge');

    const supervisor = new BridgeSupervisor();
    let shutdownRequested = false;
    

    // Handle graceful shutdown via Ctrl+C
    process.once('SIGINT', async () => {
        if (shutdownRequested) {
            return;
        }

        shutdownRequested = true;
        console.log("\nSIGINT received. Shutting down gracefully.");
        const forceExitTimer = setTimeout(() => {
            console.error('[SHUTDOWN] Force exit timer elapsed before graceful shutdown completed.');
            logActiveProcessState();
            process.exit(1);
        }, 10000);
        forceExitTimer.unref();

        try {
            console.log('[SHUTDOWN] Awaiting supervisor shutdown...');
            await supervisor.requestShutdown();
            console.log('[SHUTDOWN] Supervisor shutdown resolved. Exiting process.');
            clearTimeout(forceExitTimer);
            process.exit(0);
        } catch (error) {
            console.error('Shutdown failed:', error);
            process.exit(1);
        }
    });

    await supervisor.start();
    // Script finishes here after disconnection is complete.
}

main().catch(console.error);
