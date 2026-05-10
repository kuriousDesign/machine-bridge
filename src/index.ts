//import "dotenv/config"; // auto-loads .env
import BridgeSupervisor from './BridgeSupervisor';


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
        const forceExitTimer = setTimeout(() => process.exit(1), 10000);
        forceExitTimer.unref();

        try {
            await supervisor.requestShutdown();
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
