//import "dotenv/config"; // auto-loads .env
import OpcuaClientManager from './OpcuaHmiManager';


async function main() {
    console.log('🚀 Starting Opcua Hmi Manager');


    const manager = new OpcuaClientManager();
    

    // Handle graceful shutdown via Ctrl+C
    process.on('SIGINT', async () => {
        console.log("\nSIGINT received. Shutting down gracefully.");
        manager.requestShutdown();
        // Give the state machine loop time to complete the disconnect process
        // A better approach in a real app might use a promise/event listener here
        setTimeout(() => process.exit(0), 5000);
    });

    await manager.manageConnectionLoop();
    // Script finishes here after disconnection is complete.
}

main().catch(console.error);
