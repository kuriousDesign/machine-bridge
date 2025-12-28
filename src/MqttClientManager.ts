// MqttClientManager.ts

import { TopicData } from '@kuriousdesign/machine-sdk';
import Config from './config';
import { MqttClient, connect, MqttProtocol } from 'mqtt';


enum MqttState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Disconnecting,
}


// Define the type for the handler function
type MessageHandler = (topic: string, message: Buffer) => void;

export default class MqttClientManager {
    private state: MqttState = MqttState.Disconnected;
    private client: MqttClient | null = null;
    private shutdownRequested: boolean = false;
    private previousState: MqttState = MqttState.Disconnected;
    // Map to store all registered handlers centrally
    private handlers = new Map<string, MessageHandler>(); 


    public requestShutdown(): void {
        this.shutdownRequested = true;
        console.log("[MQTT] Shutdown requested. Transitioning to Disconnecting state.");
    }

    public async manageConnectionLoop(): Promise<void> {
        while (!this.shutdownRequested) {
            if (this.state !== this.previousState) {
                console.log(`[MQTT] STATE CHANGED: ${MqttState[this.previousState]} → ${MqttState[this.state]}`);
                this.previousState = this.state;
            }
            switch (this.state) {
                case MqttState.Disconnected:
                case MqttState.Reconnecting:
                    await this.handleConnection();
                    break;
                case MqttState.Connected:
                    // Once connected, we just wait for the OpcuaClientManager to call publish()
                    await new Promise(resolve => setTimeout(resolve, 500));
                    break;
                case MqttState.Disconnecting:
                    await this.handleDisconnect();
                    return;
            }
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        await this.handleDisconnect();
    }

    private async handleConnection(): Promise<void> {
        this.state = MqttState.Connecting;
        try {
            console.log(`[MQTT] Attempting to connect to MQTT broker at ${Config.MQTT_URL}...`);
            this.client = connect(Config.MQTT_URL, Config.MQTT_OPTIONS);

            await new Promise<void>((resolve, reject) => {
                this.client!.on('connect', () => { 
                    this.state = MqttState.Connected; 
                    this.setupSingleMessageHandler(); // Call the new setup function once
                    resolve(); 
                });
                this.client!.once('error', (error) => { reject(error); });
            });

            this.client!.on('error', (error) => {
                console.error('[MQTT] ❌ MQTT ongoing connection error:', error.message);
                this.state = MqttState.Reconnecting;
            });
            this.client!.on('close', () => {
                console.warn('[MQTT] MQTT connection closed.');
                if (!this.shutdownRequested) this.state = MqttState.Reconnecting;
            });

        } catch (err) {
            console.error(`[MQTT] ❌ Failed during MQTT connection phase: ${err instanceof Error ? err.message : String(err)}`);
            if (this.client) this.client.end(true);
            this.client = null;
            this.state = MqttState.Reconnecting;
            await new Promise(resolve => setTimeout(resolve, Config.RECONNECT_DELAY_MS));
        }
    }

    public clearAllHandlers(): void {
        this.handlers.clear();
    }

    private async handleDisconnect(): Promise<void> {
        this.state = MqttState.Disconnecting;
        if (this.client) {
            // Remove all handlers upon disconnect to prevent memory leaks if client instance changes
            this.clearAllHandlers();
            this.client.removeAllListeners('message'); 
            await new Promise<void>(resolve => { this.client!.end(true, () => { this.client = null; this.state = MqttState.Disconnected; resolve(); }); });
        }
    }

    /**
     * Set up a SINGLE 'message' listener that routes all incoming messages internally.
     */
    private setupSingleMessageHandler(): void {
        if (!this.client) return;
        
        // This attaches ONE listener to the client instance, resolving the MaxListeners issue.
        this.client.on('message', (recvTopic, message) => {
            // Use the stored handlers map to find the correct function for this topic
            // Note: This simple lookup works best for exact topic matches, not wildcards efficiently.
            const handler = this.handlers.get(recvTopic);
            if (handler) {
                handler(recvTopic, message);
            } else {
                // If you use wildcards (e.g., # or +), this logic needs to be more sophisticated 
                // to match the incoming topic against registered wildcard topics.
                // console.log(`[MQTT] Received message for unsubscribed topic or topic without handler: ${recvTopic}`);
            }
        });
    }

    /**
     * The method the OpcuaClientManager will call to send data.
     */
    public async publish(topic: string, payload: any): Promise<void> {
        if (this.state === MqttState.Connected && this.client && this.client.connected) {
            const message: TopicData = {
                timestamp: Date.now(),
                payload: payload
            };

            try {
                this.client.publish(topic, JSON.stringify(message));
                //console.log(`[MQTT] Published to ${topic}:`);
            } catch (err) {
                console.error(`[MQTT] ❌ Error publishing to ${topic}: ${err instanceof Error ? err.message : String(err)}`);
                // set state to reconnect on publish error
                this.state = MqttState.Reconnecting;
                //properly close the client
                if (this.client) {
                    this.client.end(true);
                    this.client = null;
                }
            }
        } else {
            console.warn(`[MQTT] Cannot publish to ${topic}, MQTT client not operational.`);
        }
    }

    /**
     * Register a new topic to subscribe to and store its associated handler function.
     */
    public async subscribe(topic: string, messageHandler: MessageHandler): Promise<void> {
        if (this.state === MqttState.Connected && this.client && this.client.connected) {
            this.client.subscribe(topic, { qos: 1 }, (err) => {
                if (err) {
                    console.error(`[MQTT] ❌ Failed to subscribe to topic ${topic}: ${err.message}`);
                } else {
                    //console.log(`[MQTT] Subscribed to topic: ${topic}`);
                    // Store the handler in our local map
                    this.handlers.set(topic, messageHandler); 
                }
            });
            // CRITICAL CHANGE: Removed this.client.on('message', ...) from this function
            // The single listener set up in handleConnection() handles the routing now.
        } else {
            console.warn(`[MQTT] Cannot subscribe to ${topic}, MQTT client not operational.`);
        }
    }
}

//create main program
async function main() {
    const mqttManager = new MqttClientManager();
    process.on('SIGINT', async () => {
        console.log("[MQTT] Caught interrupt signal, shutting down MQTT client...");
        mqttManager.requestShutdown();
    });
    await mqttManager.manageConnectionLoop();
    console.log("[MQTT] MQTT client has shut down gracefully.");
    process.exit(0);
}

// Run main program if this file is executed directly
if (require.main === module) {
    main();
}
