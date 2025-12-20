// MqttClientManager.ts

import { TopicData } from '@kuriousdesign/machine-sdk';
import * as mqtt from 'mqtt';
import { MqttClient, connect, IClientOptions, MqttProtocol } from 'mqtt';

// --- Configuration ---

const RECONNECT_DELAY_MS = 3000;

enum MqttState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Disconnecting,
}

  const mqttLocalUrl = process.env.MQTT_LOCAL_BROKER_URL || "ws://localhost:9002/mqtt"; //process.env.MQTT_BROKER_URL || "wss://9c4d3c046b704d16a1d64328cc4e4604.s1.eu.hivemq.cloud:8884/mqtt";
  const mqttCloudUrl = process.env.MQTT_CLOUD_BROKER_URL || "wss://9c4d3c046b704d16a1d64328cc4e4604.s1.eu.hivemq.cloud:8884/mqtt";

  const mqttOptionsHiveMQ = {
    username: process.env.MQTT_BROKER_USERNAME || "admin",
    password: process.env.MQTT_BROKER_PASSWORD || "Admin1234",
    reconnectPeriod: 1000,
    keepalive: 60,
    protocol: "wss" as MqttProtocol,
    //port: 8884,
    rejectUnauthorized: true,
    // ca: [Buffer.from('-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----', 'utf8')],
  };

  const mqttLocalOptions = {
    username: process.env.MQTT_BROKER_USERNAME || "admin",
    password: process.env.MQTT_BROKER_PASSWORD || "Admin1234",
    reconnectPeriod: 1000,
    keepalive: 60,
    protocol: "ws" as MqttProtocol,
    //port: 9002,
    rejectUnauthorized: false,
    // ca: [Buffer.from('-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----', 'utf8')],
  };
  let mqttUrl = mqttLocalUrl;
  let mqttOptions = mqttLocalOptions;
  if (process.env.MQTT_BROKER_TYPE === "cloud") {
    mqttUrl = mqttCloudUrl;
    mqttOptions = mqttOptionsHiveMQ;
  }

export default class MqttClientManager {
    private state: MqttState = MqttState.Disconnected;
    private client: MqttClient | null = null;
    private shutdownRequested: boolean = false;
    private previousState: MqttState = MqttState.Disconnected;

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
            console.log(`[MQTT] Attempting to connect to MQTT broker at ${mqttUrl}...`);
            this.client = connect(mqttUrl, mqttOptions);

            await new Promise<void>((resolve, reject) => {
                this.client!.on('connect', () => { this.state = MqttState.Connected; resolve(); });
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
            await new Promise(resolve => setTimeout(resolve, RECONNECT_DELAY_MS));
        }
    }

    private async handleDisconnect(): Promise<void> {
        this.state = MqttState.Disconnecting;
        if (this.client) {
            await new Promise<void>(resolve => { this.client!.end(true, () => { this.client = null; this.state = MqttState.Disconnected; resolve(); }); });
        }
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

    public async subscribe(topic: string, messageHandler: (topic: string, message: Buffer) => void): Promise<void> {
        if (this.state === MqttState.Connected && this.client && this.client.connected) {
            this.client.subscribe(topic, { qos: 1 }, (err) => {
                if (err) {
                    console.error(`[MQTT] ❌ Failed to subscribe to topic ${topic}: ${err.message}`);
                } else {
                    console.log(`[MQTT] Subscribed to topic: ${topic}`);
                }
            });

            this.client.on('message', (recvTopic, message) => {
                if (recvTopic === topic) {
                    messageHandler(recvTopic, message);
                }
            });
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