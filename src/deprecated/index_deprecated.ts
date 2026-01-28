import { MqttProtocol } from "mqtt";
//import OpcuaMqttBridge from "./OpcuaMqtt/OpcuaMqttBridgeV1";
import OpcuaMqttBridge from "../OpcuaMqtt/OpcuaMqttBridge";
import "dotenv/config"; // auto-loads .env

// Feature flag: Set to 'v2' to use the new simplified bridge architecture
// V1: Subscribes to individual device properties (many subscriptions, many MQTT topics)
// V2: Subscribes to top-level device nodes (fewer subscriptions, one MQTT topic per device)

async function main() {
  // MQTT setup
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

  const opcuaControllerName =
    process.env.OPCUA_CONTROLLER_NAME || "DefaultController";

  const opcuaEndpoint = `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;

  let bridge: OpcuaMqttBridge | null = null;

  try {
    
    console.log('🚀 Starting OPC UA ↔ MQTT Bridge (simplified architecture)');
    bridge = new OpcuaMqttBridge(
      mqttUrl,
      mqttOptions,
      opcuaEndpoint,
      opcuaControllerName
    );
    console.log(`✅ OPC UA ↔ MQTT Bridge running`);
  } catch (err) {
    console.error("Bridge error:", err);
    process.exit(1);
  }

  // --- Graceful Shutdown Handlers ---
  const shutdown = async (signal: string) => {
    console.log(`\n${signal} received — shutting down bridge...`);
    if (bridge) {
      try {
        await bridge.shutdown();
      } catch (e) {
        console.error("Error during shutdown:", e);
      }
    }
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("uncaughtException", (err) => {
    console.error("Uncaught exception:", err);
    shutdown("UNCAUGHT_EXCEPTION");
  });
  process.on("unhandledRejection", (reason) => {
    console.error("Unhandled rejection:", reason);
    shutdown("UNHANDLED_REJECTION");
  });
}

main();
