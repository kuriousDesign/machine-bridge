import { MqttProtocol } from "mqtt";
import OpcuaMqttBridge from "./OpcuaMqtt/OpcuaMqttBridge";
import "dotenv/config"; // auto-loads .env

async function main() {
  // MQTT setup
  const mqttUrl = process.env.MQTT_BROKER_URL || "wss://9c4d3c046b704d16a1d64328cc4e4604.s1.eu.hivemq.cloud:8884/mqtt";
  const mqttOptions = {
      username: 'admin', 
      password: 'Admin1234',
      reconnectPeriod: 1000,
      keepalive: 60,
      protocol: 'wss' as MqttProtocol,
      port: 8884,
      rejectUnauthorized: true, // Enforce strict certificate validation
      // Uncomment and set if a custom CA certificate is needed
      // ca: [Buffer.from('-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----', 'utf8')],
    };

  const opcuaControllerName = process.env.OPCUA_CONTROLLER_NAME || "DefaultController";
  // OPC UA setup
  const opcuaEndpoint = `opc.tcp://${process.env.OPCUA_SERVER_IP_ADDRESS}:${process.env.OPCUA_PORT}`;
  

  try {
    const bridge = new OpcuaMqttBridge(mqttUrl, mqttOptions, opcuaEndpoint, opcuaControllerName);
    console.log("🚀 OPC UA ↔ MQTT Bridge running");
  } catch (err) {
    console.error("Bridge error:", err);
    process.exit(1);
  }
}

main();
