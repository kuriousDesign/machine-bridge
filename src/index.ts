import { connect } from "mqtt";
import { OpcuaMqttBridge } from "./OpcuaMqttBridge";

async function main() {
  // MQTT setup
  const mqttUrl = "mqtt://localhost:1883"; // adjust to your broker
  const mqttClient = connect(mqttUrl);

  mqttClient.on("connect", () => {
    console.log("✅ Connected to MQTT broker:", mqttUrl);
  });

  // OPC UA setup
  const opcuaEndpoint = "opc.tcp://localhost:4840"; // adjust to your OPC UA server
  const bridge = new OpcuaMqttBridge(mqttClient, opcuaEndpoint);

  try {
    await bridge.connect();

    // Subscribe to OPC UA nodes and publish to MQTT
    await bridge.subscribeToNodes([
      { nodeId: "ns=1;s=Temperature", mqttTopic: "sensors/temperature" },
      { nodeId: "ns=1;s=Pressure", mqttTopic: "sensors/pressure" }
    ]);

    // Subscribe to MQTT commands for those topics
    bridge.subscribeMqttCommands(["sensors/temperature", "sensors/pressure"]);

    console.log("🚀 OPC UA ↔ MQTT Bridge running");
  } catch (err) {
    console.error("Bridge error:", err);
    process.exit(1);
  }
}

main();
