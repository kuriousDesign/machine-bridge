import { OPCUAClient, ClientSession, DataType, AttributeIds } from 'node-opcua';
import { MqttClient } from 'mqtt';

interface OpcuaNode {
  nodeId: string;
  mqttTopic: string;
}

interface MqttCommand {
  topic: string;
  nodeId: string;
}

export class OpcuaMqttBridge {
  private opcuaClient: OPCUAClient;
  private mqttClient: MqttClient;
  private session: ClientSession | null = null;
  private subscriptions: Map<string, any> = new Map();

  constructor(mqttClient: MqttClient, opcuaEndpoint: string) {
    this.mqttClient = mqttClient;
    this.opcuaClient = OPCUAClient.create({
      applicationName: 'OpcuaMqttBridge',
      connectionStrategy: {
        initialDelay: 1000,
        maxRetry: 1
      }
    });
  }

  async connect(): Promise<void> {
    await this.opcuaClient.connect();
    this.session = await this.opcuaClient.createSession();
    this.setupMqttCommandListener();
  }

  async disconnect(): Promise<void> {
    if (this.session) {
      await this.session.close();
    }
    await this.opcuaClient.disconnect();
  }

  async subscribeToNodes(nodes: OpcuaNode[]): Promise<void> {
    if (!this.session) throw new Error('Not connected to OPC UA server');

    for (const node of nodes) {
      const subscription = await this.session.createSubscription2({
        requestedPublishingInterval: 1000,
        requestedLifetimeCount: 100,
        requestedMaxKeepAliveCount: 10,
        maxNotificationsPerPublish: 100,
        publishingEnabled: true,
        priority: 10
      });

      const monitoredItem = await subscription.monitor({
        nodeId: node.nodeId,
        attributeId: AttributeIds.Value
      });

      monitoredItem.on('changed', (dataValue) => {
        this.mqttClient.publish(node.mqttTopic, JSON.stringify({
          value: dataValue.value.value,
          timestamp: dataValue.sourceTimestamp,
          quality: dataValue.statusCode.name
        }));
      });

      this.subscriptions.set(node.nodeId, subscription);
    }
  }

  private setupMqttCommandListener(): void {
    this.mqttClient.on('message', async (topic, message) => {
      if (topic.endsWith('/cmd')) {
        try {
          const command = JSON.parse(message.toString());
          await this.writeToOpcua(command.nodeId, command.value);
        } catch (error) {
          console.error('Error processing MQTT command:', error);
        }
      }
    });
  }

  private async writeToOpcua(nodeId: string, value: any): Promise<void> {
    if (!this.session) throw new Error('Not connected to OPC UA server');

    await this.session.write({
      nodeId: nodeId,
      attributeId: AttributeIds.Value,
      value: {
        value: {
          dataType: DataType.Variant,
          value: value
        }
      }
    });
  }

  subscribeMqttCommands(topics: string[]): void {
    topics.forEach(topic => {
      this.mqttClient.subscribe(`${topic}/cmd`);
    });
  }
}