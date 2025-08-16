import mqtt, { MqttClient } from 'mqtt';
import {
  AttributeIds,
  ClientMonitoredItemBase,
  ClientMonitoredItemGroup,
  ClientSession,
  ClientSubscription,
  ConnectionStrategyOptions,
  DataType,
  DataValue,
  MessageSecurityMode,
  NodeId,
  OPCUAClient,
  OPCUAClientOptions,
  resolveNodeId,
  SecurityPolicy,
  StatusCode,
  StatusCodes,
  TimestampsToReturn,
  VariantArrayType,
  WriteValueOptions,
} from 'node-opcua';
import config from 'config';
import {
  NewValue,
  UserInput,
  LoginInput,
  InputNodeId,
  RecipeDeleteInput,
  RecipeData,
} from '@kuriousdesign/machine-sdk';

const endpointUrl: string = process.env.opcuaEndpointUrl;
const controllerName: string = process.env.opcuaControllerName;
const mqttBrokerUrl: string = process.env.mqttBrokerUrl || 'mqtt://localhost:1883';

const connectionStrategy: ConnectionStrategyOptions = {
  initialDelay: 1000,
  maxDelay: 4000,
};

const opcuaOptions: OPCUAClientOptions = {
  applicationName: 'NodeOPCUA-Client',
  connectionStrategy: connectionStrategy,
  securityMode: MessageSecurityMode.None,
  securityPolicy: SecurityPolicy.None,
  endpointMustExist: true,
  keepSessionAlive: true,
};

const subscriptionOptions = {
  maxNotificationsPerPublish: 1000,
  publishingEnabled: true,
  requestedLifetimeCount: 100,
  requestedMaxKeepAliveCount: 10,
  requestedPublishingInterval: 50,
};

const optionsGroup = {
  discardOldest: true,
  queueSize: 1,
  samplingInterval: 50,
};

interface MqttConnection {
  id: string;
  opcuaSession: ClientSession;
  opcuaClient: OPCUAClient;
}

interface HeartBeatConnection {
  opcuaSession: ClientSession;
  opcuaClient: OPCUAClient;
  timer: NodeJS.Timer;
}

class OpcuaMqttBridge {
  private mqttClient: MqttClient;
  private heartBeatConnection: HeartBeatConnection;
  private connection: MqttConnection;
  private nodeListPrefix: string;
  private nodeTypePrefix: string;
  private readonly topicPrefix: string = 'opcua';

  constructor() {
    this.nodeListPrefix = config.get('nodeListPrefix') + controllerName + '.Application.';
    this.nodeTypePrefix = config.get('nodeTypePrefix') + controllerName + '.Application.';
    console.log('Opcua Server Name: ' + controllerName);
    console.log('Opcua Server location: ' + endpointUrl);
    console.log('MQTT Broker: ' + mqttBrokerUrl);
    this.initialize();
  }

  private async initialize() {
    try {
      // Initialize MQTT client
      this.mqttClient = mqtt.connect(mqttBrokerUrl);
      
      this.mqttClient.on('connect', () => {
        console.log('MQTT client connected');
        this.publishStatus('connected');
        this.subscribeToTopics();
      });

      this.mqttClient.on('error', (error) => {
        console.error('MQTT error:', error);
        this.publishStatus('error');
      });

      this.mqttClient.on('message', (topic, message) => {
        this.handleMqttMessage(topic, message);
      });

      // Initialize heartbeat connection
      const hbOpcuaClient: OPCUAClient = OPCUAClient.create(opcuaOptions);
      await hbOpcuaClient.connect(endpointUrl);
      const hbOpcuaSession: ClientSession = await hbOpcuaClient.createSession();

      const heartbeatPLCNodeId = `${this.nodeListPrefix}${config.get('heartbeatPLCNodeId')}`;
      const heartbeatHMINodeId = `${this.nodeListPrefix}${config.get('heartbeatHMINodeId')}`;

      const timer: NodeJS.Timer = setInterval(async () => {
        try {
          const receivingValue = await hbOpcuaSession.read({
            nodeId: heartbeatPLCNodeId,
            attributeId: AttributeIds.Value,
          });

          const sendingValue: WriteValueOptions = {
            nodeId: heartbeatHMINodeId,
            attributeId: AttributeIds.Value,
            value: {
              value: {
                dataType: DataType.Byte,
                value: receivingValue.value.value,
              },
            },
          };
          await hbOpcuaSession.write(sendingValue);
        } catch (error) {
          console.error('Heartbeat error:', error.message);
        }
      }, 500);

      console.log('Starting heartbeat');

      this.heartBeatConnection = {
        opcuaClient: hbOpcuaClient,
        opcuaSession: hbOpcuaSession,
        timer,
      };

      // Initialize main OPC UA connection
      await this.initializeOpcuaConnection();

      process.on('SIGINT', async () => {
        await this.shutdown();
      });
    } catch (error) {
      console.error('Initialization error:', error.message);
    }
  }

  private async initializeOpcuaConnection() {
    const opcuaClient: OPCUAClient = OPCUAClient.create(opcuaOptions);
    
    opcuaClient.on('connected', () => {
      console.log('Main OPC UA client connected');
      this.publishStatus('opcua_connected');
    });

    opcuaClient.on('start_reconnection', () => {
      console.log('OPC UA client trying to reconnect');
      this.publishStatus('opcua_reconnecting');
    });

    await opcuaClient.connect(endpointUrl);
    const opcuaSession: ClientSession = await opcuaClient.createSession();

    opcuaSession.on('session_restored', () => {
      console.log('OPC UA session restored');
      this.publishStatus('opcua_reconnected');
    });

    this.connection = {
      id: 'main',
      opcuaSession,
      opcuaClient,
    };

    // Setup monitoring
    await this.setupMonitoring(opcuaSession);
  }

  private async setupMonitoring(opcuaSession: ClientSession) {
    const itemsToMonitor = (<Array<string>>config.get('nodeList')).map(nodeId => {
      return {
        attributeId: AttributeIds.Value,
        nodeId: `${this.nodeListPrefix}${nodeId}`,
      };
    });

    const subscription: ClientSubscription = await opcuaSession.createSubscription2(subscriptionOptions);
    const monitoredItemGroup = ClientMonitoredItemGroup.create(
      subscription,
      itemsToMonitor,
      optionsGroup,
      TimestampsToReturn.Both,
    );

    monitoredItemGroup.on(
      'changed',
      (monitoredItem: ClientMonitoredItemBase, dataValue: DataValue) => {
        const newValue: NewValue = {
          nodeId: monitoredItem.itemToMonitor.nodeId
            .toString()
            .replace(this.nodeListPrefix, ''),
          value:
            dataValue.value.arrayType === VariantArrayType.Array
              ? Array.from(dataValue.value.value)
              : dataValue.value.value,
        };

        console.log(monitoredItem.itemToMonitor.nodeId.toString());
        this.publishNewValue(newValue);
      },
    );
  }

  private subscribeToTopics() {
    const topics = [
      `${this.topicPrefix}/login`,
      `${this.topicPrefix}/logout`,
      `${this.topicPrefix}/writeSimple`,
      `${this.topicPrefix}/writeArray`,
      `${this.topicPrefix}/writeButtonPress`,
      `${this.topicPrefix}/writeExtension`,
      `${this.topicPrefix}/recipeDelete`,
    ];

    topics.forEach(topic => {
      this.mqttClient.subscribe(topic, (err) => {
        if (err) {
          console.error(`Failed to subscribe to ${topic}:`, err);
        } else {
          console.log(`Subscribed to ${topic}`);
        }
      });
    });
  }

  private async handleMqttMessage(topic: string, message: Buffer) {
    try {
      const data = JSON.parse(message.toString());
      const opcuaSession = this.connection?.opcuaSession;

      if (!opcuaSession) {
        console.error('No OPC UA session available');
        return;
      }

      switch (topic) {
        case `${this.topicPrefix}/login`:
          await this.handleLogin(opcuaSession, data as LoginInput);
          break;
        case `${this.topicPrefix}/logout`:
          await this.handleLogout(opcuaSession);
          break;
        case `${this.topicPrefix}/writeSimple`:
          await this.handleWriteSimple(opcuaSession, data as UserInput);
          break;
        case `${this.topicPrefix}/writeArray`:
          await this.handleWriteArray(opcuaSession, data as UserInput);
          break;
        case `${this.topicPrefix}/writeButtonPress`:
          await this.handleWriteButtonPress(opcuaSession, data as UserInput);
          break;
        case `${this.topicPrefix}/writeExtension`:
          await this.handleWriteExtension(opcuaSession, data as UserInput);
          break;
        case `${this.topicPrefix}/recipeDelete`:
          await this.handleRecipeDelete(opcuaSession, data as RecipeDeleteInput);
          break;
        default:
          console.log(`Unhandled topic: ${topic}`);
      }
    } catch (error) {
      console.error(`Error handling message for topic ${topic}:`, error);
    }
  }

  private publishStatus(status: string) {
    this.mqttClient.publish(`${this.topicPrefix}/status`, status);
  }

  private publishNewValue(newValue: NewValue) {
    this.mqttClient.publish(`${this.topicPrefix}/newValue`, JSON.stringify(newValue));
  }

  private async handleLogin(opcuaSession: ClientSession, data: LoginInput) {
    const usernameNodeToWrite: WriteValueOptions = {
      nodeId: `${this.nodeListPrefix}${InputNodeId.Username}`,
      attributeId: AttributeIds.Value,
      value: {
        value: {
          dataType: DataType.String,
          value: data.username,
        },
      },
    };

    const passwordNodeToWrite: WriteValueOptions = {
      nodeId: `${this.nodeListPrefix}${InputNodeId.Password}`,
      attributeId: AttributeIds.Value,
      value: {
        value: {
          dataType: DataType.String,
          value: data.password,
        },
      },
    };

    const loginBtnNodeToWrite: WriteValueOptions = {
      nodeId: `${this.nodeListPrefix}${InputNodeId.LoginBtnBlick}`,
      attributeId: AttributeIds.Value,
      value: {
        value: {
          dataType: DataType.Boolean,
          value: true,
        },
      },
    };

    await opcuaSession.write(usernameNodeToWrite);
    await opcuaSession.write(passwordNodeToWrite);
    await opcuaSession.write(loginBtnNodeToWrite);
  }

  private async handleLogout(opcuaSession: ClientSession) {
    const logoutBtnNodeToWrite: WriteValueOptions = {
      nodeId: `${this.nodeListPrefix}${InputNodeId.LogoutBtnBlick}`,
      attributeId: AttributeIds.Value,
      value: {
        value: {
          dataType: DataType.Boolean,
          value: true,
        },
      },
    };
    await opcuaSession.write(logoutBtnNodeToWrite);
  }

  private async handleWriteSimple(opcuaSession: ClientSession, data: UserInput) {
    console.log(`user input\n`, data);
    const nodeToWrite: WriteValueOptions = {
      nodeId: `${this.nodeListPrefix}${data.nodeId}`,
      attributeId: AttributeIds.Value,
      value: {
        value: {
          dataType: <number>data.nodeDataType,
          value: data.value,
        },
      },
    };
    await opcuaSession.write(nodeToWrite);
  }

  private async handleWriteArray(opcuaSession: ClientSession, data: UserInput) {
    console.log(`user input\n`, data);
    const arrayOfValue = new Int16Array(data.value);
    const nodeToWrite: WriteValueOptions = {
      nodeId: `${this.nodeListPrefix}${data.nodeId}`,
      attributeId: AttributeIds.Value,
      value: {
        value: {
          dataType: DataType.Int16,
          arrayType: VariantArrayType.Array,
          value: arrayOfValue,
        },
      },
    };
    await opcuaSession.write(nodeToWrite);
  }

  private async handleWriteButtonPress(opcuaSession: ClientSession, data: UserInput) {
    console.log(`user input\n`, data);
    const nodeToWrite: WriteValueOptions = {
      nodeId: `${this.nodeListPrefix}${data.nodeId}`,
      attributeId: AttributeIds.Value,
      value: {
        value: {
          dataType: DataType.Boolean,
          value: data.value,
        },
      },
    };
    await opcuaSession.write(nodeToWrite);

    if (data.value === true) {
      const nodeToWriteBackToFalse: WriteValueOptions = { ...nodeToWrite };
      nodeToWriteBackToFalse.value.value.value = false;
      setTimeout(async () => {
        await opcuaSession.write(nodeToWriteBackToFalse);
      }, <number>config.get('buttonPressDelayMS'));
    }
  }

  private async handleWriteExtension(opcuaSession: ClientSession, data: UserInput) {
    console.log(`user input\n`, data);
    const nodeId = `${this.nodeListPrefix}${data.nodeId}`;
    const extObject = await opcuaSession.constructExtensionObject(
      resolveNodeId(`${this.nodeTypePrefix}${data.nodeDataType}`),
      data.value,
    );
    console.log(extObject);
    const statusCode: StatusCode = await opcuaSession.write({
      nodeId: nodeId,
      attributeId: AttributeIds.Value,
      value: {
        statusCode: StatusCodes.Good,
        value: {
          dataType: DataType.ExtensionObject,
          value: extObject,
        },
      },
    });
    console.log(statusCode);
  }

  private async handleRecipeDelete(opcuaSession: ClientSession, data: RecipeDeleteInput) {
    const recipeDataNodeId: string = config.get('recipeDataNodeId');
    const recipeData: RecipeData[] = (
      await opcuaSession.read({
        nodeId: `${this.nodeListPrefix}${recipeDataNodeId}`,
        attributeId: AttributeIds.Value,
      })
    ).value.value;
    const indexToDelete = data.index;
    recipeData[indexToDelete] = {
      ncProgramNumber: 0,
      extrusionLength_mm: 0,
      extrusionType: 0,
      partName: '',
      partWeight_kg: 0,
    };

    recipeData.sort((a, b) => b.ncProgramNumber - a.ncProgramNumber);

    const extObjArray = [];
    for (let index = 0; index < recipeData.length; index++) {
      const extObject = await opcuaSession.constructExtensionObject(
        resolveNodeId(`${this.nodeTypePrefix}RecipeData`),
        recipeData[index],
      );
      extObjArray.push(extObject);
    }

    const arrayNodeToWrite: WriteValueOptions = {
      nodeId: `${this.nodeListPrefix}${recipeDataNodeId}`,
      attributeId: AttributeIds.Value,
      value: {
        value: {
          dataType: DataType.ExtensionObject,
          arrayType: VariantArrayType.Array,
          value: extObjArray,
        },
      },
    };

    await opcuaSession.write(arrayNodeToWrite);
  }

  private async shutdown() {
    console.log('Shutting down connections');
    
    if (this.connection) {
      await this.connection.opcuaSession.close();
      await this.connection.opcuaClient.disconnect();
    }

    console.log('Ending heartbeat');
    if (this.heartBeatConnection) {
      clearInterval(this.heartBeatConnection.timer);
      await this.heartBeatConnection.opcuaSession.close();
      await this.heartBeatConnection.opcuaClient.disconnect();
    }

    if (this.mqttClient) {
      this.mqttClient.end();
    }

    process.exit();
  }
}

export default OpcuaMqttBridge;
