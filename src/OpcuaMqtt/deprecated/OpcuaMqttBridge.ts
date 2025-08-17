/**
 * OPCUA MQTT BRIDGE - Functional Description
 * 
 * # HOW THE BRIDGE WORKS
This bridge will manage a connection between a single machine (OpcuaServer) and a single MQTT Broker.
The bridge will publish device-based topics and some system topics to the MQTT Broker continuously.
Mqtt Clients will subscribe and unsubscripe to topics at will, which the broker will manage (not this code)

## OPCUA CONNECTION AND SUBSCRIPTION PROCEDURE
1. The bridge will establish connection via a single session with the opcua server
2. Once connected, establish a heartbeat connection (read and writing from and to two specific heartbeat nodes),  2. Sets up a timer that reads from PLC heartbeat node and writes to HMI heartbeat node every 500ms
3. Once heartbeat health with plc is determined, it will create links subscription to opcua nodes to be relayed to the mqtt and vice versa:
    a. the list of links is first determined by reading an array tag called Machine.RegisteredDevices of type RegisteredDevice{deviceIndex:UINT, deviceType:DeviceTypes enum, devicePath: string}, first device is the machine itself with its id
4. subscribe to machineConnectTopic and listen for mqtt clients who have just connected to the broker and want to setup their connection to the bridge.
5. 
# HOW LINKS WORK
Changes to Device-based Opcua Monitored Nodes will trigger publish to mqtt topics to the broker, the topics use the devicePath plus the specific topic (e.g. status, cfg, data) to build the topic string, prepended with the machineId. Ideally this also occurs at some consistent period (250ms).

## DATA STORAGE ON PLC
Machine.Devices[] for generic device data {Cfg, Status, Errors} and Machine.<DeviceType>[] for deviceType specific data {Cfg, Data}

The Link will have to translate the opcua data into the corresponding json before publishing to broker. Mqtt data will contain the following structure MqttMessage{timestamp: number, payload: unknown}

UI will wait for the registered devices topic to publish before it creates its top level navigation for devices.
 * 
 
 *
 * MQTT CLIENT CONNECTION FLOW:
 * 4. When a mqtt client sends a connection message
 *    - store clientId in a map of mqttConnectedClients, if not already there
 *    - if playload contains deviceRegistration, then send the device registration array.
 *    - if payload from client contains heartbeat, then update heartbeat information (we will want to clear old clients)

 * 
 * REAL-TIME DATA MONITORING:
 * 5. Creates subscriptions to monitor specified OPC UA nodes for value changes
 * 6. When monitored values change, automatically emits 'newValue' events to the web client
 * 7. Handles both scalar values and arrays appropriately
 * 
 * CLIENT OPERATION HANDLERS:
 * 8. 'login' - Writes username/password to OPC UA nodes and triggers login button
 * 9. 'logout' - Triggers logout button in OPC UA server
 * 10. 'writeSimple' - Writes basic data types (string, number, boolean) to OPC UA nodes
 * 11. 'writeArray' - Writes array data (specifically Int16Array) to OPC UA nodes
 * 12. 'writeButtonPress' - Simulates button press (writes true, then false after delay)
 * 13. 'writeExtension' - Writes complex extension objects to OPC UA nodes
 * 14. 'recipeDelete' - Complex operation that reads recipe array, deletes item, sorts, and writes back
 * 
 * CONNECTION MANAGEMENT:
 * 15. Tracks all active connections in a Map for cleanup purposes
 * 16. When client disconnects: terminates subscriptions, closes sessions, disconnects OPC UA clients
 * 17. On process shutdown (SIGINT): gracefully closes all connections and stops heartbeat
 * 
 * KEY GOTCHAS:
 * - Each Socket.IO client gets its own dedicated OPC UA connection (not shared)
 * - Heartbeat runs independently of client connections
 * - Button presses auto-reset to false after configured delay
 * - Recipe deletion involves complex array manipulation and sorting
 * - All OPC UA node IDs are prefixed with controller-specific paths
 */




import { Server, Socket } from 'socket.io';
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
} from '@robotic-workcell/sdk';

// const endpointUrl = 'opc.tcp://5UVSEL0-ZENBOOK:4334/UA/MyLittleServer';
// const endpointUrl: string = config.get('opcuaEndpointUrl');
const endpointUrl: string = process.env.opcuaEndpointUrl;
const controllerName: string = process.env.opcuaControllerName;

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
  requestedPublishingInterval: 50, //affects lag, decrease to reduce lag
};

const optionsGroup = {
  discardOldest: true,
  queueSize: 1,
  samplingInterval: 50, //affects lag, decrease to reduce lag
};

interface ClientConnection {
  id: string;
  socketConnection: Socket;
  opcuaSession: ClientSession;
  opcuaClient: OPCUAClient;
}

interface HeartBeatConnection {
  opcuaSession: ClientSession;
  opcuaClient: OPCUAClient;
  timer: NodeJS.Timer;
}



class OpcuaManager {
  private socketio: Server;
  // private opcuaClient: OPCUAClient;
  private heartBeatConnection: HeartBeatConnection | null = null;
  private connections: Map<string, ClientConnection>;
  private nodeListPrefix: string;
  private nodeTypePrefix: string;

  constructor(socketio: Server) {
    this.connections = new Map<string, ClientConnection>();
    this.socketio = socketio;
    this.nodeListPrefix = config.get('nodeListPrefix') + controllerName + '.Application.';
    this.nodeTypePrefix = config.get('nodeTypePrefix') + controllerName + '.Application.';
    console.log('Opcua Server Name: ' + controllerName);
    console.log('Opcua Server location: ' + endpointUrl);
    this.initialize();
  }

  private async initialize() {
    try {
      const hbOpcuaClient: OPCUAClient = OPCUAClient.create(opcuaOptions);
      await hbOpcuaClient.connect(endpointUrl);
      const hbOpcuaSession: ClientSession = await hbOpcuaClient.createSession();

      const heartbeatPLCNodeId = `${this.nodeListPrefix}${config.get(
        'heartbeatPLCNodeId',
      )}`;
      const heartbeatHMINodeId = `${this.nodeListPrefix}${config.get(
        'heartbeatHMINodeId',
      )}`;

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
          console.error(error.message);
        }
      }, 500);

      console.log('Starting heartbeat');

      this.heartBeatConnection = {
        opcuaClient: hbOpcuaClient,
        opcuaSession: hbOpcuaSession,
        timer,
      };

      this.socketio.on('connection', async (socket: Socket) => {
        // socket.emit('welcome', 'hello there!');
        console.log(`new socket connection ${socket.id}`);
        const opcuaClient: OPCUAClient = OPCUAClient.create(opcuaOptions);
        opcuaClient.on('connected', () => {
          console.log('opcua client connected');
          socket.emit('status', 'connected');
        });
        await opcuaClient.connect(endpointUrl);
        const opcuaSession: ClientSession = await opcuaClient.createSession();
        this.connections.set(socket.id, {
          id: socket.id,
          socketConnection: socket,
          opcuaSession,
          opcuaClient,
        });

        opcuaClient.on('start_reconnection', () => {
          console.log('opcua client trying to reconnect');
          socket.emit('status', 'reconnecting');
        });
        opcuaSession.on('session_restored', () => {
          console.log('session restored');
          socket.emit('status', 'reconnected');
        });
        const itemsToMonitor = (<Array<string>>config.get('nodeList')).map(nodeId => {
          return {
            attributeId: AttributeIds.Value,
            nodeId: `${this.nodeListPrefix}${nodeId}`,
          };
        });

        const subscription: ClientSubscription = await opcuaSession.createSubscription2(
          subscriptionOptions,
        );
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
            socket.emit('newValue', newValue);
          },
        );
        socket.on('login', async (data: LoginInput) => {
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
        });

        socket.on('logout', async () => {
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
        });

        socket.on('writeSimple', async (data: UserInput) => {
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
        });

        socket.on('writeArray', async (data: UserInput) => {
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
        });

        socket.on('writeButtonPress', async (data: UserInput) => {
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

          //make button press false after several milliseconds
          if (data.value === true) {
            const nodeToWriteBackToFalse: WriteValueOptions = { ...nodeToWrite };
            nodeToWriteBackToFalse.value.value.value = false;
            setTimeout(async () => {
              await opcuaSession.write(nodeToWriteBackToFalse);
            }, <number>config.get('buttonPressDelayMS'));
          }
        });

        socket.on('writeExtension', async (data: UserInput) => {
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
          // const nodeToWrite: WriteValueOptions = {
          //   nodeId: `${this.nodeListPrefix}${data.nodeId}`,
          //   attributeId: AttributeIds.Value,
          //   value: {
          //     value: {
          //       dataType: DataType.String,
          //       value: data.value,
          //     },
          //   },
          // };
          // opcuaSession.write(nodeToWrite, (err, result) => {
          //   if (err) {
          //     console.warn(err);
          //   }
          // });
        });

        socket.on('recipeDelete', async (data: RecipeDeleteInput) => {
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
        });

        socket.on('disconnect', async () => {
          console.log(`deleting socket connection ${socket.id}`);
          await subscription.terminate();
          await opcuaSession.close();
          await opcuaClient.disconnect();
          this.connections.delete(socket.id);
        });
      });
      process.on('SIGINT', async () => {
        console.log('shutting down connections');
        for (const [socketId, connection] of this.connections.entries()) {
          this.socketio.to(socketId).emit('status', 'shutdown');
          await connection.opcuaSession.close();
          await connection.opcuaClient.disconnect();
        }

        console.log('Ending heartbeat');
        clearInterval(this.heartBeatConnection.timer);
        await this.heartBeatConnection.opcuaSession.close();
        await this.heartBeatConnection.opcuaClient.disconnect();
        process.exit();
      });
    } catch (error) {
      console.error(error.message);
    }
  }
}

export default OpcuaManager;
