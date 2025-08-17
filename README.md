# GETTING STARTED
run npm install to install all packages and dependencies
run npm run dev to launch local server

# DESCRIPTION
This websocket server provides a cloud-based server to facilitate communication between a plc (machine) with internet connection and an HMI and/or remote monitoring dashboard UIs

# CONNECTION
the websocket server acts as a middleman between the machine data/control and hmi/dashboards
- PLC connects to the websocket server by using a bridge service that connects OPCUA tags
- UIs connect to the websocket server by using a websocket client (inherent to the UI application)

# TESTING
run the test -> python-device-publisher.py to emulate a machine that has a robot device

# DEPLOY DETAILS
This is now being deployed on render:
https://machine-websocket-server.onrender.com

It was being deployed use flyio, but they didn't have a free tier.

# HOW THE BRIDGE WORKS
This bridge will manage a connection between a single machine (OpcuaServer) and a single MQTT Broker.
The bridge will publish device-based topics and some system topics to the MQTT Broker continuously.
Mqtt Clients will subscribe and unsubscripe to topics at will, which the broker will manage.

## OPCUA CONNECTION AND SUBSCRIPTION TO MONITORED NODES PROCEDURE
1. The bridge will await connection via a single session with the opcua server
2. Once connected, await a heartbeat connection (read and writing from and to two specific heartbeat nodes), and maintain this every 500ms
3. Read tag for an array tag called Machine.RegisteredDevices of type RegisteredDevice{deviceStoreIndex:UINT, deviceType:DeviceTypes enum, deviceTypeStoreIndex:UINT, deviceMqttTopicPath: string}, first device is the machine itself with its id. Each non empty entry in the registeredDeviceArray will be used to create a map of links, key = nodeId and Link{deviceIndex}. There will. the key will be the nodeId, derived from 
it will create a map of links: key will be nodeId subscription to opcua nodes to be relayed to the mqtt:

# HOW LINKS WORK
Changes to Device-based Opcua Monitored Nodes will trigger publish to mqtt topics to the broker, the topics use the devicePath plus the specific topic (e.g. status, cfg, data) to build the topic string, prepended with the machineId. Ideally this also occurs at some consistent period (250ms).

The Link will have to relay the opcua data into the corresponding json before publishing to broker. Mqtt data will contain the following structure MqttMessage{timestamp: number, payload: unknown}

MonitoredItems can immediately start publishing to the mqtt topic without waiting for mqtt clients to connect.

# MQTT CLIENT CONNECTION FLOW:
When a mqtt client sends a connection message
- store clientId in a map of mqttConnectedClients, if not already there
- if playload contains deviceRegistration, then send the device registration array.
- if payload from client contains heartbeat, then update heartbeat information (we will want to clear old clients)

# DATA STORAGE ON PLC
Machine.Devices[] for deviceStore which contains an array of device data (type Device) and Machine.<DeviceType>[] for deviceTypeStore (e.g. Machine.Axes[], Machine.Robots[])

# ON EXIT
On process shutdown (SIGINT): gracefully closes mqtt and opcua connections and stops heartbeat



UI will wait for the registered devices topic to publish before it creates its top level navigation for devices.