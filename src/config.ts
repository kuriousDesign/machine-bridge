// config.ts
import 'dotenv/config'; // Load .env FIRST
import mqtt from 'mqtt';
import { nodeListString } from '@kuriousdesign/machine-sdk';
import { MessageSecurityMode, SecurityPolicy, CreateSubscriptionRequestOptions, DataChangeFilter, MonitoringParametersOptions, DataChangeTrigger, DeadbandType } from 'node-opcua';


// Centralized configuration object
export const Config: any = {
    ENABLE_DIAGNOSTICS: process.env.ENABLE_DIAGNOSTICS === 'true' || true,
    OPCUA_CONTROLLER_NAME: process.env.OPCUA_CONTROLLER_NAME || "DefaultController",
    OPCUA_SERVER_IP_ADDRESS: process.env.OPCUA_SERVER_IP_ADDRESS,
    OPCUA_PORT: process.env.OPCUA_PORT,
    MQTT_LOCAL_BROKER_URL: process.env.MQTT_LOCAL_BROKER_URL, // || "ws://localhost:9002/mqtt",
    MQTT_CLOUD_BROKER_URL: process.env.MQTT_CLOUD_BROKER_URL || "wss://9c4d3c046b704d16a1d64328cc4e4604.s1.eu.hivemq.cloud:8884/mqtt",
    MQTT_BROKER_USERNAME: process.env.MQTT_BROKER_USERNAME || "admin",
    MQTT_BROKER_PASSWORD: process.env.MQTT_BROKER_PASSWORD || "Admin1234",
    MQTT_BROKER_TYPE: process.env.MQTT_BROKER_TYPE,

};

// Derive complex config values using the Config object
Config.OPCUA_ENDPOINT = `opc.tcp://${Config.OPCUA_SERVER_IP_ADDRESS}:${Config.OPCUA_PORT}`;
Config.NODE_LIST_PREFIX = nodeListString + Config.OPCUA_CONTROLLER_NAME + '.Application.';

Config.POLLING_RATE_MS = 250;
Config.REPUBLISH_RATE_MS = 500;
Config.LOOP_DELAY_MS = 250; // Small delay to prevent tight loop
Config.DIAG_READS_TO_SKIP_AT_START = 10;
Config.RECONNECT_DELAY_MS = 3000; // Time to wait before attempting reconnection
Config.CHUNK_SIZE = 100; // Number of nodes to read per chunk
Config.PUBLISHING_INTERVAL = 500; // OPC UA Publishing Interval in ms

Config.OPCUA_OPTIONS = {
    applicationName: 'OpcuaMqttBridge',
    securityMode: MessageSecurityMode.None,
    securityPolicy: SecurityPolicy.None,
    endpointMustExist: true,
    keepSessionAlive: true, // Let node-opcua handle internal session heartbeat
};

Config.SUBSCRIPTION_OPTIONS = {
    maxNotificationsPerPublish: 2000,
    publishingEnabled: true,
    requestedLifetimeCount: 100,
    requestedMaxKeepAliveCount: 10,
    requestedPublishingInterval: Config.PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
} as CreateSubscriptionRequestOptions;

const filter = new DataChangeFilter({
    trigger: DataChangeTrigger.StatusValue, // Report on any of Status, Value, or Timestamp
    deadbandType: DeadbandType.None,                 // No deadband suppression
    deadbandValue: 0
});

Config.OPTIONS_GROUP = {
    discardOldest: true,
    queueSize: 1,
    samplingInterval: Config.PUBLISHING_INTERVAL, //affects lag, decrease to reduce lag
    filter
} as MonitoringParametersOptions;


Config.BRIDGE_API_UPDATE_DEVICE = "bridge/api/update_device";

// Derive MQTT URL and Options based on Config settings
Config.MQTT_URL = Config.MQTT_BROKER_TYPE === "cloud" ? Config.MQTT_CLOUD_BROKER_URL : Config.MQTT_LOCAL_BROKER_URL;

Config.MQTT_OPTIONS = Config.MQTT_BROKER_TYPE === "cloud" ? {
    //cloud
    username: Config.MQTT_BROKER_USERNAME,
    password: Config.MQTT_BROKER_PASSWORD,
    reconnectPeriod: 1000,
    keepalive: 60,
    protocol: "wss" as mqtt.MqttProtocol,
    rejectUnauthorized: true,
} : { 
    //local
    username: Config.MQTT_BROKER_USERNAME,
    password: Config.MQTT_BROKER_PASSWORD,
    reconnectPeriod: 1000,
    keepalive: 60,
    protocol: "ws" as mqtt.MqttProtocol,
    rejectUnauthorized: false,
};

// Export Config from here
export default Config;
