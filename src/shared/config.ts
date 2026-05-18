// config.ts
import 'dotenv/config'; // Load .env FIRST
import os from 'os';
import mqtt from 'mqtt';
import { nodeListString } from '@kuriousdesign/machine-sdk';
import { makeApplicationUrn, MessageSecurityMode, SecurityPolicy, OPCUAClientOptions, CreateSubscriptionRequestOptions, DataChangeFilter, MonitoringParametersOptions, DataChangeTrigger, DeadbandType } from 'node-opcua';

const sharedApplicationName = process.env.OPCUA_APPLICATION_NAME || 'OpcuaMqttBridge';

export const ApplicationIdentity = {
    applicationName: sharedApplicationName,
    applicationUri: process.env.OPCUA_APPLICATION_URI || makeApplicationUrn(os.hostname(), sharedApplicationName),
} as const;

export function createSharedOpcuaClientOptions(overrides: Partial<OPCUAClientOptions> = {}): OPCUAClientOptions {
    return {
        applicationName: ApplicationIdentity.applicationName,
        applicationUri: ApplicationIdentity.applicationUri,
        securityMode: MessageSecurityMode.None,
        securityPolicy: SecurityPolicy.None,
        endpointMustExist: true,
        keepSessionAlive: true,
        ...overrides,
    } as OPCUAClientOptions;
}


// Centralized configuration object
export const Config: any = {
    ENABLE_DIAGNOSTICS: process.env.ENABLE_DIAGNOSTICS === 'true' || true,
    SHOW_SUCCESSFUL_TAG_SUBSCRIPTION_LOGS: process.env.SHOW_SUCCESSFUL_TAG_SUBSCRIPTION_LOGS === 'true',
    OPCUA_CONTROLLER_NAME: process.env.OPCUA_CONTROLLER_NAME || "DefaultController",
    OPCUA_SERVER_IP_ADDRESS: process.env.OPCUA_SERVER_IP_ADDRESS,
    OPCUA_PORT: process.env.OPCUA_PORT,
    ENABLE_STALE_POLLING_REPUBLISH: process.env.ENABLE_STALE_POLLING_REPUBLISH === 'true',
    MQTT_LOCAL_BROKER_URL: process.env.MQTT_LOCAL_BROKER_URL, // || "ws://localhost:9002/mqtt",
    MQTT_REMOTE_BROKER_URL: process.env.MQTT_REMOTE_BROKER_URL,
    MQTT_CLOUD_BROKER_URL: process.env.MQTT_CLOUD_BROKER_URL || "wss://9c4d3c046b704d16a1d64328cc4e4604.s1.eu.hivemq.cloud:8884/mqtt",
    MQTT_BROKER_USERNAME: process.env.MQTT_BROKER_USERNAME || "admin",
    MQTT_BROKER_PASSWORD: process.env.MQTT_BROKER_PASSWORD || "Admin1234",
    MQTT_BROKER_TYPE: process.env.MQTT_BROKER_TYPE,
    ApplicationIdentity,

};

// Derive complex config values using the Config object
Config.OPCUA_ENDPOINT = `opc.tcp://${Config.OPCUA_SERVER_IP_ADDRESS}:${Config.OPCUA_PORT}`;
Config.NODE_LIST_PREFIX = nodeListString + Config.OPCUA_CONTROLLER_NAME + '.Application.';

Config.BRIDGE_STATUS_PUBLISH_INTERVAL_MS = 1000;
Config.POLLING_RATE_MS = 250;
Config.REPUBLISH_RATE_MS = 5000; // only used when ENABLE_STALE_POLLING_REPUBLISH is true, to prevent tight loop of republishing unchanged values
//Config.LOOP_DELAY_MS = 250; // Small delay to prevent tight loop
Config.DIAG_READS_TO_SKIP_AT_START = 10;
Config.RECONNECT_DELAY_MS = 1000; // Time to wait before attempting reconnection
Config.OPCUA_CONNECT_LOG_INTERVAL_MS = 10000;
Config.CHUNK_SIZE = 100; // Number of nodes to read per chunk
Config.PUBLISHING_INTERVAL = 0; // change this to 0 to disable (only fires update when value changes) OPC UA Publishing Interval in ms
Config.SAMPLING_INTERVAL = 100; // OPC UA Sampling Interval in ms, affects how often we check for changes

Config.OPCUA_OPTIONS = createSharedOpcuaClientOptions({
    requestedSessionTimeout: 60000,
    defaultTransactionTimeout: 30000,
}) as OPCUAClientOptions;

Config.SUBSCRIPTION_OPTIONS = {
    maxNotificationsPerPublish: 2000,
    publishingEnabled: true,
    requestedLifetimeCount: 100,
    requestedMaxKeepAliveCount: 5,
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
    samplingInterval: Config.SAMPLING_INTERVAL, //affects lag, decrease to reduce lag
    filter
} as MonitoringParametersOptions;


Config.BRIDGE_API_UPDATE_DEVICE = "bridge/api/update_device";
Config.BRIDGE_API_WRITE_TAG = "bridge/api/write_tag";

// Derive MQTT URL and Options based on Config settings
const selectedMqttUrl = Config.MQTT_BROKER_TYPE === "cloud"
    ? Config.MQTT_CLOUD_BROKER_URL
    : Config.MQTT_BROKER_TYPE === "remote"
        ? Config.MQTT_REMOTE_BROKER_URL
        : Config.MQTT_LOCAL_BROKER_URL;

const inferMqttProtocol = (brokerUrl: string | undefined): mqtt.MqttProtocol => {
    if (brokerUrl?.startsWith('mqtts://')) {
        return 'mqtts';
    }

    if (brokerUrl?.startsWith('mqtt://')) {
        return 'mqtt';
    }

    if (brokerUrl?.startsWith('wss://')) {
        return 'wss';
    }

    return 'ws';
};

Config.MQTT_URL = selectedMqttUrl;
const selectedMqttProtocol = inferMqttProtocol(selectedMqttUrl);

Config.MQTT_OPTIONS = Config.MQTT_BROKER_TYPE === "cloud" ? {
    //cloud
    username: Config.MQTT_BROKER_USERNAME,
    password: Config.MQTT_BROKER_PASSWORD,
    clean: true,
    reconnectPeriod: 1000,
    keepalive: 60,
    protocol: "wss" as mqtt.MqttProtocol,
    queueQoSZero: false,
    rejectUnauthorized: true,
} : { 
    //local or remote
    username: Config.MQTT_BROKER_USERNAME,
    password: Config.MQTT_BROKER_PASSWORD,
    clean: true,
    reconnectPeriod: 1000,
    keepalive: 60,
    protocol: selectedMqttProtocol,
    queueQoSZero: false,
    rejectUnauthorized: false,
};

// Export Config from here
export default Config;
