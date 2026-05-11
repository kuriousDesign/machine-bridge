import mqtt, { MqttClient } from 'mqtt';

import Config from '../src/shared/config';

function parseNumericOption(name: string, envName: string, fallback: number): number {
    const arg = process.argv.slice(2).find((value) => value.startsWith(`--${name}=`));
    if (arg) {
        const parsed = Number(arg.split('=')[1]);
        if (Number.isFinite(parsed) && parsed > 0) {
            return parsed;
        }
    }

    const npmConfigValue = Number(process.env[`npm_config_${name.toLowerCase()}`]);
    if (Number.isFinite(npmConfigValue) && npmConfigValue > 0) {
        return npmConfigValue;
    }

    const envValue = Number(process.env[envName]);
    if (Number.isFinite(envValue) && envValue > 0) {
        return envValue;
    }

    return fallback;
}

function parseDurationMs(): number {
    return parseNumericOption('durationMs', 'MQTT_TEST_DURATION_MS', 60000);
}

function parseTopicCount(): number {
    return parseNumericOption('topicCount', 'MQTT_TEST_TOPIC_COUNT', 25);
}

function parsePublishIntervalMs(): number {
    return parseNumericOption('publishIntervalMs', 'MQTT_TEST_PUBLISH_INTERVAL_MS', 250);
}

function parseStringOption(name: string, envName: string): string | undefined {
    const arg = process.argv.slice(2).find((value) => value.startsWith(`--${name}=`));
    if (arg) {
        const parsed = arg.split('=').slice(1).join('=').trim();
        if (parsed) {
            return parsed;
        }
    }

    const envValue = process.env[envName]?.trim();
    if (envValue) {
        return envValue;
    }

    return undefined;
}

function sanitizeOptions(options: Record<string, unknown>): Record<string, unknown> {
    return {
        ...options,
        password: options.password ? '***' : undefined,
    };
}

async function main(): Promise<void> {
    const durationMs = parseDurationMs();
    const topicCount = parseTopicCount();
    const publishIntervalMs = parsePublishIntervalMs();
    const startedAt = Date.now();
    const mqttUrl = parseStringOption('url', 'MQTT_TEST_URL') ?? Config.MQTT_URL;
    const protocolOverride = parseStringOption('protocol', 'MQTT_TEST_PROTOCOL') as mqtt.MqttProtocol | undefined;
    const mqttOptions = {
        ...(Config.MQTT_OPTIONS as mqtt.IClientOptions),
        ...(protocolOverride ? { protocol: protocolOverride } : {}),
    } as mqtt.IClientOptions;

    if (!mqttUrl) {
        throw new Error('Config.MQTT_URL is not set. Check MQTT_BROKER_TYPE and broker URL environment variables.');
    }

    console.log('[MQTT_TEST] Starting broker connection probe');
    console.log('[MQTT_TEST] URL:', mqttUrl);
    console.log('[MQTT_TEST] Options:', sanitizeOptions(mqttOptions as Record<string, unknown>));
    console.log(`[MQTT_TEST] Stable connection target: ${durationMs}ms`);
    console.log(`[MQTT_TEST] Load profile: ${topicCount} topic(s), publish every ${publishIntervalMs}ms`);

    const clientId = `machine-bridge-mqtt-probe-${process.pid}-${Date.now()}`;
    const client: MqttClient = mqtt.connect(mqttUrl, {
        ...mqttOptions,
        clientId,
        reconnectPeriod: 0,
    });

    let settled = false;
    let stabilityTimer: NodeJS.Timeout | null = null;
    let publishTimer: NodeJS.Timeout | null = null;
    let publishSequence = 0;
    const probeTopics = Array.from({ length: topicCount }, (_, index) => `bridge/test/mqtt_probe/${clientId}/${index + 1}`);

    const cleanup = (): void => {
        if (stabilityTimer) {
            clearTimeout(stabilityTimer);
            stabilityTimer = null;
        }
        if (publishTimer) {
            clearInterval(publishTimer);
            publishTimer = null;
        }
        client.removeAllListeners();
    };

    const publishLoadBurst = (): void => {
        publishSequence += 1;
        const timestamp = Date.now();

        for (const topic of probeTopics) {
            client.publish(topic, JSON.stringify({
                timestamp,
                payload: {
                    sequence: publishSequence,
                    topic,
                },
            }), { qos: 0 }, (publishError) => {
                if (publishError) {
                    finishFailure(`publish failed for ${topic}`, publishError);
                }
            });
        }

        if (publishSequence % 20 === 0) {
            console.log(`[MQTT_TEST] Published ${publishSequence} burst(s) across ${topicCount} topic(s)`);
        }
    };

    const subscribeToProbeTopics = async (): Promise<void> => {
        for (const topic of probeTopics) {
            await new Promise<void>((resolve, reject) => {
                client.subscribe(topic, { qos: 1 }, (subscribeError) => {
                    if (subscribeError) {
                        reject(subscribeError);
                        return;
                    }

                    resolve();
                });
            });
        }
    };

    const finishSuccess = (): void => {
        if (settled) {
            return;
        }

        settled = true;
        cleanup();
        console.log(`[MQTT_TEST] Success: connection remained alive for ${Date.now() - startedAt}ms`);
        client.end(true, {}, () => {
            process.exit(0);
        });
    };

    const finishFailure = (reason: string, error?: unknown): void => {
        if (settled) {
            return;
        }

        settled = true;
        cleanup();
        console.error(`[MQTT_TEST] Failure: ${reason}`);
        if (error) {
            console.error(error);
        }
        client.end(true, {}, () => {
            process.exit(1);
        });
    };

    client.on('connect', async () => {
        console.log('[MQTT_TEST] Connected to broker');

        try {
            await subscribeToProbeTopics();
            console.log(`[MQTT_TEST] Subscribed to ${topicCount} probe topic(s)`);
            publishLoadBurst();
            publishTimer = setInterval(() => {
                publishLoadBurst();
            }, publishIntervalMs);
            stabilityTimer = setTimeout(() => {
                finishSuccess();
            }, durationMs);
        } catch (error) {
            finishFailure('subscribe phase failed during load probe', error);
        }
    });

    client.on('message', (topic, payload) => {
        if (publishSequence <= 3) {
            console.log('[MQTT_TEST] Received message:', topic, payload.toString());
        }
    });

    client.on('close', () => {
        if (!settled) {
            finishFailure(`broker closed the connection after ${Date.now() - startedAt}ms`);
        }
    });

    client.on('offline', () => {
        console.warn('[MQTT_TEST] Client went offline');
    });

    client.on('end', () => {
        console.log('[MQTT_TEST] Client ended');
    });

    client.on('error', (error) => {
        if (!settled) {
            finishFailure('client error before stability target elapsed', error);
        }
    });
}

void main().catch((error) => {
    console.error('[MQTT_TEST] Unhandled failure:', error);
    process.exit(1);
});
