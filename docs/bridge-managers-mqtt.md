# Bridge Managers MQTT Map

This file lists the active MQTT subscriptions and publishes used by the bridge managers.

## Publish Manager

Source: `src/publish/PublishManagerCore.ts`, `src/publish/PublishCommands.ts`, `src/publish/PublishStatus.ts`

Subscriptions:
- `bridge/cmd`

Publishes:
- `bridge/status`
- `bridge/control`
- `deviceMap`
- `bridge/cache`
- dynamic runtime topics from validated polling items, for example `machine/...`, `machine/<path>/<deviceId>/...`

Notes:
- `bridge/cache` is published on `GET_CACHE` bridge commands.
- `deviceMap` is published during status updates and in response to `CONNECT` bridge commands.
- runtime polling publishes are driven by OPC UA monitored item changes and stale republish logic.

## HMI Write Manager

Source: `src/writers/HmiWriteManager.ts`

Subscriptions:
- `bridge/api/write_tag`
- `bridge/api/hmi_writes/recipe`
- `bridge/api/hmi_writes/job`
- `bridge/api/hmi_writes/active_recipe_index`
- `hmi/action_req/<deviceId>` for every device in the current device map

Publishes:
- none directly to MQTT

Notes:
- This manager consumes MQTT requests and converts them into OPC UA writes.
- HMI action requests arrive as `TopicData` envelopes and are unwrapped before validation.

## External Service Write Manager

Source: `src/writers/ExternalServiceWriteManager.ts`

Subscriptions:
- `bridge/api/update_device/<deviceId>/sts` for devices where `device.isExternalService === true`

Publishes:
- none directly to MQTT

Notes:
- This manager consumes `TopicData` envelopes and writes the payload into the external-service device status object through OPC UA.
- Runtime `sts` publication still comes from the Publish Manager's normal PLC polling path, so published values reflect the PLC state rather than the inbound request payload.