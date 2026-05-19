# Bridge Topic Cheatsheet

## Incoming Topics

| Topic | Consumed By | Purpose |
| --- | --- | --- |
| `bridge/cmd` | Publish Manager | Bridge lifecycle commands like connect and get-cache |
| `bridge/api/write_tag` | HMI Write Manager | Arbitrary PLC tag writes |
| `bridge/api/hmi_writes/recipe` | HMI Write Manager | Recipe writes |
| `bridge/api/hmi_writes/job` | HMI Write Manager | Job writes |
| `bridge/api/hmi_writes/active_recipe_index` | HMI Write Manager | Active recipe index writes |
| `hmi/action_req/<deviceId>` | HMI Write Manager | Device action requests from UI |
| `bridge/api/update_device/<deviceId>/sts` | External Service Write Manager | External service status updates |

## Outgoing Topics

| Topic | Produced By | Purpose |
| --- | --- | --- |
| `bridge/status` | Publish Manager | Bridge runtime state snapshot |
| `bridge/control` | Publish Manager | Kiosk control state |
| `deviceMap` | Publish Manager | Registered device map |
| `bridge/cache` | Publish Manager | Bootstrap cache plus replayable cached topics |
| `machine/...` and device runtime topics | Publish Manager | Live OPC UA topic stream |
| `machine/<path>/<deviceId>/sts` | Publish Manager | Live and stale-republished device status snapshots from the PLC polling path |

## Envelope Rules

- MQTT messages published through `MqttClientManager.publish()` are wrapped as `TopicData`:
  - `timestamp`
  - `payload`
- HMI action requests and external service updates both arrive in that wrapped form.
- `bridge/api/write_tag` is currently parsed as raw write-tag payloads, not `TopicData`.