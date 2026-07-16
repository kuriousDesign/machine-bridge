# MQTT API Hooks

This document describes the primary MQTT hooks exposed by the bridge in [src/publish/PublishCommands.ts](../src/publish/PublishCommands.ts), [src/publish/PublishStatus.ts](../src/publish/PublishStatus.ts), [src/writers/HmiWriteManager.ts](../src/writers/HmiWriteManager.ts), and [src/writers/ExternalServiceWriteManager.ts](../src/writers/ExternalServiceWriteManager.ts).

## Overview

The bridge has two broad classes of MQTT hooks:

- command and hydration hooks that help a UI discover bridge state and device topology
- write hooks that forward HMI or service-originated writes into OPC UA

The bridge does not implement these as synchronous request/response APIs. A client typically publishes to one topic, then listens for one or more response topics.

## Topic Summary

| Topic | Direction | Purpose |
| --- | --- | --- |
| `bridge/cmd` | client -> bridge | Bridge command channel. Includes connect requests. |
| `bridge/status` | bridge -> clients | Retained bridge status snapshot. |
| `deviceMap` | bridge -> clients | Current registered device map. Published in response to bridge commands. |
| `bridge/control` | bridge -> clients | Kiosk control status snapshot. |
| `hmi/action_req/:deviceId` | client -> bridge | Execute a device action through the HMI writer. |
| `bridge/api/write_tag` | client -> bridge | Write an arbitrary nested OPC UA tag. |
| `bridge/api/update_device/:deviceId/sts` | client -> bridge | Write a device status payload for external-service devices. |

## Connect Hook

This is the hook you asked about.

### Request topic

`bridge/cmd`

### Request payload

The payload is read as a `TopicData` wrapper with a `payload` object. For connect, the command value comes from `BridgeCmds.CONNECT`, which is currently `2`.

```ts
{
  timestamp: Date.now(),
  payload: {
    cmd: 2
  }
}
```

Optional kiosk-control fields are also accepted on the same command payload:

```ts
{
  timestamp: Date.now(),
  payload: {
    cmd: 2,
    kioskId: "kiosk-1",
    requestControl: true,
    allowedKioskIds: ["kiosk-1"]
  }
}
```

### Bridge handler

The request is subscribed in [src/publish/PublishManagerCore.ts](../src/publish/PublishManagerCore.ts) by `subscribeToBridgeCommandTopic()`, which delegates to `handlePublishBridgeCommand(...)` in [src/publish/PublishCommands.ts](../src/publish/PublishCommands.ts).

### Response behavior

On `CONNECT`, the bridge does not publish a dedicated ack topic. Instead, it responds by publishing the latest bridge state on existing topics:

- `deviceMap`: immediate publish of the current `deviceMapEntries` when available
- `bridge/control`: current kiosk-control snapshot
- `bridge/status`: published by the normal bridge status loop and retained for later subscribers

In practice, a UI should treat `deviceMap` and `bridge/status` as the hydration response to `bridge/cmd` connect.

### Example client flow

1. Subscribe to `bridge/status`, `deviceMap`, and optionally `bridge/control`.
2. Publish `{ timestamp, payload: { cmd: 2 } }` to `bridge/cmd`.
3. Wait for `deviceMap` to build device navigation and `bridge/status` to confirm bridge readiness.

## Bridge Status Hook

### Topic

`bridge/status`

### Direction

bridge -> clients

### Behavior

The bridge publishes a retained snapshot that includes MQTT connectivity, OPC UA state, publish-manager state, registered device count, and supervisor/write-manager health.

Representative shape:

```ts
{
  mqttConnected: true,
  opcuaState: 3,
  opcuaStateLabel: "Polling",
  publishManagerStatus: "polling",
  registeredDeviceCount: 12,
  machineId: "machine-01",
  supervisorState: "healthy",
  writeManagers: {
    hmi: {
      state: "running",
      resetCount: 0,
      lastResetReason: null,
      lastResetAt: null,
      lastError: null
    },
    externalService: {
      state: "running",
      resetCount: 0,
      lastResetReason: null,
      lastResetAt: null,
      lastError: null
    }
  }
}
```

Clients should prefer this topic for connection state rather than inferring readiness from the raw existence of the MQTT session.

## Device Map Hook

### Topic

`deviceMap`

### Direction

bridge -> clients

### Behavior

This topic is published:

- when a client sends `BridgeCmds.CONNECT` on `bridge/cmd`
- during the bridge status publish cycle when device registrations are available

The payload is the array returned from `Array.from(this.deviceMap.entries())`, so consumers should expect key/value tuples of device id to `DeviceRegistration`.

Representative entry:

```ts
[
  7,
  {
    id: 7,
    mnemonic: "RB1",
    parentId: 1,
    childIdArray: [],
    deviceType: 4,
    isExternalService: false,
    devicePath: ["Machine", "Robot1"]
  }
]
```

## HMI Action Hook

### Topic

`hmi/action_req/:deviceId`

Example: `hmi/action_req/7`

### Direction

client -> bridge

### Payload

The bridge expects `DeviceActionRequestData`:

```ts
{
  UniqueActionRequestId: 101,
  SenderId: 9001,
  ActionType: 1,
  ActionId: 42,
  ParamArray: [1, 0, 0, 0]
}
```

### Behavior

The HMI writer resolves `deviceId` from the topic suffix, validates that the device exists in the bridge device map, then forwards the action into OPC UA via `driver.requestAction(...)`.

There is no dedicated MQTT response topic from this handler. Success or failure is observed indirectly through normal device state topics and bridge logs.

## Direct Tag Write Hook

### Topic

`bridge/api/write_tag`

### Direction

client -> bridge

### Payload

```ts
{
  tag: "ns=4;s=|var|CODESYS Control.Application.Device.SomeField",
  value: {
    enabled: true,
    speed: 120
  }
}
```

### Behavior

The HMI writer subscribes to this topic once it is running. The bridge uses `driver.writeNestedObject(tag, value, true)` to write the provided nested object into OPC UA.

When multiple `write_tag` messages arrive back-to-back, the HMI writer drains them into a single batch and builds one runtime write plan before writing OPC UA leaf nodes sequentially. The topic still accepts the original single `{ tag, value }` payload, and may also accept an array of those payloads.

Use this only when you already know the exact OPC UA tag path and need a direct bridge-side write.

## HMI Recipe Write Hook

### Topic

`bridge/api/hmi_writes/recipe`

### Direction

client -> bridge

### Payload

```ts
{
  index: 3,
  recipe: {
    dbId: "6642d4...",
    index: 3,
    tubeTypeString: "..."
  }
}
```

### Behavior

The HMI writer resolves the machine-specific recipe path inside the bridge and writes only `recipeStore.recipes[index]` through the driver runtime write-plan path. This keeps the UI decoupled from exact PLC recipe tag naming while limiting write size to a single recipe.

## HMI Job Write Hook

### Topic

`bridge/api/hmi_writes/job`

### Direction

client -> bridge

### Payload

```ts
{
  job: {
    jobName: "WO-123",
    activeRecipeIndex: 2
  }
}
```

### Behavior

The HMI writer resolves the machine-specific job path inside the bridge and writes the job payload through the same runtime write-plan path used for other HMI writes. The UI does not need to know whether the PLC job tag is machine-specific.

## HMI Active Recipe Index Write Hook

### Topic

`bridge/api/hmi_writes/active_recipe_index`

### Direction

client -> bridge

### Payload

```ts
{
  index: 2
}
```

### Behavior

The HMI writer updates only `machine.job.activeRecipeIndex` through the bridge. This keeps recipe activation buttons out of the raw PLC tag API while limiting the write surface to the single active recipe index field.

## External Service Status Write Hook

### Topic

`bridge/api/update_device/:deviceId/sts`

Example: `bridge/api/update_device/15/sts`

### Direction

client -> bridge

### Payload

The bridge expects a `TopicData` wrapper and writes `payload` into the external-service device `Sts` tag.

```ts
{
  timestamp: Date.now(),
  payload: {
    online: true,
    jobId: 1234,
    iExtService: {
      state: 2
    }
  }
}
```

### Behavior

- Only devices marked `isExternalService` are subscribed.
- The bridge resolves the machine-specific `Sts` tag with `OptionalDevicePollingTags(device, machineId).Sts`.
- If `payload.iExtService.o` exists, the bridge removes that nested `o` property before writing.

This hook is intended for external-service devices that need to push status into the PLC model through the bridge.

## Practical Client Guidance

- Subscribe before publishing. The bridge often answers by publishing to shared state topics rather than an ack topic.
- Treat `bridge/status` as the canonical connection-state topic.
- Treat `deviceMap` as the hydration payload after connect.
- Use `hmi/action_req/:deviceId` for device actions and `bridge/api/write_tag` only for known raw tag writes.
- Use `bridge/api/update_device/:deviceId/sts` only for devices configured as external services.