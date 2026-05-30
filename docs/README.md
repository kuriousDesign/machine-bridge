# Machine Bridge Docs

This folder documents the MQTT-facing hooks exposed by the bridge.

## Guides

- [MQTT API Hooks](mqtt-api-hooks.md): request and response topics used by the bridge for connect, hydration, HMI actions, direct tag writes, and external-service writes.
- [machine-sdk local development workflow](../../machine-sdk/README.local-development.md): how to rebuild the local SDK, when bridge installs need to be refreshed, and how to switch the bridge between local and published SDK sources.

## Notes

- The bridge uses MQTT topics as its public API surface.
- Most HMI-facing flows are request-by-topic and response-by-publish rather than HTTP-style request/response.
- Topic names and payload shapes in this folder are derived from the current code in `src/`, including bridge-local PLC tag mapping in `src/opcua/plc-tags.ts`, plus shared contracts from `@kuriousdesign/machine-sdk`.