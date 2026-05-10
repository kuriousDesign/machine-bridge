import OpcuaClientManager, {
    BridgeStatusSnapshot,
    OpcuaClientManagerCallbacks,
    OpcuaClientManagerDependencies,
    PublishManagerStatus,
} from './OpcuaMqttManager';

export type PublishManagerCallbacks = OpcuaClientManagerCallbacks;
export type PublishManagerDependencies = OpcuaClientManagerDependencies;
export { PublishManagerStatus };
export type { BridgeStatusSnapshot };

export default class PublishManager extends OpcuaClientManager {
    constructor(
        callbacks: PublishManagerCallbacks = {},
        dependencies: PublishManagerDependencies = {},
    ) {
        super(callbacks, dependencies);
    }
}