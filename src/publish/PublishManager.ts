import PublishManagerCore, {
    PublishManagerCallbacks,
    PublishManagerDependencies,
} from './PublishManagerCore';
import { BridgeStatusSnapshot, PublishManagerStatus } from './PublishManagerContracts';

export type { PublishManagerCallbacks, PublishManagerDependencies };
export { PublishManagerStatus };
export type { BridgeStatusSnapshot };

export default class PublishManager extends PublishManagerCore {
    constructor(
        callbacks: PublishManagerCallbacks = {},
        dependencies: PublishManagerDependencies = {},
    ) {
        super(callbacks, dependencies);
    }
}