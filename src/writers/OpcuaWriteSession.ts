import { ClientSession, OPCUAClient } from 'node-opcua';

import { DeviceId } from '@kuriousdesign/machine-sdk';

import CodesysOpcuaDriver from '../opcua/codesys-opcua-driver';
import Config from '../shared/config';

export default class OpcuaWriteSession {
    private client: OPCUAClient | null = null;
    private session: ClientSession | null = null;
    private driver: CodesysOpcuaDriver | null = null;

    constructor(
        private readonly deviceId: DeviceId,
        private readonly label: string,
        private readonly getMachineId?: () => string | null,
    ) {}

    public async ensureConnected(): Promise<void> {
        if (this.session && this.driver) {
            this.driver.setMachineId(this.getMachineId?.() ?? null);
            return;
        }

        await this.disconnect();

        console.log(`[${this.label}] Connecting dedicated OPC UA session...`);
        this.client = OPCUAClient.create(Config.OPCUA_OPTIONS);
        this.client.on('connection_lost', () => {
            console.warn(`[${this.label}] OPC UA connection lost.`);
            this.driver = null;
            this.session = null;
        });

        await this.client.connect(Config.OPCUA_ENDPOINT);
        this.session = await this.client.createSession();
        this.driver = new CodesysOpcuaDriver(this.deviceId, this.session, Config.OPCUA_CONTROLLER_NAME);
        this.driver.setMachineId(this.getMachineId?.() ?? null);
        console.log(`[${this.label}] Dedicated OPC UA session ready.`);
    }

    public getDriver(): CodesysOpcuaDriver | null {
        return this.driver;
    }

    public async reset(reason: string, error?: unknown): Promise<void> {
        if (error) {
            console.warn(`[${this.label}] Resetting dedicated OPC UA session: ${reason}`, error);
        } else {
            console.warn(`[${this.label}] Resetting dedicated OPC UA session: ${reason}`);
        }

        await this.disconnect();
    }

    public async disconnect(): Promise<void> {
        if (this.session) {
            try {
                await this.session.close();
            } catch (error) {
                console.warn(`[${this.label}] Failed to close OPC UA session:`, error);
            } finally {
                this.session = null;
            }
        }

        if (this.client) {
            try {
                this.client.removeAllListeners('connection_lost');
                await this.client.disconnect();
            } catch (error) {
                console.warn(`[${this.label}] Failed to disconnect OPC UA client:`, error);
            } finally {
                this.client = null;
            }
        }

        this.driver = null;
    }
}