interface DeviceBase {
  deviceId: string;
  status?: string;
  config?: Record<string, any>;
}

interface RobotData {
  armPosition?: number;
  gripperState?: string;
}

type RobotDevice = DeviceBase & RobotData;
type PartialRobotUpdate = Partial<RobotDevice> & { deviceId: string };
