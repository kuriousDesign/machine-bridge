import http from 'http';
import { WebSocket, WebSocketServer } from 'ws';
import * as dotenv from 'dotenv';
dotenv.config({ path: '.env.local' });

interface Filter {
  machineId: string;
  deviceId: string;
  dataType: string;
}

interface Message {
  type: 'subscribe' | 'unsubscribe' | 'device_update';
  machineId: string;
  deviceId: string;
  dataType: string;
  [key: string]: any;
}

interface DeviceState {
  machineId: string;
  deviceId: string;
  dataType: string;
  [key: string]: any;
}

interface MachineData {
  id: string;
  name?: string;
  [key: string]: any;
}

const server = http.createServer();
const wss = new WebSocketServer({ server });
const PORT: number | string = process.env.PORT || 5000;

const clients = new Map<WebSocket, Set<string>>(); // ws -> Set<subscriptionKey>
const deviceCache = new Map<string, DeviceState>(); // subscriptionKey -> state
const machines = new Map<string, MachineData>(); // machineId -> machine info

function getSubscriptionKey({ machineId, deviceId, dataType }: Filter): string {
  return `${machineId}:${deviceId}:${dataType}`;
}

function parseMessage(msg: Buffer): Message | null {
  try {
    return JSON.parse(msg.toString());
  } catch {
    return null;
  }
}

function handleSubscribe(ws: WebSocket, payload: Filter): void {
  const key = getSubscriptionKey(payload);
  if (!clients.has(ws)) clients.set(ws, new Set());
  clients.get(ws)!.add(key);
  console.log(`Client subscribed to ${key}`);

  // Send latest full snapshot
  const fullState = deviceCache.get(key);
  if (fullState) {
    const machineInfo = machines.get(payload.machineId);
    ws.send(JSON.stringify({ type: 'full', ...fullState, machineInfo }));
  }
}

function handleUnsubscribe(ws: WebSocket, payload: Filter): void {
  const key = getSubscriptionKey(payload);
  if (clients.has(ws)) {
    clients.get(ws)!.delete(key);
    if (clients.get(ws)!.size === 0) {
      clients.delete(ws);
    }
  }
}

function handleDeviceUpdate(partial: DeviceState): void {
  const key = getSubscriptionKey(partial);
  const previous = deviceCache.get(key) || {};
  const merged: DeviceState = { ...previous, ...partial };
  deviceCache.set(key, merged);

  if (partial.machineInfo) {
    machines.set(partial.machineId, { id: partial.machineId, ...partial.machineInfo });
  }

  for (const [client, subscriptions] of clients.entries()) {
    if (subscriptions.has(key)) {
      try {
        client.send(JSON.stringify({ type: 'partial', ...partial }));
      } catch (err) {
        console.warn('Failed to send to client. Dropping:', err);
        clients.delete(client);
      }
    }
  }
}

function handleMessage(ws: WebSocket, raw: Buffer): void {
  const msg = parseMessage(raw);
  if (!msg || typeof msg !== 'object' || !msg.type) return;

  switch (msg.type) {
    case 'subscribe':
      //console.log(`Client subscribing to machine: ${msg.machineId}, device: ${msg.deviceId}, dataType: ${msg.dataType}`);
      handleSubscribe(ws, msg);
      break;
    case 'unsubscribe':
      handleUnsubscribe(ws, msg);
      break;
    case 'device_update':
      handleDeviceUpdate(msg);
      ws.send(JSON.stringify({ type: 'ack', message: 'Update received' }));
      break;
    default:
      console.warn('Unknown message type:', msg.type);
  }
}

function handleConnection(ws: WebSocket): void {
  console.log('New client connected', ws.url);
  
  ws.send(JSON.stringify({ type: 'welcome', message: 'Welcome to the WebSocket server!' }));
  clients.set(ws, new Set());
  // record client id or other metadata if needed
  ws.url


  ws.on('message', (msg: Buffer) => handleMessage(ws, msg));
  ws.on('close', () => {
    console.log('Client disconnected');
    clients.delete(ws);
  });
}

wss.on('connection', handleConnection);

server.listen(PORT, () => {
  console.log(`✅ WebSocket server running on ws://localhost:${PORT}`);
});
