#!/usr/bin/env python3

import asyncio
import websockets
import json
import random
import time
import os
from dotenv import load_dotenv

# Choose appropriate URL
#WS_URL = "ws://localhost:3000"  # For local dev

# Load environment variables from .env.local
load_dotenv('.env.local')

# Get WebSocket URL from environment variable, fallback to production URL
WS_URL = "ws://localhost:5000" #"wss://machine-websocket-server.onrender.com")

MACHINE_ID = "machineABC"
DEVICE_ID = "robot123"
DATA_TYPE = "robot"

def generate_pose():
    return {
        "x": round(random.uniform(-100, 100), 2),
        "y": round(random.uniform(-100, 100), 2),
        "z": round(random.uniform(0, 50), 2)
    }

def generate_machine_info():
    return {
        "name": "ABB IRB-120 Cell",
        "location": "Line 2",
        "status": "online"
    }


def sort_int_array_biggest_fist(arr):
    arr.sort(reverse=True)
    arr[0]+= 1  # Increment the first element
    return arr


def generate_cfg_params():
    amount = 0
    if amount == 0:
        return {}  # Return empty dict for now
    return {}


async def publish(ws):
    while True:
        pose = generate_pose()
        message = {
            "type": "device_update",
            "machineId": MACHINE_ID,
            "deviceId": DEVICE_ID,
            "dataType": DATA_TYPE,
            "pose": pose,
            "timestamp": int(time.time() * 1000),
            "machineInfo": generate_machine_info()  # Only needed on first connect
        }
        try:
            await ws.send(json.dumps(message))
        except Exception as e:
            print(f"[ERROR] Failed to send message: {e}")
            break  # Exit publish loop to trigger reconnect
        await asyncio.sleep(0.200)  # 200 ms

async def connect_and_publish():
    while True:
        try:
            print(f"[INFO] Connecting to {WS_URL}...")
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                print("[INFO] Connected. Publishing robot123 pose data every 20ms.")
                await publish(ws)
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"[WARN] Connection closed: {e}. Retrying in 2 seconds...")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}. Retrying in 2 seconds...")
            await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(connect_and_publish())
    except KeyboardInterrupt:
        print("\n[INFO] Exiting publisher.")
