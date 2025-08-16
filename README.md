# GETTING STARTED
run npm install to install all packages and dependencies
run npm run dev to launch local server

# DESCRIPTION
This websocket server provides a cloud-based server to facilitate communication between a plc (machine) with internet connection and an HMI and/or remote monitoring dashboard UIs

# CONNECTION
the websocket server acts as a middleman between the machine data/control and hmi/dashboards
- PLC connects to the websocket server by using a bridge service that connects OPCUA tags
- UIs connect to the websocket server by using a websocket client (inherent to the UI application)

# TESTING
run the test -> python-device-publisher.py to emulate a machine that has a robot device

# DEPLOY DETAILS
This is now being deployed on render:
https://machine-websocket-server.onrender.com

It was being deployed use flyio, but they didn't have a free tier.