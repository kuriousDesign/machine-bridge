# Use the reliable Debian-based Node 22 slim image
FROM node:22-bookworm-slim

# Install essential network utilities needed for testing on Debian/Bookworm
RUN apt-get update && apt-get install -y \
    iputils-ping curl wget netcat-openbsd && rm -rf /var/lib/apt/lists/*

# The dependencies section in your package.json ensures these dev tools get installed locally:
# "typescript": "^5.9.2", "nodemon": "^3.1.7", "ts-node": "^10.9.2"

# Use workspace root so machine-bridge can consume file:../machine-sdk
WORKDIR /workspaces

# Copy SDK first so file-based dependency exists during npm install
COPY machine-sdk ./machine-sdk

# Copy package files first (for Docker layer caching)
COPY machine-bridge/package*.json ./machine-bridge/

# Install bridge dependencies (includes local file dependency ../machine-sdk)
WORKDIR /workspaces/machine-bridge
RUN npm install

# Copy source code and config
COPY machine-bridge/src ./src
COPY machine-bridge/nodemon.json ./
COPY machine-bridge/tsconfig.json ./

RUN npm run build

# Set default command for development using the 'dev' script from package.json
#CMD ["npm", "run", "dev"]
CMD ["npm", "start"]
