# Use the reliable Debian-based Node 22 slim image
FROM node:22-bookworm-slim

# Install essential network utilities needed for testing on Debian/Bookworm
RUN apt-get update && apt-get install -y \
    iputils-ping curl wget netcat-openbsd && rm -rf /var/lib/apt/lists/*

# The dependencies section in your package.json ensures these dev tools get installed locally:
# "typescript": "^5.9.2", "nodemon": "^3.1.7", "ts-node": "^10.9.2"

# Set working directory to the standard path
WORKDIR /workspaces/machine-bridge

# Copy package files first (for Docker layer caching)
COPY package*.json ./

# Install dependencies (Node 22 is compatible with all engines)
RUN npm install

# Copy source code and config
COPY src ./src
COPY nodemon.json ./

# Set default command for development using the 'dev' script from package.json
CMD ["npm", "run", "dev"]
