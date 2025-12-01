# Use the official Canonical Ubuntu image with Node 22 pre-installed
FROM ubuntu/node:22

# The base image already has Node.js, npm, curl, wget, and basic tools installed.

# Install additional network utilities needed for testing on Ubuntu
# Note: Ubuntu uses 'netcat-openbsd' just like Debian
RUN apt-get update && apt-get install -y \
    iputils-ping netcat-openbsd && rm -rf /var/lib/apt/lists/*

# The 'typescript' installation via 'npm install -g' is fine on Ubuntu too
RUN npm install -g typescript

# Set working directory
WORKDIR /app

# Copy package files first (for layer caching)
COPY package*.json ./

# Install local project dependencies
# This leverages the caching and the pre-installed Node 22
RUN npm install

# Copy source code
COPY src ./src

# Copy nodemon config
COPY nodemon.json ./

# Set default command for development using the ESM loader fix
CMD ["npx", "nodemon", "--watch", "src", "--exec", "node", "--loader", "ts-node/esm", "src/index.ts"]
