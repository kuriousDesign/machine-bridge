# Use Microsoft's official Dev Container Ubuntu base image (22.04 LTS Jammy Jellyfish)
FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

# Install base Node.js and npm using standard apt commands
RUN apt-get update && apt-get install -y nodejs npm \
    && rm -rf /var/lib/apt/lists/*

# Install TypeScript globally using npm (this is the key change)
# The 'npm install -g' command should be its own RUN step after apt installs are finished
RUN npm install -g typescript

# Set working directory
WORKDIR /app

# Copy package files first (for layer caching)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY src ./src

# Copy nodemon config
COPY nodemon.json ./

# Set default command for dev
CMD ["npx", "nodemon", "--watch", "src", "--exec", "ts-node", "src/index.ts"]
