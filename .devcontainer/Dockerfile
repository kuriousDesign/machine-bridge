# Use Microsoft's official Dev Container Ubuntu base image (22.04 LTS Jammy Jellyfish)
FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

# Install Node.js, npm, and TypeScript compiler using standard apt commands
RUN apt-get update && apt-get install -y nodejs npm typescript \
    && rm -rf /var/lib/apt/lists/*
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
