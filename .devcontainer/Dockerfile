# Use Node LTS slim image with basic utilities
FROM node:20-bookworm-slim

# Install network utilities for testing
RUN apt-get update && apt-get install -y \
    iputils-ping curl wget netcat && rm -rf /var/lib/apt/lists/*

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
