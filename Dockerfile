# Use official Node.js LTS image (alpine for smaller size)
FROM node:20-alpine

# Create app directory
WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install

# Copy source code
COPY . .

# Compile TypeScript to JavaScript
RUN npm run build

# Expose the port expected by Fly.io
EXPOSE 3000

# Start the server
CMD ["npm", "start"]