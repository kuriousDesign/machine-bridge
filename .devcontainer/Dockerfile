# .devcontainer/Dockerfile
# Use the official Microsoft Dev Container image (includes node, git, zsh, etc.)
FROM mcr.microsoft.com/devcontainers/typescript-node:1-22-bookworm

# Set working directory to the standard Dev Container path
# VS Code will open this folder as the workspace
WORKDIR /workspaces/machine-bridge

# Copy package files first (for Docker layer caching)
COPY package*.json ./

# Install dependencies (npm, pnpm, yarn – whatever you use)
# If you use pnpm (recommended), replace with: RUN corepack enable && corepack prepare pnpm@latest --activate && pnpm install
RUN npm install

# Copy the entire project (since your repo root IS the Next.js app)
COPY . .

# Optional: Expose port (VS Code does this anyway)
# EXPOSE 3000

# Default command — overridden by docker-compose.yml (sleep infinity) when using Dev Containers
CMD ["npm", "run", "dev"]