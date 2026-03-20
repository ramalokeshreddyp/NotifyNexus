# ---- Build Stage ----
FROM node:20-alpine AS builder

WORKDIR /app

# Copy dependency manifests
COPY package.json package-lock.json ./

# Install all dependencies (including dev for tsx)
RUN npm ci

# Copy source code
COPY . .

# ---- Production Stage ----
FROM node:20-alpine

WORKDIR /app

# Install curl for health checks
RUN apk add --no-cache curl

# Copy everything from builder
COPY --from=builder /app /app

# Expose application port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
  CMD curl -f http://localhost:3000/api/health || exit 1

# Start the application
CMD ["npx", "tsx", "server.ts"]
