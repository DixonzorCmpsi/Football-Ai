# Dashboard/predictor-frontend/Dockerfile

# --- Stage 1: Build ---
FROM node:22-slim AS build

WORKDIR /app

# Copy package files
COPY package.json package-lock.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY . .

# Build the app (creates /app/dist)
RUN npm run build

# Inject runtime env loader script tag into built index.html so that
# `env.js` (written at container start) is loaded before the app.
RUN sed -i '/<\/head>/i <script src="/env.js"></script>' /app/dist/index.html || true

# --- Stage 2: Serve ---
FROM nginx:alpine

# Copy build artifacts from Stage 1 to Nginx
COPY --from=build /app/dist /usr/share/nginx/html

# Copy custom nginx config (proxies /api to backend service)
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy entrypoint script that writes env.js at container start
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Expose port 80
EXPOSE 80

# Start the entrypoint (writes env.js then starts nginx)
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]