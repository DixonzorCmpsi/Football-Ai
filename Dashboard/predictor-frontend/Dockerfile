# Dashboard/predictor-frontend/Dockerfile

# --- Stage 1: Build ---
FROM node:18-alpine as build

WORKDIR /app

# Copy package files
COPY package.json package-lock.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY . .

# Build the app (creates /app/dist)
RUN npm run build

# --- Stage 2: Serve ---
FROM nginx:alpine

# Copy build artifacts from Stage 1 to Nginx
COPY --from=build /app/dist /usr/share/nginx/html

# Expose port 80
EXPOSE 80

# Start Nginx
CMD ["nginx", "-g", "daemon off;"]