#!/bin/sh
set -e

# Default API base if not provided
API_BASE=${API_BASE_URL:-/api}

# Write env.js to be consumed by the SPA at runtime
cat > /usr/share/nginx/html/env.js <<EOF
window.__env = window.__env || {};
window.__env.API_BASE_URL = "${API_BASE}";
EOF

# Start Nginx in foreground
exec nginx -g 'daemon off;'
