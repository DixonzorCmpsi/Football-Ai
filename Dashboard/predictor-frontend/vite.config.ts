import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const BACKEND = process.env.VITE_API_BASE_URL?.replace(/\/api\/?$/, '') || 'http://localhost:8000'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'build',
  },
  server: {
    proxy: {
      // dev: forward /api/<x> to <BACKEND>/<x> so the frontend can use the same
      // `/api` prefix it uses behind nginx in production.
      '/api': {
        target: BACKEND,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
