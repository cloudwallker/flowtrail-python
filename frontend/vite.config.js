import { defineConfig } from 'vite';

// The browser only calls the same origin. The token never enters a URL.
const proxy = {
  '/api': { target: 'http://127.0.0.1:8000', changeOrigin: true },
  '/healthz': { target: 'http://127.0.0.1:8000', changeOrigin: true },
};

export default defineConfig({
  server: { port: 5173, strictPort: true, proxy },
  preview: { port: 4173, strictPort: true, proxy },
});
