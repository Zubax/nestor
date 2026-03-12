import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    proxy: {
      '/cf3d/api/v1': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
  build: {
    target: 'esnext',
    outDir: 'dist',
  },
})
