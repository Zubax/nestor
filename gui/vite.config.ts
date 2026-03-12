import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    proxy: {
      '/api': {
        target: 'https://cyphalcloud.zubax.com',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '/cf3d/api/v1'),
        secure: true,
      },
    },
  },
  build: {
    target: 'esnext',
    outDir: 'dist',
  },
})
