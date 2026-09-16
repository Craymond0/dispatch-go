import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The build lands inside the Go module so `go build` embeds it. emptyOutDir
// wipes the directory, including the committed .gitkeep that keeps
// //go:embed compiling on a clone that has never run this build, so the
// build script recreates it afterwards.
export default defineConfig({
  plugins: [react()],
  build: { outDir: '../internal/web/dist', emptyOutDir: true },
  server: {
    // In development the Go API runs on 8088 (docker compose) and Vite on 5173.
    proxy: {
      '/jobs': 'http://127.0.0.1:8088',
      '/metrics': 'http://127.0.0.1:8088',
      '/tracker': 'http://127.0.0.1:8088',
      '/auth': 'http://127.0.0.1:8088',
      '/healthz': 'http://127.0.0.1:8088',
    },
  },
})
