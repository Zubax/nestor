# Nestor CF3D Data Viewer

Canvas-based visualization for CF3D CAN data, inspired by [Gerasim](https://github.com/pavel-kirienko/gerasim).

## Features

- **Device view** — CAN adapters/loggers as nodes in a circle
- **Bus connections** — dashed lines between devices on the same bus
- **Timeline** — CAN messages over time, color-coded by CAN ID
- **Records table** — detailed message view with hex data
- **Pan/zoom** — mouse drag and wheel on the canvas

## Stack

- TypeScript (vanilla, no React)
- Canvas 2D rendering
- Vite (dev server + bundler)

## Development

```bash
npm install
npm run dev
```

Dev server runs at `http://localhost:5173` with API proxy to `cyphalcloud.zubax.com/cf3d/api/v1`.

## Build

```bash
npm run build
```

Output in `dist/`.

## API

Proxied endpoints:
- `GET /api/devices` — list recording devices
- `GET /api/boots?device=X` — boot sessions for a device
- `GET /api/records?device=X&boot_id=Y` — CAN records for a boot session
