# SignalDB Grafana Datasource Plugin

Native Grafana datasource plugin for SignalDB - unified observability signal database.

## Features

- **Unified Querying**: Single datasource for traces, metrics, and logs
- **Multiple Signal Types**:
  - Traces (TraceQL)
  - Metrics (PromQL)
  - Logs (LogQL)
- **Flexible Connectivity**: HTTP or Arrow Flight protocol
- **Rust Backend**: High-performance backend using grafana-plugin-sdk
- **TypeScript Frontend**: Modern React-based UI with Grafana plugin SDK

## Quick Start

### Prerequisites

- Node.js >= 22
- Rust toolchain (for backend compilation)
- Docker and Docker Compose

### Installation

```bash
# Install dependencies (from workspace root)
npm install

# Build the plugin
npm run build
```

### Development

```bash
# Start SignalDB services (from repository root)
docker compose up

# Build and run Grafana with plugin
cd src/grafana-plugin
npm install
npm run build
docker compose up
```

Access Grafana at http://localhost:3000 (credentials: admin/admin)

## Development Workflow

### Frontend Development

Watch mode for frontend changes:
```bash
npm run dev
```

### Backend Development

Build Rust backend:
```bash
npm run build:backend
```

Watch mode with cargo-watch:
```bash
npm run dev:backend
```

### Full Build

Build both frontend and backend:
```bash
npm run build
```

## Configuration

When adding the SignalDB datasource in Grafana, configure:

- **Router URL**: SignalDB Router address (default: `http://localhost:3001`)
- **Protocol**: Choose between HTTP or Arrow Flight
- **Timeout**: Query timeout in seconds (default: 30)
- **API Key**: Optional authentication key

## Query Examples

### Traces (TraceQL)
```
{ service.name = "my-service" } | duration > 100ms
```

### Metrics (PromQL)
```
up{job="my-job"}
```

### Logs (LogQL)
```
{app="my-app"} |= "error"
```

## Architecture

### Frontend
- TypeScript/React application
- Grafana plugin SDK (@grafana/data, @grafana/ui)
- Query and configuration editors
- Signal type selection

### Backend
- Rust binary (grafana-plugin-sdk 0.4.0)
- gRPC communication with Grafana
- HTTP/Flight client for SignalDB Router
- Query handling and health checks

## Project Structure

```
src/grafana-plugin/
├── backend/              # Rust backend
│   ├── src/
│   │   └── main.rs      # Datasource implementation
│   └── Cargo.toml
├── src/                  # TypeScript frontend
│   ├── components/      # React components
│   ├── datasource.ts    # DataSource class
│   ├── module.ts        # Plugin entry
│   ├── plugin.json      # Metadata
│   └── types.ts         # TypeScript types
├── dist/                # Build output
├── build-backend.sh     # Backend build script
├── docker-compose.yaml  # Dev environment
└── package.json         # npm config
```

## Troubleshooting

### Plugin Not Loading
- Ensure `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS` includes `signaldb-signaldb-datasource`
- Check Grafana logs for plugin errors
- Verify backend binary exists in `dist/` directory

### Build Errors
- Ensure Node.js >= 22 is installed
- Run `npm install` from workspace root
- For Rust errors, ensure Rust toolchain is up to date

### Connection Issues
- Verify SignalDB Router is running and accessible
- Check Router URL in datasource configuration
- Test connection using "Save & Test" button

## Contributing

This plugin is part of the SignalDB project. See the main repository README for contribution guidelines.

## License

Apache-2.0 - See LICENSE file in the repository root.
