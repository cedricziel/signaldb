/// <reference types="vite/client" />

// Injected via `define` in vite.config.ts from SIGNALDB_TENANT/_DATASET.
declare const __SIGNALDB_DEFAULT_TENANT__: string | undefined;
declare const __SIGNALDB_DEFAULT_DATASET__: string | undefined;

// Telemetry config baked in at build time (see src/telemetry).
// OTLP/HTTP endpoint browser spans are exported to; empty disables export.
declare const __SIGNALDB_OTLP_ENDPOINT__: string | undefined;
// service.name for exported spans (default "signaldb-ui").
declare const __SIGNALDB_TELEMETRY_SERVICE_NAME__: string | undefined;
// service.version, taken from package.json at build time.
declare const __SIGNALDB_UI_VERSION__: string | undefined;
