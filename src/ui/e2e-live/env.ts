// Shared constants and the on-disk handoff between global-setup and the
// live specs. Global setup runs in its own process before any spec file
// loads, so specs read what it discovered (base URL, tenant/dataset,
// service names) from a JSON file rather than a module-level variable.
import { readFileSync } from "node:fs";
import path from "node:path";

export const TENANT_ID = "e2e";
export const DATASET_ID = "live";
export const ADMIN_API_KEY = "e2e-admin-key";
export const TENANT_API_KEY = "e2e-tenant-key";
export const LOGIN_EMAIL = "e2e@example.com";
export const LOGIN_PASSWORD = "e2e-live-suite-password-123";

// A service known to exist in the rideshare estate (see
// src/signal-producer/src/topology.rs) that always has both an inbound
// caller and a downstream dependency, so its entity-detail page exercises
// operations, dependencies, and error groups.
export const SEEDED_SERVICE = "trip-service";

// The monolithic `signaldb` binary hard-codes these ports
// (common::endpoints::DEFAULT_ROUTER_HTTP_PORT / DEFAULT_OTLP_GRPC_PORT) —
// no CLI flag, env var, or config key overrides them in monolithic mode —
// so only one instance (dev or this suite) can run at a time.
export const ROUTER_HTTP_PORT = 3000;
export const ACCEPTOR_GRPC_PORT = 4317;

export const STATE_FILE = path.join(import.meta.dirname, ".live-env.json");
export const STORAGE_STATE_FILE = path.join(
  import.meta.dirname,
  ".live-storage-state.json",
);

export interface LiveEnv {
  baseURL: string;
  tenant: string;
  dataset: string;
  seededService: string;
}

export function readLiveEnv(): LiveEnv {
  return JSON.parse(readFileSync(STATE_FILE, "utf-8")) as LiveEnv;
}
