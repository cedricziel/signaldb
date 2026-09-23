// Starts a real `signaldb` monolith against a throwaway SQLite/local-storage
// temp dir, seeds it with realistic OTLP data via the signal-producer binary,
// waits for that data to be queryable, then logs a browser session in and
// saves it as Playwright storageState so every live spec starts already
// authenticated. See src/ui/playwright.live.config.ts for how this plugs in
// and docs/contributing/ui-e2e-live.md for the full picture.
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import type { FullConfig } from "@playwright/test";

import {
  ACCEPTOR_GRPC_PORT,
  ADMIN_API_KEY,
  DATASET_ID,
  LOGIN_EMAIL,
  LOGIN_PASSWORD,
  ROUTER_HTTP_PORT,
  SEEDED_SERVICE,
  STATE_FILE,
  STORAGE_STATE_FILE,
  TENANT_API_KEY,
  TENANT_ID,
} from "./env";

const REPO_ROOT = path.resolve(import.meta.dirname, "../../..");
const UI_DIR = path.resolve(import.meta.dirname, "..");
const BASE_URL = `http://127.0.0.1:${ROUTER_HTTP_PORT}`;

// Populated by setup, read by teardown via module scope: Playwright runs
// both in the same worker process for a given config.
let serverProcess: ChildProcess | undefined;
let tempDir: string | undefined;

function run(cmd: string, args: string[], opts: Record<string, unknown> = {}) {
  const result = spawnSync(cmd, args, {
    stdio: "inherit",
    cwd: REPO_ROOT,
    ...opts,
  });
  if (result.status !== 0) {
    throw new Error(`${cmd} ${args.join(" ")} exited with ${result.status}`);
  }
}

async function waitFor(
  label: string,
  check: () => Promise<boolean>,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await check().catch(() => false)) return;
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${label}`);
    }
    await new Promise((r) => setTimeout(r, 500));
  }
}

function writeConfig(dir: string): string {
  const configPath = path.join(dir, "signaldb.live.toml");
  const toml = `
[database]
dsn = "sqlite://${dir}/signaldb.db"

[discovery]
dsn = "sqlite://${dir}/signaldb.db"

[storage]
dsn = "file://${dir}/storage"

[schema]
catalog_type = "sql"
catalog_uri = "sqlite://${dir}/iceberg_catalog.db"

# Shrink the WAL flush window so the traces/logs/metrics the signal
# producer emits become queryable within the test's poll window instead of
# the 30s production default.
[wal]
flush_interval = "1s"

[auth]
admin_api_key = "${ADMIN_API_KEY}"

[[auth.tenants]]
id = "${TENANT_ID}"
slug = "${TENANT_ID}"
name = "E2E Live Tenant"
default_dataset = "${DATASET_ID}"

[[auth.tenants.api_keys]]
key = "${TENANT_API_KEY}"
name = "E2E Live Key"

[[auth.tenants.datasets]]
id = "${DATASET_ID}"
slug = "${DATASET_ID}"
is_default = true
`;
  writeFileSync(configPath, toml);
  return configPath;
}

export default async function globalSetup(_config: FullConfig) {
  tempDir = mkdtempSync(path.join(tmpdir(), "signaldb-ui-e2e-live-"));
  const configPath = writeConfig(tempDir);

  console.log("[e2e-live] building signaldb, signaldb-cli, signal-producer …");
  run("cargo", [
    "build",
    "--bin",
    "signaldb",
    "--bin",
    "signaldb-cli",
    "--bin",
    "signal-producer",
  ]);

  console.log("[e2e-live] building the UI …");
  run("pnpm", ["run", "build"], { cwd: UI_DIR });

  const targetDir = path.join(REPO_ROOT, "target", "debug");

  console.log("[e2e-live] starting the signaldb monolith …");
  serverProcess = spawn(
    path.join(targetDir, "signaldb"),
    ["--config", configPath],
    {
      cwd: REPO_ROOT,
      stdio: "inherit",
      env: {
        ...process.env,
        RUST_LOG: "info",
        SIGNALDB_UI_DIR: path.join(UI_DIR, "dist"),
        // The monolithic binary hard-codes its ports (see env.ts) — no
        // *_ADDR override applies in this mode, unlike the per-service
        // binaries run-dev.sh's "services" mode starts.
      },
    },
  );
  serverProcess.on("exit", (code, signal) => {
    if (code !== null && code !== 0) {
      console.error(
        `[e2e-live] signaldb exited early (code=${code} signal=${signal})`,
      );
    }
  });

  await waitFor("signaldb router health", async () => {
    const res = await fetch(`${BASE_URL}/health`);
    return res.ok;
  });
  console.log("[e2e-live] router is healthy");

  console.log("[e2e-live] bootstrapping the login user …");
  run(
    path.join(targetDir, "signaldb-cli"),
    [
      "--url",
      BASE_URL,
      "--admin-key",
      ADMIN_API_KEY,
      "user",
      "create",
      LOGIN_EMAIL,
      "--tenant",
      TENANT_ID,
      "--role",
      "admin",
      "--instance-admin",
    ],
    { env: { ...process.env, SIGNALDB_USER_PASSWORD: LOGIN_PASSWORD } },
  );

  console.log("[e2e-live] seeding OTLP data via signal-producer …");
  run(
    path.join(targetDir, "signal-producer"),
    [
      "--endpoint",
      `http://127.0.0.1:${ACCEPTOR_GRPC_PORT}`,
      "--estate",
      "rideshare",
      "--count",
      "3",
      "--interval",
      "1",
      "--traces-per-tick",
      "8",
    ],
    {
      env: {
        ...process.env,
        OTEL_EXPORTER_OTLP_HEADERS: `x-tenant-id=${TENANT_ID},x-dataset-id=${DATASET_ID},authorization=Bearer ${TENANT_API_KEY}`,
      },
    },
  );

  console.log("[e2e-live] waiting for seeded traces to be queryable …");
  await waitFor(
    "seeded traces to appear in a query",
    async () => {
      const res = await fetch(`${BASE_URL}/api/v1/query`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${TENANT_API_KEY}`,
          "x-tenant-id": TENANT_ID,
          "x-dataset-id": DATASET_ID,
        },
        body: JSON.stringify({
          irVersion: 1,
          from: "traces",
          range: { from: "now-1h", to: "now" },
          result: "rows",
          pipeline: [
            {
              filter: {
                expr: {
                  op: "eq",
                  field: "service.name",
                  value: SEEDED_SERVICE,
                },
              },
            },
            { limit: { count: 1 } },
          ],
        }),
      });
      if (!res.ok) return false;
      const body = (await res.json()) as { rows?: unknown[] };
      return Array.isArray(body.rows) && body.rows.length > 0;
    },
    90_000,
  );
  console.log("[e2e-live] seeded data is queryable");

  console.log("[e2e-live] logging in and saving storage state …");
  const loginRes = await fetch(`${BASE_URL}/ui/session`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      email: LOGIN_EMAIL,
      password: LOGIN_PASSWORD,
      tenant: TENANT_ID,
    }),
  });
  if (!loginRes.ok) {
    throw new Error(
      `login failed: ${loginRes.status} ${await loginRes.text()}`,
    );
  }
  const setCookie = loginRes.headers.get("set-cookie");
  if (!setCookie)
    throw new Error("login response carried no Set-Cookie header");
  const [cookiePair] = setCookie.split(";");
  const [cookieName, cookieValue] = cookiePair.split("=");

  writeFileSync(
    STORAGE_STATE_FILE,
    JSON.stringify({
      cookies: [
        {
          name: cookieName,
          value: cookieValue,
          domain: "127.0.0.1",
          path: "/",
          expires: -1,
          httpOnly: true,
          secure: false,
          sameSite: "Lax",
        },
      ],
      origins: [],
    }),
  );

  writeFileSync(
    STATE_FILE,
    JSON.stringify({
      baseURL: BASE_URL,
      tenant: TENANT_ID,
      dataset: DATASET_ID,
      seededService: SEEDED_SERVICE,
      pid: serverProcess.pid,
      tempDir,
    }),
  );

  console.log("[e2e-live] setup complete");
}
