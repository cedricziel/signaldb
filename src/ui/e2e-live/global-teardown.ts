// Stops the signaldb monolith started by global-setup.ts and removes its
// temp dir. Reads the pid/tempDir back from STATE_FILE rather than relying
// on module-scope state, since Playwright does not guarantee globalSetup and
// globalTeardown share a process.
import { existsSync, readFileSync, rmSync } from "node:fs";

import { STATE_FILE, STORAGE_STATE_FILE } from "./env";

export default async function globalTeardown() {
  if (!existsSync(STATE_FILE)) return;

  const state = JSON.parse(readFileSync(STATE_FILE, "utf-8")) as {
    pid?: number;
    tempDir?: string;
  };

  if (state.pid) {
    try {
      process.kill(state.pid, "SIGTERM");
    } catch {
      // Already exited.
    }
  }

  if (state.tempDir) {
    rmSync(state.tempDir, { recursive: true, force: true });
  }

  rmSync(STATE_FILE, { force: true });
  rmSync(STORAGE_STATE_FILE, { force: true });
}
