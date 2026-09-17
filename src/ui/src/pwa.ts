import { registerSW } from "virtual:pwa-register";
import {
  schedulePeriodicUpdateCheck,
  setUpdateAvailable,
} from "./lib/pwaUpdate";

let started = false;

// registerType: "prompt" (see vite.config.ts) installs a new service worker
// without activating it; `onNeedRefresh` fires once one is waiting. The
// deferral logic (banner, auto-apply on route change while no form is dirty)
// lives in lib/pwaUpdate.ts, which — unlike this file — can be unit tested,
// since importing `virtual:pwa-register` only resolves in a real Vite/PWA
// build (see vite.config.ts's coverage exclusions). This file just wires the
// SDK's callbacks to that module and keeps a long-lived tab checking; see
// schedulePeriodicUpdateCheck. Idempotent, matching initTelemetry's
// convention.
export function initPwaUpdates(): void {
  if (started) return;
  started = true;

  const updateSW = registerSW({
    onNeedRefresh() {
      setUpdateAvailable(updateSW);
    },
    onRegisteredSW(_swUrl, registration) {
      if (registration) schedulePeriodicUpdateCheck(registration);
    },
  });
}
