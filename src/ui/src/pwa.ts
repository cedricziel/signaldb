import { registerSW } from "virtual:pwa-register";
import { schedulePeriodicUpdateCheck } from "./lib/pwaUpdate";

let started = false;

// registerType: "autoUpdate" (see vite.config.ts) already installs and
// activates new service workers without prompting; this just makes sure a
// long-lived tab keeps checking. See schedulePeriodicUpdateCheck. Idempotent,
// matching initTelemetry's convention.
export function initPwaUpdates(): void {
  if (started) return;
  started = true;

  registerSW({
    onRegisteredSW(_swUrl, registration) {
      if (registration) schedulePeriodicUpdateCheck(registration);
    },
  });
}
