export const PWA_UPDATE_CHECK_INTERVAL_MS = 60 * 60 * 1000;

// A service worker only re-checks for updates on navigation/registration by
// default, but an ops dashboard is often left open in one tab — sometimes
// several — for days. Without an explicit poll, such a tab could never learn
// a new build exists. The check only fires while the tab is visible: a
// background tab gains nothing from a fresher build nobody is looking at
// yet, and `registration.update()` forces a network revalidation that, once
// autoUpdate detects a change, reloads the page — wasted work, repeated
// across however many tabs are open, if it ran unconditionally.
export function schedulePeriodicUpdateCheck(
  registration: ServiceWorkerRegistration,
  intervalMs: number = PWA_UPDATE_CHECK_INTERVAL_MS,
): () => void {
  let lastCheck = Date.now();

  const maybeCheck = () => {
    if (document.visibilityState !== "visible") return;
    if (Date.now() - lastCheck < intervalMs) return;
    lastCheck = Date.now();
    void registration.update();
  };

  const intervalId = setInterval(maybeCheck, intervalMs);
  document.addEventListener("visibilitychange", maybeCheck);

  return () => {
    clearInterval(intervalId);
    document.removeEventListener("visibilitychange", maybeCheck);
  };
}
