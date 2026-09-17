import { anyDirty } from "./dirtyForms";

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

// --- deferred update-apply flow ---------------------------------------------
//
// registerType: "prompt" (vite.config.ts) installs a new service worker in
// the background but leaves it waiting rather than reloading immediately —
// autoUpdate's silent reload could land mid-edit and wipe a half-typed
// API-key form, a pending consent selection, origin chips, or a registry
// edit (see lib/dirtyForms.ts). `pwa.ts`'s `onNeedRefresh` calls
// `setUpdateAvailable` with the SDK's `updateSW` callback; from there the
// update is applied either by the visitor clicking Reload on
// `features/shell/UpdateBanner.tsx`, or automatically on the next route
// change once no form is dirty (`App.tsx` calls `maybeAutoApplyUpdate`).

/** The virtual `pwa-register` module's `updateSW`: activates the waiting
 * service worker and, when `reloadPage` is true, reloads once it takes over. */
export type UpdateSWFn = (reloadPage?: boolean) => Promise<void>;

interface UpdateState {
  /** The pending update's activation callback, or `null` when none is
   * waiting. */
  updateSW: UpdateSWFn | null;
}

let updateState: UpdateState = { updateSW: null };
const updateListeners = new Set<() => void>();

function setUpdateState(next: UpdateState): void {
  updateState = next;
  for (const listener of updateListeners) listener();
}

/** Current snapshot (stable identity until it changes — safe for
 * `useSyncExternalStore`). */
export function getUpdateState(): UpdateState {
  return updateState;
}

export function subscribeUpdateState(listener: () => void): () => void {
  updateListeners.add(listener);
  return () => updateListeners.delete(listener);
}

/** Record that a new service-worker version has installed and is ready —
 * called from `pwa.ts`'s `onNeedRefresh`. */
export function setUpdateAvailable(updateSW: UpdateSWFn): void {
  setUpdateState({ updateSW });
}

/** Activate the pending update now, reloading once it takes over. A no-op
 * when none is pending. */
export function applyPendingUpdate(): void {
  const { updateSW } = updateState;
  if (!updateSW) return;
  setUpdateState({ updateSW: null });
  void updateSW(true);
}

/** Apply a pending update, but only when no form is dirty — the auto-apply
 * half of the deferral: called on every route change (see App.tsx). Never
 * reloads out from under an in-progress form; the visitor can still apply it
 * manually via the banner regardless of dirty state. */
export function maybeAutoApplyUpdate(): void {
  if (updateState.updateSW && !anyDirty()) applyPendingUpdate();
}

/** Test hook: forget any pending update. */
export function resetUpdateState(): void {
  setUpdateState({ updateSW: null });
}
