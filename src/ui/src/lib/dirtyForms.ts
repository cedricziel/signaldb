// Registry of which open forms currently hold unsaved input, so the PWA
// update flow (pwa.ts, features/shell/UpdateBanner.tsx, App.tsx) never
// reloads out from under a half-typed API-key form, a pending consent
// selection, an origin chip list, or a registry edit. A form registers under
// a stable id via `useDirtyForm`; nothing here knows about any particular
// form's shape — it only tracks the set of currently-dirty ids.

import { useEffect } from "react";

const dirtyIds = new Set<string>();
const listeners = new Set<() => void>();

function notify(): void {
  for (const listener of listeners) listener();
}

/** Record whether the form identified by `id` currently has unsaved input. */
export function markDirty(id: string, dirty: boolean): void {
  const wasDirty = dirtyIds.has(id);
  if (dirty === wasDirty) return;
  if (dirty) dirtyIds.add(id);
  else dirtyIds.delete(id);
  notify();
}

/** True while any registered form is dirty. */
export function anyDirty(): boolean {
  return dirtyIds.size > 0;
}

/** Subscribe to dirty-set changes; returns an unsubscribe function (the shape
 * `useSyncExternalStore` expects). */
export function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** Test hook: forget every registered form. */
export function resetDirtyForms(): void {
  if (dirtyIds.size === 0) return;
  dirtyIds.clear();
  notify();
}

/**
 * Register a form's dirty state under `id` for as long as the component is
 * mounted, clearing it on unmount (or id change) so a closed or reset form
 * never keeps the app pinned against auto-applying a pending update.
 */
export function useDirtyForm(id: string, isDirty: boolean): void {
  useEffect(() => {
    markDirty(id, isDirty);
    return () => markDirty(id, false);
  }, [id, isDirty]);
}
