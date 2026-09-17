// App-wide guard for every registered dirty form (registry editor, API-key
// form, consent, origin picker — see lib/dirtyForms.ts): mounted once in the
// route tree's root layout (routes.tsx), above both the explore shell and
// the top-level views (`/oauth/consent`, `/login`) that sit outside it, it
// blocks in-app navigation — top-bar links, the signal tab strip, the user
// menu, browser Back/Forward — while any form is dirty, prompting to
// confirm before discarding the edit. `useBlocker` only intercepts router
// navigation, so reload/close is still each dirty form's own `beforeunload`
// handler, not this component's job.

import { useCallback, useSyncExternalStore } from "react";
import { useBlocker } from "react-router";
import { Dialog } from "../../components/Dialog";
import { anyDirty, subscribe } from "../../lib/dirtyForms";
import "./UnsavedChangesGuard.css";

export function UnsavedChangesGuard() {
  const dirty = useSyncExternalStore(subscribe, anyDirty, anyDirty);
  // Only block an actual location change — re-rendering with the same
  // location (e.g. a search-param rewrite the shell makes itself) must not
  // trip the guard.
  const shouldBlock = useCallback(
    ({
      currentLocation,
      nextLocation,
    }: {
      currentLocation: { pathname: string; search: string; hash: string };
      nextLocation: { pathname: string; search: string; hash: string };
    }) =>
      dirty &&
      (currentLocation.pathname !== nextLocation.pathname ||
        currentLocation.search !== nextLocation.search ||
        currentLocation.hash !== nextLocation.hash),
    [dirty],
  );
  const blocker = useBlocker(shouldBlock);

  if (blocker.state !== "blocked") return null;

  return (
    <Dialog label="Unsaved changes" className="login-panel unsaved-guard">
      <h2>Unsaved changes</h2>
      <p>Leaving now will discard them.</p>
      <div className="unsaved-guard-actions">
        <button type="button" className="btn" onClick={() => blocker.reset()}>
          Stay
        </button>
        <button
          type="button"
          className="btn btn-primary"
          onClick={() => blocker.proceed()}
        >
          Leave
        </button>
      </div>
    </Dialog>
  );
}
