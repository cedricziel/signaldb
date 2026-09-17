// The unobtrusive prompt for a service-worker update that has installed and
// is waiting to take over (see lib/pwaUpdate.ts for why this isn't an
// automatic reload — a half-typed form would be lost). Renders nothing until
// one is pending. The visitor can apply it now via Reload; otherwise it
// applies itself on the next route change once no form is dirty (see
// App.tsx's useEffect on `location`).
import { useSyncExternalStore } from "react";
import {
  applyPendingUpdate,
  getUpdateState,
  subscribeUpdateState,
} from "../../lib/pwaUpdate";
import "./UpdateBanner.css";

export function useUpdateAvailable(): boolean {
  return useSyncExternalStore(
    subscribeUpdateState,
    () => getUpdateState().updateSW !== null,
    () => false,
  );
}

export function UpdateBanner() {
  const available = useUpdateAvailable();
  if (!available) return null;
  return (
    <div
      className="warn-callout update-banner"
      role="status"
      aria-live="polite"
    >
      <span>A new version is ready</span>
      <button type="button" onClick={() => applyPendingUpdate()}>
        Reload
      </button>
    </div>
  );
}
