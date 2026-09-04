import { useEffect } from "react";

/**
 * Calls `onEscape` when Escape is pressed anywhere in the window while
 * `active` is true. The one listener behind every dismissible overlay that
 * has no focused element of its own to hang a key handler on (the mobile
 * sidebar drawer, the user menu popover, an armed ConfirmButton).
 *
 * `exclusive` claims the key outright: the listener runs in the capture
 * phase and stops the event, so nothing beneath it (a surrounding Dialog's
 * own Escape handling included) sees the same press. Escape then backs out
 * one layer at a time, the innermost first. Exclusive listeners are peers
 * of one another, not layers: `stopPropagation` (not the immediate form)
 * lets every armed one fire, so one press cancels every armed
 * confirmation rather than whichever happened to arm first.
 */
export function useEscapeKey(
  active: boolean,
  onEscape: () => void,
  { exclusive = false }: { exclusive?: boolean } = {},
) {
  useEffect(() => {
    if (!active) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      if (exclusive) event.stopPropagation();
      onEscape();
    };
    window.addEventListener("keydown", onKey, exclusive);
    return () => window.removeEventListener("keydown", onKey, exclusive);
  }, [active, onEscape, exclusive]);
}
