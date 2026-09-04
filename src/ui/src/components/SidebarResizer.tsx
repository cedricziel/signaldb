import {
  useCallback,
  useEffect,
  useRef,
  type MouseEvent as ReactMouseEvent,
} from "react";
import type { PanelWidth } from "../lib/sidebarWidth";

/**
 * Drag handle for a resizable panel (see lib/sidebarWidth.ts). Renders as a
 * child of the element it resizes; width state lives outside React (a CSS
 * custom property on `<html>` plus localStorage), so this needs no width
 * prop and stays in sync regardless of which page's instance is mounted.
 * The panel carries its own drag direction, class and label, so a handle
 * cannot be wired to the wrong side.
 */
export function SidebarResizer({ panel }: { panel: PanelWidth }) {
  const dragRef = useRef<{
    startX: number;
    startWidth: number;
    width: number;
  } | null>(null);

  const onPointerMove = useCallback(
    (e: MouseEvent) => {
      const drag = dragRef.current;
      if (!drag) return;
      const dx = e.clientX - drag.startX;
      drag.width = panel.apply(
        drag.startWidth + (panel.grows === "left" ? -dx : dx),
      );
    },
    [panel],
  );

  const onPointerUp = useCallback(() => {
    const drag = dragRef.current;
    dragRef.current = null;
    window.removeEventListener("mousemove", onPointerMove);
    window.removeEventListener("mouseup", onPointerUp);
    // Persist once, at the end of the drag, not on every move.
    if (drag) panel.set(drag.width);
  }, [panel, onPointerMove]);

  // Unmounting mid-drag (navigating away with the button held) must not
  // leave the window listeners resizing a panel that is no longer there.
  useEffect(
    () => () => {
      dragRef.current = null;
      window.removeEventListener("mousemove", onPointerMove);
      window.removeEventListener("mouseup", onPointerUp);
    },
    [onPointerMove, onPointerUp],
  );

  const startDrag = useCallback(
    (e: ReactMouseEvent) => {
      e.preventDefault();
      const startWidth = panel.read();
      dragRef.current = { startX: e.clientX, startWidth, width: startWidth };
      window.addEventListener("mousemove", onPointerMove);
      window.addEventListener("mouseup", onPointerUp);
    },
    [panel, onPointerMove, onPointerUp],
  );

  return (
    <div
      className={panel.resizerClassName}
      role="separator"
      aria-orientation="vertical"
      aria-label={panel.resizerLabel}
      onMouseDown={startDrag}
    />
  );
}
