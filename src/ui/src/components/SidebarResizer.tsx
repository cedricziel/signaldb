import { useCallback, useRef, type MouseEvent as ReactMouseEvent } from "react";
import { readSidebarWidth, setSidebarWidth } from "../lib/sidebarWidth";

/**
 * Drag handle for the shared facet/field sidebar (logs' field panel and
 * traces' facets — see lib/sidebarWidth.ts). Renders as a child of the
 * `.sidebar` element it resizes; width state lives outside React (a CSS
 * custom property on `<html>` plus localStorage), so this needs no props and
 * stays in sync regardless of which page's instance is mounted.
 */
export function SidebarResizer() {
  const dragRef = useRef<{ startX: number; startWidth: number } | null>(null);

  const onPointerMove = useCallback((e: MouseEvent) => {
    const drag = dragRef.current;
    if (!drag) return;
    setSidebarWidth(drag.startWidth + (e.clientX - drag.startX));
  }, []);

  const onPointerUp = useCallback(() => {
    dragRef.current = null;
    window.removeEventListener("mousemove", onPointerMove);
    window.removeEventListener("mouseup", onPointerUp);
  }, [onPointerMove]);

  const startDrag = useCallback(
    (e: ReactMouseEvent) => {
      e.preventDefault();
      dragRef.current = { startX: e.clientX, startWidth: readSidebarWidth() };
      window.addEventListener("mousemove", onPointerMove);
      window.addEventListener("mouseup", onPointerUp);
    },
    [onPointerMove, onPointerUp],
  );

  return (
    <div
      className="sidebar-resizer"
      role="separator"
      aria-orientation="vertical"
      aria-label="Resize sidebar"
      onMouseDown={startDrag}
    />
  );
}
