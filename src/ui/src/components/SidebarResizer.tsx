import {
  useRef,
  type KeyboardEvent as ReactKeyboardEvent,
  type PointerEvent as ReactPointerEvent,
} from "react";
import type { PanelWidth } from "../lib/sidebarWidth";

const KEY_STEP = 16;
const KEY_STEP_LARGE = 64;

/** The facet sidebar's handle sits inside its pane; the span-detail handle
 * sits just before the (`display: contents`) drawer wrapping its pane. */
function paneOf(handle: HTMLElement): Element | null {
  const parent = handle.parentElement;
  if (parent?.tagName === "ASIDE") return parent;
  return handle.nextElementSibling?.querySelector("aside") ?? null;
}

function drawnStart(panel: PanelWidth, handle: HTMLElement): number {
  return panel.dragStart(paneOf(handle)?.getBoundingClientRect().width);
}

/**
 * Drag handle for a resizable panel (see lib/sidebarWidth.ts). Renders as a
 * child of the element it resizes; width state lives outside React (a CSS
 * custom property on `<html>` plus localStorage), so this needs no width
 * prop and stays in sync regardless of which page's instance is mounted.
 * The panel carries its own drag direction, class and label, so a handle
 * cannot be wired to the wrong side. Mouse, touch and pen all drag through
 * pointer capture; arrow keys resize a focused handle.
 */
export function SidebarResizer({ panel }: { panel: PanelWidth }) {
  const dragRef = useRef<{
    pointerId: number;
    startX: number;
    startWidth: number;
    width: number;
  } | null>(null);

  const onPointerDown = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (e.button !== 0) return;
    e.preventDefault();
    e.currentTarget.setPointerCapture(e.pointerId);
    const startWidth = drawnStart(panel, e.currentTarget);
    dragRef.current = {
      pointerId: e.pointerId,
      startX: e.clientX,
      startWidth,
      width: startWidth,
    };
  };

  const onPointerMove = (e: ReactPointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current;
    if (drag?.pointerId !== e.pointerId) return;
    const dx = e.clientX - drag.startX;
    const next = panel.clamp(
      drag.startWidth + (panel.grows === "left" ? -dx : dx),
    );
    if (next !== drag.width) drag.width = panel.apply(next);
  };

  const endDrag = (e: ReactPointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current;
    if (drag?.pointerId !== e.pointerId) return;
    dragRef.current = null;
    // Persist once, at the end of the drag, not on every move — and not for
    // a click that moved nothing, which would overwrite a saved width with
    // the capped one it is drawn at.
    if (drag.width !== drag.startWidth) panel.set(drag.width);
  };

  const onKeyDown = (e: ReactKeyboardEvent<HTMLDivElement>) => {
    if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
    e.preventDefault();
    const step = e.shiftKey ? KEY_STEP_LARGE : KEY_STEP;
    const widens = (e.key === "ArrowLeft") === (panel.grows === "left");
    panel.set(drawnStart(panel, e.currentTarget) + (widens ? step : -step));
  };

  return (
    <div
      className={`pane-resizer ${panel.resizerClassName}`}
      role="separator"
      aria-orientation="vertical"
      aria-label={panel.resizerLabel}
      tabIndex={0}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={endDrag}
      onPointerCancel={endDrag}
      onKeyDown={onKeyDown}
    />
  );
}
