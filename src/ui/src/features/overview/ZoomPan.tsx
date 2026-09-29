// Zoom and pan around a child that has no zoom of its own (the service map):
// +/−/FIT buttons, ⌘/Ctrl + wheel zooming at the pointer, and dragging
// empty space to pan. Clicks on buttons and graph nodes pass through. A
// child wider than the host (the map stops shrinking at MIN_GRAPH_SCALE)
// fades out on each clipped side, and FIT zooms out until it all shows.

import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";

const MIN_ZOOM = 0.5;
const MAX_ZOOM = 3;

interface View {
  z: number;
  x: number;
  y: number;
}

const HOME: View = { z: 1, x: 0, y: 0 };

interface Size {
  hostW: number;
  contentW: number;
}

/** The view that shows all of a `contentW`-wide child in a `hostW`-wide
 * host: natural size when it fits, zoomed out (down to MIN_ZOOM) when not. */
export function fitView({ hostW, contentW }: Size): View {
  if (hostW <= 0 || contentW <= hostW) return HOME;
  return { z: Math.max(MIN_ZOOM, hostW / contentW), x: 0, y: 0 };
}

/** Which sides of the host clip the child under `view`, give or take a
 * pixel of rounding. */
export function clippedSides(
  view: View,
  { hostW, contentW }: Size,
): { left: boolean; right: boolean } {
  return {
    left: view.x < -1,
    right: view.x + contentW * view.z > hostW + 1,
  };
}

/** `view` zoomed by `k` around the point (`cx`, `cy`) in host coordinates,
 * which stays put on screen. */
export function zoomAround(
  view: View,
  k: number,
  cx: number,
  cy: number,
): View {
  const z = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, +(view.z * k).toFixed(3)));
  if (z === view.z) return view;
  return {
    z,
    x: cx - ((cx - view.x) * z) / view.z,
    y: cy - ((cy - view.y) * z) / view.z,
  };
}

export function ZoomPan({
  children,
  controls = true,
}: {
  children: ReactNode;
  /** Off when there is nothing to zoom, e.g. an empty map. */
  controls?: boolean;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);
  const [view, setView] = useState<View>(HOME);
  const [dragging, setDragging] = useState(false);
  const [hostW, setHostW] = useState(0);
  const [contentW, setContentW] = useState(0);
  const size: Size = { hostW, contentW };

  // The inner box's scrollWidth is the child's untransformed width, as long
  // as the child lets its overflow spill (overview.css does for
  // `.service-graph`), which is why useContainerWidth, reporting only the
  // observed box's own width, doesn't fit here. Observe both boxes.
  useEffect(() => {
    const host = hostRef.current;
    const inner = innerRef.current;
    if (!host || !inner) return;
    const measure = () => {
      setHostW(host.clientWidth);
      setContentW(inner.scrollWidth);
    };
    const observer = new ResizeObserver(measure);
    observer.observe(host);
    observer.observe(inner);
    return () => observer.disconnect();
  }, []);

  const zoomBy = useCallback((k: number, cx?: number, cy?: number) => {
    const r = hostRef.current?.getBoundingClientRect();
    const px = cx ?? (r ? r.width / 2 : 0);
    const py = cy ?? (r ? r.height / 2 : 0);
    setView((v) => zoomAround(v, k, px, py));
  }, []);

  // React's onWheel is passive; preventing the page scroll on ⌘/Ctrl+wheel
  // needs a native, non-passive listener.
  useEffect(() => {
    const el = hostRef.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      if (!e.ctrlKey && !e.metaKey) return;
      e.preventDefault();
      const r = el.getBoundingClientRect();
      zoomBy(
        e.deltaY < 0 ? 1.15 : 1 / 1.15,
        e.clientX - r.left,
        e.clientY - r.top,
      );
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [zoomBy]);

  const onPointerDown = (e: React.PointerEvent) => {
    if (e.button !== 0) return;
    const target = e.target as HTMLElement;
    if (target.closest("button, a, [role='button'], .sg-node")) return;
    const sx = e.clientX;
    const sy = e.clientY;
    const origin = view;
    setDragging(true);
    const move = (ev: PointerEvent) =>
      setView({
        ...origin,
        x: origin.x + ev.clientX - sx,
        y: origin.y + ev.clientY - sy,
      });
    const up = () => {
      setDragging(false);
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };

  const moved = view.z !== 1 || view.x !== 0 || view.y !== 0;
  const clipped = clippedSides(view, size);
  const hidden = clipped.left || clipped.right;
  let hint = " · ⌘/ctrl + scroll to zoom";
  if (hidden) hint = " · drag to pan or FIT to see all";
  else if (moved) hint = " · drag to pan";
  const className = [
    "zoompan",
    dragging && "dragging",
    clipped.left && "clipped-left",
    clipped.right && "clipped-right",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <div ref={hostRef} className={className} onPointerDown={onPointerDown}>
      <div
        ref={innerRef}
        className="zoompan-inner"
        style={{
          transform: `translate(${view.x}px, ${view.y}px) scale(${view.z})`,
        }}
      >
        {children}
      </div>
      {controls && (
        <>
          <div className="zoompan-controls">
            <button
              type="button"
              aria-label="Zoom in"
              title="Zoom in"
              onClick={() => zoomBy(1.25)}
            >
              +
            </button>
            <button
              type="button"
              aria-label="Zoom out"
              title="Zoom out"
              onClick={() => zoomBy(0.8)}
            >
              −
            </button>
            <button
              type="button"
              className={hidden ? "zoompan-cue" : undefined}
              aria-label="Fit to view"
              title="Fit to view"
              onClick={() => setView(fitView(size))}
            >
              <span className="zoompan-fit">FIT</span>
            </button>
          </div>
          <div className="zoompan-readout" aria-live="polite">
            {Math.round(view.z * 100)}%{hint}
          </div>
        </>
      )}
    </div>
  );
}
