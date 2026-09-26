// Zoom and pan around a child that has no zoom of its own (the service map):
// +/−/FIT buttons, ⌘/Ctrl + wheel zooming at the pointer, and dragging
// empty space to pan. Clicks on buttons and graph nodes pass through.

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

export function ZoomPan({ children }: { children: ReactNode }) {
  const hostRef = useRef<HTMLDivElement>(null);
  const [view, setView] = useState<View>(HOME);
  const [dragging, setDragging] = useState(false);

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
  return (
    <div
      ref={hostRef}
      className={`zoompan${dragging ? " dragging" : ""}`}
      onPointerDown={onPointerDown}
    >
      <div
        className="zoompan-inner"
        style={{
          transform: `translate(${view.x}px, ${view.y}px) scale(${view.z})`,
        }}
      >
        {children}
      </div>
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
          aria-label="Reset zoom"
          title="Reset zoom"
          onClick={() => setView(HOME)}
        >
          <span className="zoompan-fit">FIT</span>
        </button>
      </div>
      <div className="zoompan-readout" aria-live="polite">
        {Math.round(view.z * 100)}%
        {moved ? " · drag to pan" : " · ⌘/ctrl + scroll to zoom"}
      </div>
    </div>
  );
}
