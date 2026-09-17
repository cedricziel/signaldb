import { useEffect, useState, type RefObject } from "react";

/**
 * Track a container element's real pixel width via `ResizeObserver`,
 * starting from `fallback` for the one frame before the observer reports —
 * the shared pattern behind every inline-SVG chart's `viewBox` (so
 * `preserveAspectRatio="none"` never has to stretch it) and any other panel
 * that needs its host's live width.
 */
export function useContainerWidth(
  ref: RefObject<HTMLElement | null>,
  fallback: number,
): number {
  const [width, setWidth] = useState(fallback);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width;
      if (w) setWidth(w);
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, [ref]);
  return width;
}
