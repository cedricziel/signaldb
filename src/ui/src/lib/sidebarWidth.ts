// Draggable panel width persistence, shared by every resizable pane in the
// app: the facet/field sidebar (logs' field panel and traces' facets — the
// same furniture, see explore.css) and the trace waterfall's span-detail
// pane. Each panel gets its own storage key, CSS custom property, and size
// range, applied to <html> the same way lib/theme.ts applies the saved
// theme, so the value is available wherever the panel renders regardless of
// which page's instance is mounted. Width never lives in React state: a
// drag only rewrites the custom property, so nothing re-renders per move.

export interface PanelWidth {
  /** Which side of its resizer the panel sits on; a drag toward it widens. */
  readonly grows: "left" | "right";
  /** Class and accessible name of the panel's drag handle. */
  readonly resizerClassName: string;
  readonly resizerLabel: string;
  /** The saved width, clamped; the default when nothing was saved. */
  read(): number;
  /** Apply the saved width (if any) to <html> before first paint. */
  init(): void;
  /** Clamp and apply a width without persisting it (mid-drag). */
  apply(px: number): number;
  /** Clamp, apply, and persist a width. Returns the clamped value. */
  set(px: number): number;
  clamp(px: number): number;
}

interface PanelWidthConfig {
  storageKey: string;
  cssVar: string;
  min: number;
  max: number;
  defaultPx: number;
  grows: "left" | "right";
  resizerClassName: string;
  resizerLabel: string;
}

export function createPanelWidth({
  storageKey,
  cssVar,
  min,
  max,
  defaultPx,
  grows,
  resizerClassName,
  resizerLabel,
}: PanelWidthConfig): PanelWidth {
  const clamp = (px: number): number => Math.min(max, Math.max(min, px));

  const read = (): number => {
    try {
      const stored = Number(localStorage.getItem(storageKey));
      return stored > 0 ? clamp(stored) : defaultPx;
    } catch {
      return defaultPx;
    }
  };

  const apply = (px: number): number => {
    const clamped = clamp(px);
    document.documentElement.style.setProperty(cssVar, `${clamped}px`);
    return clamped;
  };

  const init = (): void => {
    try {
      apply(read());
    } catch {
      // localStorage unavailable
    }
  };

  const set = (px: number): number => {
    const clamped = apply(px);
    try {
      localStorage.setItem(storageKey, String(clamped));
    } catch {
      // localStorage unavailable
    }
    return clamped;
  };

  return {
    grows,
    resizerClassName,
    resizerLabel,
    read,
    init,
    apply,
    set,
    clamp,
  };
}

/** Facet/field sidebar: logs' field panel and traces' facets. */
export const sidebarWidth = createPanelWidth({
  storageKey: "signaldb.explore.sidebarWidth",
  cssVar: "--sidebar-w",
  min: 200,
  max: 480,
  defaultPx: 248,
  grows: "right",
  resizerClassName: "sidebar-resizer",
  resizerLabel: "Resize sidebar",
});

/** Span-detail pane in the trace waterfall, right of its resizer. */
export const spanDetailWidth = createPanelWidth({
  storageKey: "signaldb.trace.sidebarWidth",
  cssVar: "--span-detail-w",
  min: 260,
  max: 640,
  defaultPx: 320,
  grows: "left",
  resizerClassName: "trace-resizer",
  resizerLabel: "Resize span details",
});
