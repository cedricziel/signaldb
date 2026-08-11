// Facet/field sidebar width persistence. One width for every signal — logs'
// field panel and traces' facets are the same furniture (see explore.css) —
// so the saved value lives under one key and is applied to <html> as a CSS
// custom property, the same way lib/theme.ts applies the saved theme.

const STORAGE_KEY = "signaldb.explore.sidebarWidth";
const CSS_VAR = "--sidebar-w";

export const SIDEBAR_MIN_PX = 200;
export const SIDEBAR_MAX_PX = 480;
export const SIDEBAR_DEFAULT_PX = 248;

export function clampSidebarWidth(px: number): number {
  return Math.min(SIDEBAR_MAX_PX, Math.max(SIDEBAR_MIN_PX, px));
}

/** The saved width, clamped; the default when nothing was saved. */
export function readSidebarWidth(): number {
  try {
    const stored = Number(localStorage.getItem(STORAGE_KEY));
    return stored > 0 ? clampSidebarWidth(stored) : SIDEBAR_DEFAULT_PX;
  } catch {
    return SIDEBAR_DEFAULT_PX;
  }
}

/** Apply the saved sidebar width (if any) to <html> before first paint. */
export function initSidebarWidth(): void {
  try {
    document.documentElement.style.setProperty(
      CSS_VAR,
      `${readSidebarWidth()}px`,
    );
  } catch {
    // localStorage unavailable
  }
}

/** Clamp, persist, and apply a new sidebar width. Returns the clamped value. */
export function setSidebarWidth(px: number): number {
  const clamped = clampSidebarWidth(px);
  document.documentElement.style.setProperty(CSS_VAR, `${clamped}px`);
  try {
    localStorage.setItem(STORAGE_KEY, String(clamped));
  } catch {
    // localStorage unavailable
  }
  return clamped;
}
