// Theme persistence. The saved choice lives in localStorage under
// "signaldb-theme"; global.css falls back to prefers-color-scheme when no
// data-theme attribute is set on <html>.

const STORAGE_KEY = "signaldb-theme";

/** Apply the saved theme (if any) to <html> before first paint. */
export function initTheme(): void {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved === "dark" || saved === "light") {
      document.documentElement.setAttribute("data-theme", saved);
    }
  } catch {
    // localStorage unavailable
  }
}

/** Whether `<html>` is currently in dark mode, honoring an explicit
 * data-theme override before falling back to prefers-color-scheme. */
export function isDarkTheme(): boolean {
  const current = document.documentElement.getAttribute("data-theme");
  return (
    current === "dark" ||
    (!current && window.matchMedia("(prefers-color-scheme: dark)").matches)
  );
}

/** Flip the current theme and persist the new choice. */
export function toggleTheme(): void {
  const root = document.documentElement;
  const next = isDarkTheme() ? "light" : "dark";
  root.setAttribute("data-theme", next);
  try {
    localStorage.setItem(STORAGE_KEY, next);
  } catch {
    // localStorage unavailable
  }
}

// subscribeTheme is called once per chart/panel that tracks the theme (every
// MetricsChart on a busy dashboard, say); one MutationObserver plus one
// matchMedia listener shared by all of them, rather than a pair per
// subscriber, is enough to serve them all. Lazily created on the first
// subscriber and torn down once the last one unsubscribes.
let listeners: Set<() => void> | null = null;
let observer: MutationObserver | null = null;
let media: MediaQueryList | null = null;

function notifyListeners(): void {
  listeners?.forEach((cb) => cb());
}

/**
 * Calls `cb` whenever the effective theme may have changed: an explicit
 * `data-theme` toggle (this tab's own {@link toggleTheme}, or a `settings`
 * page in another tab reaching the same `<html>`) and a system-level
 * `prefers-color-scheme` flip for a page with no explicit override, either
 * of which a chart drawn with colours resolved once at mount would
 * otherwise miss. Returns an unsubscribe function.
 */
export function subscribeTheme(cb: () => void): () => void {
  if (!listeners) {
    listeners = new Set();
    observer = new MutationObserver(notifyListeners);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme"],
    });
    media = window.matchMedia("(prefers-color-scheme: dark)");
    media.addEventListener("change", notifyListeners);
  }
  listeners.add(cb);
  return () => {
    listeners?.delete(cb);
    if (listeners && listeners.size === 0) {
      observer?.disconnect();
      media?.removeEventListener("change", notifyListeners);
      listeners = null;
      observer = null;
      media = null;
    }
  };
}
