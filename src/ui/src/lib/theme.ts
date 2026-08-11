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

/** Flip the current theme and persist the new choice. */
export function toggleTheme(): void {
  const root = document.documentElement;
  const current = root.getAttribute("data-theme");
  const isDark =
    current === "dark" ||
    (!current && window.matchMedia("(prefers-color-scheme: dark)").matches);
  const next = isDark ? "light" : "dark";
  root.setAttribute("data-theme", next);
  try {
    localStorage.setItem(STORAGE_KEY, next);
  } catch {
    // localStorage unavailable
  }
}
