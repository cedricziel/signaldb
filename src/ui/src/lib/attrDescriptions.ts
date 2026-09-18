// Persistence for the "show descriptions" reading-mode toggle shared by the
// log detail (LogList.tsx) and the span panel (TracesView.tsx): both read
// the same key so the choice carries from one view to the other, and both
// write through this module rather than touching localStorage directly (see
// lib/sidebarWidth.ts for the same try/catch pattern).
const STORAGE_KEY = "signaldb.ui.attrDescriptions";

export function readAttrDescriptions(): boolean {
  try {
    return localStorage.getItem(STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

export function writeAttrDescriptions(value: boolean): void {
  try {
    localStorage.setItem(STORAGE_KEY, value ? "1" : "0");
  } catch {
    // localStorage unavailable (private mode, disabled storage, ...).
  }
}
