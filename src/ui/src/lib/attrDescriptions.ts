// Persistence for the "show descriptions" reading-mode toggle shared by the
// log detail (LogList.tsx) and the span panel (TracesView.tsx): both read
// the same key so the choice carries from one view to the other, and both
// write through this module rather than touching localStorage directly (see
// lib/sidebarWidth.ts for the same try/catch pattern).
import { useState } from "react";

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

/**
 * The reading-mode toggle's state plus its flip action, backed by the
 * persisted value above — `LogList.tsx` and `TracesView.tsx`'s `SpanDetail`
 * both own one of these rather than hand-rolling the read/write/setState
 * sequence themselves.
 */
export function useAttrDescriptions(): [boolean, () => void] {
  const [showDescriptions, setShowDescriptions] = useState(readAttrDescriptions);
  const toggle = () => {
    const next = !showDescriptions;
    writeAttrDescriptions(next);
    setShowDescriptions(next);
  };
  return [showDescriptions, toggle];
}
