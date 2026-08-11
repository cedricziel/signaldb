import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  SIDEBAR_DEFAULT_PX,
  SIDEBAR_MAX_PX,
  SIDEBAR_MIN_PX,
  clampSidebarWidth,
  initSidebarWidth,
  readSidebarWidth,
  setSidebarWidth,
} from "./sidebarWidth";

const STORAGE_KEY = "signaldb.explore.sidebarWidth";

function cssVar(): string {
  return document.documentElement.style.getPropertyValue("--sidebar-w");
}

beforeEach(() => {
  localStorage.clear();
  document.documentElement.style.removeProperty("--sidebar-w");
});

afterEach(() => {
  localStorage.clear();
  document.documentElement.style.removeProperty("--sidebar-w");
});

describe("clampSidebarWidth", () => {
  it("passes values inside the allowed range through unchanged", () => {
    expect(clampSidebarWidth(300)).toBe(300);
  });

  it("clamps below the minimum", () => {
    expect(clampSidebarWidth(1)).toBe(SIDEBAR_MIN_PX);
  });

  it("clamps above the maximum", () => {
    expect(clampSidebarWidth(10_000)).toBe(SIDEBAR_MAX_PX);
  });
});

describe("readSidebarWidth", () => {
  it("falls back to the default when nothing was saved", () => {
    expect(readSidebarWidth()).toBe(SIDEBAR_DEFAULT_PX);
  });

  it("returns a saved, in-range value", () => {
    localStorage.setItem(STORAGE_KEY, "300");
    expect(readSidebarWidth()).toBe(300);
  });

  it("clamps a saved out-of-range value", () => {
    localStorage.setItem(STORAGE_KEY, "9999");
    expect(readSidebarWidth()).toBe(SIDEBAR_MAX_PX);
  });
});

describe("initSidebarWidth", () => {
  it("applies the saved width to <html> as --sidebar-w", () => {
    localStorage.setItem(STORAGE_KEY, "310");
    initSidebarWidth();
    expect(cssVar()).toBe("310px");
  });

  it("applies the default when nothing was saved", () => {
    initSidebarWidth();
    expect(cssVar()).toBe(`${SIDEBAR_DEFAULT_PX}px`);
  });
});

describe("setSidebarWidth", () => {
  it("clamps, persists, and applies the new width", () => {
    const applied = setSidebarWidth(350);
    expect(applied).toBe(350);
    expect(cssVar()).toBe("350px");
    expect(localStorage.getItem(STORAGE_KEY)).toBe("350");
  });

  it("clamps an out-of-range width before persisting", () => {
    setSidebarWidth(-50);
    expect(cssVar()).toBe(`${SIDEBAR_MIN_PX}px`);
    expect(localStorage.getItem(STORAGE_KEY)).toBe(String(SIDEBAR_MIN_PX));
  });
});
