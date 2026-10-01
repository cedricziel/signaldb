import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createPanelWidth,
  sidebarWidth,
  spanDetailWidth,
} from "./sidebarWidth";

const CSS_VAR = "--test-panel-w";
const panel = createPanelWidth({
  storageKey: "signaldb.test.panelWidth",
  cssVar: CSS_VAR,
  min: 200,
  max: 480,
  defaultPx: 248,
  grows: "right",
  resizerClassName: "test-resizer",
  resizerLabel: "Resize test panel",
});

const cssVar = () => document.documentElement.style.getPropertyValue(CSS_VAR);

beforeEach(() => {
  localStorage.clear();
  document.documentElement.style.removeProperty(CSS_VAR);
});

afterEach(() => {
  localStorage.clear();
  document.documentElement.style.removeProperty(CSS_VAR);
});

describe("createPanelWidth", () => {
  it("clamps to the configured range", () => {
    expect(panel.clamp(300)).toBe(300);
    expect(panel.clamp(1)).toBe(200);
    expect(panel.clamp(10_000)).toBe(480);
  });

  it("reads the default when nothing was saved", () => {
    expect(panel.read()).toBe(248);
  });

  it("reads a saved width, clamped", () => {
    localStorage.setItem("signaldb.test.panelWidth", "9999");
    expect(panel.read()).toBe(480);
  });

  it("init applies the saved width to <html>", () => {
    localStorage.setItem("signaldb.test.panelWidth", "300");
    panel.init();
    expect(cssVar()).toBe("300px");
  });

  it("apply sets the custom property without persisting", () => {
    expect(panel.apply(320)).toBe(320);
    expect(cssVar()).toBe("320px");
    expect(localStorage.getItem("signaldb.test.panelWidth")).toBeNull();
  });

  it("set clamps, applies, and persists", () => {
    expect(panel.set(9999)).toBe(480);
    expect(cssVar()).toBe("480px");
    expect(localStorage.getItem("signaldb.test.panelWidth")).toBe("480");
  });

  it("dragStart uses the drawn width when the CSS caps the saved one", () => {
    localStorage.setItem("signaldb.test.panelWidth", "400");
    expect(panel.dragStart(252.6)).toBe(253);
  });

  it("dragStart keeps the saved width when the pane is drawn at it", () => {
    localStorage.setItem("signaldb.test.panelWidth", "300");
    expect(panel.dragStart(300)).toBe(300);
    expect(panel.dragStart(500)).toBe(300);
  });

  it("dragStart falls back to the saved width with nothing measured", () => {
    localStorage.setItem("signaldb.test.panelWidth", "300");
    expect(panel.dragStart()).toBe(300);
    expect(panel.dragStart(0)).toBe(300);
  });
});

describe("panel instances", () => {
  it("keep the keys and directions their handles rely on", () => {
    expect(sidebarWidth.grows).toBe("right");
    expect(spanDetailWidth.grows).toBe("left");
    sidebarWidth.set(260);
    expect(localStorage.getItem("signaldb.explore.sidebarWidth")).toBe("260");
    spanDetailWidth.set(400);
    expect(localStorage.getItem("signaldb.trace.sidebarWidth")).toBe("400");
    document.documentElement.style.removeProperty("--sidebar-w");
    document.documentElement.style.removeProperty("--span-detail-w");
  });
});
