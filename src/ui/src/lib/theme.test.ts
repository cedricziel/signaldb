import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { initTheme, toggleTheme } from "./theme";

describe("theme", () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute("data-theme");
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute("data-theme");
  });

  describe("initTheme", () => {
    it("applies a saved dark theme to <html>", () => {
      localStorage.setItem("signaldb-theme", "dark");
      initTheme();
      expect(document.documentElement.getAttribute("data-theme")).toBe("dark");
    });

    it("applies a saved light theme to <html>", () => {
      localStorage.setItem("signaldb-theme", "light");
      initTheme();
      expect(document.documentElement.getAttribute("data-theme")).toBe("light");
    });

    it("leaves data-theme unset when nothing was saved", () => {
      initTheme();
      expect(document.documentElement.hasAttribute("data-theme")).toBe(false);
    });

    it("ignores an invalid stored value", () => {
      localStorage.setItem("signaldb-theme", "solarized");
      initTheme();
      expect(document.documentElement.hasAttribute("data-theme")).toBe(false);
    });
  });

  describe("toggleTheme", () => {
    it("switches from light to dark and persists it", () => {
      document.documentElement.setAttribute("data-theme", "light");
      toggleTheme();
      expect(document.documentElement.getAttribute("data-theme")).toBe("dark");
      expect(localStorage.getItem("signaldb-theme")).toBe("dark");
    });

    it("switches from dark to light and persists it", () => {
      document.documentElement.setAttribute("data-theme", "dark");
      toggleTheme();
      expect(document.documentElement.getAttribute("data-theme")).toBe("light");
      expect(localStorage.getItem("signaldb-theme")).toBe("light");
    });
  });
});
