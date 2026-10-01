import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  initTheme,
  subscribeTheme,
  syncThemeMeta,
  THEME_COLOR,
  toggleTheme,
} from "./theme";

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

  describe("theme metas", () => {
    let metas: HTMLMetaElement[] = [];

    function meta(attrs: Record<string, string>): HTMLMetaElement {
      const el = document.createElement("meta");
      for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
      document.head.appendChild(el);
      metas.push(el);
      return el;
    }

    beforeEach(() => {
      metas = [];
    });

    afterEach(() => {
      metas.forEach((el) => el.remove());
    });

    function setup() {
      return {
        scheme: meta({ name: "color-scheme", content: "light dark" }),
        lightColor: meta({
          name: "theme-color",
          content: THEME_COLOR.light,
          media: "(prefers-color-scheme: light)",
        }),
        darkColor: meta({ name: "theme-color", content: THEME_COLOR.dark }),
      };
    }

    it("toggleTheme points both metas at the forced theme", () => {
      const { scheme, lightColor, darkColor } = setup();
      document.documentElement.setAttribute("data-theme", "light");
      toggleTheme();
      expect(scheme.content).toBe("dark");
      expect(lightColor.content).toBe(THEME_COLOR.dark);
      expect(darkColor.content).toBe(THEME_COLOR.dark);
    });

    it("initTheme applies a saved theme to the metas", () => {
      const { scheme, darkColor } = setup();
      localStorage.setItem("signaldb-theme", "light");
      initTheme();
      expect(scheme.content).toBe("light");
      expect(darkColor.content).toBe(THEME_COLOR.light);
    });

    it("syncThemeMeta(null) restores the OS-following defaults", () => {
      const { scheme, lightColor, darkColor } = setup();
      syncThemeMeta("dark");
      syncThemeMeta(null);
      expect(scheme.content).toBe("light dark");
      expect(lightColor.content).toBe(THEME_COLOR.light);
      expect(darkColor.content).toBe(THEME_COLOR.dark);
    });

    it("index.html's pre-paint script uses the same key and colours", () => {
      const html = readFileSync(
        resolve(dirname(fileURLToPath(import.meta.url)), "../../index.html"),
        "utf-8",
      );
      expect(html).toContain('localStorage.getItem("signaldb-theme")');
      expect(html).toContain(
        `{ light: "${THEME_COLOR.light}", dark: "${THEME_COLOR.dark}" }`,
      );
    });
  });

  describe("subscribeTheme", () => {
    it("notifies when data-theme changes", async () => {
      const cb = vi.fn();
      const unsubscribe = subscribeTheme(cb);
      document.documentElement.setAttribute("data-theme", "dark");
      await vi.waitFor(() => expect(cb).toHaveBeenCalled());
      unsubscribe();
    });

    it("stops notifying once unsubscribed", async () => {
      const cb = vi.fn();
      const unsubscribe = subscribeTheme(cb);
      unsubscribe();
      document.documentElement.setAttribute("data-theme", "dark");
      // Give a MutationObserver microtask a chance to fire, if it were
      // (wrongly) still connected.
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(cb).not.toHaveBeenCalled();
    });
  });
});
