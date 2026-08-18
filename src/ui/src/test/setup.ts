import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

afterEach(() => {
  cleanup();
});

// The app formats numbers in the viewer's own locale — `Intl.NumberFormat()`
// and `toLocaleString()` with no explicit locale — which is right for users
// and non-deterministic for tests: `2018` renders "2,018" under en-US and
// "2.018" under de-DE, so assertions pass or fail according to the developer's
// shell rather than the code. Pin the default for the test process only. Tests
// that pass an explicit locale are untouched, and production behavior is
// unchanged: this shim exists nowhere but here.
const TEST_LOCALE = "en-US";

const RealNumberFormat = Intl.NumberFormat;
Intl.NumberFormat = new Proxy(RealNumberFormat, {
  apply: (target, _this, args: Parameters<typeof Intl.NumberFormat>) =>
    target(args[0] ?? TEST_LOCALE, args[1]),
  construct: (target, args: Parameters<typeof Intl.NumberFormat>) =>
    new target(args[0] ?? TEST_LOCALE, args[1]),
});

const realNumberToLocaleString = Number.prototype.toLocaleString;
Number.prototype.toLocaleString = function (
  locales?: Intl.LocalesArgument,
  options?: Intl.NumberFormatOptions,
) {
  return realNumberToLocaleString.call(this, locales ?? TEST_LOCALE, options);
};

// jsdom reports zero layout sizes, which makes @tanstack/react-virtual render
// an empty window. Give scroll containers a plausible size so virtualized
// lists materialize rows in component tests.
Object.defineProperty(HTMLElement.prototype, "clientHeight", {
  configurable: true,
  get() {
    return 800;
  },
});
Object.defineProperty(HTMLElement.prototype, "clientWidth", {
  configurable: true,
  get() {
    return 1200;
  },
});
Element.prototype.getBoundingClientRect = function () {
  return {
    width: 1200,
    height: 800,
    top: 0,
    left: 0,
    bottom: 800,
    right: 1200,
    x: 0,
    y: 0,
    toJSON: () => ({}),
  } as DOMRect;
};

// jsdom has no ResizeObserver; @tanstack/react-virtual needs one to learn the
// scroll container's size. Report the stubbed rect once on observe.
class ResizeObserverStub implements ResizeObserver {
  private readonly cb: ResizeObserverCallback;

  constructor(cb: ResizeObserverCallback) {
    this.cb = cb;
  }

  observe(target: Element) {
    const rect = target.getBoundingClientRect();
    this.cb(
      [
        {
          target,
          contentRect: rect,
          borderBoxSize: [{ inlineSize: rect.width, blockSize: rect.height }],
          contentBoxSize: [{ inlineSize: rect.width, blockSize: rect.height }],
          devicePixelContentBoxSize: [
            { inlineSize: rect.width, blockSize: rect.height },
          ],
        } as ResizeObserverEntry,
      ],
      this,
    );
  }

  unobserve() {}

  disconnect() {}
}

globalThis.ResizeObserver = globalThis.ResizeObserver ?? ResizeObserverStub;

// jsdom has no matchMedia; uPlot queries it at module load for pixel-ratio
// tracking.
if (typeof window.matchMedia !== "function") {
  window.matchMedia = (query: string) =>
    ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }) as MediaQueryList;
}
