import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

afterEach(() => {
  cleanup();
});

// The app formats in the viewer's own locale — `Intl.NumberFormat()`,
// `Intl.DateTimeFormat()`, and `toLocaleString()` with no explicit locale —
// which is right for users and non-deterministic for tests: `2018` renders
// "2,018" under en-US and "2.018" under de-DE, and dates differ likewise, so
// assertions pass or fail according to the developer's shell rather than the
// code. Pin the default for the test process only. Tests that pass an
// explicit locale are untouched, and production behavior is unchanged: this
// shim exists nowhere but here.
//
// Pinning the process locale instead (`LC_ALL`) would cover every
// locale-sensitive built-in at once and is what CI effectively does, but it
// has to be set before the process starts — there is no portable way to do
// that from an npm script, and Node has no `--icu-default-locale` flag to
// reach for. So the surfaces are patched individually; anything added here
// must cover both the `Intl` constructor and the matching `toLocale*`
// prototype methods, since code in this repo uses both.
const TEST_LOCALE = "en-US";

function pinLocale<T extends object>(target: T): T {
  return new Proxy(target, {
    apply: (fn, _this, args: unknown[]) =>
      (fn as (...a: unknown[]) => unknown)(args[0] ?? TEST_LOCALE, args[1]),
    construct: (fn, args: unknown[]) =>
      new (fn as new (...a: unknown[]) => object)(
        args[0] ?? TEST_LOCALE,
        args[1],
      ),
  });
}

Intl.NumberFormat = pinLocale(Intl.NumberFormat);
Intl.DateTimeFormat = pinLocale(Intl.DateTimeFormat);

const realNumberToLocaleString = Number.prototype.toLocaleString;
Number.prototype.toLocaleString = function (
  locales?: Intl.LocalesArgument,
  options?: Intl.NumberFormatOptions,
) {
  return realNumberToLocaleString.call(this, locales ?? TEST_LOCALE, options);
};

// `Date.prototype.toLocaleString` is a different method from the Number one
// and is not covered by the Intl patches above — `RegistryList.tsx` calls it,
// so leaving it out made the "deterministic suite" claim false.
for (const method of [
  "toLocaleString",
  "toLocaleDateString",
  "toLocaleTimeString",
] as const) {
  const real = Date.prototype[method];
  Date.prototype[method] = function (
    locales?: Intl.LocalesArgument,
    options?: Intl.DateTimeFormatOptions,
  ) {
    return real.call(this, locales ?? TEST_LOCALE, options);
  };
}

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
