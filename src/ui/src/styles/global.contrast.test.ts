// WCAG contrast regression for design tokens used as text-on-tint (a
// `color-mix` background rather than a flat surface), where a token that
// reads fine on the plain surface can still fall short once mixed with a
// signal color. Currently covers `--warn-banner-text` (ThrottleBanner.css),
// which replaced `--warn` there because `--warn` on light theme's throttle
// tint measured ~3.9:1, under WCAG AA's 4.5:1 for normal text.
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { SERIES_COLOR_VARS } from "../lib/promSeries";

const here = dirname(fileURLToPath(import.meta.url));
const css = readFileSync(resolve(here, "global.css"), "utf-8");

/** The `{ ... }` body of the first block whose selector matches `selector`. */
function block(selector: RegExp): string {
  const match = selector.exec(css);
  if (!match) {
    throw new Error(`no block matched ${selector} in global.css`);
  }
  const start = match.index + match[0].length;
  let depth = 1;
  let end = start;
  while (depth > 0) {
    if (css[end] === "{") depth++;
    else if (css[end] === "}") depth--;
    end++;
  }
  return css.slice(start, end - 1);
}

/** The hex value of `--name: #rrggbb;` within `cssBlock`. */
function token(cssBlock: string, name: string): [number, number, number] {
  const match = new RegExp(`--${name}:\\s*#([0-9a-fA-F]{6})`).exec(cssBlock);
  if (!match) {
    throw new Error(`--${name} not found in block`);
  }
  const hex = match[1];
  if (!hex) {
    throw new Error(`--${name} matched but captured no hex digits`);
  }
  return [
    Number.parseInt(hex.slice(0, 2), 16),
    Number.parseInt(hex.slice(2, 4), 16),
    Number.parseInt(hex.slice(4, 6), 16),
  ];
}

function mix(
  a: [number, number, number],
  b: [number, number, number],
  weightA: number,
): [number, number, number] {
  const weightB = 1 - weightA;
  return [
    a[0] * weightA + b[0] * weightB,
    a[1] * weightA + b[1] * weightB,
    a[2] * weightA + b[2] * weightB,
  ];
}

function relativeLuminance([r, g, b]: [number, number, number]): number {
  const channel = (v: number) => {
    const c = v / 255;
    return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
}

function contrastRatio(
  fg: [number, number, number],
  bg: [number, number, number],
): number {
  const l1 = relativeLuminance(fg);
  const l2 = relativeLuminance(bg);
  const [hi, lo] = l1 > l2 ? [l1, l2] : [l2, l1];
  return (hi + 0.05) / (lo + 0.05);
}

const themeBlocks = {
  light: () => block(/:root\s*\{/),
  dark: () => block(/@media \(prefers-color-scheme: dark\)\s*\{\s*:root\s*\{/),
} as const;

describe("ThrottleBanner text meets WCAG AA on its tinted background", () => {
  for (const [theme, getBlock] of Object.entries(themeBlocks)) {
    it(`${theme} theme: --warn-banner-text on color-mix(--warn-bar 14%, --surface) is >= 4.5:1`, () => {
      const cssBlock = getBlock();
      const text = token(cssBlock, "warn-banner-text");
      const warnBar = token(cssBlock, "warn-bar");
      const surface = token(cssBlock, "surface");
      const background = mix(warnBar, surface, 0.14);

      const ratio = contrastRatio(text, background);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });
  }
});

describe("--dim meets WCAG AA on both surface tokens", () => {
  for (const [theme, getBlock] of Object.entries(themeBlocks)) {
    for (const surfaceName of ["surface", "surface2"] as const) {
      it(`${theme} theme: --dim on --${surfaceName} is >= 4.5:1`, () => {
        const cssBlock = getBlock();
        const dim = token(cssBlock, "dim");
        const surface = token(cssBlock, surfaceName);

        expect(contrastRatio(dim, surface)).toBeGreaterThanOrEqual(4.5);
      });
    }
  }
});

describe("--on-accent on --accent (solid-background buttons)", () => {
  it("dark theme: --on-accent on --accent is >= 4.5:1", () => {
    const cssBlock = themeBlocks.dark();
    const onAccent = token(cssBlock, "on-accent");
    const accent = token(cssBlock, "accent");

    expect(contrastRatio(onAccent, accent)).toBeGreaterThanOrEqual(4.5);
  });

  it("light theme: --on-accent on --accent is >= 4.5:1", () => {
    const cssBlock = themeBlocks.light();
    const onAccent = token(cssBlock, "on-accent");
    const accent = token(cssBlock, "accent");

    expect(contrastRatio(onAccent, accent)).toBeGreaterThanOrEqual(4.5);
  });
});

describe("--accent as a text color meets WCAG AA on --surface", () => {
  // `--accent` is used as `color` (not just borders/backgrounds) in several
  // places — links, active-tab labels, icon-adjacent text — so it needs the
  // same 4.5:1 normal-text floor as any other text color, not just the
  // solid-button case above.
  for (const [theme, getBlock] of Object.entries(themeBlocks)) {
    it(`${theme} theme: --accent on --surface is >= 4.5:1`, () => {
      const cssBlock = getBlock();
      const accent = token(cssBlock, "accent");
      const surface = token(cssBlock, "surface");

      expect(contrastRatio(accent, surface)).toBeGreaterThanOrEqual(4.5);
    });
  }
});

describe("categorical series colors clear 3:1 against --surface", () => {
  // The 12-color palette `promSeries.ts` cycles through (`--svc-a`…`--svc-l`)
  // — a series swatch/line only needs to read as distinct from its
  // background, not carry text-contrast requirements, so 3:1 (WCAG AA for
  // non-text UI components) rather than 4.5:1.
  const seriesTokens = [
    "svc-a",
    "svc-b",
    "svc-c",
    "svc-d",
    "svc-e",
    "svc-f",
    "svc-g",
    "svc-h",
    "svc-i",
    "svc-j",
    "svc-k",
    "svc-l",
  ] as const;

  for (const [theme, getBlock] of Object.entries(themeBlocks)) {
    for (const name of seriesTokens) {
      it(`${theme} theme: --${name} on --surface is >= 3:1`, () => {
        const cssBlock = getBlock();
        const color = token(cssBlock, name);
        const surface = token(cssBlock, "surface");

        expect(contrastRatio(color, surface)).toBeGreaterThanOrEqual(3);
      });
    }
  }
});

describe("promSeries' SERIES_COLOR_VARS resolve to pairwise-distinct colors", () => {
  // Two series sharing a hue on the very first (solid) pass through the
  // palette read as one series on a chart — `promSeries.ts` only varies dash
  // pattern once the *palette itself* repeats, not within a single pass.
  // `--svc-a` matching `--info` (both series 1 and 2) was exactly that bug.
  const seriesVars = SERIES_COLOR_VARS.map((v) => v.replace(/^--/, ""));

  for (const [theme, getBlock] of Object.entries(themeBlocks)) {
    it(`${theme} theme: every SERIES_COLOR_VARS token is a distinct color`, () => {
      const cssBlock = getBlock();
      const colors = seriesVars.map((name) => token(cssBlock, name).join(","));

      expect(new Set(colors).size).toBe(colors.length);
    });
  }
});

describe("data-theme scoping applies to nested elements, not just :root", () => {
  it("a non-root [data-theme=dark] selector exists and sets --bg/--text tokens", () => {
    const match =
      /\[data-theme="dark"\][^{]*\{[^}]*--bg:\s*#0d1014/.exec(css) !== null;
    expect(match).toBe(true);
  });

  it("a non-root [data-theme] selector paints its own background/color", () => {
    const match =
      /\[data-theme="dark"\]:not\(:root\)[^{]*\{[^}]*background:\s*var\(--bg\);\s*color:\s*var\(--text\)/.exec(
        css,
      );
    expect(match).not.toBeNull();
  });
});

describe("--ok-text meets WCAG AA on --surface", () => {
  for (const [theme, getBlock] of Object.entries(themeBlocks)) {
    it(`${theme} theme: --ok-text on --surface is >= 4.5:1`, () => {
      const cssBlock = getBlock();
      const okText = token(cssBlock, "ok-text");
      const surface = token(cssBlock, "surface");

      expect(contrastRatio(okText, surface)).toBeGreaterThanOrEqual(4.5);
    });
  }
});
