// Regression guard for specs/explore-ui-query-surface/spec.md, "A guard
// stops regressions": everything first-party goes through the Query IR
// (`POST /api/v1/query`) now, never the Tempo/Loki/Prometheus/Pyroscope
// compat endpoints those exist for external clients (Grafana). This scans
// every hand-written `src/ui/src` file for a compat path literal or an
// import of a generated compat SDK function, and fails the build if a
// regression reintroduces one.
import { readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const SRC_ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

const EXCLUDED_PATHS = [
  join(SRC_ROOT, "api", "gen"),
  join(SRC_ROOT, "lib", "proxiedPaths.ts"),
  join(SRC_ROOT, "test", "connectionInfo.ts"),
];

const COMPAT_PATH_LITERAL = /\/(loki|prometheus|tempo|pyroscope)\//;

/** Compat SDK function names, read from the generated client itself so this
 * guard tracks a regenerated `sdk.gen.ts` without a manual list to maintain.
 * `search` (the Tempo trace-search operation) is deliberately excluded — the
 * bare word is too common in hand-written prose/regex to use as a signal;
 * its compat-ness is covered by the path-literal check on `/tempo/` and by
 * `search_tags` and friends. */
function compatSdkFunctionNames(): string[] {
  const sdk = readFileSync(join(SRC_ROOT, "api", "gen", "sdk.gen.ts"), "utf8");
  const names: string[] = [];
  for (const m of sdk.matchAll(/^export const ([a-zA-Z0-9_]+) =/gm)) {
    const name = m[1]!;
    if (/^(logql|promql|pyroscope)/i.test(name) || /^searchTag/.test(name)) {
      names.push(name);
    }
  }
  return names;
}

function isExcluded(path: string): boolean {
  return EXCLUDED_PATHS.some(
    (excluded) => path === excluded || path.startsWith(excluded + "/"),
  );
}

function walk(dir: string, out: string[]): string[] {
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (isExcluded(path)) continue;
    const st = statSync(path);
    if (st.isDirectory()) {
      walk(path, out);
    } else if (/\.(ts|tsx)$/.test(entry) && !/\.test\.tsx?$/.test(entry)) {
      // (excludes both `.test.ts` and `.test.tsx`)
      out.push(path);
    }
  }
  return out;
}

describe("compat-endpoint guard", () => {
  const files = walk(SRC_ROOT, []);
  const compatNames = compatSdkFunctionNames();
  const importRegex = new RegExp(
    `\\b(${compatNames.map((n) => n.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")})\\b`,
  );

  it("found the generated compat functions to guard against", () => {
    // A regenerated client that renamed/removed every compat operation would
    // otherwise make this guard silently vacuous.
    expect(compatNames.length).toBeGreaterThan(0);
  });

  it("scanned at least the known IR-only modules", () => {
    expect(files.length).toBeGreaterThan(50);
  });

  for (const file of files) {
    const rel = relative(SRC_ROOT, file);
    it(`${rel} does not reference a Loki/Tempo/Prometheus/Pyroscope compat path or SDK function`, () => {
      const content = readFileSync(file, "utf8");
      expect(content).not.toMatch(COMPAT_PATH_LITERAL);
      if (compatNames.length > 0) {
        expect(content).not.toMatch(importRegex);
      }
    });
  }
});
