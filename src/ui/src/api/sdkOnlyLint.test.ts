import { ESLint } from "eslint";
import { describe, expect, it } from "vitest";

const RULE = "no-restricted-syntax";
// Loads the whole config (typescript-eslint, storybook); shared across cases.
const eslint = new ESLint({ cwd: process.cwd() });

async function fetchViolations(code: string, filePath: string) {
  const [result] = await eslint.lintText(code, { filePath });
  return (result?.messages ?? []).filter((m) => m.ruleId === RULE);
}

describe("SDK-only HTTP lint rule", { timeout: 30_000 }, () => {
  it.each([
    'fetch("/api/v1/query");',
    'window.fetch("/api/v1/query");',
    'globalThis.fetch("/api/v1/query");',
  ])("flags %s in application code", async (code) => {
    const violations = await fetchViolations(code, "src/feature/scratch.ts");
    expect(violations).toHaveLength(1);
    expect(violations[0]?.message).toMatch(/generated client/);
  });

  it("leaves unrelated .fetch() methods alone", async () => {
    const violations = await fetchViolations(
      "const cache = { fetch: (k: string) => k };\ncache.fetch('x');\n",
      "src/feature/scratch.ts",
    );
    expect(violations).toHaveLength(0);
  });
});
