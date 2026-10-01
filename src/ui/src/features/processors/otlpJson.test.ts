import { describe, expect, it } from "vitest";
import { diffLines } from "./lineDiff";
import { canonicalOtlpJson } from "./otlpJson";
import { SAMPLE_PAYLOADS } from "./samples";
import { REDACTED_LOGS_ECHO } from "./serverEcho.fixture";

describe("canonicalOtlpJson", () => {
  it("leaves only the rewritten attribute in a diff against the server's echo", () => {
    const changed = diffLines(
      canonicalOtlpJson(SAMPLE_PAYLOADS.logs),
      canonicalOtlpJson(REDACTED_LOGS_ECHO),
    ).filter((line) => line.kind !== "same");

    expect(changed).toEqual([
      { kind: "removed", text: expect.stringContaining("alice@example.com") },
      { kind: "added", text: expect.stringContaining("[redacted]") },
    ]);
  });

  it("keeps a string attribute whose value is \"0\"", () => {
    expect(
      canonicalOtlpJson({ key: "retries", value: { stringValue: "0" } }),
    ).toContain('"stringValue": "0"');
  });
});
