import { afterEach, describe, expect, it } from "vitest";
import type { SdkLogRecord } from "@opentelemetry/sdk-logs";
import { setTenantContext } from "../api/http";
import { SessionLogRecordProcessor } from "./sessionLogRecordProcessor";
import type { SessionManager } from "./session";

afterEach(() => {
  setTenantContext({ tenant: "", dataset: "" });
});

const fixedSession: SessionManager = {
  getId: () => "session-xyz",
  current: () => ({ id: "session-xyz", startedAt: 0, expiresAt: 0 }),
};

/** Minimal SdkLogRecord stand-in that records the attributes set on it. */
function fakeLogRecord(): {
  logRecord: SdkLogRecord;
  attrs: Record<string, unknown>;
} {
  const attrs: Record<string, unknown> = {};
  const logRecord = {
    setAttribute(key: string, value: unknown) {
      attrs[key] = value;
      return this;
    },
  } as unknown as SdkLogRecord;
  return { logRecord, attrs };
}

describe("SessionLogRecordProcessor", () => {
  it("stamps session.id on every log record", () => {
    const { logRecord, attrs } = fakeLogRecord();
    new SessionLogRecordProcessor(fixedSession).onEmit(logRecord);
    expect(attrs["session.id"]).toBe("session-xyz");
  });

  it("stamps tenant and dataset when a context is set", () => {
    setTenantContext({ tenant: "acme", dataset: "prod" });
    const { logRecord, attrs } = fakeLogRecord();
    new SessionLogRecordProcessor(fixedSession).onEmit(logRecord);
    expect(attrs["tenant.id"]).toBe("acme");
    expect(attrs["dataset.id"]).toBe("prod");
  });

  it("omits tenant and dataset for the ambient default context", () => {
    const { logRecord, attrs } = fakeLogRecord();
    new SessionLogRecordProcessor(fixedSession).onEmit(logRecord);
    expect(attrs).not.toHaveProperty("tenant.id");
    expect(attrs).not.toHaveProperty("dataset.id");
  });

  it("never throws from forceFlush/shutdown", async () => {
    const processor = new SessionLogRecordProcessor(fixedSession);
    await expect(processor.forceFlush()).resolves.toBeUndefined();
    await expect(processor.shutdown()).resolves.toBeUndefined();
  });
});
