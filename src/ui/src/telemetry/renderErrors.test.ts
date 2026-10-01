import { logs } from "@opentelemetry/api-logs";
import {
  ATTR_EXCEPTION_MESSAGE,
  ATTR_EXCEPTION_STACKTRACE,
  ATTR_EXCEPTION_TYPE,
} from "@opentelemetry/semantic-conventions";
import {
  InMemoryLogRecordExporter,
  LoggerProvider,
  type ReadableLogRecord,
  SimpleLogRecordProcessor,
} from "@opentelemetry/sdk-logs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { recordRenderError } from "./renderErrors";

/** `SimpleLogRecordProcessor` exports asynchronously (`void doExport()`), so
 * a just-emitted record isn't in the exporter synchronously — poll until
 * exactly one lands rather than asserting immediately after
 * `recordRenderError`. */
async function waitForOneRecord(
  exporter: InMemoryLogRecordExporter,
): Promise<ReadableLogRecord> {
  return vi.waitFor(() => {
    const records = exporter.getFinishedLogRecords();
    if (records.length === 0) throw new Error("no log record exported yet");
    expect(records).toHaveLength(1);
    return records[0]!;
  });
}

describe("recordRenderError", () => {
  let exporter: InMemoryLogRecordExporter;

  beforeEach(() => {
    exporter = new InMemoryLogRecordExporter();
    const provider = new LoggerProvider({
      processors: [new SimpleLogRecordProcessor({ exporter })],
    });
    logs.setGlobalLoggerProvider(provider);
  });

  afterEach(() => {
    logs.disable();
  });

  it("emits one exception log record with type, message, stacktrace and pathname", async () => {
    const error = new TypeError("boom");
    recordRenderError(error, "/traces/abc123");

    const record = await waitForOneRecord(exporter);
    expect(record.attributes[ATTR_EXCEPTION_TYPE]).toBe("TypeError");
    expect(record.attributes[ATTR_EXCEPTION_MESSAGE]).toBe("boom");
    expect(record.attributes[ATTR_EXCEPTION_STACKTRACE]).toBe(error.stack);
    expect(record.attributes["url.full"]).toBe(
      "http://localhost:3000/traces/abc123",
    );
  });

  it("coerces a non-Error throw into an Error", async () => {
    recordRenderError("plain string throw", "/logs");

    const record = await waitForOneRecord(exporter);
    expect(record.attributes[ATTR_EXCEPTION_MESSAGE]).toBe(
      "plain string throw",
    );
  });

  it("redacts sensitive query params from the pathname like navigation URLs", async () => {
    recordRenderError(new Error("x"), "/logs?api_key=secret");

    const record = await waitForOneRecord(exporter);
    expect(record.attributes["url.full"]).toContain("api_key=REDACTED");
    expect(record.attributes["url.full"]).not.toContain("secret");
  });

  it("never throws even when the logger provider is misbehaving", () => {
    logs.setGlobalLoggerProvider({
      getLogger: () => {
        throw new Error("provider exploded");
      },
    } as unknown as Parameters<typeof logs.setGlobalLoggerProvider>[0]);

    expect(() => recordRenderError(new Error("x"), "/logs")).not.toThrow();
  });
});
