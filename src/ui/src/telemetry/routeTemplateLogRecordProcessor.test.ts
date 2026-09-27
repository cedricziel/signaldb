import { afterEach, describe, expect, it } from "vitest";
import type { SdkLogRecord } from "@opentelemetry/sdk-logs";
import {
  RouteTemplateLogRecordProcessor,
  setRouteTemplate,
} from "./routeTemplateLogRecordProcessor";

afterEach(() => {
  setRouteTemplate(undefined);
});

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

describe("RouteTemplateLogRecordProcessor", () => {
  it("stamps url.template once one has been set", () => {
    setRouteTemplate("/traces/:traceId");
    const { logRecord, attrs } = fakeLogRecord();
    new RouteTemplateLogRecordProcessor().onEmit(logRecord);
    expect(attrs["url.template"]).toBe("/traces/:traceId");
  });

  it("omits url.template before the router has set one", () => {
    const { logRecord, attrs } = fakeLogRecord();
    new RouteTemplateLogRecordProcessor().onEmit(logRecord);
    expect(attrs).not.toHaveProperty("url.template");
  });

  it("picks up a later route change on the next emit", () => {
    setRouteTemplate("/logs");
    const first = fakeLogRecord();
    new RouteTemplateLogRecordProcessor().onEmit(first.logRecord);
    expect(first.attrs["url.template"]).toBe("/logs");

    setRouteTemplate("/traces/:traceId");
    const second = fakeLogRecord();
    new RouteTemplateLogRecordProcessor().onEmit(second.logRecord);
    expect(second.attrs["url.template"]).toBe("/traces/:traceId");
  });
});
