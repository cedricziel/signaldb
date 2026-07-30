import { afterEach, describe, expect, it } from "vitest";
import type { Span } from "@opentelemetry/sdk-trace-base";
import { setTenantContext } from "../api/http";
import { SessionSpanProcessor } from "./sessionSpanProcessor";
import type { SessionManager } from "./session";

afterEach(() => {
  setTenantContext({ tenant: "", dataset: "" });
});

const fixedSession: SessionManager = {
  getId: () => "session-xyz",
  current: () => ({ id: "session-xyz", startedAt: 0, expiresAt: 0 }),
};

/** Minimal Span stand-in that records the attributes set on it. */
function fakeSpan(): { span: Span; attrs: Record<string, unknown> } {
  const attrs: Record<string, unknown> = {};
  const span = {
    setAttribute(key: string, value: unknown) {
      attrs[key] = value;
      return this;
    },
  } as unknown as Span;
  return { span, attrs };
}

describe("SessionSpanProcessor", () => {
  it("stamps session.id on every span", () => {
    const { span, attrs } = fakeSpan();
    new SessionSpanProcessor(fixedSession).onStart(span);
    expect(attrs["session.id"]).toBe("session-xyz");
  });

  it("stamps tenant and dataset when a context is set", () => {
    setTenantContext({ tenant: "acme", dataset: "prod" });
    const { span, attrs } = fakeSpan();
    new SessionSpanProcessor(fixedSession).onStart(span);
    expect(attrs["tenant.id"]).toBe("acme");
    expect(attrs["dataset.id"]).toBe("prod");
  });

  it("omits tenant and dataset for the ambient default context", () => {
    const { span, attrs } = fakeSpan();
    new SessionSpanProcessor(fixedSession).onStart(span);
    expect(attrs).not.toHaveProperty("tenant.id");
    expect(attrs).not.toHaveProperty("dataset.id");
  });
});
