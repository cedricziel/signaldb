// Exercises the acceptance path end-to-end: a route that throws during
// render shows the `RouteErrorBoundary` fallback and records exactly one
// exception log record. Recording happens only in the boundary's effect
// (see renderErrors.ts / main.tsx) — `onCaughtError` is deliberately not
// wired in main.tsx, since React 19 fires it for every boundary that
// catches, including this one, and double-wiring would double-count. This
// test only exercises the `errorElement` path (the boundary itself), not
// `main.tsx`'s `onUncaughtError` — that path only fires for a render error
// with no boundary above it, which cannot happen through this route tree.
import { logs } from "@opentelemetry/api-logs";
import { ATTR_EXCEPTION_MESSAGE } from "@opentelemetry/semantic-conventions";
import {
  InMemoryLogRecordExporter,
  LoggerProvider,
  SimpleLogRecordProcessor,
} from "@opentelemetry/sdk-logs";
import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { renderWithRouter } from "../../test/render";
import { RouteErrorBoundary } from "./RouteErrorBoundary";

function Bomb(): never {
  throw new Error("render blew up");
}

describe("RouteErrorBoundary", () => {
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

  it("shows the fallback and records exactly one exception log", async () => {
    renderWithRouter(
      [
        {
          path: "/boom",
          element: <Bomb />,
          errorElement: <RouteErrorBoundary />,
        },
      ],
      ["/boom"],
    );

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Something went wrong",
    );
    expect(screen.getByRole("button", { name: "Reload" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Go home" })).toBeInTheDocument();

    const record = await waitFor(() => {
      const records = exporter.getFinishedLogRecords();
      expect(records).toHaveLength(1);
      return records[0]!;
    });
    expect(record.attributes[ATTR_EXCEPTION_MESSAGE]).toBe("render blew up");
    expect(record.attributes["url.full"]).toBe("http://localhost:3000/boom");
  });
});
