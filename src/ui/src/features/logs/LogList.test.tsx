import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { LogRow } from "../../api/loki";
import { LogList, traceIdOf } from "./LogList";

const row = (over: Partial<LogRow>): LogRow => ({
  tsNs: "1000000000",
  tsMs: 1000,
  line: "hello",
  labels: {},
  ...over,
});

describe("traceIdOf", () => {
  it("finds trace ids under common label spellings", () => {
    expect(traceIdOf(row({ labels: { trace_id: "abc" } }))).toBe("abc");
    expect(traceIdOf(row({ labels: { traceID: "def" } }))).toBe("def");
    expect(traceIdOf(row({ labels: {} }))).toBeNull();
  });
});

describe("LogList", () => {
  const rows: LogRow[] = [
    row({
      tsNs: "3000000000",
      tsMs: 3000,
      line: "payment failed",
      labels: {
        level: "error",
        service_name: "payments",
        trace_id: "cafe1234beef",
      },
    }),
    row({
      tsNs: "2000000000",
      tsMs: 2000,
      line: "request handled",
      labels: { level: "info", service_name: "gateway" },
    }),
  ];

  it("renders virtualized rows with level and service", () => {
    render(
      <LogList rows={rows} onAddFilter={() => {}} onOpenTrace={() => {}} />,
    );
    expect(screen.getByText("payment failed")).toBeInTheDocument();
    expect(screen.getByText("request handled")).toBeInTheDocument();
    expect(screen.getByText("ERROR")).toBeInTheDocument();
    expect(screen.getByText("gateway")).toBeInTheDocument();
  });

  it("expands a row to show attributes and filter actions", async () => {
    const onAddFilter = vi.fn();
    render(
      <LogList rows={rows} onAddFilter={onAddFilter} onOpenTrace={() => {}} />,
    );
    await userEvent.click(screen.getByText("payment failed"));
    expect(screen.getByText("trace_id")).toBeInTheDocument();
    await userEvent.click(
      screen.getByRole("button", {
        name: "Filter for service_name = payments",
      }),
    );
    expect(onAddFilter).toHaveBeenCalledWith({
      label: "service_name",
      op: "=",
      value: "payments",
    });
  });

  it("supports exclude filters from the detail view", async () => {
    const onAddFilter = vi.fn();
    render(
      <LogList rows={rows} onAddFilter={onAddFilter} onOpenTrace={() => {}} />,
    );
    await userEvent.click(screen.getByText("payment failed"));
    await userEvent.click(
      screen.getByRole("button", { name: "Filter out level = error" }),
    );
    expect(onAddFilter).toHaveBeenCalledWith({
      label: "level",
      op: "!=",
      value: "error",
    });
  });

  it("pivots to the trace from a row with a trace id", async () => {
    const onOpenTrace = vi.fn();
    render(
      <LogList rows={rows} onAddFilter={() => {}} onOpenTrace={onOpenTrace} />,
    );
    await userEvent.click(screen.getByText("payment failed"));
    await userEvent.click(
      screen.getByRole("button", { name: /View trace cafe1234/ }),
    );
    expect(onOpenTrace).toHaveBeenCalledWith("cafe1234beef");
  });

  it("shows an empty state", () => {
    render(<LogList rows={[]} onAddFilter={() => {}} onOpenTrace={() => {}} />);
    expect(screen.getByText(/No log lines/)).toBeInTheDocument();
  });
});
