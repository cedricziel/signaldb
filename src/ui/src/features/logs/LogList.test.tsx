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
  metadata: {},
  ...over,
});

describe("traceIdOf", () => {
  it("prefers structured metadata over any label", () => {
    expect(traceIdOf(row({ metadata: { trace_id: "meta-id" } }))).toBe(
      "meta-id",
    );
    expect(
      traceIdOf(
        row({
          metadata: { trace_id: "meta-id" },
          labels: { trace_id: "label-id" },
        }),
      ),
    ).toBe("meta-id");
  });

  it("falls back to common label spellings when metadata is absent", () => {
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

  it("sorts expanded attributes alphabetically within each scope", async () => {
    const { container } = render(
      <LogList
        rows={[
          row({
            labels: { zebra: "last", alpha: "first" },
            metadata: { omega: "last", beta: "first" },
          }),
        ]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
      />,
    );

    await userEvent.click(screen.getByText("hello"));

    expect(
      [...container.querySelectorAll(".attr-row[data-scope='label'] dt")].map(
        (element) => element.textContent,
      ),
    ).toEqual(["alpha", "zebra"]);
    expect(
      [...container.querySelectorAll(".attr-row[data-scope='metadata'] dt")].map(
        (element) => element.textContent,
      ),
    ).toEqual(["beta", "omega"]);
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

describe("LogList structured metadata", () => {
  const metaRow = row({
    tsNs: "4000000000",
    tsMs: 4000,
    line: "checkout timed out",
    labels: { level: "error", service_name: "checkout" },
    metadata: { trace_id: "abc123", span_id: "def456" },
  });

  it("shows per-line metadata alongside stream labels", async () => {
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    expect(screen.getByText("span_id")).toBeInTheDocument();
    expect(screen.getByText("def456")).toBeInTheDocument();
    expect(screen.getByText("abc123")).toBeInTheDocument();
  });

  it("marks metadata as per-line so it is not mistaken for a stream label", async () => {
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    const spanRow = screen.getByText("span_id").closest(".attr-row");
    expect(spanRow).toHaveAttribute("data-scope", "metadata");
    expect(
      screen.getByText("service_name").closest(".attr-row"),
    ).toHaveAttribute("data-scope", "label");
  });

  // Structured metadata varies per line, so a stream selector cannot match it.
  it("offers no stream-selector filter actions for metadata", async () => {
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    expect(
      screen.queryByRole("button", { name: "Filter for span_id = def456" }),
    ).toBeNull();
    expect(
      screen.getByRole("button", {
        name: "Filter for service_name = checkout",
      }),
    ).toBeInTheDocument();
  });

  it("copies individual label and metadata values", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
      />,
    );

    await userEvent.click(screen.getByText("checkout timed out"));
    await userEvent.click(
      screen.getByRole("button", { name: "Copy value for service_name" }),
    );
    expect(
      screen.getByRole("button", { name: "Copied value for service_name" }),
    ).toBeInTheDocument();
    await userEvent.click(
      screen.getByRole("button", { name: "Copy value for span_id" }),
    );

    expect(writeText).toHaveBeenNthCalledWith(1, "checkout");
    expect(writeText).toHaveBeenNthCalledWith(2, "def456");
  });

  it("copies metadata as well as labels", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    await userEvent.click(screen.getByRole("button", { name: "Copy JSON" }));
    const copied = JSON.parse(writeText.mock.calls[0]![0] as string);
    expect(copied.span_id).toBe("def456");
    expect(copied.service_name).toBe("checkout");
    vi.unstubAllGlobals();
  });
});
