import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { LogRow } from "../../api/ir/logs";
import { resetSemanticsCache } from "../../hooks/useSemantics";
import { stubFetchRoutes } from "../../test/render";
import { LogList, rowKey } from "./LogList";

afterEach(() => {
  resetSemanticsCache();
  vi.unstubAllGlobals();
  localStorage.clear();
});

/** The resource section is collapsed behind a summary by default; every
 * test that reaches into it clicks the section head first. */
const expandResource = () =>
  userEvent.click(screen.getByRole("button", { name: /^Resource/ }));

const row = (over: Partial<LogRow>): LogRow => ({
  tsNs: "1000000000",
  tsMs: 1000,
  body: "hello",
  serviceName: "",
  severityText: "",
  traceId: null,
  spanId: null,
  scopeName: "",
  logAttributes: {},
  scopeAttributes: {},
  resourceAttributes: {},
  ...over,
});

describe("LogList", () => {
  const rows: LogRow[] = [
    row({
      tsNs: "3000000000",
      tsMs: 3000,
      body: "payment failed",
      severityText: "error",
      serviceName: "payments",
      traceId: "cafe1234beef",
    }),
    row({
      tsNs: "2000000000",
      tsMs: 2000,
      body: "request handled",
      severityText: "info",
      serviceName: "gateway",
    }),
  ];

  it("renders virtualized rows with level and service", () => {
    render(
      <LogList
        rows={rows}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    expect(screen.getByText("payment failed")).toBeInTheDocument();
    expect(screen.getByText("request handled")).toBeInTheDocument();
    expect(screen.getByText("ERROR")).toBeInTheDocument();
    expect(screen.getByText("gateway")).toBeInTheDocument();
  });

  it("expands a row to show attributes and filter actions", async () => {
    const onAddFilter = vi.fn();
    render(
      <LogList
        rows={rows}
        onAddFilter={onAddFilter}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("payment failed"));
    // trace_id is a per-record field: visible under "This line" without
    // expanding the resource section.
    expect(screen.getByText("trace_id")).toBeInTheDocument();
    await expandResource();
    await userEvent.click(
      screen.getByRole("button", {
        name: "Filter for service.name = payments",
      }),
    );
    expect(onAddFilter).toHaveBeenCalledWith({
      label: "service.name",
      op: "=",
      value: "payments",
    });
  });

  it("sorts expanded attributes alphabetically within each scope", async () => {
    const { container } = render(
      <LogList
        rows={[
          row({
            resourceAttributes: { zebra: "last", alpha: "first" },
            logAttributes: { omega: "last", beta: "first" },
          }),
        ]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByText("hello"));
    await expandResource();

    expect(
      [
        ...container.querySelectorAll(
          ".attrtable-row[data-scope='resource'] dt",
        ),
      ].map((element) => element.textContent),
    ).toEqual(["alpha", "zebra"]);
    expect(
      [
        ...container.querySelectorAll(".attrtable-row[data-scope='line'] dt"),
      ].map((element) => element.textContent),
    ).toEqual(["beta", "omega"]);
  });

  it("supports exclude filters from the detail view", async () => {
    const onAddFilter = vi.fn();
    render(
      <LogList
        rows={rows}
        onAddFilter={onAddFilter}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("payment failed"));
    await userEvent.click(
      screen.getByRole("button", {
        name: "Filter out trace_id = cafe1234beef",
      }),
    );
    expect(onAddFilter).toHaveBeenCalledWith({
      label: "trace_id",
      op: "!=",
      value: "cafe1234beef",
    });
  });

  it("pivots to the trace from a row with a trace id", async () => {
    const onOpenTrace = vi.fn();
    render(
      <LogList
        rows={rows}
        onAddFilter={() => {}}
        onOpenTrace={onOpenTrace}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("payment failed"));
    await userEvent.click(
      screen.getByRole("button", { name: /View trace cafe1234/ }),
    );
    expect(onOpenTrace).toHaveBeenCalledWith("cafe1234beef");
  });

  it("shows the shared empty state", () => {
    render(
      <LogList
        rows={[]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    const status = screen.getByRole("status");
    expect(status).toHaveTextContent(/No log lines/);
  });

  it("keeps a row expanded when a newer row is prepended and shifts its index", async () => {
    const { rerender } = render(
      <LogList
        rows={rows}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("request handled"));
    await expandResource();
    expect(screen.getByText("service.name")).toBeInTheDocument();

    const prepended: LogRow[] = [
      row({
        tsNs: "4000000000",
        tsMs: 4000,
        body: "new row",
        severityText: "info",
        serviceName: "gateway",
      }),
      ...rows,
    ];
    rerender(
      <LogList
        rows={prepended}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    // "request handled" is now at index 2, not 1 — expansion keyed by index
    // alone would have collapsed it (or expanded the wrong row).
    expect(screen.getByText("service.name")).toBeInTheDocument();
  });
});

describe("rowKey", () => {
  it("is stable across an index shift and distinguishes rows without one", () => {
    const a = row({ tsNs: "1", body: "a" });
    const b = row({ tsNs: "2", body: "b" });
    expect(rowKey(a)).not.toBe(rowKey(b));
    expect(rowKey(a)).toBe(rowKey({ ...a }));
  });

  it("distinguishes two rows sharing a timestamp and body but no span/trace id", () => {
    const a = row({
      tsNs: "1",
      body: "same line",
      resourceAttributes: { "host.name": "web-1" },
    });
    const b = row({
      tsNs: "1",
      body: "same line",
      resourceAttributes: { "host.name": "web-2" },
    });
    expect(rowKey(a)).not.toBe(rowKey(b));
  });

  it("is unaffected by attribute insertion order", () => {
    const a = row({
      resourceAttributes: { "host.name": "web-1", zone: "a" },
      logAttributes: { a: "1", b: "2" },
    });
    const b = row({
      resourceAttributes: { zone: "a", "host.name": "web-1" },
      logAttributes: { b: "2", a: "1" },
    });
    expect(rowKey(a)).toBe(rowKey(b));
  });

  it("distinguishes records whose plain `,`/`=` join would collide", () => {
    const a = row({ resourceAttributes: { a: "b,c=d" } });
    const b = row({ resourceAttributes: { a: "b", c: "d" } });
    expect(rowKey(a)).not.toBe(rowKey(b));
  });
});

describe("LogList per-record and scope attributes", () => {
  const metaRow = row({
    tsNs: "4000000000",
    tsMs: 4000,
    body: "checkout timed out",
    severityText: "error",
    serviceName: "checkout",
    traceId: "abc123",
    spanId: "def456",
  });

  it("shows trace_id/span_id under This line", async () => {
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    expect(screen.getByText("span_id")).toBeInTheDocument();
    expect(screen.getByText("def456")).toBeInTheDocument();
    expect(screen.getByText("abc123")).toBeInTheDocument();
  });

  it("renders per-record fields under This line and resource attributes under Resource, in that order", async () => {
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    const thisLine = screen.getByText("This line");
    const resourceToggle = screen.getByRole("button", { name: /^Resource/ });
    const spanRow = screen.getByText("span_id").closest(".attrtable-row")!;
    expect(
      thisLine.compareDocumentPosition(spanRow) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    expect(
      spanRow.compareDocumentPosition(resourceToggle) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();

    await expandResource();
    const serviceRow = screen
      .getByText("service.name")
      .closest(".attrtable-row")!;
    expect(
      resourceToggle.compareDocumentPosition(serviceRow) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  it("shows Scope only when the row carries scope attributes", async () => {
    render(
      <LogList
        rows={[row({ scopeAttributes: { "otel.scope.name": "otel-lib" } })]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("hello"));
    expect(screen.getByRole("button", { name: /^Scope/ })).toBeInTheDocument();
  });

  it("keeps a key in both This line and Resource separate, per scope", async () => {
    render(
      <LogList
        rows={[
          row({
            logAttributes: { "service.name": "line-value" },
            resourceAttributes: { "service.name": "resource-value" },
          }),
        ]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("hello"));
    expect(screen.getByText("line-value")).toBeInTheDocument();
    await expandResource();
    expect(screen.getByText("resource-value")).toBeInTheDocument();
  });

  it("offers filter actions on line attributes as well as resource attributes", async () => {
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    expect(
      screen.getByRole("button", { name: "Filter for span_id = def456" }),
    ).toBeInTheDocument();
    await expandResource();
    expect(
      screen.getByRole("button", {
        name: "Filter for service.name = checkout",
      }),
    ).toBeInTheDocument();
  });

  it("copies individual attribute values", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByText("checkout timed out"));
    await userEvent.click(
      screen.getByRole("button", { name: "Copy value for span_id" }),
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Copy log message" }),
    );

    expect(writeText).toHaveBeenNthCalledWith(1, "def456");
    expect(writeText).toHaveBeenNthCalledWith(2, "checkout timed out");
  });

  it("copies the row as JSON, including resource attributes", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    render(
      <LogList
        rows={[
          row({ ...metaRow, resourceAttributes: { "k8s.pod.name": "web-1" } }),
        ]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    await userEvent.click(screen.getByRole("button", { name: "Copy JSON" }));
    const copied = JSON.parse(writeText.mock.calls[0]![0] as string);
    expect(copied.span_id).toBe("def456");
    expect(copied["k8s.pod.name"]).toBe("web-1");
    vi.unstubAllGlobals();
  });
});

describe("LogList semantic labels", () => {
  const podUid = {
    key: "k8s.pod.uid",
    brief: "The UID of the Pod.",
    type: "string",
    group_id: "registry.k8s.pod",
    group_display_name: "Kubernetes Attributes",
    namespace: "otel",
    version: "1.43.0",
    source: "bundled",
  };
  const podName = {
    ...podUid,
    key: "k8s.pod.name",
    brief: "The name of the Pod.",
  };
  const semRow = row({
    body: "pod started",
    logAttributes: {
      "k8s.pod.uid": "275ecb36",
      "k8s.pod.name": "web-1",
      "app.order.id": "o-1",
    },
  });

  it("enriches known per-line keys and leaves unknown ones bare", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: {
          hits: [],
          resolutions: [
            { key: "k8s.pod.uid", hits: [podUid], primary: podUid },
            { key: "k8s.pod.name", hits: [podName], primary: podName },
            { key: "app.order.id", hits: [] },
          ],
        },
      },
    ]);
    const { container } = render(
      <LogList
        rows={[semRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("pod started"));
    expect(await screen.findByText("Kubernetes")).toBeInTheDocument();
    expect(screen.getByText("otel")).toBeInTheDocument();
    expect(screen.getByText("Other")).toBeInTheDocument();
    expect(screen.queryByText("The UID of the Pod.")).not.toBeInTheDocument();
    const podKey = screen.getByText("k8s.pod.uid", {
      selector: ".semkey-name",
    });
    expect(podKey).toHaveAttribute("data-known", "");
    const orderDt = [
      ...container.querySelectorAll(".attrtable-row[data-scope='line'] dt"),
    ].find((el) => el.textContent === "app.order.id");
    expect(orderDt).toBeDefined();
  });

  it("adds the description line only with the descriptions toggle on", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: {
          hits: [],
          resolutions: [
            { key: "k8s.pod.uid", hits: [podUid], primary: podUid },
            { key: "k8s.pod.name", hits: [podName], primary: podName },
          ],
        },
      },
    ]);
    render(
      <LogList
        rows={[semRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("pod started"));
    await screen.findByText("Kubernetes");
    expect(screen.queryByText(/The UID of the Pod\./)).not.toBeInTheDocument();
    await userEvent.click(screen.getByLabelText("Show descriptions"));
    expect(screen.getByText(/The UID of the Pod\./)).toBeInTheDocument();
  });

  it("shows raw keys and no error when the registry fails", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: { error: "boom" },
        status: 500,
      },
    ]);
    const { container } = render(
      <LogList
        rows={[semRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("pod started"));
    await new Promise((r) => setTimeout(r, 60));
    expect(screen.queryByText(/boom/)).not.toBeInTheDocument();
    expect(
      [...container.querySelectorAll(".attrtable-row dt")].map(
        (el) => el.textContent,
      ),
    ).toEqual(["app.order.id", "k8s.pod.name", "k8s.pod.uid"]);
  });
});

describe("LogList entity pivots", () => {
  const podName = {
    key: "k8s.pod.name",
    brief: "The name of the Pod.",
    type: "string",
    group_id: "registry.k8s.pod",
    group_display_name: "Kubernetes Attributes",
    namespace: "otel",
    version: "1.43.0",
    source: "bundled",
    entity_roles: [
      { namespace: "otel", entity: "k8s.pod", role: "identifying" },
    ],
  };
  const pivotRow = row({
    body: "pod started",
    traceId: "abc123",
    logAttributes: {
      "k8s.pod.name": "web-1",
      "k8s.namespace.name": "prod",
    },
  });

  it("offers traces/catalog pivots for an identifying key and an open-trace action for trace_id", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: {
          hits: [],
          resolutions: [
            { key: "k8s.pod.name", hits: [podName], primary: podName },
          ],
        },
      },
    ]);
    const update = vi.fn();
    const onOpenTrace = vi.fn();
    render(
      <LogList
        rows={[pivotRow]}
        onAddFilter={() => {}}
        onOpenTrace={onOpenTrace}
        update={update}
      />,
    );
    await userEvent.click(screen.getByText("pod started"));

    await userEvent.click(
      await screen.findByRole("button", {
        name: "Traces with k8s.pod.name = web-1",
      }),
    );
    expect(update).toHaveBeenCalledWith(
      {
        signal: "traces",
        trace: "",
        search: "",
        group: "",
        filters: [],
        raw: "",
        traceFilters: [{ field: "k8s.pod.name", value: "web-1" }],
      },
      { push: true },
    );

    await userEvent.click(
      screen.getByRole("button", { name: "Open pod web-1 in the catalog" }),
    );
    expect(update).toHaveBeenLastCalledWith(
      {
        signal: "catalog",
        trace: "",
        group: "",
        search: "",
        traceFilters: [],
        filters: [],
        raw: "",
        catalogEntity: "k8s_pod",
        catalogPrimary: "web-1\u001fprod",
        catalogSecondary: "",
      },
      { push: true },
    );

    await userEvent.click(
      screen.getByRole("button", { name: "Open trace abc123" }),
    );
    expect(onOpenTrace).toHaveBeenCalledWith("abc123");
  });
});

describe("log detail attribute styling", () => {
  it("keeps the semantic tooltip out of the clipping key cell", async () => {
    const { readFileSync } = await import("node:fs");
    const { join } = await import("node:path");
    const css = readFileSync(
      join(import.meta.dirname, "../../styles/global.css"),
      "utf8",
    );
    const start = css.indexOf(".sem-tip {");
    expect(start, "shared .sem-tip rule").toBeGreaterThan(-1);
    const rule = css.slice(start, css.indexOf("}", start));
    expect(rule).toMatch(/position:\s*fixed/);
  });
});
