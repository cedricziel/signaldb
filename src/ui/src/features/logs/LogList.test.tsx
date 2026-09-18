import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { LogRow } from "../../api/loki";
import { resetSemanticsCache } from "../../hooks/useSemantics";
import { stubFetchRoutes } from "../../test/render";
import { LogList, rowKey, traceIdOf } from "./LogList";

afterEach(() => {
  resetSemanticsCache();
  vi.unstubAllGlobals();
  localStorage.clear();
});

/** The resource/stream section is collapsed behind a summary by default;
 * every test that reaches into it clicks the section head first. */
const expandResource = () =>
  userEvent.click(screen.getByRole("button", { name: /Resource · stream/ }));

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
  // Only service_name/level as labels, trace_id as per-line metadata —
  // mirrors what a real row actually carries (see router's
  // `batches_to_streams`).
  const rows: LogRow[] = [
    row({
      tsNs: "3000000000",
      tsMs: 3000,
      line: "payment failed",
      labels: { level: "error", service_name: "payments" },
      metadata: { trace_id: "cafe1234beef" },
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
      <LogList rows={rows} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    expect(screen.getByText("payment failed")).toBeInTheDocument();
    expect(screen.getByText("request handled")).toBeInTheDocument();
    expect(screen.getByText("ERROR")).toBeInTheDocument();
    expect(screen.getByText("gateway")).toBeInTheDocument();
  });

  it("expands a row to show attributes and filter actions", async () => {
    const onAddFilter = vi.fn();
    render(
      <LogList rows={rows} onAddFilter={onAddFilter} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("payment failed"));
    // trace_id is per-line metadata: visible under "This line" without
    // expanding the resource section.
    expect(screen.getByText("trace_id")).toBeInTheDocument();
    await expandResource();
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
        update={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByText("hello"));
    await expandResource();

    // Labels (zebra, alpha) land in the resource scope; unresolved metadata
    // (omega, beta) stays on the line.
    expect(
      [
        ...container.querySelectorAll(".attrtable-row[data-scope='resource'] dt"),
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
      <LogList rows={rows} onAddFilter={onAddFilter} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("payment failed"));
    await expandResource();
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
      <LogList rows={rows} onAddFilter={() => {}} onOpenTrace={onOpenTrace} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("payment failed"));
    await userEvent.click(
      screen.getByRole("button", { name: /View trace cafe1234/ }),
    );
    expect(onOpenTrace).toHaveBeenCalledWith("cafe1234beef");
  });

  it("shows the shared empty state", () => {
    render(<LogList rows={[]} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />);
    const status = screen.getByRole("status");
    expect(status).toHaveTextContent(/No log lines/);
  });

  it("keeps a row expanded when a newer row is prepended and shifts its index", async () => {
    const { rerender } = render(
      <LogList rows={rows} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("request handled"));
    await expandResource();
    expect(screen.getByText("service_name")).toBeInTheDocument();

    const prepended: LogRow[] = [
      row({
        tsNs: "4000000000",
        tsMs: 4000,
        line: "new row",
        labels: { level: "info", service_name: "gateway" },
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
    expect(screen.getByText("service_name")).toBeInTheDocument();
  });
});

describe("rowKey", () => {
  it("is stable across an index shift and distinguishes rows without one", () => {
    const a = row({ tsNs: "1", line: "a" });
    const b = row({ tsNs: "2", line: "b" });
    expect(rowKey(a)).not.toBe(rowKey(b));
    expect(rowKey(a)).toBe(rowKey({ ...a }));
  });

  it("distinguishes two streams sharing a timestamp and line but no span/trace id", () => {
    const a = row({
      tsNs: "1",
      line: "same line",
      labels: { host: "web-1", service_name: "checkout" },
    });
    const b = row({
      tsNs: "1",
      line: "same line",
      labels: { host: "web-2", service_name: "checkout" },
    });
    expect(rowKey(a)).not.toBe(rowKey(b));
  });

  it("is unaffected by label/metadata insertion order", () => {
    const a = row({
      labels: { host: "web-1", service_name: "checkout" },
      metadata: { a: "1", b: "2" },
    });
    const b = row({
      labels: { service_name: "checkout", host: "web-1" },
      metadata: { b: "2", a: "1" },
    });
    expect(rowKey(a)).toBe(rowKey(b));
  });

  it("distinguishes records whose plain `,`/`=` join would collide", () => {
    // `{ a: "b,c=d" }` and `{ a: "b", c: "d" }` both join to `a=b,c=d` under
    // a naive `k=v` joiner — an unambiguous encoding must keep them apart.
    const a = row({ labels: { a: "b,c=d" } });
    const b = row({ labels: { a: "b", c: "d" } });
    expect(rowKey(a)).not.toBe(rowKey(b));
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
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    expect(screen.getByText("span_id")).toBeInTheDocument();
    expect(screen.getByText("def456")).toBeInTheDocument();
    expect(screen.getByText("abc123")).toBeInTheDocument();
  });

  it("renders metadata under This line and stream labels under Resource · stream, so metadata is not mistaken for a stream label", async () => {
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
    const resourceToggle = screen.getByRole("button", {
      name: /Resource · stream/,
    });
    const spanRow = screen.getByText("span_id").closest(".attrtable-row")!;
    // Metadata sits between the "This line" heading and the resource
    // toggle, not after it — under "This line", not "Resource · stream".
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
      .getByText("service_name")
      .closest(".attrtable-row")!;
    // The stream label renders after the toggle it expanded from, under
    // "Resource · stream".
    expect(
      resourceToggle.compareDocumentPosition(serviceRow) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  // Structured metadata varies per line, so a stream selector cannot match it.
  it("offers no stream-selector filter actions for metadata", async () => {
    render(
      <LogList
        rows={[metaRow]}
        onAddFilter={() => {}}
        onOpenTrace={() => {}}
        update={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByText("checkout timed out"));
    await expandResource();
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
        update={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByText("checkout timed out"));
    await expandResource();
    await userEvent.click(
      screen.getByRole("button", { name: "Copy value for service_name" }),
    );
    expect(
      screen.getByRole("button", { name: "Copied value for service_name" }),
    ).toBeInTheDocument();
    await userEvent.click(
      screen.getByRole("button", { name: "Copy value for span_id" }),
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Copy log message" }),
    );

    expect(writeText).toHaveBeenNthCalledWith(1, "checkout");
    expect(writeText).toHaveBeenNthCalledWith(2, "def456");
    expect(writeText).toHaveBeenNthCalledWith(3, "checkout timed out");
  });

  it("copies metadata as well as labels", async () => {
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
    await userEvent.click(screen.getByRole("button", { name: "Copy JSON" }));
    const copied = JSON.parse(writeText.mock.calls[0]![0] as string);
    expect(copied.span_id).toBe("def456");
    expect(copied.service_name).toBe("checkout");
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
  // A second Kubernetes key alongside k8s.pod.uid: a titled group left with
  // only one row folds into "Other" (see foldSingletonGroups) rather than
  // keeping its own heading, so these tests need two resolved rows in the
  // group to exercise the heading. Neither carries an entity role, so both
  // stay in the "This line" scope regardless of resolution (see
  // logScopes.ts) — no need to expand the resource section to see them.
  const podName = {
    ...podUid,
    key: "k8s.pod.name",
    brief: "The name of the Pod.",
  };
  const semRow = row({
    line: "pod started",
    labels: { service_name: "api" },
    metadata: {
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
            { key: "service_name", hits: [] },
          ],
        },
      },
    ]);
    const { container } = render(
      <LogList rows={[semRow]} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("pod started"));
    // Enrichment shows on the group heading (title + namespace, once) and a
    // dotted-underline key — no inline brief without the descriptions
    // toggle, see "adds the description line only with the toggle on".
    expect(
      await screen.findByText("Kubernetes"),
    ).toBeInTheDocument();
    expect(screen.getByText("otel")).toBeInTheDocument();
    expect(screen.getByText("Other")).toBeInTheDocument();
    expect(screen.queryByText("The UID of the Pod.")).not.toBeInTheDocument();
    const podKey = screen.getByText("k8s.pod.uid", { selector: ".semkey-name" });
    expect(podKey).toHaveAttribute("data-known", "");
    // Unknown key: bare text, as before — still under "This line", it has
    // no entity role.
    const orderDt = [
      ...container.querySelectorAll(".attrtable-row[data-scope='line'] dt"),
    ].find((el) => el.textContent === "app.order.id");
    expect(orderDt).toBeDefined();
    // The resource scope had nothing resolvable: no title, bare key.
    await expandResource();
    expect(
      container.querySelector(".attrtable-row[data-scope='resource'] dt")!
        .textContent,
    ).toBe("service_name");
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
      <LogList rows={[semRow]} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />,
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
      <LogList rows={[semRow]} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("pod started"));
    await expandResource();
    await new Promise((r) => setTimeout(r, 60));
    expect(screen.queryByText(/boom/)).not.toBeInTheDocument();
    // "This line" (metadata) renders before the resource section, so the
    // metadata keys come first in document order.
    expect(
      [...container.querySelectorAll(".attrtable-row dt")].map(
        (el) => el.textContent,
      ),
    ).toEqual(["app.order.id", "k8s.pod.name", "k8s.pod.uid", "service_name"]);
  });
});

describe("LogList resource/stream section", () => {
  const bigRow = row({
    line: "many labels",
    labels: {
      service_name: "checkout",
      level: "info",
      "cloud.region": "us-east-1",
      zone: "a",
      pod: "checkout-7",
    },
    metadata: { trace_id: "abc123" },
  });

  it("is collapsed behind a one-line summary by default and expands on click", async () => {
    render(
      <LogList rows={[bigRow]} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("many labels"));

    const toggle = screen.getByRole("button", { name: /Resource · stream/ });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(
      screen.getByText("service_name", { selector: ".attrtable-summary-k" }),
    ).toBeInTheDocument();
    expect(
      screen.getByText("checkout", { selector: ".attrtable-summary-v" }),
    ).toBeInTheDocument();
    expect(screen.queryByText("checkout-7")).not.toBeInTheDocument();

    await userEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText("checkout-7")).toBeInTheDocument();
  });

  it("renders per-line fields above the resource section", async () => {
    render(
      <LogList rows={[bigRow]} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("many labels"));
    const thisLine = screen.getByText("This line");
    const resource = screen.getByRole("button", { name: /Resource · stream/ });
    expect(
      thisLine.compareDocumentPosition(resource) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  it("persists the descriptions toggle across an unmount/remount", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: {
          hits: [],
          resolutions: [
            {
              key: "cloud.region",
              hits: [
                {
                  key: "cloud.region",
                  brief: "The geographic region.",
                  type: "string",
                  group_id: "registry.cloud",
                  group_display_name: "Cloud",
                  namespace: "otel",
                  version: "1.43.0",
                  source: "bundled",
                },
              ],
              primary: {
                key: "cloud.region",
                brief: "The geographic region.",
                type: "string",
                group_id: "registry.cloud",
                group_display_name: "Cloud",
                namespace: "otel",
                version: "1.43.0",
                source: "bundled",
              },
            },
          ],
        },
      },
    ]);
    const { unmount } = render(
      <LogList rows={[bigRow]} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("many labels"));
    await userEvent.click(screen.getByLabelText("Show descriptions"));
    expect(localStorage.getItem("signaldb.ui.attrDescriptions")).toBe("1");
    unmount();

    render(
      <LogList rows={[bigRow]} onAddFilter={() => {}} onOpenTrace={() => {}} update={vi.fn()} />,
    );
    await userEvent.click(screen.getByText("many labels"));
    expect(screen.getByLabelText("Show descriptions")).toBeChecked();
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
    line: "pod started",
    labels: { service_name: "api" },
    metadata: {
      trace_id: "abc123",
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
    // k8s.pod.name carries an identifying entity role, so once the registry
    // answers it moves into the (collapsed-by-default) resource section.
    await expandResource();

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
        catalogPrimary: "web-1prod",
        catalogSecondary: "",
      },
      { push: true },
    );

    // trace_id has no entity role of its own, so it stays under "This line".
    await userEvent.click(
      screen.getByRole("button", { name: "Open trace abc123" }),
    );
    expect(onOpenTrace).toHaveBeenCalledWith("abc123");
  });
});

describe("log detail attribute styling", () => {
  it("keeps the semantic tooltip out of the clipping key cell", async () => {
    // jsdom does not lay out CSS, so pin the mechanism instead: `.sem-tip`
    // is portaled to <body> with fixed placement (SemanticHover), so the
    // dt's `overflow: hidden` for its ellipsis cannot clip it.
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
