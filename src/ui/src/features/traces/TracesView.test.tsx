import { fireEvent, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { resetSemanticsCache } from "../../hooks/useSemantics";
import { TracesView } from "./TracesView";
import * as traceGroupsApi from "../../api/traceGroups";
import type { TraceGroup } from "../../api/traceGroups";
import * as traceGroupMembersApi from "../../api/traceGroupMembers";
import type { TraceGroupMember } from "../../api/traceGroupMembers";
import * as unresolvedGroupApi from "./unresolvedGroup";
import * as traceVolumeApi from "../../api/traceVolume";

// The group table is a server-side aggregate (see api/traceGroups) — mocked
// at the module boundary rather than at the network layer, the way the rest
// of this file mocks `tempoSearch`/`fetchTraceVolume` via `stubFetchRoutes`.
// `importOriginal` keeps the real `groupsFromIrResponse`/`buildGroupDoc` etc.
// (covered by api/traceGroups.test.ts) and swaps only the network-touching
// entry point.
vi.mock("../../api/traceGroups", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/traceGroups")>();
  return { ...actual, fetchTraceGroups: vi.fn() };
});

// Same treatment for the group drill-in (api/traceGroupMembers.test.ts covers
// buildMembersDoc/membersFromIrResponse) and the #1070 unresolved-dimension
// guard's window-total query (unresolvedGroup.test.ts covers its doc/shape).
vi.mock("../../api/traceGroupMembers", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/traceGroupMembers")>();
  return { ...actual, fetchTraceGroupMembers: vi.fn() };
});
vi.mock("./unresolvedGroup", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./unresolvedGroup")>();
  return { ...actual, fetchWindowTotal: vi.fn() };
});

const fetchTraceGroups = vi.mocked(traceGroupsApi.fetchTraceGroups);
const fetchTraceGroupMembers = vi.mocked(
  traceGroupMembersApi.fetchTraceGroupMembers,
);
const fetchWindowTotal = vi.mocked(unresolvedGroupApi.fetchWindowTotal);

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  localStorage.clear();
  resetSemanticsCache();
});

beforeEach(() => {
  fetchTraceGroups.mockReset();
  fetchTraceGroupMembers.mockReset();
  fetchWindowTotal.mockReset();
  // Quiet defaults so tests that don't care about a particular query (trace
  // detail, facet sidebar, ...) don't each have to stub it.
  fetchTraceGroups.mockResolvedValue({ groups: [], truncated: false });
  fetchTraceGroupMembers.mockResolvedValue([]);
  fetchWindowTotal.mockResolvedValue(0);
});

function group(
  values: (string | null)[],
  count: number,
  errors: number,
  p50Ms: number,
  p95Ms: number,
  lastNs: string,
): TraceGroup {
  return { values, count, errors, p50Ms, p95Ms, lastNs };
}

function member(
  traceId: string,
  spanId: string,
  spanName: string,
  serviceName: string,
  startNs: string,
  durationMs: number,
  statusCode = "Ok",
): TraceGroupMember {
  return {
    traceId,
    spanId,
    parentSpanId: null,
    spanName,
    serviceName,
    startNs,
    durationNanos: String(durationMs * 1e6),
    statusCode,
  };
}

const TRACE_BODY = {
  traceID: "t1cafe",
  rootServiceName: "gateway",
  rootTraceName: "POST /api/checkout",
  startTimeUnixNano: "1000",
  durationMs: 412,
  spanSets: [
    {
      matched: 3,
      spans: [
        {
          spanID: "root",
          startTimeUnixNano: "1000000000",
          durationNanos: "412000000",
          name: "POST /api/checkout",
          serviceName: "gateway",
          status: "ok",
          attributes: {},
        },
        {
          spanID: "charge",
          parentSpanID: "root",
          startTimeUnixNano: "1040000000",
          durationNanos: "258000000",
          name: "charge",
          serviceName: "payments",
          status: "error",
          attributes: {
            "payment.provider": {
              key: "payment.provider",
              value: { stringValue: "stripe" },
            },
          },
          events: [
            {
              name: "exception",
              timeUnixNano: "1055000000",
              attributes: {
                "exception.type": {
                  key: "exception.type",
                  value: { stringValue: "PaymentError" },
                },
                "exception.message": {
                  key: "exception.message",
                  value: { stringValue: "card declined" },
                },
                "exception.stacktrace": {
                  key: "exception.stacktrace",
                  value: { stringValue: "Error: card declined" },
                },
              },
            },
          ],
        },
      ],
    },
  ],
};

function renderView(state: Partial<ExploreState> = {}) {
  const update = vi.fn();
  renderWithClient(
    <TracesView
      state={{ ...DEFAULT_STATE, signal: "traces", ...state }}
      update={update}
    />,
  );
  return update;
}

describe("TracesView group list", () => {
  // Every test here renders the group table, which fires the (unrelated)
  // span-volume aggregate too — an unmatched fetch 404s harmlessly, so an
  // empty stub is enough unless a test cares about the volume response.
  beforeEach(() => {
    stubFetchRoutes([]);
  });

  it("renders the server's aggregates verbatim", async () => {
    fetchTraceGroups.mockResolvedValue({
      groups: [
        group(["POST /api/checkout"], 42, 3, 12, 91, "1700000000000000000"),
        group(["GET /login"], 7, 0, 4, 9, "1700000030000000000"),
      ],
      truncated: false,
    });
    renderView();
    // `findAllByRole` would race the pending skeleton rows (also role="row")
    // — wait for real content first.
    await screen.findByText("POST /api/checkout");
    const rows = screen.getAllByRole("row");
    // Header row + one row per group.
    expect(rows).toHaveLength(3);
    expect(rows[1]).toHaveTextContent("POST /api/checkout");
    expect(rows[1]).toHaveTextContent("42");
    expect(rows[1]).toHaveTextContent("7%"); // 3 errors / 42 = 7%, rounded
    expect(rows[1]).toHaveTextContent("12 ms");
    expect(rows[1]).toHaveTextContent("91 ms");
    expect(rows[2]).toHaveTextContent("GET /login");
    expect(rows[2]).toHaveTextContent("7");
  });

  it("keeps a group with zero errors listed, showing a dash", async () => {
    fetchTraceGroups.mockResolvedValue({
      groups: [group(["GET /health"], 5, 0, 1, 2, "1700000000000000000")],
      truncated: false,
    });
    renderView();
    await screen.findByText("GET /health");
    const row = screen.getAllByRole("row")[1]!;
    expect(row).toHaveTextContent("GET /health");
    expect(row).toHaveTextContent("–");
  });

  it("does not change the displayed aggregates when the row limit changes", async () => {
    fetchTraceGroups.mockResolvedValue({
      groups: [
        group(["POST /api/checkout"], 42, 0, 12, 91, "1700000000000000000"),
      ],
      truncated: false,
    });
    const small = renderWithClient(
      <TracesView
        state={{ ...DEFAULT_STATE, signal: "traces", limit: 10 }}
        update={vi.fn()}
      />,
    );
    await within(small.container).findByText("POST /api/checkout");
    expect(
      within(small.container).getByRole("row", {
        name: /POST \/api\/checkout/,
      }),
    ).toHaveTextContent("42");
    small.unmount();

    const big = renderWithClient(
      <TracesView
        state={{ ...DEFAULT_STATE, signal: "traces", limit: 5000 }}
        update={vi.fn()}
      />,
    );
    await within(big.container).findByText("POST /api/checkout");
    expect(
      within(big.container).getByRole("row", { name: /POST \/api\/checkout/ }),
    ).toHaveTextContent("42");
  });

  it("drills into a group on click, building the composite key from group values", async () => {
    fetchTraceGroups.mockResolvedValue({
      groups: [
        group(
          ["POST /api/checkout", "gateway"],
          2,
          0,
          10,
          20,
          "1700000000000000000",
        ),
      ],
      truncated: false,
    });
    const update = renderView({ groupBy: "span.name,service.name" });
    await userEvent.click(
      await screen.findByRole("button", { name: "POST /api/checkout" }),
    );
    expect(update).toHaveBeenCalledWith(
      { group: "POST /api/checkoutgateway" },
      { push: true },
    );
  });

  it("maps a null dimension value to the not-set sentinel in the drill-in key", async () => {
    fetchTraceGroups.mockResolvedValue({
      groups: [group([null], 3, 0, 5, 6, "1700000000000000000")],
      truncated: false,
    });
    const update = renderView();
    await userEvent.click(
      await screen.findByRole("button", { name: "(not set)" }),
    );
    expect(update).toHaveBeenCalledWith({ group: "(not set)" }, { push: true });
  });

  it("issues a new query with a different sort on header click, rather than re-sorting the page", async () => {
    fetchTraceGroups.mockResolvedValue({
      groups: [
        group(["POST /api/checkout"], 2, 0, 10, 20, "1700000000000000000"),
      ],
      truncated: false,
    });
    renderView();
    await screen.findByText("POST /api/checkout");
    expect(fetchTraceGroups).toHaveBeenCalledTimes(1);
    expect(fetchTraceGroups).toHaveBeenLastCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.anything(),
      { key: "n", dir: "desc" },
    );

    await userEvent.click(screen.getByRole("button", { name: "P95" }));
    expect(fetchTraceGroups).toHaveBeenCalledTimes(2);
    expect(fetchTraceGroups).toHaveBeenLastCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.anything(),
      { key: "p95", dir: "desc" },
    );

    await userEvent.click(screen.getByRole("button", { name: "P95" }));
    expect(fetchTraceGroups).toHaveBeenCalledTimes(3);
    expect(fetchTraceGroups).toHaveBeenLastCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.anything(),
      { key: "p95", dir: "asc" },
    );
  });

  it("sorts the Rate column by count — the same ordering over a fixed window", async () => {
    renderView();
    await screen.findByLabelText("Group by");
    await userEvent.click(screen.getByRole("button", { name: "Rate" }));
    expect(fetchTraceGroups).toHaveBeenLastCalledWith(
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.anything(),
      { key: "n", dir: "asc" },
    );
  });

  it("shows a truncation note only when the result is truncated", async () => {
    fetchTraceGroups.mockResolvedValue({
      groups: [group(["a"], 1, 0, 1, 1, "1")],
      truncated: true,
    });
    const truncated = renderWithClient(
      <TracesView
        state={{ ...DEFAULT_STATE, signal: "traces" }}
        update={vi.fn()}
      />,
    );
    expect(
      await within(truncated.container).findByText(/top 500 groups/i),
    ).toBeInTheDocument();
    truncated.unmount();

    fetchTraceGroups.mockResolvedValue({
      groups: [group(["a"], 1, 0, 1, 1, "1")],
      truncated: false,
    });
    const full = renderWithClient(
      <TracesView
        state={{ ...DEFAULT_STATE, signal: "traces" }}
        update={vi.fn()}
      />,
    );
    await within(full.container).findByText("a");
    expect(
      within(full.container).queryByText(/top 500 groups/i),
    ).not.toBeInTheDocument();
  });

  it("renders skeleton rows while the query is pending", async () => {
    fetchTraceGroups.mockImplementation(() => new Promise(() => {}));
    const { container } = renderWithClient(
      <TracesView
        state={{ ...DEFAULT_STATE, signal: "traces" }}
        update={vi.fn()}
      />,
    );
    expect(await screen.findByRole("table")).toHaveAttribute(
      "aria-busy",
      "true",
    );
    expect(container.querySelectorAll(".skeleton-bar").length).toBeGreaterThan(
      0,
    );
  });

  it("labels the count column by grain and refetches when the grain toggle changes", async () => {
    const update = renderView();
    expect(
      await screen.findByRole("columnheader", { name: "Traces" }),
    ).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByLabelText("Grain"), "spans");
    expect(update).toHaveBeenCalledWith({ grain: "spans", group: "" });
  });

  it("shows the Spans column label at span grain", async () => {
    renderView({ grain: "spans" });
    expect(
      await screen.findByRole("columnheader", { name: "Spans" }),
    ).toBeInTheDocument();
  });

  it("suggests span grain when trace grain finds nothing under active filters", async () => {
    renderView({
      grain: "traces",
      traceFilters: [{ field: "name", value: "charge" }],
    });
    expect(await screen.findByText(/root span/i)).toBeInTheDocument();
    expect(screen.getByText(/span grain/i)).toBeInTheDocument();
  });

  it("does not suggest span grain when nothing is filtered", async () => {
    renderView();
    await screen.findByText(/no groups in this time range/i);
    expect(screen.queryByText(/span grain/i)).not.toBeInTheDocument();
  });

  it("adds a secondary dimension via the then-by picker", async () => {
    const update = renderView();
    await screen.findByLabelText("Then by");
    await userEvent.selectOptions(
      screen.getByLabelText("Then by"),
      "service.name",
    );
    expect(update).toHaveBeenCalledWith({
      groupBy: "span.name,service.name",
      group: "",
    });
  });

  it("lets a user type a custom attribute as a grouping dimension", async () => {
    const update = renderView();
    await userEvent.type(
      screen.getByLabelText("Custom dimension"),
      "http.route{enter}",
    );
    expect(update).toHaveBeenCalledWith({ groupBy: "http.route", group: "" });
  });

  // #1073: the custom dimension input suggests attribute keys actually
  // observed in the window (via the querier's tag discovery), merged with
  // schema-registry hits — the traces-tab analog of the logs tab's
  // FilterChips key autocomplete.
  it("suggests observed tag names merged with registry hits for the custom dimension", async () => {
    stubFetchRoutes([
      {
        match: "/tempo/api/search/tags",
        body: { tagNames: ["http.route", "http.retries", "deployment.env"] },
      },
      {
        match: "/api/v1/schema/attributes",
        body: {
          hits: [
            {
              key: "http.route",
              brief: "The matched route.",
              type: "string",
              group_id: "registry.http",
              namespace: "otel",
              version: "1.43.0",
              source: "bundled",
            },
          ],
        },
      },
    ]);
    const update = renderView();
    await userEvent.type(screen.getByLabelText("Custom dimension"), "http.re");

    // The registry hit and the observed-tags list land via independent
    // async fetches; wait for the registry hit's brief specifically so the
    // list below is read after both have merged in, not just whichever
    // landed first.
    await screen.findByText("The matched route.");
    const list = screen.getByRole("listbox", { name: "Attribute suggestions" });
    const options = within(list).getAllByRole("option");
    expect(options.map((o) => o.getAttribute("data-key"))).toEqual([
      "http.route",
      "http.retries",
    ]);
    expect(options[0]).toHaveTextContent("The matched route.");
    expect(options[0]).toHaveTextContent("seen");

    await userEvent.click(options[1]!);
    expect(update).toHaveBeenCalledWith({
      groupBy: "http.retries",
      group: "",
    });
    expect(
      screen.queryByRole("listbox", { name: "Attribute suggestions" }),
    ).not.toBeInTheDocument();
  });

  it("keeps a non-builtin groupBy selectable in the picker", async () => {
    renderView({ groupBy: "resource.env" });
    expect(await screen.findByLabelText("Group by")).toHaveValue(
      "resource.env",
    );
  });

  it("surfaces group aggregate errors", async () => {
    fetchTraceGroups.mockRejectedValue(new Error("boom"));
    renderView();
    expect(await screen.findByRole("alert")).toHaveTextContent(/boom/);
  });

  it("opens a trace by pasted id", async () => {
    const update = renderView();
    await userEvent.type(screen.getByLabelText("Trace ID"), "  deadbeef  ");
    await userEvent.click(screen.getByRole("button", { name: "Open" }));
    expect(update).toHaveBeenCalledWith({ trace: "deadbeef" }, { push: true });
  });
});

describe("TracesView unresolvable-dimension guard (#1070)", () => {
  beforeEach(() => {
    stubFetchRoutes([]);
  });

  it("reports an unresolvable dimension instead of rendering the bogus row", async () => {
    // Signature: exactly one null-labelled group whose count equals the
    // window total under the same scope.
    fetchTraceGroups.mockResolvedValue({
      groups: [group([null], 1000, 0, 1, 1, "1")],
      truncated: false,
    });
    fetchWindowTotal.mockResolvedValue(1000);
    renderView({ groupBy: "http.rotue" });
    // The picked dimension also shows up in the "Group by" select and the
    // (now moot) column header, so scope the name check to the note itself.
    const note = await screen.findByText(/isn.t queryable/i);
    expect(note).toHaveTextContent("http.rotue");
    expect(screen.queryByText("(not set)")).not.toBeInTheDocument();
  });

  it("still renders a genuine null group whose count is under the window total", async () => {
    // Same shape (one null group), but its count is a strict subset of the
    // window — a real "(not set)" bucket, not an unresolved field.
    fetchTraceGroups.mockResolvedValue({
      groups: [group([null], 3, 0, 1, 1, "1")],
      truncated: false,
    });
    fetchWindowTotal.mockResolvedValue(1000);
    renderView();
    expect(await screen.findByText("(not set)")).toBeInTheDocument();
    expect(screen.queryByText(/isn.t queryable/i)).not.toBeInTheDocument();
  });

  it("renders normally, without checking the window total, when a null group sits alongside real groups", async () => {
    fetchTraceGroups.mockResolvedValue({
      groups: [
        group([null], 1000, 0, 1, 1, "1"),
        group(["GET /health"], 5, 0, 1, 1, "1"),
      ],
      truncated: false,
    });
    renderView();
    await screen.findByText("(not set)");
    expect(screen.getByText("GET /health")).toBeInTheDocument();
    // The result already isn't the unresolved shape, so the companion
    // window-total query must never fire.
    expect(fetchWindowTotal).not.toHaveBeenCalled();
  });
});

describe("TracesView group detail", () => {
  it("lists the group's members, newest first", async () => {
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1cafe", "s1", "POST /api/checkout", "gateway", "100", 412),
      member("t2feed", "s2", "POST /api/checkout", "gateway", "200", 88),
    ]);
    renderView({ group: "POST /api/checkout" });
    // `findAllByRole` would race the pending skeleton rows (also
    // role="row") — wait for real content first.
    await screen.findByText("t2feed");
    const rows = screen.getAllByRole("row");
    expect(rows).toHaveLength(3);
    expect(rows[1]).toHaveTextContent("t2feed");
    expect(rows[2]).toHaveTextContent("t1cafe");
  });

  it("parses a resolvable group key into per-dimension values for the members query", async () => {
    renderView({
      groupBy: "span.name,service.name",
      group: "POST /api/checkoutgateway",
    });
    await screen.findByRole("heading", {
      name: "POST /api/checkout · gateway",
    });
    expect(fetchTraceGroupMembers).toHaveBeenCalledWith(
      ["span.name", "service.name"],
      ["POST /api/checkout", "gateway"],
      expect.anything(),
      [],
      "traces",
      500,
    );
  });

  it("maps the not-set sentinel back to null for the members query", async () => {
    renderView({ groupBy: "resource.env", group: "(not set)" });
    await screen.findByRole("heading", { name: "(not set)" });
    expect(fetchTraceGroupMembers).toHaveBeenCalledWith(
      ["resource.env"],
      [null],
      expect.anything(),
      [],
      "traces",
      500,
    );
  });

  it("opens a trace on click", async () => {
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1cafe", "s1", "POST /api/checkout", "gateway", "100", 412),
    ]);
    const update = renderView({ group: "POST /api/checkout" });
    await userEvent.click(await screen.findByText("t1cafe"));
    expect(update).toHaveBeenCalledWith({ trace: "t1cafe" }, { push: true });
  });

  it("navigates back to the group list", async () => {
    const update = renderView({ group: "POST /api/checkout" });
    await userEvent.click(
      await screen.findByRole("button", { name: "← groups" }),
    );
    expect(update).toHaveBeenCalledWith({ group: "" });
  });

  it("notes when the group has no traces in range", async () => {
    renderView({ group: "POST /api/checkout" });
    expect(await screen.findByText(/no traces/i)).toBeInTheDocument();
  });

  it("states the list is bounded by the row limit", async () => {
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1cafe", "s1", "POST /api/checkout", "gateway", "100", 412),
    ]);
    renderView({ group: "POST /api/checkout", limit: 250 });
    await screen.findByText("t1cafe");
    expect(screen.getByText(/up to 250 traces/i)).toBeInTheDocument();
  });

  it("labels the identity column Root at trace grain — a row is a whole trace", async () => {
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1cafe", "s1", "POST /api/checkout", "gateway", "100", 412),
    ]);
    renderView({ group: "POST /api/checkout", grain: "traces" });
    await screen.findByText("t1cafe");
    expect(
      screen.getByRole("columnheader", { name: "Root" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: "Span" }),
    ).not.toBeInTheDocument();
  });

  it("labels the identity column Span at span grain — a row is a matching span, not necessarily the root", async () => {
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1cafe", "s1", "charge", "payments", "100", 42),
    ]);
    renderView({ group: "charge", grain: "spans" });
    await screen.findByText("t1cafe");
    expect(
      screen.getByRole("columnheader", { name: "Span" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: "Root" }),
    ).not.toBeInTheDocument();
  });

  it("states the bound in spans at span grain, not traces", async () => {
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1cafe", "s1", "charge", "payments", "100", 42),
    ]);
    renderView({ group: "charge", grain: "spans", limit: 250 });
    await screen.findByText("t1cafe");
    expect(screen.getByText(/up to 250 spans/i)).toBeInTheDocument();
    expect(screen.queryByText(/up to 250 traces/i)).not.toBeInTheDocument();
  });

  it("notes no spans (not traces) for an empty group at span grain", async () => {
    renderView({ group: "charge", grain: "spans" });
    expect(
      await screen.findByText(/no spans for this group/i),
    ).toBeInTheDocument();
  });

  it("renders skeleton rows while the drill-in query is pending", async () => {
    fetchTraceGroupMembers.mockImplementation(() => new Promise(() => {}));
    const { container } = renderWithClient(
      <TracesView
        state={{
          ...DEFAULT_STATE,
          signal: "traces",
          group: "POST /api/checkout",
        }}
        update={vi.fn()}
      />,
    );
    expect(screen.getByRole("table")).toHaveAttribute("aria-busy", "true");
    expect(container.querySelectorAll(".skeleton-bar").length).toBeGreaterThan(
      0,
    );
  });

  it("surfaces member query errors", async () => {
    fetchTraceGroupMembers.mockRejectedValue(new Error("boom"));
    renderView({ group: "POST /api/checkout" });
    expect(await screen.findByRole("alert")).toHaveTextContent(/boom/);
  });

  it("sorts the member list by a clicked column", async () => {
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1cafe", "s1", "POST /api/checkout", "gateway", "100", 412),
      member("t2feed", "s2", "POST /api/checkout", "gateway", "200", 88),
    ]);
    renderView({ group: "POST /api/checkout" });
    await screen.findByText("t1cafe");
    // Newest first by default; duration sorts descending first.
    expect(screen.getAllByRole("row")[1]).toHaveTextContent("t2feed");
    await userEvent.click(screen.getByRole("button", { name: "Duration" }));
    expect(screen.getAllByRole("row")[1]).toHaveTextContent("t1cafe");
    await userEvent.click(screen.getByRole("button", { name: "Duration" }));
    expect(screen.getAllByRole("row")[1]).toHaveTextContent("t2feed");
  });
});

describe("TracesView detail", () => {
  it("renders the waterfall with the error span preselected", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    renderView({ trace: "t1cafe" });
    const spans = await within(
      await screen.findByRole("list", { name: "Spans" }),
    ).findAllByRole("listitem");
    expect(spans).toHaveLength(2);
    // Error span is preselected, so its attributes show in the detail panel.
    expect(screen.getByText("payment.provider")).toBeInTheDocument();
    expect(screen.getByText("stripe")).toBeInTheDocument();
    expect(screen.getByText(/1 error/)).toBeInTheDocument();
  });

  it("colors waterfall bars by span kind and shows a legend, fetched over Query IR", async () => {
    stubFetchRoutes([
      { match: "/tempo/api/traces/t1cafe", body: TRACE_BODY },
      {
        match: "/api/v1/query",
        body: {
          result: "rows",
          window: { start_ns: 0, end_ns: 0 },
          columns: [
            { name: "span_id", type: "string" },
            { name: "span_kind", type: "string" },
          ],
          rows: [
            ["root", "SERVER"],
            ["charge", "CLIENT"],
          ],
        },
      },
    ]);
    renderView({ trace: "t1cafe" });
    await screen.findByRole("list", { name: "Spans" });

    const legend = await screen.findByLabelText("Span kind legend");
    expect(within(legend).getByText("CLIENT")).toBeInTheDocument();
    expect(within(legend).getByText("SERVER")).toBeInTheDocument();

    // The preselected span ("charge", the error span) shows its kind chip
    // in the detail panel too.
    expect(
      within(screen.getByLabelText("Span details")).getByText("CLIENT"),
    ).toBeInTheDocument();
  });

  it("shows a rich tooltip with service, namespace, version, and kind when hovering a span", async () => {
    const chargeSpan = TRACE_BODY.spanSets[0]!.spans[1]!;
    stubFetchRoutes([
      {
        match: "/tempo/api/traces/t1cafe",
        body: {
          ...TRACE_BODY,
          spanSets: [
            {
              matched: 2,
              spans: [
                TRACE_BODY.spanSets[0]!.spans[0]!,
                {
                  ...chargeSpan,
                  attributes: {
                    ...chargeSpan.attributes,
                    "resource.service.namespace": {
                      key: "resource.service.namespace",
                      value: { stringValue: "shop" },
                    },
                    "resource.service.version": {
                      key: "resource.service.version",
                      value: { stringValue: "2.4.1" },
                    },
                  },
                },
              ],
            },
          ],
        },
      },
      {
        match: "/api/v1/query",
        body: {
          result: "rows",
          window: { start_ns: 0, end_ns: 0 },
          columns: [
            { name: "span_id", type: "string" },
            { name: "span_kind", type: "string" },
          ],
          rows: [
            ["root", "SERVER"],
            ["charge", "CLIENT"],
          ],
        },
      },
    ]);
    renderView({ trace: "t1cafe" });
    const spans = await within(
      await screen.findByRole("list", { name: "Spans" }),
    ).findAllByRole("listitem");
    // Wait for the kinds enrichment to land (legend appears with it).
    await screen.findByLabelText("Span kind legend");

    expect(screen.queryByRole("tooltip")).toBeNull();
    fireEvent.pointerMove(spans[1]!, { clientX: 200, clientY: 40 });
    const tip = screen.getByRole("tooltip");
    expect(within(tip).getByText("charge")).toBeInTheDocument();
    const rows = within(tip).getAllByTestId("viz-tip-row");
    expect(rows.map((r) => r.textContent)).toEqual([
      "servicepayments",
      "namespaceshop",
      "version2.4.1",
      "kindCLIENT",
      "duration258 ms",
      "statuserror",
    ]);
    // Kind swatch matches the bar colour for CLIENT spans.
    expect(
      within(rows[3]!).getByTestId("viz-tip-swatch").style.background,
    ).toBe("var(--svc-b)");
    expect(spans[1]).toHaveAttribute("aria-describedby", tip.id);

    // A span without namespace/version keeps the rows, muted, and no kind
    // swatch before enrichment for an unknown kind.
    fireEvent.pointerMove(spans[0]!, { clientX: 200, clientY: 20 });
    const rootRows = within(screen.getByRole("tooltip")).getAllByTestId(
      "viz-tip-row",
    );
    expect(rootRows[1]).toHaveTextContent("namespace–");
    expect(rootRows[1]).toHaveClass("viz-tip-muted");
    expect(rootRows[2]).toHaveTextContent("version–");

    fireEvent.pointerLeave(screen.getByRole("list", { name: "Spans" }));
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("shows each event's time offset from the span start", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    renderView({ trace: "t1cafe" });

    // charge span starts at 1_040_000_000ns; the exception event fires at
    // 1_055_000_000ns — 15ms later.
    expect(await screen.findByText("+15 ms")).toBeInTheDocument();
  });

  it("prefers preselecting an error span that recorded an exception over one that didn't", async () => {
    stubFetchRoutes([
      {
        match: "/tempo/api/traces/tpropagated",
        body: {
          ...TRACE_BODY,
          traceID: "tpropagated",
          spanSets: [
            {
              matched: 2,
              spans: [
                {
                  spanID: "root",
                  startTimeUnixNano: "1000000000",
                  durationNanos: "412000000",
                  name: "POST /api/checkout",
                  serviceName: "gateway",
                  status: "error",
                  attributes: {},
                },
                {
                  spanID: "charge",
                  parentSpanID: "root",
                  startTimeUnixNano: "1040000000",
                  durationNanos: "258000000",
                  name: "charge",
                  serviceName: "payments",
                  status: "error",
                  attributes: {},
                  events: [
                    {
                      name: "exception",
                      attributes: {
                        "exception.message": {
                          key: "exception.message",
                          value: { stringValue: "card declined" },
                        },
                      },
                    },
                  ],
                },
              ],
            },
          ],
        },
      },
    ]);
    renderView({ trace: "tpropagated" });

    expect(
      await screen.findByRole("heading", { name: "charge", level: 4 }),
    ).toBeInTheDocument();
  });

  it("selects a span on click and shows its details", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    renderView({ trace: "t1cafe" });
    const rows = await screen.findAllByRole("listitem");
    await userEvent.click(rows[0]!);
    expect(rows[0]).toHaveAttribute("aria-selected", "true");
    expect(
      screen.getByRole("heading", { name: "POST /api/checkout", level: 4 }),
    ).toBeInTheDocument();
  });

  it("copies span and promoted exception attribute values", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    renderView({ trace: "t1cafe" });

    await screen.findByText("payment.provider");
    await userEvent.click(
      screen.getByRole("button", { name: "Copy value for payment.provider" }),
    );
    await userEvent.click(
      screen.getByRole("button", {
        name: "Copy value for exception.type",
      }),
    );
    await userEvent.click(
      screen.getByRole("button", {
        name: "Copy value for exception.message",
      }),
    );
    await userEvent.click(
      screen.getByRole("button", {
        name: "Copy value for exception.stacktrace",
      }),
    );

    expect(writeText).toHaveBeenNthCalledWith(1, "stripe");
    expect(writeText).toHaveBeenNthCalledWith(2, "PaymentError");
    expect(writeText).toHaveBeenNthCalledWith(3, "card declined");
    expect(writeText).toHaveBeenNthCalledWith(4, "Error: card declined");
  });

  it("groups span attributes separately from resource attributes, sorted within each", async () => {
    stubFetchRoutes([
      {
        match: "/tempo/api/traces/tgrouped",
        body: {
          ...TRACE_BODY,
          traceID: "tgrouped",
          spanSets: [
            {
              matched: 1,
              spans: [
                {
                  spanID: "root",
                  startTimeUnixNano: "1000000000",
                  durationNanos: "412000000",
                  name: "POST /api/checkout",
                  serviceName: "gateway",
                  status: "ok",
                  attributes: {
                    "url.full": {
                      key: "url.full",
                      value: { stringValue: "https://x" },
                    },
                    "session.id": {
                      key: "session.id",
                      value: { stringValue: "abc" },
                    },
                    "resource.telemetry.sdk.version": {
                      key: "resource.telemetry.sdk.version",
                      value: { stringValue: "2.1.0" },
                    },
                    "resource.service.name": {
                      key: "resource.service.name",
                      value: { stringValue: "signaldb-ui" },
                    },
                  },
                },
              ],
            },
          ],
        },
      },
    ]);
    renderView({ trace: "tgrouped" });

    const spanSection = await screen.findByText("Span");
    const resourceSection = await screen.findByText("Resource");
    // Span section (with keys as sent, unprefixed) precedes Resource.
    expect(
      spanSection.compareDocumentPosition(resourceSection) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();

    const dtOrder = screen.getAllByRole("term").map((el) => el.textContent);
    expect(dtOrder).toEqual([
      "session.id",
      "url.full",
      "service.name",
      "telemetry.sdk.version",
    ]);
  });

  it("enriches resolved attribute keys with brief, title and namespace; unknown keys stay bare", async () => {
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
    stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: {
          hits: [],
          resolutions: [
            { key: "k8s.pod.uid", hits: [podUid], primary: podUid },
            { key: "app.order.id", hits: [] },
            { key: "payment.provider", hits: [] },
          ],
        },
      },
      {
        match: "/tempo/api/traces/tsem",
        body: {
          ...TRACE_BODY,
          traceID: "tsem",
          spanSets: [
            {
              matched: 1,
              spans: [
                {
                  spanID: "root",
                  startTimeUnixNano: "1000000000",
                  durationNanos: "412000000",
                  name: "POST /api/checkout",
                  serviceName: "gateway",
                  status: "ok",
                  attributes: {
                    "payment.provider": {
                      key: "payment.provider",
                      value: { stringValue: "stripe" },
                    },
                    "resource.k8s.pod.uid": {
                      key: "resource.k8s.pod.uid",
                      value: { stringValue: "275ecb36" },
                    },
                    "resource.app.order.id": {
                      key: "resource.app.order.id",
                      value: { stringValue: "o-1" },
                    },
                  },
                },
              ],
            },
          ],
        },
      },
    ]);
    renderView({ trace: "tsem" });

    const detail = await screen.findByLabelText("Span details");
    expect(
      await within(detail).findByText("The UID of the Pod."),
    ).toBeInTheDocument();
    expect(
      within(detail).getByText("Kubernetes Attributes"),
    ).toBeInTheDocument();
    expect(within(detail).getByText("otel")).toBeInTheDocument();
    // Unknown keys stay bare, grouped under "Other" after the titled group.
    expect(within(detail).getByText("Other")).toBeInTheDocument();
    const orderTerm = within(detail)
      .getAllByRole("term")
      .find((el) => el.textContent === "app.order.id");
    expect(orderTerm).toBeDefined();
    // The Span section had nothing resolvable, so it renders as before: no
    // sub-heading, key text only.
    expect(
      within(detail)
        .getAllByRole("term")
        .find((el) => el.textContent === "payment.provider"),
    ).toBeDefined();
    expect(within(detail).getAllByText("Other")).toHaveLength(1);
  });

  it("renders raw keys and no error when the schema endpoint fails", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: { error: "boom" },
        status: 500,
      },
      { match: "/tempo/api/traces/t1cafe", body: TRACE_BODY },
    ]);
    renderView({ trace: "t1cafe" });
    const detail = await screen.findByLabelText("Span details");
    expect(
      await within(detail).findByText("payment.provider"),
    ).toBeInTheDocument();
    await new Promise((r) => setTimeout(r, 60));
    expect(within(detail).queryByText(/boom/)).not.toBeInTheDocument();
    expect(within(detail).getByText("payment.provider").textContent).toBe(
      "payment.provider",
    );
  });

  it("resizes the span-detail sidebar by dragging the resizer, clamped and persisted", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    renderView({ trace: "t1cafe" });

    const resizer = await screen.findByRole("separator", {
      name: "Resize span details",
    });
    const body = resizer.parentElement as HTMLElement;
    expect(body.style.getPropertyValue("--span-detail-w")).toBe("320px");

    // Dragging left (negative dx) widens the sidebar, which sits to its right.
    fireEvent.mouseDown(resizer, { clientX: 500 });
    fireEvent.mouseMove(window, { clientX: 400 });
    fireEvent.mouseUp(window);
    expect(body.style.getPropertyValue("--span-detail-w")).toBe("420px");
    expect(localStorage.getItem("signaldb.trace.sidebarWidth")).toBe("420");

    // Clamped to the configured max (320 + 1000 would be 1320, way over 640).
    fireEvent.mouseDown(resizer, { clientX: 500 });
    fireEvent.mouseMove(window, { clientX: -500 });
    fireEvent.mouseUp(window);
    expect(body.style.getPropertyValue("--span-detail-w")).toBe("640px");
  });

  it("pivots to logs filtered by trace_id", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    const update = renderView({ trace: "t1cafe" });
    await userEvent.click(
      await screen.findByRole("button", { name: "Logs for this trace →" }),
    );
    expect(update).toHaveBeenCalledWith({
      signal: "logs",
      trace: "",
      raw: "",
      filters: [{ label: "trace_id", op: "=", value: "t1cafe" }],
    });
  });

  it("links to a profile captured during the selected span", async () => {
    stubFetchRoutes([
      {
        match: "/tempo/api/traces/t1cafe",
        body: {
          ...TRACE_BODY,
          profiles: [
            {
              profileID: "prof-1",
              timeUnixNano: "1040000000",
              durationNano: "258000000",
              sampleType: "cpu",
              sampleUnit: "nanoseconds",
              serviceName: "payments",
              spanID: "charge",
            },
          ],
        },
      },
    ]);
    // "charge" (the error span with a recorded exception) is preselected.
    const update = renderView({ trace: "t1cafe" });

    await userEvent.click(
      await screen.findByRole("button", { name: "Profile: cpu →" }),
    );
    expect(update).toHaveBeenCalledWith(
      { signal: "profiles", trace: "", profileId: "prof-1" },
      { push: true },
    );
  });

  it("shows no profile link when the selected span has none", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    renderView({ trace: "t1cafe" });
    await screen.findByRole("button", { name: "Logs for this trace →" });
    expect(
      screen.queryByRole("button", { name: /^Profile:/ }),
    ).not.toBeInTheDocument();
  });

  it("navigates back to the search list", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    const update = renderView({ trace: "t1cafe" });
    await userEvent.click(
      await screen.findByRole("button", { name: "← traces" }),
    );
    expect(update).toHaveBeenCalledWith({ trace: "" });
  });

  it("shows a dedicated not-found page for a 404, with a way back to search", async () => {
    stubFetchRoutes([
      {
        match: "/tempo/api/traces/missing",
        body: { error: "Trace not found", errorType: "not_found" },
        status: 404,
      },
    ]);
    const update = renderView({ trace: "missing" });
    expect(
      await screen.findByRole("heading", { name: "Trace not found" }),
    ).toBeInTheDocument();
    // The raw API error body isn't dumped onto the page.
    expect(screen.queryByText(/errorType/)).toBeNull();

    await userEvent.click(screen.getByRole("button", { name: "← traces" }));
    expect(update).toHaveBeenCalledWith({ trace: "" });
  });

  it("surfaces non-404 trace lookup failures verbatim", async () => {
    // 500 is not retried by `retryingFetch` (only 429 and, for GET,
    // 502/503/504 are), so the failure surfaces immediately.
    stubFetchRoutes([
      {
        match: "/tempo/api/traces/broken",
        body: { error: "Flight backend unavailable" },
        status: 500,
      },
    ]);
    renderView({ trace: "broken" });
    expect(await screen.findByRole("alert")).toHaveTextContent(/500/);
  });

  it("renders a skeleton while the trace is loading", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    const { container } = renderWithClient(
      <TracesView
        state={{ ...DEFAULT_STATE, signal: "traces", trace: "t1cafe" }}
        update={vi.fn()}
      />,
    );
    expect(container.querySelector('[aria-busy="true"]')).toBeTruthy();
    expect(container.querySelectorAll(".skeleton-bar").length).toBeGreaterThan(
      0,
    );
  });
});

const VOLUME_BODY = {
  result: "series",
  window: {
    start_ns: 1_700_000_000_000_000_000,
    end_ns: 1_700_003_600_000_000_000,
  },
  step_ns: 300_000_000_000,
  series: [
    {
      labels: { status_code: "Unspecified" },
      points: [
        [1_700_000_000_000_000_000, 2018],
        [1_700_000_300_000_000_000, 41],
      ],
    },
    {
      labels: { status_code: "Error" },
      points: [[1_700_000_300_000_000_000, 7]],
    },
  ],
};

describe("TracesView span-volume chart", () => {
  const routes = [{ match: "/api/v1/query", body: VOLUME_BODY }];

  it("renders span volume stacked by status", async () => {
    stubFetchRoutes(routes);
    renderView();
    await screen.findByRole("img", { name: /span volume/i });
    expect(screen.getByRole("button", { name: "Histogram" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    const cols = screen.getAllByTestId("svol-col");
    expect(cols.length).toBeGreaterThan(1);
    // 2018 unset, then 41 unset + 7 error.
    expect(cols[0]).toHaveAccessibleName(/2,018 spans/);
  });

  it("switches the span volume visualization to an area chart without refetching", async () => {
    stubFetchRoutes(routes);
    renderView();
    await screen.findByRole("img", { name: /span volume/i });

    const volumeQueries = () =>
      vi
        .mocked(globalThis.fetch)
        .mock.calls.map(([input]) => input)
        .filter(
          (input): input is Request =>
            input instanceof Request && input.url.includes("/api/v1/query"),
        ).length;
    const queryCount = volumeQueries();

    await userEvent.click(screen.getByRole("button", { name: "Area" }));

    expect(screen.getByRole("button", { name: "Area" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(
      screen.getByRole("img", { name: /span volume area chart/i }),
    ).toBeInTheDocument();
    const area = screen.getByTestId("trace-volume-area");
    expect(
      area.querySelector('.trace-area-series[stroke="var(--err-bar)"]'),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("svol-col")).not.toBeInTheDocument();
    expect(volumeQueries()).toBe(queryCount);
  });

  it("submits a v2 native IR heatmap only when switching views", async () => {
    stubFetchRoutes(routes);
    renderView();
    await screen.findByRole("img", { name: /span volume/i });
    const heatmapFetch = stubFetchRoutes([
      {
        match: "/api/v1/query",
        body: {
          result: "heatmap",
          window: { start_ns: 0, end_ns: 60_000_000_000 },
          heatmap: {
            x: { step_ns: 60_000_000_000, align: "epoch" },
            y: {
              of: "duration",
              type: "duration_ns",
              bounds: [10_000_000],
              overflow: true,
            },
            value: "count",
            cells: [{ time_bucket_ns: 0, duration_bucket: 0, count: 2 }],
          },
        },
      },
    ]);

    await userEvent.click(screen.getByRole("button", { name: "Heatmap" }));

    expect(screen.getByRole("button", { name: "Heatmap" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    const heatmap = screen.getByTestId("trace-volume-heatmap");
    expect(heatmap).toHaveAccessibleName(/span latency heatmap/i);
    expect(
      within(heatmap).getAllByTestId("trace-volume-heatmap-cell").length,
    ).toBeGreaterThan(0);
    const cell = within(heatmap).getByLabelText(/2 spans/i);
    expect(cell).toHaveAttribute("fill", "var(--info-bar)");
    expect(
      within(heatmap).getByText(/intensity represents span count/i),
    ).toBeInTheDocument();
    const requests = heatmapFetch.mock.calls
      .map(([input]) => input)
      .filter(
        (input): input is Request =>
          input instanceof Request && input.url.includes("/api/v1/query"),
      );
    expect(requests).toHaveLength(1);
    const body = await requests[0]!.clone().json();
    expect(body).toEqual(
      expect.objectContaining({ irVersion: 2, result: "heatmap" }),
    );
    expect(body.pipeline.at(-1)).toEqual({
      heatmap: expect.objectContaining({
        x: { step: expect.any(String), align: "epoch" },
        y: expect.objectContaining({ of: "duration", overflow: true }),
        value: { fn: "count", as: "count" },
      }),
    });
  });

  it("renders the heatmap when the independent volume query fails", async () => {
    vi.spyOn(traceVolumeApi, "fetchTraceVolume").mockRejectedValue(
      new Error("volume failed"),
    );
    vi.spyOn(traceVolumeApi, "fetchTraceLatencyHeatmap").mockResolvedValue({
      window: { start_ns: 0, end_ns: 60_000_000_000 },
      x: { step_ns: 60_000_000_000, align: "epoch" },
      y: {
        of: "duration",
        type: "duration_ns",
        bounds: [10_000_000],
        overflow: true,
      },
      value: "count",
      cells: [{ time_bucket_ns: 0, duration_bucket: 0, count: 2 }],
    });
    renderView();

    await userEvent.click(screen.getByRole("button", { name: "Heatmap" }));

    expect(
      await screen.findByTestId("trace-volume-heatmap"),
    ).toBeInTheDocument();
  });

  it("asks for the volume aggregate without a row limit", async () => {
    stubFetchRoutes(routes);
    renderView({ limit: 25 });
    await screen.findByRole("img", { name: /span volume/i });
    const body = vi
      .mocked(globalThis.fetch)
      .mock.calls.map(([input]) => input)
      .find(
        (i): i is Request =>
          i instanceof Request && i.url.includes("/api/v1/query"),
      );
    expect(body).toBeDefined();
    const doc = await body!.clone().json();
    expect(doc.from).toBe("traces");
    expect(doc.result).toBe("series");
    expect(JSON.stringify(doc)).not.toContain("limit");
  });

  it("keeps the chart visible when the group table has nothing to show", async () => {
    stubFetchRoutes(routes);
    renderView();
    await screen.findByRole("img", { name: /span volume/i });
    expect(
      screen.getByText(/no groups in this time range/i),
    ).toBeInTheDocument();
  });
});

describe("TracesView facet filtering", () => {
  const FACET_BODY = {
    result: "table",
    window: { start_ns: 0, end_ns: 1 },
    columns: [
      { name: "v", type: "string" },
      { name: "n", type: "int64" },
    ],
    rows: [["gateway", 2]],
  };
  const routes = [{ match: "/api/v1/query", body: FACET_BODY }];

  it("sends the compiled TraceQL selector as the active filter's query", async () => {
    fetchTraceGroups.mockResolvedValue({
      groups: [group(["POST /api/checkout"], 1, 0, 1, 1, "1")],
      truncated: false,
    });
    stubFetchRoutes(routes);
    renderView({ traceFilters: [{ field: "service.name", value: "gateway" }] });
    await screen.findByText("POST /api/checkout");
    expect(fetchTraceGroups).toHaveBeenLastCalledWith(
      expect.anything(),
      expect.anything(),
      [{ field: "service.name", value: "gateway" }],
      expect.anything(),
      expect.anything(),
    );
  });

  it("shows an active filter as a removable chip", async () => {
    stubFetchRoutes(routes);
    const update = renderView({
      traceFilters: [{ field: "service.name", value: "gateway" }],
    });
    const chip = await screen.findByRole("button", {
      name: "Remove filter service.name = gateway",
    });
    await userEvent.click(chip);
    expect(update).toHaveBeenCalledWith({ traceFilters: [], group: "" });
  });

  it("narrows the span-volume chart with the same filters", async () => {
    stubFetchRoutes(routes);
    renderView({ traceFilters: [{ field: "service.name", value: "gateway" }] });
    // Sync point: the group table's default (empty) mock has resolved once
    // this text is up, and the volume fetch fires alongside it.
    await screen.findByText(/no groups/i);
    const bodies = await Promise.all(
      vi
        .mocked(globalThis.fetch)
        .mock.calls.map(([i]) => i)
        .filter(
          (i): i is Request =>
            i instanceof Request && i.url.includes("/api/v1/query"),
        )
        .map((r) => r.clone().json()),
    );
    const volume = bodies.find((b) =>
      JSON.stringify(b).includes("status.code"),
    );
    expect(JSON.stringify(volume?.pipeline?.[0])).toContain("gateway");
  });
});
