import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { ProfilesView } from "./ProfilesView";

afterEach(() => {
  vi.unstubAllGlobals();
});

const TYPES = [
  {
    ID: "cpu:cpu:nanoseconds",
    name: "cpu",
    sampleType: "cpu",
    sampleUnit: "nanoseconds",
  },
];
const SERVICES = { names: ["signaldb-router", "signaldb-querier"] };
const LABEL_NAMES = { names: ["service_name", "region"] };
const LABEL_VALUES = { names: ["eu", "us"] };

function flamegraphBody(fg: {
  names: string[];
  levels: number[][];
  total: number;
  max_self: number;
  truncated?: boolean;
  locations?: Array<{ file: string; line: number } | null>;
}) {
  return {
    result: "flamegraph",
    window: { start_ns: 0, end_ns: 0 },
    flamegraph: { truncated: false, ...fg },
  };
}

const FLAMEGRAPH = flamegraphBody({
  names: ["total", "main", "work"],
  levels: [
    [0, 100, 0, 0],
    [0, 100, 20, 1],
    [0, 80, 80, 2],
  ],
  total: 100,
  max_self: 80,
});

const DISCOVERY_ROUTES = [
  { match: "/pyroscope/profile-types", body: TYPES },
  { match: "/pyroscope/label-names", body: LABEL_NAMES },
  {
    match: /\/pyroscope\/label-values\?.*label=service_name/,
    body: SERVICES,
    method: "GET" as const,
  },
];

function state(overrides: Partial<ExploreState> = {}): ExploreState {
  return { ...DEFAULT_STATE, signal: "profiles", ...overrides };
}

describe("ProfilesView", () => {
  it("populates selectors and renders a flame graph", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);

    // Service and profile-type selectors fill from the discovery endpoints.
    expect(
      await screen.findByRole("option", { name: "signaldb-router" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "cpu · nanoseconds" }),
    ).toBeInTheDocument();

    // Flame frames render with their names (accessible name — the visible
    // label may be simplified, and the name also appears in the hover
    // tooltip, so a plain text query would be ambiguous).
    expect(
      await screen.findByRole("button", { name: "main" }),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "work" })).toBeInTheDocument();
  });

  // The view no longer waits on GitHub-linked state before handing
  // FlameGraph a tenant — SourceSnippet gates the trigger itself (see
  // lib/useSourceContextEnabled.ts) — so the Top-functions Source column
  // appears whenever the view has a tenant, whether or not GitHub ends up
  // being linked for it.
  it("passes the view's tenant to the flame graph's Top-functions table", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      {
        match: "/api/v1/query",
        body: flamegraphBody({
          names: ["total", "main", "work"],
          levels: [
            [0, 100, 0, 0],
            [0, 100, 20, 1],
            [0, 80, 80, 2],
          ],
          total: 100,
          max_self: 80,
          locations: [null, { file: "src/main.rs", line: 10 }, null],
        }),
      },
      {
        match: "/source-context",
        method: "GET",
        body: { configured: true, linked: true },
      },
    ]);

    renderWithClient(
      <ProfilesView state={state({ tenant: "acme" })} update={vi.fn()} />,
    );
    await screen.findByRole("button", { name: "main" });
    await userEvent.click(screen.getByRole("tab", { name: "Top functions" }));

    expect(
      await screen.findByRole("button", { name: "View source" }),
    ).toBeInTheDocument();
  });

  it("submits a where-pipeline scoped to the selected service and sample type", async () => {
    const fetchMock = stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);

    renderWithClient(
      <ProfilesView
        state={state({ profileService: "signaldb-router" })}
        update={vi.fn()}
      />,
    );
    await screen.findByRole("button", { name: "main" });

    const irCall = fetchMock.mock.calls.find((c) =>
      String((c[0] as Request).url).includes("/api/v1/query"),
    );
    const body = await (irCall![0] as Request).clone().json();
    expect(body.from).toBe("profiles");
    expect(body.result).toBe("flamegraph");
    expect(body.pipeline).toEqual([
      { where: { field: "service.name", op: "eq", value: "signaldb-router" } },
      { where: { field: "sample.type", op: "eq", value: "cpu" } },
    ]);
  });

  it("zooms into a frame on click and steps back out via the breadcrumb", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);

    const work = await screen.findByRole("button", { name: /work/ });
    await userEvent.click(work);

    // Breadcrumb shows the full ancestor path down to the focused frame:
    // "root" (reset) plus every zoomed-into frame's name — scope the
    // lookup to the breadcrumb, since "main" also labels a flame bar.
    const crumbs = () => within(screen.getByLabelText("Zoom path"));
    expect(crumbs().getByRole("button", { name: "root" })).toBeInTheDocument();
    expect(crumbs().getByRole("button", { name: "main" })).toBeInTheDocument();
    expect(crumbs().getByRole("button", { name: "work" })).toBeInTheDocument();

    // Stepping back one level (via the ancestor crumb) re-widens the view
    // without dropping all the way to the root: "work" drops out, "main"
    // remains as the new focus.
    await userEvent.click(crumbs().getByRole("button", { name: "main" }));
    expect(screen.queryByLabelText("Zoom path")).toBeInTheDocument();
    expect(
      crumbs().queryByRole("button", { name: "work" }),
    ).not.toBeInTheDocument();
    expect(crumbs().getByRole("button", { name: "main" })).toBeInTheDocument();

    // "root" clears the zoom stack entirely — the breadcrumb disappears.
    await userEvent.click(crumbs().getByRole("button", { name: "root" }));
    expect(screen.queryByLabelText("Zoom path")).not.toBeInTheDocument();
  });

  it("updates URL state when a service is chosen", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);
    const update = vi.fn();

    renderWithClient(<ProfilesView state={state()} update={update} />);
    await screen.findByRole("option", { name: "signaldb-router" });

    await userEvent.selectOptions(
      screen.getByLabelText("Profile service"),
      "signaldb-querier",
    );
    expect(update).toHaveBeenCalledWith({ profileService: "signaldb-querier" });
  });

  it("highlights matching frames and reports the matched share", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
    await screen.findByRole("button", { name: "work" });

    await userEvent.type(screen.getByLabelText("Highlight frames"), "work");

    // "work" (self 80 of 100 ticks) is the only match.
    expect(screen.getByText(/80\.0% matched/)).toBeInTheDocument();
    // Non-matching frames are dimmed; the match is not.
    expect(screen.getByRole("button", { name: /main/ })).toHaveClass("dim");
    expect(screen.getByRole("button", { name: /work/ })).not.toHaveClass("dim");
  });

  it("shows an empty state when the profile has no frames", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      {
        match: "/api/v1/query",
        body: flamegraphBody({
          names: ["total"],
          levels: [[0, 0, 0, 0]],
          total: 0,
          max_self: 0,
        }),
      },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
    expect(
      await screen.findByText(/No profiles in this range/),
    ).toBeInTheDocument();
  });

  it("renders a genuine single-frame profile instead of calling it empty", async () => {
    // Regression: isEmpty used to key off levels.length <= 1, which
    // misclassified a real root-only profile (nonzero self time, no
    // children) as empty. numTicks is the correct signal.
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      {
        match: "/api/v1/query",
        body: flamegraphBody({
          names: ["total"],
          levels: [[0, 1, 1, 0]],
          total: 1,
          max_self: 1,
        }),
      },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);

    expect(
      await screen.findByRole("button", { name: "total" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/No profiles in this range/),
    ).not.toBeInTheDocument();
  });

  it("prompts to enable self-profiling when no types exist", async () => {
    stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: [] },
      { match: "/pyroscope/label-names", body: { names: [] } },
      { match: "/pyroscope/label-values", body: { names: [] } },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
    expect(await screen.findByText(/profiles_enabled/)).toBeInTheDocument();
  });

  it("does not show the no-profiles note alongside a failed types fetch", async () => {
    stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: {}, status: 500 },
      { match: "/pyroscope/label-names", body: { names: [] } },
      { match: "/pyroscope/label-values", body: { names: [] } },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
    expect(await screen.findByRole("alert")).toBeInTheDocument();
    expect(screen.queryByText(/profiles_enabled/)).not.toBeInTheDocument();
  });

  it("shows a truncation note when the flamegraph was capped", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      {
        match: "/api/v1/query",
        body: flamegraphBody({
          names: ["total"],
          levels: [[0, 1, 1, 0]],
          total: 1,
          max_self: 1,
          truncated: true,
        }),
      },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
    expect(
      await screen.findByText(/Too many matching profiles/),
    ).toBeInTheDocument();
  });

  it("choosing an attribute label clears any prior value and notifies the URL state", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);
    const update = vi.fn();

    renderWithClient(<ProfilesView state={state()} update={update} />);
    await screen.findByRole("option", { name: "signaldb-router" });

    await userEvent.selectOptions(
      screen.getByLabelText("Attribute label"),
      "region",
    );
    expect(update).toHaveBeenCalledWith({
      profileMatcherLabel: "region",
      profileMatcherValue: "",
    });
  });

  it("choosing an attribute value notifies the URL state", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      {
        match: /\/pyroscope\/label-values\?.*label=region/,
        body: LABEL_VALUES,
        method: "GET",
      },
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);
    const update = vi.fn();

    renderWithClient(
      <ProfilesView
        state={state({ profileMatcherLabel: "region" })}
        update={update}
      />,
    );

    await screen.findByRole("option", { name: "eu" });
    await userEvent.selectOptions(
      screen.getByLabelText("Attribute value"),
      "eu",
    );
    expect(update).toHaveBeenCalledWith({ profileMatcherValue: "eu" });
  });

  it("adds an attribute where-stage to the flamegraph query once a matcher is set", async () => {
    const fetchMock = stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);

    renderWithClient(
      <ProfilesView
        state={state({
          profileMatcherLabel: "region",
          profileMatcherValue: "eu",
        })}
        update={vi.fn()}
      />,
    );
    await screen.findByRole("button", { name: "main" });

    const irCall = fetchMock.mock.calls.find((c) =>
      String((c[0] as Request).url).includes("/api/v1/query"),
    );
    const body = await (irCall![0] as Request).clone().json();
    expect(body.pipeline).toContainEqual({
      where: { field: "region", op: "eq", value: "eu" },
    });
  });

  it("renders baseline and comparison panes in compare mode", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);

    renderWithClient(
      <ProfilesView state={state({ profileCompare: true })} update={vi.fn()} />,
    );

    // Both panes render the same stubbed flamegraph independently.
    const panes = await screen.findAllByRole("button", { name: /work/ });
    expect(panes).toHaveLength(2);
    // "Baseline" also labels the range picker field, so scope to the two
    // rendered flame panes.
    const titles = document.querySelectorAll(
      ".profiles-compare-pane .flame-title",
    );
    expect(Array.from(titles).map((t) => t.textContent)).toEqual([
      "Baseline",
      "Comparison",
    ]);
  });

  it("toggling Compare flips profileCompare in URL state", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);
    const update = vi.fn();

    renderWithClient(<ProfilesView state={state()} update={update} />);
    await screen.findByRole("button", { name: "main" });

    await userEvent.click(screen.getByRole("checkbox", { name: "Compare" }));
    // Also seeds a distinct baseline (see the next test) — the default
    // baseline starts equal to `range`, which would otherwise make both
    // panes identical the moment Compare turns on.
    expect(update).toHaveBeenCalledWith(
      expect.objectContaining({ profileCompare: true }),
    );
  });

  it("switching on Compare defaults the baseline to the window before the current range", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);
    const update = vi.fn();

    renderWithClient(<ProfilesView state={state()} update={update} />);
    await screen.findByRole("button", { name: "main" });

    await userEvent.click(screen.getByRole("checkbox", { name: "Compare" }));

    const [patch] = update.mock.calls[0] as [
      { profileCompare: boolean; profileBaseline: { type: string; fromMs: number; toMs: number } },
    ];
    expect(patch.profileCompare).toBe(true);
    expect(patch.profileBaseline.type).toBe("absolute");
    // Immediately preceding, same length as the 1h default range.
    const span = patch.profileBaseline.toMs - patch.profileBaseline.fromMs;
    expect(span).toBe(3600_000);
  });

  it("leaves an already-distinct baseline alone when toggling Compare", async () => {
    stubFetchRoutes([
      ...DISCOVERY_ROUTES,
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);
    const update = vi.fn();
    const distinctBaseline = {
      type: "absolute" as const,
      fromMs: 1,
      toMs: 2,
    };

    renderWithClient(
      <ProfilesView
        state={state({ profileBaseline: distinctBaseline })}
        update={update}
      />,
    );
    await screen.findByRole("button", { name: "main" });

    await userEvent.click(screen.getByRole("checkbox", { name: "Compare" }));
    expect(update).toHaveBeenCalledWith({ profileCompare: true });
  });

  it("renders a single profile by id and offers a way back", async () => {
    stubFetchRoutes([{ match: "/api/v1/query", body: FLAMEGRAPH }]);
    const update = vi.fn();

    renderWithClient(
      <ProfilesView state={state({ profileId: "abc123" })} update={update} />,
    );

    expect(await screen.findByText("abc123")).toBeInTheDocument();
    expect(
      await screen.findByRole("button", { name: "work" }),
    ).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /profiles/ }));
    expect(update).toHaveBeenCalledWith({ profileId: "" });
  });

  it("carries the unit from the named profile type into a by-id profile's tooltip", async () => {
    stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: TYPES },
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);

    renderWithClient(
      <ProfilesView
        state={state({ profileId: "abc123", profileType: TYPES[0]!.ID })}
        update={vi.fn()}
      />,
    );

    const work = await screen.findByRole("button", { name: "work" });
    fireEvent.pointerMove(work, { clientX: 10, clientY: 10 });
    const tooltip = screen.getByRole("tooltip");
    // TYPES[0].sampleUnit is "nanoseconds"; self=80 ticks -> "80ns", not a
    // bare "80" the way an unknown/empty unit would render.
    expect(tooltip).toHaveTextContent("80ns");
  });

  it("refetches the by-id profile's type lookup under a new tenant instead of reusing the previous tenant's cache", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: TYPES },
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const stateAcme = state({
      profileId: "abc123",
      profileType: TYPES[0]!.ID,
      tenant: "acme",
      dataset: "production",
    });
    const stateGlobex = state({
      profileId: "abc123",
      profileType: TYPES[0]!.ID,
      tenant: "globex",
      dataset: "main",
    });
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <ProfilesView state={stateAcme} update={vi.fn()} />
      </QueryClientProvider>,
    );
    const profileTypesCalls = () =>
      fetchMock.mock.calls.filter((call) => {
        const req = call[0];
        const url = req instanceof Request ? req.url : String(req);
        return url.includes("/pyroscope/profile-types");
      }).length;

    await screen.findByRole("button", { name: "work" });
    await waitFor(() => expect(profileTypesCalls()).toBeGreaterThan(0));
    const callsForAcme = profileTypesCalls();

    rerender(
      <QueryClientProvider client={client}>
        <ProfilesView state={stateGlobex} update={vi.fn()} />
      </QueryClientProvider>,
    );

    await waitFor(() =>
      expect(profileTypesCalls()).toBeGreaterThan(callsForAcme),
    );
  });

  it("carries the unit passed directly (from a trace's linked-profile action) into a by-id profile's tooltip", async () => {
    // No /pyroscope/profile-types stub: TracesView's link carries the unit
    // straight from the trace's ProfileSummaryView, which has no type id for
    // a `profileType` lookup to resolve — the direct unit must be enough on
    // its own, with no fallback fetch.
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/query", body: FLAMEGRAPH },
    ]);

    renderWithClient(
      <ProfilesView
        state={state({ profileId: "abc123", profileUnit: "nanoseconds" })}
        update={vi.fn()}
      />,
    );

    const work = await screen.findByRole("button", { name: "work" });
    fireEvent.pointerMove(work, { clientX: 10, clientY: 10 });
    const tooltip = screen.getByRole("tooltip");
    expect(tooltip).toHaveTextContent("80ns");
    expect(
      fetchMock.mock.calls.some(([input]) =>
        String(input instanceof Request ? input.url : input).includes(
          "/pyroscope/profile-types",
        ),
      ),
    ).toBe(false);
  });

  it("shows a not-found message for an unknown profile id", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/query",
        body: flamegraphBody({ names: [], levels: [], total: 0, max_self: 0 }),
      },
    ]);

    renderWithClient(
      <ProfilesView state={state({ profileId: "missing" })} update={vi.fn()} />,
    );
    expect(await screen.findByText(/not found/)).toBeInTheDocument();
  });

  describe("collapsing small frames", () => {
    // total(1000) -> main(997, self 997), tiny(3, self 3) — "tiny" is
    // 0.3% of root, below the default 0.5% collapse threshold.
    const SPARSE = flamegraphBody({
      names: ["total", "main", "tiny"],
      levels: [
        [0, 1000, 0, 0],
        [0, 997, 997, 1, 0, 3, 3, 2],
      ],
      total: 1000,
      max_self: 997,
    });

    it("folds a below-threshold frame into (other) by default", async () => {
      stubFetchRoutes([
        ...DISCOVERY_ROUTES,
        { match: "/api/v1/query", body: SPARSE },
      ]);

      renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);

      expect(
        await screen.findByRole("button", { name: "(other)" }),
      ).toBeInTheDocument();
      expect(
        screen.queryByRole("button", { name: "tiny" }),
      ).not.toBeInTheDocument();
    });

    it("reveals the folded frame when the threshold is turned off", async () => {
      stubFetchRoutes([
        ...DISCOVERY_ROUTES,
        { match: "/api/v1/query", body: SPARSE },
      ]);

      renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
      await screen.findByRole("button", { name: "(other)" });

      await userEvent.selectOptions(
        screen.getByLabelText("Collapse small frames"),
        "Off",
      );

      expect(
        await screen.findByRole("button", { name: "tiny" }),
      ).toBeInTheDocument();
      expect(
        screen.queryByRole("button", { name: "(other)" }),
      ).not.toBeInTheDocument();
    });
  });

  describe("top functions view", () => {
    it("lists functions sorted by self time, highest first", async () => {
      stubFetchRoutes([
        ...DISCOVERY_ROUTES,
        { match: "/api/v1/query", body: FLAMEGRAPH },
      ]);

      renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
      await screen.findByRole("button", { name: "work" });

      await userEvent.click(screen.getByRole("tab", { name: "Top functions" }));

      const rows = await screen.findAllByRole("row");
      // rows[0] is the header; work (self 80) outranks main (self 20).
      expect(
        within(rows[1]!).getByRole("button", { name: "work" }),
      ).toBeInTheDocument();
      expect(
        within(rows[2]!).getByRole("button", { name: "main" }),
      ).toBeInTheDocument();
    });

    it("clicking a function row highlights it back on the flame graph", async () => {
      stubFetchRoutes([
        ...DISCOVERY_ROUTES,
        { match: "/api/v1/query", body: FLAMEGRAPH },
      ]);

      renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
      await screen.findByRole("button", { name: "work" });
      await userEvent.click(screen.getByRole("tab", { name: "Top functions" }));

      await userEvent.click(screen.getByRole("button", { name: "work" }));

      // Back on the flame graph, "work" is highlighted (not dimmed) and
      // the other frames are.
      expect(screen.getByRole("tab", { name: "Flame graph" })).toHaveAttribute(
        "aria-selected",
        "true",
      );
      expect(screen.getByLabelText("Highlight frames")).toHaveValue("work");
      expect(screen.getByRole("button", { name: /main/ })).toHaveClass("dim");
    });
  });

  describe("long Rust symbol names", () => {
    const LONG_NAME =
      "<std::hash::random::RandomState as core::hash::BuildHasher>::hash_one::<&uuid::Uuid>";
    const LONG_NAME_PROFILE = flamegraphBody({
      names: ["total", LONG_NAME],
      levels: [
        [0, 100, 0, 0],
        [0, 100, 100, 1],
      ],
      total: 100,
      max_self: 100,
    });

    it("shows a simplified label on the bar but the full name in the tooltip and accessible name", async () => {
      stubFetchRoutes([
        ...DISCOVERY_ROUTES,
        { match: "/api/v1/query", body: LONG_NAME_PROFILE },
      ]);

      renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);

      // Accessible name (used for hit-testing/screen readers) is the real,
      // untruncated symbol — findable even though the label is shortened.
      const bar = await screen.findByRole("button", { name: LONG_NAME });
      expect(bar).toHaveTextContent("RandomState::hash_one");
      expect(bar).not.toHaveTextContent(LONG_NAME);

      // The hover tooltip is the shared VizTooltip: it follows the pointer
      // (not the frame), and carries the full name plus self/total —
      // unsimplified, unlike the bar's own label.
      expect(screen.queryByRole("tooltip")).toBeNull();
      fireEvent.pointerMove(bar, { clientX: 120, clientY: 30 });
      const tooltip = screen.getByRole("tooltip");
      expect(within(tooltip).getByText(LONG_NAME)).toBeInTheDocument();
      const rows = within(tooltip).getAllByTestId("viz-tip-row");
      expect(rows.map((r) => r.textContent)).toEqual([
        "self100ns (100.0%)",
        "total100ns (100.0%)",
      ]);
      expect(bar).toHaveAttribute("aria-describedby", tooltip.id);
      // Anchored at the pointer, offset from it, not at the frame's edge.
      expect(tooltip.style.left).toBe("120px");
      expect(tooltip.style.top).toBe("30px");

      fireEvent.pointerLeave(bar.closest(".flame-rows")!);
      expect(screen.queryByRole("tooltip")).toBeNull();
    });

    it("shows the same tooltip on the top-functions rows", async () => {
      stubFetchRoutes([
        ...DISCOVERY_ROUTES,
        { match: "/api/v1/query", body: LONG_NAME_PROFILE },
      ]);
      renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
      await screen.findByRole("button", { name: LONG_NAME });
      await userEvent.click(screen.getByRole("tab", { name: "Top functions" }));
      const cell = screen.getByRole("button", { name: LONG_NAME });
      fireEvent.pointerMove(cell, { clientX: 50, clientY: 50 });
      const tooltip = screen.getByRole("tooltip");
      expect(within(tooltip).getByText(LONG_NAME)).toBeInTheDocument();
      expect(within(tooltip).getAllByTestId("viz-tip-row")).toHaveLength(2);
    });
  });
});
