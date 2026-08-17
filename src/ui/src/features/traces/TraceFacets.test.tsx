import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resetSemanticsCache } from "../../hooks/useSemantics";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { TraceFacets } from "./TraceFacets";

afterEach(() => {
  vi.unstubAllGlobals();
  resetSemanticsCache();
});

const range = { fromMs: 0, toMs: 3600_000 };

const table = (rows: [string | null, number][]) => ({
  result: "table",
  window: { start_ns: 0, end_ns: 1 },
  columns: [
    { name: "v", type: "string" },
    { name: "n", type: "int64" },
  ],
  rows,
});

function renderFacets(props: Partial<Parameters<typeof TraceFacets>[0]> = {}) {
  const onAddFilter = vi.fn();
  const onRemoveFilter = vi.fn();
  renderWithClient(
    <TraceFacets
      range={range}
      rangeKey="1h|t|d"
      filters={[]}
      onAddFilter={onAddFilter}
      onRemoveFilter={onRemoveFilter}
      {...props}
    />,
  );
  return { onAddFilter, onRemoveFilter };
}

describe("TraceFacets", () => {
  it("offers an errors-only checkbox that toggles the status = Error filter", async () => {
    stubFetchRoutes([{ match: "/api/v1/query", body: table([]) }]);
    const { onAddFilter } = renderFacets();
    const box = screen.getByRole("checkbox", { name: "Errors only" });
    expect(box).not.toBeChecked();
    await userEvent.click(box);
    // Same filter the status facet would add, so groups, list, volume, and
    // facet counts all narrow together.
    expect(onAddFilter).toHaveBeenCalledWith({
      field: "status",
      value: "Error",
    });
  });

  it("shows the errors-only checkbox checked while the filter is active and clears it on click", async () => {
    stubFetchRoutes([{ match: "/api/v1/query", body: table([]) }]);
    const { onRemoveFilter } = renderFacets({
      filters: [{ field: "status", value: "Error" }],
    });
    const box = screen.getByRole("checkbox", { name: "Errors only" });
    expect(box).toBeChecked();
    await userEvent.click(box);
    expect(onRemoveFilter).toHaveBeenCalledWith({
      field: "status",
      value: "Error",
    });
  });

  it("lists the enumerable facet fields", () => {
    stubFetchRoutes([{ match: "/api/v1/query", body: table([]) }]);
    renderFacets();
    expect(
      screen.getByRole("button", { name: /service\.name/ }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /span\.name/ }),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /status/ })).toBeInTheDocument();
  });

  it("shows values with counts, most frequent first", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/query",
        body: table([
          ["signaldb", 2903],
          ["signaldb-ui", 980],
        ]),
      },
    ]);
    renderFacets();
    await userEvent.click(
      screen.getByRole("button", { name: /service\.name/ }),
    );
    const values = await screen.findAllByTestId("facet-value");
    expect(values[0]).toHaveTextContent("signaldb");
    expect(values[0]).toHaveTextContent("2,903");
    expect(values[1]).toHaveTextContent("980");
  });

  it("lists every span kind as a checkbox with its count, checked for the selected ones", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/query",
        body: table([
          ["Server", 218135],
          ["Internal", 83586],
        ]),
      },
    ]);
    const { onAddFilter, onRemoveFilter } = renderFacets({
      filters: [
        { field: "kind", value: "Server" },
        { field: "kind", value: "Client" },
      ],
    });
    await userEvent.click(screen.getByRole("button", { name: /span\.kind/ }));
    // Every kind is offered, in the fixed order, even with no data for it.
    const boxes = await screen.findAllByRole("checkbox", {
      name: /^(Server|Client|Internal|Producer|Consumer)/,
    });
    expect(boxes.map((b) => b.getAttribute("aria-label"))).toEqual([
      "Server",
      "Client",
      "Internal",
      "Producer",
      "Consumer",
    ]);
    expect(screen.getByRole("checkbox", { name: "Server" })).toBeChecked();
    expect(screen.getByRole("checkbox", { name: "Client" })).toBeChecked();
    expect(
      screen.getByRole("checkbox", { name: "Internal" }),
    ).not.toBeChecked();
    const values = screen.getAllByTestId("facet-value");
    expect(values[0]).toHaveTextContent("218,135");
    expect(values[1]).toHaveTextContent("0");

    await userEvent.click(screen.getByRole("checkbox", { name: "Internal" }));
    expect(onAddFilter).toHaveBeenCalledWith({
      field: "kind",
      value: "Internal",
    });
    await userEvent.click(screen.getByRole("checkbox", { name: "Server" }));
    expect(onRemoveFilter).toHaveBeenCalledWith({
      field: "kind",
      value: "Server",
    });
  });

  it("selects a value", async () => {
    stubFetchRoutes([
      { match: "/api/v1/query", body: table([["signaldb", 2903]]) },
    ]);
    const { onAddFilter } = renderFacets();
    await userEvent.click(
      screen.getByRole("button", { name: /service\.name/ }),
    );
    await userEvent.click(await screen.findByTestId("facet-value"));
    expect(onAddFilter).toHaveBeenCalledWith({
      field: "service.name",
      value: "signaldb",
    });
  });

  it("discloses a truncated value list", async () => {
    const rows = Array.from({ length: 11 }, (_, i): [string, number] => [
      `v${i}`,
      100 - i,
    ]);
    stubFetchRoutes([{ match: "/api/v1/query", body: table(rows) }]);
    renderFacets();
    await userEvent.click(screen.getByRole("button", { name: /span\.name/ }));
    expect(await screen.findByText(/more values/i)).toBeInTheDocument();
  });

  it("labels an absent value rather than showing a blank row", async () => {
    stubFetchRoutes([{ match: "/api/v1/query", body: table([[null, 12]]) }]);
    renderFacets();
    await userEvent.click(screen.getByRole("button", { name: /status/ }));
    expect(await screen.findByText(/\(none\)/i)).toBeInTheDocument();
  });

  // #1070: an unresolvable field answers 200 with one "null" group holding the
  // window total. Showing it would invent a facet that does not exist.
  it("reports a field the server could not resolve", async () => {
    stubFetchRoutes([
      { match: "/api/v1/query", body: table([["null", 21_503]]) },
    ]);
    renderFacets();
    await userEvent.click(screen.getByRole("button", { name: /status/ }));
    expect(await screen.findByText(/not available/i)).toBeInTheDocument();
    expect(screen.queryByTestId("facet-value")).toBeNull();
  });

  it("marks the active value and offers removal", async () => {
    stubFetchRoutes([
      { match: "/api/v1/query", body: table([["signaldb", 2903]]) },
    ]);
    const { onRemoveFilter } = renderFacets({
      filters: [{ field: "service.name", value: "signaldb" }],
    });
    await userEvent.click(
      screen.getByRole("button", { name: /service\.name/ }),
    );
    const active = await screen.findByTestId("facet-value");
    expect(active).toHaveAttribute("aria-pressed", "true");
    await userEvent.click(active);
    expect(onRemoveFilter).toHaveBeenCalledWith({
      field: "service.name",
      value: "signaldb",
    });
  });

  it("surfaces a failed facet query", async () => {
    stubFetchRoutes([
      { match: "/api/v1/query", body: { error: "boom" }, status: 500 },
    ]);
    renderFacets();
    await userEvent.click(
      screen.getByRole("button", { name: /service\.name/ }),
    );
    expect(await screen.findByText(/could not load/i)).toBeInTheDocument();
  });

  it("opens a facet with a filter already applied", async () => {
    stubFetchRoutes([{ match: "/api/v1/query", body: table([["error", 6]]) }]);
    renderFacets({ filters: [{ field: "status", value: "error" }] });
    const statusBtn = screen.getByRole("button", { name: /status/ });
    expect(within(statusBtn).getByText("1")).toBeInTheDocument();
  });

  it("adds an info glyph with the registry tooltip to facets it knows", async () => {
    const serviceName = {
      key: "service.name",
      brief: "Logical name of the service.",
      type: "string",
      group_id: "registry.service",
      group_display_name: "Service",
      namespace: "otel",
      version: "1.43.0",
      source: "bundled",
    };
    stubFetchRoutes([
      { match: "/api/v1/query", body: table([]) },
      {
        match: "/api/v1/schema/attributes",
        body: {
          hits: [],
          resolutions: [
            { key: "service.name", hits: [serviceName], primary: serviceName },
          ],
        },
      },
    ]);
    renderFacets();
    const info = await screen.findByLabelText("About service.name");
    expect(screen.queryByLabelText("About span.name")).not.toBeInTheDocument();
    await userEvent.hover(info);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Logical name of the service.",
    );
  });
});
