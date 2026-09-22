import { useState } from "react";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { emptyQuery, type MetricQuery } from "./metricQuery";
import { QueryRow } from "./QueryRow";

const RANGE = { fromMs: 1_000_000, toMs: 2_000_000 };

afterEach(() => {
  vi.unstubAllGlobals();
});

/** QueryRow is controlled; hold its state and surface it as JSON so
 * assertions can check the structured query the row produced. */
function Harness() {
  const [query, setQuery] = useState<MetricQuery>(emptyQuery("a"));
  return (
    <>
      <QueryRow query={query} range={RANGE} onChange={setQuery} />
      <output data-testid="query">{JSON.stringify(query)}</output>
    </>
  );
}

function metadataWindow() {
  return { result: "metadata", window: { start_ns: 0, end_ns: 0 } };
}

function valuesBody(values: string[]) {
  return {
    ...metadataWindow(),
    metadata: {
      kind: "values",
      truncated: false,
      cost: {
        mode: "metadata",
        window_scoped: false,
        sampled: false,
        approximate: false,
      },
      values: values.map((v) => ({ value: v, origin: "registry" })),
    },
  };
}

function fieldsBody(
  entries: Array<{
    name: string;
    cardinality?: { estimate: number; at_least: boolean } | null;
  }>,
) {
  return {
    ...metadataWindow(),
    metadata: {
      kind: "fields",
      truncated: false,
      cost: {
        mode: "metadata",
        window_scoped: false,
        sampled: false,
        approximate: false,
      },
      fields: entries.map((e) => ({
        name: e.name,
        type: "string",
        filterable: true,
        origin: "declared",
        cardinality: e.cardinality ?? null,
      })),
    },
  };
}

/** Every Query IR document QueryRow submits is a POST to the same
 * `/api/v1/query` URL, so discovery routes match on the pipeline shape. */
function isDescribeFields(body: unknown): boolean {
  const b = body as { pipeline?: Array<{ describe?: { target?: string } }> };
  return b.pipeline?.[0]?.describe?.target === "fields";
}
function isDescribeValues(field: string) {
  return (body: unknown): boolean => {
    const b = body as {
      pipeline?: Array<{ describe?: { target?: string; field?: string } }>;
    };
    return (
      b.pipeline?.[0]?.describe?.target === "values" &&
      b.pipeline[0]?.describe?.field === field
    );
  };
}

function stubMetadata() {
  stubFetchRoutes([
    {
      match: "/api/v1/query",
      bodyMatch: isDescribeValues("metric.name"),
      body: valuesBody(["http_reqs", "up"]),
    },
    {
      match: "/api/v1/query",
      bodyMatch: isDescribeFields,
      body: fieldsBody([
        { name: "service", cardinality: { estimate: 12, at_least: false } },
        {
          name: "k8s.pod",
          cardinality: { estimate: 10000, at_least: true },
        },
        { name: "host", cardinality: null },
      ]),
    },
    {
      match: "/api/v1/query",
      bodyMatch: isDescribeValues("service"),
      body: valuesBody(["checkout"]),
    },
  ]);
}

const query = (): MetricQuery =>
  JSON.parse(screen.getByTestId("query").textContent ?? "{}") as MetricQuery;

describe("QueryRow", () => {
  it("builds a query step by step from the visual controls", async () => {
    stubMetadata();
    renderWithClient(<Harness />);
    const user = userEvent.setup();

    await user.type(screen.getByLabelText("Metric"), "http_reqs");
    expect(query().metric).toBe("http_reqs");

    await user.click(screen.getByRole("button", { name: "Add filter" }));
    await user.type(screen.getByLabelText("Filter label"), "service");
    await user.type(screen.getByLabelText("Filter value"), "checkout");
    expect(query().filters).toEqual([
      { label: "service", op: "=", value: "checkout" },
    ]);

    await user.selectOptions(screen.getByLabelText("Aggregation"), "sum");
    await user.type(screen.getByLabelText("Group by"), "service");
    expect(query().agg).toEqual({ op: "sum", by: ["service"] });

    await user.selectOptions(screen.getByLabelText("Function"), "rate");
    expect(query().range).toEqual({ fn: "rate" });
  });

  it("a selected range function exposes window and across, and clearing the function drops them", async () => {
    stubMetadata();
    renderWithClient(<Harness />);
    const user = userEvent.setup();

    expect(screen.queryByLabelText("Window")).not.toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText("Function"), "rate");
    await user.type(screen.getByLabelText("Window"), "5m");
    await user.selectOptions(screen.getByLabelText("Across"), "avg");
    expect(query().range).toEqual({ fn: "rate", window: "5m", across: "avg" });

    await user.selectOptions(screen.getByLabelText("Function"), "");
    expect(query().range).toBeUndefined();
    expect(screen.queryByLabelText("Window")).not.toBeInTheDocument();
  });

  it("switching between range functions keeps window/across", async () => {
    stubMetadata();
    renderWithClient(<Harness />);
    const user = userEvent.setup();

    await user.selectOptions(screen.getByLabelText("Function"), "rate");
    await user.type(screen.getByLabelText("Window"), "1m");
    await user.selectOptions(screen.getByLabelText("Function"), "irate");
    expect(query().range).toEqual({ fn: "irate", window: "1m" });
  });

  it("carries the full metric and group-by text in a title, for when either overflows", async () => {
    stubMetadata();
    renderWithClient(<Harness />);
    const user = userEvent.setup();

    const longMetric = "http_server_request_duration_seconds_bucket_total";
    await user.type(screen.getByLabelText("Metric"), longMetric);
    expect(screen.getByLabelText("Metric")).toHaveAttribute(
      "title",
      longMetric,
    );

    await user.selectOptions(screen.getByLabelText("Aggregation"), "sum");
    const longGroupBy = "deployment.environment.name";
    await user.type(screen.getByLabelText("Group by"), longGroupBy);
    expect(screen.getByLabelText("Group by")).toHaveAttribute(
      "title",
      longGroupBy,
    );
  });

  it("removing a filter drops it from the query", async () => {
    stubMetadata();
    renderWithClient(<Harness />);
    const user = userEvent.setup();

    await user.type(screen.getByLabelText("Metric"), "up");
    await user.click(screen.getByRole("button", { name: "Add filter" }));
    await user.type(screen.getByLabelText("Filter label"), "service");
    await user.type(screen.getByLabelText("Filter value"), "checkout");
    expect(query().filters).toEqual([
      { label: "service", op: "=", value: "checkout" },
    ]);

    await user.click(screen.getByRole("button", { name: "Remove filter" }));
    expect(query().filters).toEqual([]);
  });

  it("populates the metric picker from discovery.metricNames", async () => {
    stubMetadata();
    renderWithClient(<Harness />);
    // Datalist <option>s aren't exposed as ARIA options; assert via the DOM.
    await waitFor(() =>
      expect(
        document.querySelector('datalist option[value="http_reqs"]'),
      ).not.toBeNull(),
    );
  });

  it("warns when grouping by a high-cardinality label", async () => {
    stubMetadata();
    renderWithClient(<Harness />);
    const user = userEvent.setup();

    await user.type(screen.getByLabelText("Metric"), "http_reqs");
    await user.selectOptions(screen.getByLabelText("Aggregation"), "sum");

    // A normal dimension: no warning.
    await user.type(screen.getByLabelText("Group by"), "service");
    expect(
      screen.queryByLabelText("Cardinality warning"),
    ).not.toBeInTheDocument();

    // A high-cardinality (capped) label: warning appears with the count.
    await user.clear(screen.getByLabelText("Group by"));
    await user.type(screen.getByLabelText("Group by"), "k8s.pod");
    const warn = await screen.findByLabelText("Cardinality warning");
    expect(warn).toHaveTextContent("k8s.pod");
    expect(warn).toHaveTextContent("≥10000 values");
  });
});
