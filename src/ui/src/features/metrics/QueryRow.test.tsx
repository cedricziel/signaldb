import { useState } from "react";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { buildPromQL, emptyQuery, type MetricQuery } from "./buildPromQL";
import { QueryRow } from "./QueryRow";

const RANGE = { fromMs: 1_000_000, toMs: 2_000_000 };

afterEach(() => {
  vi.unstubAllGlobals();
});

/** QueryRow is controlled; hold its state and surface the compiled PromQL. */
function Harness() {
  const [query, setQuery] = useState<MetricQuery>(emptyQuery("a"));
  return (
    <>
      <QueryRow query={query} range={RANGE} onChange={setQuery} />
      <output data-testid="promql">{buildPromQL(query)}</output>
    </>
  );
}

function stubMetadata() {
  stubFetchRoutes([
    { match: /label\/__name__\/values/, body: metaBody(["http_reqs", "up"]) },
    { match: /\/labels\?/, body: metaBody(["service", "host"]) },
    { match: /label\/service\/values/, body: metaBody(["checkout"]) },
  ]);
}

const metaBody = (data: string[]) => ({ status: "success", data });
const promql = () => screen.getByTestId("promql").textContent;

describe("QueryRow", () => {
  it("builds a query step by step from the visual controls", async () => {
    stubMetadata();
    renderWithClient(<Harness />);
    const user = userEvent.setup();

    await user.type(screen.getByLabelText("Metric"), "http_reqs");
    expect(promql()).toBe("http_reqs");

    await user.click(screen.getByRole("button", { name: "Add filter" }));
    await user.type(screen.getByLabelText("Filter label"), "service");
    await user.type(screen.getByLabelText("Filter value"), "checkout");
    expect(promql()).toBe('http_reqs{service="checkout"}');

    await user.selectOptions(screen.getByLabelText("Aggregation"), "sum");
    await user.type(screen.getByLabelText("Group by"), "service");
    expect(promql()).toBe('sum by (service)(http_reqs{service="checkout"})');

    await user.selectOptions(screen.getByLabelText("Function"), "rate");
    expect(promql()).toBe(
      'sum by (service)(rate(http_reqs{service="checkout"}[5m]))',
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
    expect(promql()).toBe('up{service="checkout"}');

    await user.click(screen.getByRole("button", { name: "Remove filter" }));
    expect(promql()).toBe("up");
  });

  it("populates the metric picker from the __name__ endpoint", async () => {
    stubMetadata();
    renderWithClient(<Harness />);
    // Datalist <option>s aren't exposed as ARIA options; assert via the DOM.
    await waitFor(() =>
      expect(
        document.querySelector('datalist option[value="http_reqs"]'),
      ).not.toBeNull(),
    );
  });
});
