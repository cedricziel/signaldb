import { screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BrowserRouter } from "react-router";
import { AppRoutes } from "./routes";
import {
  emptyLabels,
  emptyMatrix,
  emptyStreams,
  renderWithClient,
  stubFetchRoutes,
} from "./test/render";

function renderApp(path = "/") {
  window.history.replaceState(null, "", path);
  return renderWithClient(
    <BrowserRouter>
      <AppRoutes />
    </BrowserRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
});

describe("App", () => {
  it("renders the shell with the product mark and explore tabs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp();
    expect(screen.getByRole("banner")).toHaveTextContent(/signaldb/i);
    expect(screen.getByRole("tab", { name: "Logs" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(
      await screen.findByText(/No log lines match this query/),
    ).toBeInTheDocument();
  });

  it("redirects / to /logs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp("/");
    await screen.findByText(/No log lines match this query/);
    expect(window.location.pathname).toBe("/logs");
  });

  it("redirects an unknown path to /logs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp("/bogus");
    await screen.findByText(/No log lines match this query/);
    expect(window.location.pathname).toBe("/logs");
  });

  it("changes the tenant context from the top bar", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp("/logs");
    const user = (await import("@testing-library/user-event")).default;
    await user.click(
      screen.getByTitle("Tenant / dataset context for all queries"),
    );
    await user.clear(screen.getByLabelText("Tenant"));
    await user.type(screen.getByLabelText("Tenant"), "acme");
    await user.clear(screen.getByLabelText("Dataset"));
    await user.type(screen.getByLabelText("Dataset"), "prod");
    await user.click(screen.getByRole("button", { name: "Apply" }));
    expect(window.location.search).toContain("tenant=acme");
    expect(window.location.search).toContain("dataset=prod");
    expect(screen.getByRole("button", { name: /acme/ })).toHaveTextContent(
      "acme·prod",
    );
  });

  it("switches signals via tabs, updating the path", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderApp("/logs");
    screen.getByRole("tab", { name: "Traces" }).click();
    expect(await screen.findByLabelText("Trace ID")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/traces");
  });

  it("pushes a history entry per signal switch, so back steps through them", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderApp("/logs");
    const user = (await import("@testing-library/user-event")).default;

    await user.click(screen.getByRole("tab", { name: "Traces" }));
    await screen.findByLabelText("Trace ID");
    await user.click(screen.getByRole("tab", { name: "Metrics" }));
    await screen.findByText(
      "Build a query above, or switch to PromQL, then Run to chart metrics.",
    );
    expect(window.location.pathname).toBe("/metrics");

    window.history.back();
    await waitFor(() => expect(window.location.pathname).toBe("/traces"));

    window.history.back();
    await waitFor(() => expect(window.location.pathname).toBe("/logs"));
  });

  it("drops signal-specific state when switching tabs, keeping range/tenant/dataset/live", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderApp("/logs?q=boom&range=6h&tenant=acme&dataset=prod&live=1");
    const user = (await import("@testing-library/user-event")).default;

    await user.click(screen.getByRole("tab", { name: "Traces" }));
    await screen.findByLabelText("Trace ID");

    expect(window.location.search).not.toContain("q=boom");
    expect(window.location.search).toContain("range=6h");
    expect(window.location.search).toContain("tenant=acme");
    expect(window.location.search).toContain("dataset=prod");
    expect(window.location.search).toContain("live=1");
  });

  it("re-clicking the active tab doesn't reset its state or navigate", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp("/logs?q=boom");
    const user = (await import("@testing-library/user-event")).default;

    await user.click(screen.getByRole("tab", { name: "Logs" }));

    expect(window.location.pathname).toBe("/logs");
    expect(window.location.search).toContain("q=boom");
  });

  it("navigates to /manage and back via the Manage link", async () => {
    const WHOAMI = {
      user: {
        id: "user-1",
        email: "admin@example.com",
        display_name: "Admin",
        is_instance_admin: true,
      },
      memberships: [{ tenant_id: "acme", role: "admin" }],
      tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
      datasets: [{ id: "production", slug: "production", is_default: true }],
      default_dataset: "production",
    };
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
      { match: "/api/v1/whoami", body: WHOAMI },
      { match: "/api-keys", body: [] },
      { match: "/memberships", body: [] },
    ]);
    renderApp("/logs");
    const user = (await import("@testing-library/user-event")).default;
    await user.click(await screen.findByRole("link", { name: "Manage" }));
    expect(
      await screen.findByRole("dialog", { name: "Manage tenant" }),
    ).toBeInTheDocument();
    expect(window.location.pathname).toBe("/manage");

    await user.click(screen.getByRole("button", { name: "Close management" }));
    expect(window.location.pathname).toBe("/logs");
  });

  it("redirects /manage to /logs for non-admins", async () => {
    const WHOAMI = {
      user: {
        id: "user-1",
        email: "viewer@example.com",
        display_name: "Viewer",
        is_instance_admin: false,
      },
      memberships: [{ tenant_id: "acme", role: "viewer" }],
      tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
      datasets: [{ id: "production", slug: "production", is_default: true }],
      default_dataset: "production",
    };
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
      { match: "/api/v1/whoami", body: WHOAMI },
    ]);
    renderApp("/manage");
    await screen.findByText(/No log lines match this query/);
    expect(window.location.pathname).toBe("/logs");
  });
});
