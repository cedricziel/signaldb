import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { connectionInfoBody } from "../../test/connectionInfo";
import { Instrumentation } from "./Instrumentation";

/** A Query IR `table` response with a single count cell, as
 * `fetchIngestStatus` decodes it. */
function ingestCountBody(n: number) {
  return {
    result: "table",
    window: { start_ns: 0, end_ns: 0 },
    columns: [],
    rows: [[n]],
  };
}

/** Locate the <code> element inside the "Configuration snippet" panel. */
function getCodeBlock(): HTMLElement {
  const header = screen.getByText("Configuration snippet");
  const container = header.closest(".code-snippet");
  const code = container?.querySelector("code");
  if (!code) throw new Error("code element not found in snippet container");
  return code as HTMLElement;
}

function renderInstrumentation(props: {
  state: { tenant: string; dataset: string };
}) {
  return renderWithClient(
    <MemoryRouter>
      {/* Instrumentation expects state prop */}
      <Instrumentation state={props.state} />
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Instrumentation page", () => {
  it("shows source selector with all 6 sources", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByText("OTel SDK")).toBeInTheDocument();
      expect(screen.getByText("OTel Collector")).toBeInTheDocument();
      expect(screen.getByText("Kubernetes")).toBeInTheDocument();
      expect(screen.getByText("Docker")).toBeInTheDocument();
      expect(screen.getByText("journald")).toBeInTheDocument();
      expect(screen.getByText("Prometheus")).toBeInTheDocument();
    });
  });

  it("OTel SDK selected by default", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByText("OTel SDK")).toHaveClass("selected");
      // Ensure content for OTel SDK appears
      expect(screen.getByText("OpenTelemetry SDK")).toBeInTheDocument();
    });
  });

  it("shows code snippet for selected source", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      // Should include tenant and dataset placeholders, from the response's
      // header contract.
      const codeBlock = getCodeBlock();
      expect(codeBlock).toBeInTheDocument();
      expect(codeBlock.textContent).toContain("acme");
      expect(codeBlock.textContent).toContain("production");
    });
  });

  it("clicking a source switches content", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByText("OpenTelemetry SDK")).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: "Kubernetes" }));
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Kubernetes" })).toHaveClass(
        "selected",
      );
      // Kubernetes content appears
      expect(
        screen.getByText(/Instrument Kubernetes workloads/i),
      ).toBeInTheDocument();
    });
  });

  it("shows verification status section", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
      { match: "/api/v1/query", body: ingestCountBody(0) },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByText("Verification")).toBeInTheDocument();
      expect(screen.getByText("Traces")).toBeInTheDocument();
      expect(screen.getByText("Logs")).toBeInTheDocument();
      expect(screen.getByText("Metrics")).toBeInTheDocument();
      expect(screen.getByText("Profiles")).toBeInTheDocument();
    });
  });

  describe("verification status", () => {
    /** The IR `from` field the given signal's status row queries. */
    function bodyMatchFor(from: string) {
      return (body: unknown) =>
        !!body &&
        typeof body === "object" &&
        (body as { from?: unknown }).from === from;
    }

    it('shows "Waiting for data" for every signal with no recent records', async () => {
      stubFetchRoutes([
        { match: "/api/v1/connection", body: connectionInfoBody() },
        { match: "/api/v1/query", body: ingestCountBody(0) },
      ]);
      renderInstrumentation({
        state: { tenant: "acme", dataset: "production" },
      });

      await waitFor(() => {
        const rows = screen.getAllByText("Waiting for data");
        expect(rows).toHaveLength(4);
      });
      const tracesRow = screen.getByText("Traces").closest(".status-item")!;
      expect(tracesRow.querySelector(".status-icon")).toHaveClass("waiting");
    });

    it('shows "Receiving (N in the last 15 min)" for a signal with recent records', async () => {
      stubFetchRoutes([
        { match: "/api/v1/connection", body: connectionInfoBody() },
        { match: "/api/v1/query", body: ingestCountBody(0) },
        {
          match: "/api/v1/query",
          bodyMatch: bodyMatchFor("traces"),
          body: ingestCountBody(7),
        },
      ]);
      renderInstrumentation({
        state: { tenant: "acme", dataset: "production" },
      });

      const tracesRow = await waitFor(() => {
        const row = screen.getByText("Traces").closest(".status-item")!;
        expect(row).toHaveTextContent("Receiving (7 in the last 15 min)");
        return row;
      });
      expect(tracesRow.querySelector(".status-icon")).toHaveClass("receiving");
      // Unaffected signals stay in the waiting state.
      expect(
        screen.getByText("Logs").closest(".status-item"),
      ).toHaveTextContent("Waiting for data");
    });

    it("shows the query's error inline when a signal's status query fails", async () => {
      stubFetchRoutes([
        { match: "/api/v1/connection", body: connectionInfoBody() },
        { match: "/api/v1/query", body: ingestCountBody(0) },
        {
          match: "/api/v1/query",
          bodyMatch: bodyMatchFor("logs"),
          body: { error: "boom" },
          status: 500,
        },
      ]);
      renderInstrumentation({
        state: { tenant: "acme", dataset: "production" },
      });

      const logsRow = await waitFor(() => {
        const row = screen.getByText("Logs").closest(".status-item")!;
        expect(row).toHaveTextContent(/boom/);
        return row;
      });
      expect(logsRow.querySelector(".status-icon")).toHaveClass("error");
    });
  });

  it("code snippets include the tenant/dataset headers from connection info", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/connection",
        body: connectionInfoBody({
          headers: {
            authorization: "Bearer <api-key>",
            "x-tenant-id": "acme",
            "x-dataset-id": "staging",
          },
        }),
      },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      const code = getCodeBlock();
      expect(code.textContent).toMatch(/X-Tenant-ID.*acme/);
      // The active dataset comes from the response, not the outlet state.
      expect(code.textContent).toMatch(/X-Dataset-ID.*staging/);
      expect(code.textContent).not.toMatch(/X-Dataset-ID.*production/);
    });
  });

  it("env-var snippets emit OTEL_EXPORTER_OTLP_PROTOCOL so SDKs do not default to http/protobuf", async () => {
    stubFetchRoutes([{ match: "/api/v1/connection", body: connectionInfoBody() }]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(getCodeBlock().textContent).toContain(
        'OTEL_EXPORTER_OTLP_PROTOCOL="grpc"',
      );
    });
  });

  it("renders the real endpoint from /api/v1/connection, with no insecure flag for an https endpoint", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      const code = getCodeBlock();
      expect(code.textContent).toContain(
        "ingest.acme.example.com:4317",
      );
      expect(code.textContent).not.toContain("localhost");
      expect(code.textContent).not.toMatch(/insecure/i);
      expect(code.textContent).not.toContain("WithInsecure");
    });
  });

  it("shows an insecure/plaintext flag when the endpoint is not TLS", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/connection",
        body: connectionInfoBody({ tls: false }),
      },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      const code = getCodeBlock();
      expect(code.textContent).toContain("WithInsecure");
    });
  });

  it("shows a callout with the server's notes when public endpoints are not configured", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/connection",
        body: connectionInfoBody({
          public_endpoints_configured: false,
          notes: [
            "Public endpoints are not configured ([public] in signaldb.toml); URLs fall back to localhost defaults.",
          ],
        }),
      },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(
        screen.getByText(/Public endpoints are not configured/),
      ).toBeInTheDocument();
    });
  });

  it("shows no callout when public endpoints are configured", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(getCodeBlock().textContent).toContain("ingest.acme.example.com");
    });
    expect(
      screen.queryByText(/Public endpoints are not configured/),
    ).not.toBeInTheDocument();
  });

  it("shows an error state with a retry button when connection info fails, and renders no snippet", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: { error: "boom" }, status: 500 },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent(
        /Could not load connection details/,
      );
      expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    });
    expect(screen.queryByText("Configuration snippet")).not.toBeInTheDocument();
  });

  it("retries the request when the retry button is clicked", async () => {
    const fn = stubFetchRoutes([
      { match: "/api/v1/connection", body: { error: "boom" }, status: 500 },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    });
    const callsBeforeRetry = fn.mock.calls.length;

    await userEvent.click(screen.getByRole("button", { name: "Retry" }));

    await waitFor(() => {
      expect(fn.mock.calls.length).toBeGreaterThan(callsBeforeRetry);
    });
  });

  it("shows a tenant-scoped message with no retry button on a 403", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/connection",
        body: { error: "forbidden" },
        status: 403,
      },
    ]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(
        screen.getByText(
          /current tenant does not grant access to connection details/i,
        ),
      ).toBeInTheDocument();
    });
    expect(
      screen.queryByRole("button", { name: "Retry" }),
    ).not.toBeInTheDocument();
  });
});
