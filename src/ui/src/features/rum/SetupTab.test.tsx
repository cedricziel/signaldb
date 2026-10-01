import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { connectionInfoBody } from "../../test/connectionInfo";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { DEFAULT_RANGE, resolveRange } from "../../lib/time";
import { SetupTab } from "./SetupTab";
import type { RumScope } from "./useRumData";

vi.mock("../../api/rum", async (orig) => ({
  ...(await orig<typeof import("../../api/rum")>()),
  fetchKpis: vi.fn().mockResolvedValue({}),
  fetchVitals: vi.fn().mockResolvedValue(new Map()),
  fetchTracedShare: vi.fn().mockResolvedValue({ traced: [], total: [] }),
}));

function scope(app = "storefront-web"): RumScope {
  const range = resolveRange(DEFAULT_RANGE, Date.now());
  return { range, rangeKey: "range-key", app };
}

function renderSetup(app = "storefront-web") {
  stubFetchRoutes([
    { match: "/api/v1/connection", body: connectionInfoBody() },
  ]);
  return renderWithClient(<SetupTab app={app} scope={scope(app)} />);
}

describe("SetupTab", () => {
  it("installs the browser-instrumentation package alongside the trace/log SDKs", async () => {
    renderSetup();
    const snippet = await screen.findByText(/npm install/);
    expect(snippet.textContent).toContain(
      "@opentelemetry/browser-instrumentation",
    );
    expect(snippet.textContent).toContain("@opentelemetry/sdk-logs");
    expect(snippet.textContent).toContain(
      "@opentelemetry/exporter-logs-otlp-http",
    );
  });

  it("initializes a LoggerProvider with a session log-record processor", async () => {
    renderSetup();
    const snippet = await screen.findByText(/LoggerProvider/);
    expect(snippet.textContent).toContain("session.id");
    expect(snippet.textContent).toContain("BatchLogRecordProcessor");
  });

  it("passes the log exporter as an options object, as sdk-logs requires", async () => {
    renderSetup();
    const snippet = await screen.findByText(/LoggerProvider/);
    expect(snippet.textContent).toMatch(
      /new BatchLogRecordProcessor\(\{\s*exporter: new OTLPLogExporter\(/,
    );
  });

  it("registers the Web Vitals, navigation, resource-timing, errors and user-action instrumentations", async () => {
    renderSetup();
    const snippet = await screen.findByText(/WebVitalsInstrumentation/);
    expect(snippet.textContent).toContain("NavigationInstrumentation");
    expect(snippet.textContent).toContain("NavigationTimingInstrumentation");
    expect(snippet.textContent).toContain("ResourceTimingInstrumentation");
    expect(snippet.textContent).toContain("ErrorsInstrumentation");
    expect(snippet.textContent).toContain("UserActionInstrumentation");
    expect(snippet.textContent).toContain("propagateTraceHeaderCorsUrls");
  });

  it("shows a dedicated session.id processor snippet", async () => {
    renderSetup();
    const snippet = await screen.findByText(/class SessionProcessor/);
    expect(snippet.textContent).toContain('setAttribute("session.id"');
  });

  it("still warns against putting a SignalDB API key in browser code", async () => {
    renderSetup();
    expect(
      await screen.findByText(/no SignalDB API key in browser code/i),
    ).toBeInTheDocument();
  });

  it("still shows the collector snippet with the tenant's connection details", async () => {
    renderSetup();
    const snippet = await screen.findByText(/otel-collector-config\.yaml/);
    expect(snippet).toBeInTheDocument();
  });

  it("links to the browser instrumentation guide", async () => {
    renderSetup();
    const link = await screen.findByRole("link", {
      name: /instrument a browser app/i,
    });
    expect(link).toHaveAttribute(
      "href",
      expect.stringContaining("instrument-browser-app.md"),
    );
  });
});
