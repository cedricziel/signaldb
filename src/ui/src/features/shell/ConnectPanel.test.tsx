import { screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import {
  connectionInfoBody,
  MCP_ENDPOINT as MCP,
} from "../../test/connectionInfo";
import { ConnectPanel } from "./ConnectPanel";

function renderPanel(canManage = true) {
  return renderWithClient(
    <MemoryRouter>
      <ConnectPanel
        state={{ tenant: "acme", dataset: "production" }}
        canManage={canManage}
        onClose={() => {}}
      />
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ConnectPanel", () => {
  it("shows the MCP URL and how to add it to Claude Code and connectors", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody({ mcp: MCP }) },
    ]);
    renderPanel();

    const mcp = await screen.findByRole("region", { name: "MCP" });
    expect(within(mcp).getByText(MCP.url)).toBeInTheDocument();
    expect(mcp).toHaveTextContent(
      `claude mcp add --transport http signaldb ${MCP.url}`,
    );
    expect(mcp).toHaveTextContent('--header "Authorization: Bearer <api-key>"');
    expect(mcp).toHaveTextContent('--header "X-Tenant-ID: acme"');
    expect(mcp).toHaveTextContent("Add custom connector");
  });

  it("says when the deployment has no MCP endpoint", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody({ mcp: null }) },
    ]);
    renderPanel();

    const mcp = await screen.findByRole("region", { name: "MCP" });
    expect(mcp).toHaveTextContent("No MCP endpoint is configured");
    expect(mcp).not.toHaveTextContent("claude mcp add");
  });

  it("shows the CLI environment pointing at the router", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody({ mcp: MCP }) },
    ]);
    renderPanel();

    const cli = await screen.findByRole("region", { name: "CLI" });
    expect(cli).toHaveTextContent(
      "export SIGNALDB_URL=https://acme.example.com",
    );
    expect(cli).toHaveTextContent("export SIGNALDB_API_KEY=<api-key>");
    expect(cli).toHaveTextContent("export SIGNALDB_TENANT_ID=acme");
    expect(cli).toHaveTextContent("export SIGNALDB_DATASET_ID=production");
    expect(cli).toHaveTextContent("signaldb-cli query --ir");
  });

  it("shows the API base URL, query path, OpenAPI document and a curl example", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody({ mcp: MCP }) },
    ]);
    renderPanel();

    const api = await screen.findByRole("region", { name: "HTTP API" });
    expect(api).toHaveTextContent("https://acme.example.com/api/v1/query");
    expect(api).toHaveTextContent(
      "https://acme.example.com/api/v1/openapi.json",
    );
    expect(api).toHaveTextContent(
      "curl -X POST https://acme.example.com/api/v1/query",
    );
    expect(api).toHaveTextContent('-H "X-Dataset-ID: production"');
  });

  it("links to API keys for those who can create them", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderPanel(true);

    expect(
      await screen.findByRole("link", { name: "Create an API key" }),
    ).toHaveAttribute("href", expect.stringContaining("/api-keys"));
  });

  it("hides the API key link from those who can't create one", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderPanel(false);

    await screen.findByRole("region", { name: "HTTP API" });
    expect(
      screen.queryByRole("link", { name: "Create an API key" }),
    ).not.toBeInTheDocument();
  });

  it("surfaces operator notes about unset public endpoints", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/connection",
        body: connectionInfoBody({
          public_endpoints_configured: false,
          notes: ["[public].api_url is unset; using http://localhost:3000"],
        }),
      },
    ]);
    renderPanel();

    expect(
      await screen.findByText(/\[public\]\.api_url is unset/),
    ).toBeInTheDocument();
  });
});
