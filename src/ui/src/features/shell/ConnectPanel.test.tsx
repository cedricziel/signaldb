import { fireEvent, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import type { ConnectionInfoResponse } from "../../api/connection";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import {
  connectionInfoBody,
  MCP_ENDPOINT as MCP,
} from "../../test/connectionInfo";
import { ConnectPanel } from "./ConnectPanel";

function renderPanel({
  canManage = true,
  info = { mcp: MCP },
}: {
  canManage?: boolean;
  info?: Partial<ConnectionInfoResponse>;
} = {}) {
  stubFetchRoutes([
    { match: "/api/v1/connection", body: connectionInfoBody(info) },
  ]);
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

/** Opens `name`'s vertical tab and returns its panel. */
function openTab(name: string): HTMLElement {
  fireEvent.click(screen.getByRole("tab", { name }));
  return screen.getByRole("tabpanel", { name });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ConnectPanel", () => {
  it("opens on an overview with one tile per way to connect", async () => {
    renderPanel();

    expect(screen.getByRole("tab", { name: "Overview" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    const overview = screen.getByRole("tabpanel", { name: "Overview" });
    const tiles = await within(overview).findAllByRole("button");
    expect(tiles.map((t) => t.textContent)).toEqual([
      expect.stringContaining("MCP"),
      expect.stringContaining("CLI"),
      expect.stringContaining("HTTP API"),
    ]);
    expect(overview).toHaveTextContent(MCP.url);
    expect(overview).toHaveTextContent("https://acme.example.com");
  });

  it("opens a tile's tab when the tile is picked", async () => {
    renderPanel();

    const overview = screen.getByRole("tabpanel", { name: "Overview" });
    fireEvent.click(
      await within(overview).findByRole("button", { name: /^CLI/ }),
    );
    expect(screen.getByRole("tab", { name: "CLI" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByRole("tabpanel", { name: "CLI" })).toBeInTheDocument();
  });

  it("moves between tabs with the arrow keys", () => {
    renderPanel();

    const overview = screen.getByRole("tab", { name: "Overview" });
    const mcp = screen.getByRole("tab", { name: "MCP" });
    fireEvent.keyDown(overview, { key: "ArrowDown" });
    expect(mcp).toHaveFocus();
    expect(mcp).toHaveAttribute("aria-selected", "true");
    fireEvent.keyDown(mcp, { key: "ArrowUp" });
    expect(overview).toHaveFocus();
  });

  it("shows the MCP URL and how to add it to Claude Code and connectors", async () => {
    renderPanel();

    const mcp = openTab("MCP");
    expect(await within(mcp).findByText(MCP.url)).toBeInTheDocument();
    expect(mcp).toHaveTextContent(
      `claude mcp add --transport http signaldb ${MCP.url}`,
    );
    expect(mcp).toHaveTextContent('--header "Authorization: Bearer <api-key>"');
    expect(mcp).toHaveTextContent('--header "X-Tenant-ID: acme"');
    expect(mcp).toHaveTextContent("Add custom connector");
  });

  it("says when the deployment has no MCP endpoint", async () => {
    renderPanel({ info: { mcp: null } });

    const overview = screen.getByRole("tabpanel", { name: "Overview" });
    expect(
      await within(overview).findByText("Not configured"),
    ).toBeInTheDocument();
    const mcp = openTab("MCP");
    expect(mcp).toHaveTextContent("No MCP endpoint is configured");
    expect(mcp).not.toHaveTextContent("claude mcp add");
  });

  it("shows the CLI environment pointing at the router", async () => {
    renderPanel();

    const cli = openTab("CLI");
    await within(cli).findByText(/signaldb-cli whoami/);
    expect(cli).toHaveTextContent(
      "export SIGNALDB_URL=https://acme.example.com",
    );
    expect(cli).toHaveTextContent("export SIGNALDB_API_KEY=<api-key>");
    expect(cli).toHaveTextContent("export SIGNALDB_TENANT_ID=acme");
    expect(cli).toHaveTextContent("export SIGNALDB_DATASET_ID=production");
    expect(cli).toHaveTextContent("signaldb-cli query --ir");
  });

  it("shows the API base URL, query path, OpenAPI document and a curl example", async () => {
    renderPanel();

    const api = openTab("HTTP API");
    await within(api).findByText(/curl -X POST/);
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
    renderPanel({ canManage: true });

    expect(
      await screen.findByRole("link", { name: "Create an API key" }),
    ).toHaveAttribute("href", expect.stringContaining("/api-keys"));
  });

  it("hides the API key link from those who can't create one", async () => {
    renderPanel({ canManage: false });

    await screen.findByRole("button", { name: /^MCP/ });
    expect(
      screen.queryByRole("link", { name: "Create an API key" }),
    ).not.toBeInTheDocument();
  });

  it("surfaces operator notes about unset public endpoints", async () => {
    renderPanel({
      info: {
        public_endpoints_configured: false,
        notes: ["[public].api_url is unset; using http://localhost:3000"],
      },
    });

    expect(
      await screen.findByText(/\[public\]\.api_url is unset/),
    ).toBeInTheDocument();
  });
});
