import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RouterProvider } from "react-router";
import { createAppRouter } from "../../routes";
import {
  emptyIrLogs,
  emptyMatrix,
  emptyStreams,
  renderWithClient,
  stubFetchRoutes,
} from "../../test/render";

// The palette's Services group reads the catalog through the same module
// boundary CatalogView.test.tsx mocks.
vi.mock("../../api/catalog", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/catalog")>();
  return {
    ...actual,
    fetchCatalogEntities: vi.fn().mockResolvedValue({
      entities: [
        {
          values: ["checkout", null],
          observations: [{ source: "traces", count: 3 }],
          lastNs: "1",
        },
      ],
      truncated: false,
    }),
  };
});

const ADMIN = {
  user: {
    id: "u1",
    email: "ada@example.com",
    display_name: "Ada Lovelace",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme" },
  datasets: [{ id: "prod", slug: "prod", is_default: true }],
  default_dataset: "prod",
};
const VIEWER = {
  ...ADMIN,
  memberships: [{ tenant_id: "acme", role: "viewer" }],
};

function stubShell(who: unknown = ADMIN) {
  stubFetchRoutes([
    { match: "query_range", body: emptyMatrix },
    { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    { match: "/api/v1/whoami", body: who },
    { match: "/api/v1/query", body: emptyIrLogs },
    { match: "loki", body: emptyStreams },
  ]);
}

function renderApp(path = "/logs?tenant=acme&dataset=prod") {
  window.history.replaceState(null, "", path);
  return renderWithClient(<RouterProvider router={createAppRouter()} />);
}

/** Make `(max-width: …)` queries match as on a viewport `width` px wide. */
function stubViewport(width: number) {
  vi.stubGlobal("matchMedia", (query: string) => {
    const max = /max-width:\s*(\d+)px/.exec(query);
    return {
      matches: max ? width <= Number(max[1]) : false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    } as MediaQueryList;
  });
}

const sidebar = () =>
  screen.getByRole("complementary", { name: "Main navigation" });

afterEach(() => {
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
  localStorage.clear();
});

describe("sidebar", () => {
  it("shows Manage and the account for admins, not for viewers", async () => {
    stubShell(ADMIN);
    const { unmount } = renderApp();
    expect(
      await within(sidebar()).findByRole("link", { name: "Manage" }),
    ).toHaveAttribute("href", "/manage");
    expect(
      within(sidebar()).getByRole("button", { name: "Account" }),
    ).toHaveTextContent("Ada Lovelace");
    unmount();

    stubShell(VIEWER);
    renderApp();
    await within(sidebar()).findByRole("button", { name: "Account" });
    expect(
      within(sidebar()).queryByRole("link", { name: "Manage" }),
    ).toBeNull();
  });

  it("collapses to icons, remembering the choice across reloads", async () => {
    stubShell();
    const user = userEvent.setup();
    const { unmount } = renderApp();
    expect(within(sidebar()).getByText("Monitor")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Collapse sidebar" }));
    expect(within(sidebar()).queryByText("Monitor")).toBeNull();
    // Icon-only items keep their name for assistive tech and tooltips.
    expect(
      within(sidebar()).getByRole("link", { name: "Traces" }),
    ).toHaveAttribute("title", "Traces");
    expect(localStorage.getItem("sdb.sidebar.collapsed")).toBe("true");
    unmount();

    renderApp();
    expect(
      screen.getByRole("button", { name: "Expand sidebar" }),
    ).toBeInTheDocument();
  });

  it("starts collapsed on a tablet until the user chooses", () => {
    stubViewport(900);
    stubShell();
    renderApp();
    expect(
      screen.getByRole("button", { name: "Expand sidebar" }),
    ).toBeInTheDocument();
  });

  it("toggles with [ outside text fields only", async () => {
    stubShell();
    const user = userEvent.setup();
    renderApp();
    await user.keyboard("[[");
    expect(
      screen.getByRole("button", { name: "Expand sidebar" }),
    ).toBeInTheDocument();

    // Typing a bracket into the logs search box is just text.
    const field = document.querySelector<HTMLInputElement>(".explore input")!;
    await user.click(field);
    await user.keyboard("[[");
    expect(
      screen.getByRole("button", { name: "Expand sidebar" }),
    ).toBeInTheDocument();
  });
});

describe("command palette", () => {
  it("opens with ⌘K, filters, and navigates on Enter", async () => {
    stubShell();
    const user = userEvent.setup();
    renderApp();
    await user.keyboard("{Meta>}k{/Meta}");
    const dialog = screen.getByRole("dialog", { name: "Command palette" });
    const input = within(dialog).getByRole("searchbox", { name: "Search" });
    expect(input).toHaveFocus();

    await user.type(input, "tra");
    const option = within(dialog).getByRole("option", { name: /Traces/ });
    expect(option).toHaveAttribute("aria-selected", "true");
    await user.keyboard("{Enter}");

    await waitFor(() => expect(window.location.pathname).toBe("/traces"));
    expect(window.location.search).toContain("tenant=acme");
    expect(
      screen.queryByRole("dialog", { name: "Command palette" }),
    ).toBeNull();
  });

  it("opens from the header search field and closes on Escape", async () => {
    stubShell();
    const user = userEvent.setup();
    renderApp();
    await user.click(screen.getByRole("button", { name: /^Search/ }));
    expect(
      screen.getByRole("dialog", { name: "Command palette" }),
    ).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(
      screen.queryByRole("dialog", { name: "Command palette" }),
    ).toBeNull();
  });

  it("moves the selection with the arrow keys", async () => {
    stubShell();
    const user = userEvent.setup();
    renderApp();
    await user.keyboard("{Control>}k{/Control}");
    const options = within(
      screen.getByRole("listbox", { name: "Results" }),
    ).getAllByRole("option");
    expect(options[0]).toHaveAttribute("aria-selected", "true");
    await user.keyboard("{ArrowDown}{ArrowDown}{ArrowUp}");
    expect(options[1]).toHaveAttribute("aria-selected", "true");
  });

  it("jumps straight to a pasted trace id", async () => {
    stubShell();
    const user = userEvent.setup();
    renderApp();
    await user.keyboard("{Meta>}k{/Meta}");
    await user.paste("4bf92f3577b34da6a3ce929d0e0e4736");
    expect(
      screen.getByRole("option", {
        name: /Open trace 4bf92f3577b34da6a3ce929d0e0e4736/,
      }),
    ).toBeInTheDocument();
    await user.keyboard("{Enter}");
    await waitFor(() =>
      expect(window.location.pathname).toBe(
        "/traces/4bf92f3577b34da6a3ce929d0e0e4736",
      ),
    );
  });

  it("finds catalog services and recent queries", async () => {
    localStorage.setItem(
      "sdb.recentQueries",
      JSON.stringify([
        { text: "service.name=checkout", signal: "traces", href: "/traces" },
      ]),
    );
    stubShell();
    const user = userEvent.setup();
    renderApp();
    await user.keyboard("{Meta>}k{/Meta}");
    await user.keyboard("checkout");
    const service = await screen.findByRole("option", { name: /^checkout/ });
    expect(service).toHaveAttribute(
      "href",
      expect.stringMatching(/^\/catalog\/service\/checkout,/),
    );
    expect(
      screen.getByRole("group", { name: "Recent queries" }),
    ).toHaveTextContent("service.name=checkout");
  });

  it("shows the empty state when nothing matches", async () => {
    stubShell();
    const user = userEvent.setup();
    renderApp();
    await user.keyboard("{Meta>}k{/Meta}");
    await user.keyboard("zzzz");
    expect(
      await screen.findByText(/No matches for “zzzz”/),
    ).toBeInTheDocument();
  });
});

describe("mobile", () => {
  it("swaps the sidebar for a top bar and drawer", async () => {
    stubViewport(390);
    stubShell();
    const user = userEvent.setup();
    renderApp();
    expect(
      screen.queryByRole("complementary", { name: "Main navigation" }),
    ).toBeNull();
    expect(
      screen.queryByRole("navigation", { name: "Current page" }),
    ).toBeNull();

    await user.click(screen.getByRole("button", { name: "Open navigation" }));
    const drawer = screen.getByRole("navigation", { name: "Main navigation" });
    await user.click(within(drawer).getByRole("link", { name: "Traces" }));

    await waitFor(() => expect(window.location.pathname).toBe("/traces"));
    expect(
      screen.queryByRole("navigation", { name: "Main navigation" }),
    ).toBeNull();
  });

  it("opens the palette from the top bar's search button", async () => {
    stubViewport(390);
    stubShell();
    const user = userEvent.setup();
    renderApp();
    await user.click(screen.getByRole("button", { name: "Search" }));
    expect(
      screen.getByRole("dialog", { name: "Command palette" }),
    ).toBeInTheDocument();
  });
});
