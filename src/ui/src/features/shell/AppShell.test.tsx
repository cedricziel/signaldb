import { fireEvent, screen, within } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { sampleWhoami } from "../../stories/fetchStub";
import { renderWithClient } from "../../test/render";
import { AppShell } from "./AppShell";
import { useBreadcrumbLeaf } from "./breadcrumbLeaf";

const WHO = sampleWhoami();
const VIEWER = sampleWhoami({
  memberships: [{ tenant_id: "acme", role: "viewer" }],
});

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

function TraceDetailStandIn({ id }: { id: string }) {
  useBreadcrumbLeaf(id);
  return <p>trace body</p>;
}

const crumb = () => screen.getByRole("navigation", { name: "Current page" });

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("AppShell", () => {
  it("renders the frame around its children without a router", () => {
    renderWithClient(
      <AppShell page="logs">
        <p>page body</p>
      </AppShell>,
    );
    expect(
      screen.getByRole("complementary", { name: "Main navigation" }),
    ).toBeInTheDocument();
    const crumb = screen.getByRole("navigation", { name: "Current page" });
    expect(within(crumb).getByText("Investigate")).toBeInTheDocument();
    expect(within(crumb).getByText("Logs")).toBeInTheDocument();
    expect(
      within(screen.getByRole("main")).getByText("page body"),
    ).toBeInTheDocument();
  });

  it("marks the page as current in the sidebar", () => {
    renderWithClient(<AppShell page="errors" />);
    expect(screen.getByRole("link", { name: "Errors" })).toHaveAttribute(
      "aria-current",
      "page",
    );
  });

  it("shows the account and tenant from the given identity", () => {
    renderWithClient(<AppShell page="overview" who={WHO} />);
    expect(screen.getByRole("button", { name: "Account" })).toHaveAttribute(
      "title",
      "Alice",
    );
    expect(
      screen.getByRole("button", {
        name: /switch tenant or dataset \(acme · production\)/i,
      }),
    ).toBeInTheDocument();
  });

  it("shows the Settings group, with Manage, API keys and Integrations, to an admin", () => {
    renderWithClient(<AppShell page="overview" who={WHO} />);
    const nav = screen.getByRole("navigation", { name: "Pages" });
    expect(within(nav).getByText("Settings")).toBeInTheDocument();
    expect(within(nav).getByRole("link", { name: "Manage" })).toHaveAttribute(
      "href",
      "/manage",
    );
    expect(
      within(nav).getByRole("link", { name: "API keys" }),
    ).toHaveAttribute("href", "/api-keys");
    expect(
      within(nav).getByRole("link", { name: "Integrations" }),
    ).toHaveAttribute("href", "/integrations/github");
  });

  it("hides the Settings group from a non-admin", () => {
    renderWithClient(<AppShell page="overview" who={VIEWER} />);
    expect(screen.queryByText("Settings")).toBeNull();
    expect(screen.queryByRole("link", { name: "Manage" })).toBeNull();
    expect(screen.queryByRole("link", { name: "API keys" })).toBeNull();
    expect(screen.queryByRole("link", { name: "Integrations" })).toBeNull();
  });

  it("highlights a settings page and crumbs it under Settings", () => {
    renderWithClient(<AppShell page="api-keys" who={WHO} />);
    expect(screen.getByRole("link", { name: "API keys" })).toHaveAttribute(
      "aria-current",
      "page",
    );
    expect(within(crumb()).getByText("Settings")).toBeInTheDocument();
    expect(within(crumb()).getByText("API keys")).toBeInTheDocument();
  });

  it("names the instrumentation page Send data in the sidebar and breadcrumb", () => {
    renderWithClient(<AppShell page="instrumentation" />);
    expect(screen.getByRole("link", { name: "Send data" })).toHaveAttribute(
      "aria-current",
      "page",
    );
    expect(within(crumb()).getByText("Send data")).toBeInTheDocument();
    expect(screen.queryByText("Instrumentation")).toBeNull();
  });

  it("hides the mutating Schema and Processors pages in demo mode", () => {
    const { unmount } = renderWithClient(<AppShell page="overview" />);
    expect(screen.getByRole("link", { name: "Schema" })).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Processors" }),
    ).toBeInTheDocument();
    unmount();

    renderWithClient(<AppShell page="overview" isDemo />);
    expect(screen.queryByRole("link", { name: "Schema" })).toBeNull();
    expect(screen.queryByRole("link", { name: "Processors" })).toBeNull();
    expect(screen.getByRole("link", { name: "Send data" })).toBeInTheDocument();
  });

  it("adds a detail leaf to the breadcrumb, linking the page back to its section", () => {
    renderWithClient(<AppShell page="traces" detail="4bf92f35" />);
    expect(within(crumb()).getByText("Investigate")).toBeInTheDocument();
    expect(within(crumb()).getByRole("link", { name: "Traces" })).toHaveAttribute(
      "href",
      expect.stringMatching(/^\/traces/),
    );
    expect(within(crumb()).getByText("4bf92f35")).toHaveAttribute(
      "aria-current",
      "page",
    );
  });

  it("takes the leaf from a detail page's useBreadcrumbLeaf, dropping it when the page goes", () => {
    function Harness() {
      const [open, setOpen] = useState(true);
      return (
        <AppShell page="traces">
          {open && <TraceDetailStandIn id="abc12345" />}
          <button onClick={() => setOpen(false)}>leave</button>
        </AppShell>
      );
    }
    renderWithClient(<Harness />);
    expect(within(crumb()).getByText("abc12345")).toHaveAttribute(
      "aria-current",
      "page",
    );
    fireEvent.click(screen.getByRole("button", { name: "leave" }));
    expect(within(crumb()).queryByText("abc12345")).toBeNull();
    expect(within(crumb()).getByText("Traces")).toHaveAttribute(
      "aria-current",
      "page",
    );
  });

  it("shows the leaf in the mobile top bar", () => {
    stubViewport(390);
    renderWithClient(
      <AppShell page="traces">
        <TraceDetailStandIn id="abc12345" />
      </AppShell>,
    );
    expect(document.querySelector(".app-mobilebar-page")).toHaveTextContent(
      "abc12345",
    );
  });

  it("shows the demo banner in demo mode", () => {
    renderWithClient(<AppShell page="overview" isDemo />);
    expect(screen.getByText("Demo · read-only")).toBeInTheDocument();
  });

  it("follows the surrounding router's location over `page`", () => {
    renderWithClient(
      <MemoryRouter initialEntries={["/traces"]}>
        <AppShell page="logs" />
      </MemoryRouter>,
    );
    const crumb = screen.getByRole("navigation", { name: "Current page" });
    expect(within(crumb).getByText("Traces")).toBeInTheDocument();
  });
});
