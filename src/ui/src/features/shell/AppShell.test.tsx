import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { MemoryRouter } from "react-router";
import { sampleWhoami } from "../../stories/fetchStub";
import { renderWithClient } from "../../test/render";
import { AppShell } from "./AppShell";

const WHO = sampleWhoami();

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

  it("shows the account, tenant and Manage from the given identity", () => {
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
    expect(screen.getByRole("link", { name: "Manage" })).toBeInTheDocument();
  });

  it("hides Manage for a non-admin", () => {
    const viewer = sampleWhoami({
      memberships: [{ tenant_id: "acme", role: "viewer" }],
    });
    renderWithClient(<AppShell page="overview" who={viewer} />);
    expect(screen.queryByRole("link", { name: "Manage" })).toBeNull();
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
