import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { DEFAULT_STATE } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { UserMenu } from "./UserMenu";

function renderUserMenu(props: Parameters<typeof UserMenu>[0]) {
  return renderWithClient(
    <MemoryRouter>
      <UserMenu {...props} />
    </MemoryRouter>,
  );
}

const WHOAMI = {
  user: {
    id: "user-1",
    email: "jane@acme.com",
    display_name: "Jane Doe",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [{ id: "production", slug: "production", is_default: true }],
  default_dataset: "production",
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("UserMenu", () => {
  it("does not render when user is unauthenticated", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/whoami",
        body: { ...WHOAMI, user: undefined },
      },
    ]);
    renderUserMenu({ state: DEFAULT_STATE });
    // No user menu button should appear
    await waitFor(() => {
      expect(screen.queryByText("JD")).not.toBeInTheDocument();
    });
  });

  it("shows avatar with initials from display name", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderUserMenu({ state: DEFAULT_STATE });
    await waitFor(() => {
      expect(screen.getByText("JD")).toBeInTheDocument();
    });
  });

  it("shows user display name next to avatar", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderUserMenu({ state: DEFAULT_STATE });
    await waitFor(() => {
      expect(screen.getByText("Jane Doe")).toBeInTheDocument();
    });
  });

  it("opens popover on click", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderUserMenu({ state: DEFAULT_STATE });
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByRole("menu")).toBeInTheDocument();
    });
  });

  it("shows user info in popover", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderUserMenu({ state: DEFAULT_STATE });
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByText("jane@acme.com")).toBeInTheDocument();
      expect(screen.getByText("admin")).toBeInTheDocument();
    });
  });

  it("shows theme toggle in menu", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderUserMenu({ state: DEFAULT_STATE });
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByText("Appearance")).toBeInTheDocument();
    });
  });

  it("shows navigation items with correct links", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderUserMenu({ state: DEFAULT_STATE });
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByText("Send data")).toBeInTheDocument();
      expect(screen.getByRole("link", { name: /send data/i })).toHaveAttribute(
        "href",
        "/instrumentation",
      );
      expect(screen.getByRole("link", { name: /api keys/i })).toHaveAttribute(
        "href",
        "/api-keys",
      );
      expect(screen.getByText("Switch tenant")).toBeInTheDocument();
    });
  });

  it("closes popover on Escape key", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderUserMenu({ state: DEFAULT_STATE });
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByRole("menu")).toBeInTheDocument();
    });
    await userEvent.keyboard("{Escape}");
    await waitFor(() => {
      expect(screen.queryByRole("menu")).not.toBeInTheDocument();
    });
  });

  it("closes popover on backdrop click", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderUserMenu({ state: DEFAULT_STATE });
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByRole("menu")).toBeInTheDocument();
    });
    const backdrop = document.querySelector(".user-menu-backdrop");
    expect(backdrop).toBeInTheDocument();
    await userEvent.click(backdrop!);
    await waitFor(() => {
      expect(screen.queryByRole("menu")).not.toBeInTheDocument();
    });
  });

  it("shows sign out button", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderUserMenu({ state: DEFAULT_STATE });
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByText("Sign out")).toBeInTheDocument();
    });
  });
});
