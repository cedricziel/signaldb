import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import type { WhoamiIdentityResponse } from "../../api/session";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { UserMenu } from "./UserMenu";

function renderUserMenu(props: Parameters<typeof UserMenu>[0]) {
  return renderWithClient(
    <MemoryRouter>
      <UserMenu {...props} />
    </MemoryRouter>,
  );
}

const WHOAMI: WhoamiIdentityResponse = {
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
  user_id: "user-1",
  dataset: "production",
  granted_tenants: [{ tenant_id: "acme" }],
};
const ADMIN_PROPS = { who: WHOAMI };

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("UserMenu", () => {
  it("does not render when user is unauthenticated", async () => {
    renderUserMenu({ who: { ...WHOAMI, user: undefined } });
    // No user menu button should appear
    await waitFor(() => {
      expect(screen.queryByText("JD")).not.toBeInTheDocument();
    });
  });

  it("shows avatar with initials from display name", async () => {
    renderUserMenu(ADMIN_PROPS);
    await waitFor(() => {
      expect(screen.getByText("JD")).toBeInTheDocument();
    });
  });

  it("shows user display name next to avatar", async () => {
    renderUserMenu(ADMIN_PROPS);
    await waitFor(() => {
      expect(screen.getByText("Jane Doe")).toBeInTheDocument();
    });
  });

  it("opens popover on click", async () => {
    renderUserMenu(ADMIN_PROPS);
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByRole("menu")).toBeInTheDocument();
    });
  });

  it("shows user info in popover", async () => {
    renderUserMenu(ADMIN_PROPS);
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByText("jane@acme.com")).toBeInTheDocument();
      expect(screen.getByText("admin")).toBeInTheDocument();
    });
  });

  it("shows theme toggle in menu", async () => {
    renderUserMenu(ADMIN_PROPS);
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByText("Appearance")).toBeInTheDocument();
    });
  });

  it("keeps to account items, leaving pages to the sidebar", async () => {
    renderUserMenu(ADMIN_PROPS);
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByRole("menu")).toBeInTheDocument();
    });
    expect(screen.getByText("Appearance")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Switch tenant" }),
    ).toHaveAttribute("href", "/select-tenant");
    expect(screen.getByRole("button", { name: "Sign out" })).toBeInTheDocument();
    for (const name of [
      /send data/i,
      /api keys/i,
      /github/i,
      /schema/i,
      /processors/i,
    ]) {
      expect(screen.queryByRole("link", { name })).not.toBeInTheDocument();
    }
  });

  it("closes popover on Escape key", async () => {
    renderUserMenu(ADMIN_PROPS);
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
    renderUserMenu(ADMIN_PROPS);
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
    renderUserMenu(ADMIN_PROPS);
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    await waitFor(() => {
      expect(screen.getByText("Sign out")).toBeInTheDocument();
    });
  });

  it("forgets the recent-query history on sign-out", async () => {
    stubFetchRoutes([{ match: "/ui/session", method: "DELETE", body: {} }]);
    localStorage.setItem(
      "sdb.recentQueries",
      JSON.stringify([
        { text: "user.email=a@b", signal: "logs", href: "/logs" },
      ]),
    );
    const originalLocation = window.location;
    Object.defineProperty(window, "location", {
      configurable: true,
      value: { ...originalLocation, reload: vi.fn() },
    });
    try {
      renderUserMenu(ADMIN_PROPS);
      await userEvent.click(
        await screen.findByRole("button", { name: /jane doe/i }),
      );
      await userEvent.click(screen.getByText("Sign out"));
      await waitFor(() =>
        expect(localStorage.getItem("sdb.recentQueries")).toBeNull(),
      );
    } finally {
      Object.defineProperty(window, "location", {
        configurable: true,
        value: originalLocation,
      });
    }
  });

  it("keeps the history when sign-out fails", async () => {
    stubFetchRoutes([
      {
        match: "/ui/session",
        method: "DELETE",
        body: { error: "boom" },
        status: 500,
      },
    ]);
    localStorage.setItem("sdb.recentQueries", "[]");
    renderUserMenu(ADMIN_PROPS);
    await userEvent.click(
      await screen.findByRole("button", { name: /jane doe/i }),
    );
    await userEvent.click(screen.getByText("Sign out"));
    await screen.findByRole("alert");
    expect(localStorage.getItem("sdb.recentQueries")).toBe("[]");
    localStorage.clear();
  });

  it("shows an inline alert and keeps the menu open, without reloading, when sign-out fails", async () => {
    stubFetchRoutes([
      {
        match: "/ui/session",
        method: "DELETE",
        body: { error: "boom" },
        status: 500,
      },
    ]);
    const originalLocation = window.location;
    const reloadSpy = vi.fn();
    Object.defineProperty(window, "location", {
      configurable: true,
      value: { ...originalLocation, reload: reloadSpy },
    });
    try {
      renderUserMenu(ADMIN_PROPS);
      const button = await screen.findByRole("button", { name: /jane doe/i });
      await userEvent.click(button);
      await userEvent.click(screen.getByText("Sign out"));

      expect(await screen.findByRole("alert")).toHaveTextContent(/500/);
      expect(screen.getByRole("menu")).toBeInTheDocument();
      expect(reloadSpy).not.toHaveBeenCalled();
    } finally {
      Object.defineProperty(window, "location", {
        configurable: true,
        value: originalLocation,
      });
    }
  });

  it("updates the Appearance label immediately after toggling", async () => {
    renderUserMenu(ADMIN_PROPS);
    const button = await screen.findByRole("button", { name: /jane doe/i });
    await userEvent.click(button);
    const before = screen.getByText("Appearance").closest("button")!;
    const initialHint = before.textContent;
    await userEvent.click(before);
    const after = screen.getByText("Appearance").closest("button")!;
    expect(after.textContent).not.toBe(initialHint);
  });
});
