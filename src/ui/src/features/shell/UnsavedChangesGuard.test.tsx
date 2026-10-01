import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Link, Outlet } from "react-router";
import { afterEach, describe, expect, it } from "vitest";
import { markDirty, resetDirtyForms } from "../../lib/dirtyForms";
import { renderWithRouter } from "../../test/render";
import { UnsavedChangesGuard } from "./UnsavedChangesGuard";

function Shell() {
  return (
    <>
      <UnsavedChangesGuard />
      <Outlet />
    </>
  );
}

function PageA() {
  return (
    <div>
      <p>Page A</p>
      <Link to="/b">Go to B</Link>
    </div>
  );
}

function PageB() {
  return <p>Page B</p>;
}

function renderShell() {
  return renderWithRouter(
    [
      {
        element: <Shell />,
        children: [
          { path: "/a", element: <PageA /> },
          { path: "/b", element: <PageB /> },
        ],
      },
    ],
    ["/a"],
  );
}

afterEach(() => {
  resetDirtyForms();
});

describe("UnsavedChangesGuard", () => {
  it("blocks navigation while a form is dirty and stays put on Stay", async () => {
    markDirty("test-form", true);
    renderShell();
    const user = userEvent.setup();

    await user.click(screen.getByRole("link", { name: "Go to B" }));
    const dialog = await screen.findByRole("dialog", {
      name: "Unsaved changes",
    });
    expect(screen.getByText("Page A")).toBeInTheDocument();

    await user.click(within(dialog).getByRole("button", { name: "Stay" }));
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.getByText("Page A")).toBeInTheDocument();
  });

  it("proceeds with the navigation on Leave", async () => {
    markDirty("test-form", true);
    renderShell();
    const user = userEvent.setup();

    await user.click(screen.getByRole("link", { name: "Go to B" }));
    const dialog = await screen.findByRole("dialog", {
      name: "Unsaved changes",
    });
    await user.click(within(dialog).getByRole("button", { name: "Leave" }));

    expect(await screen.findByText("Page B")).toBeInTheDocument();
  });

  it("navigates immediately when no form is dirty", async () => {
    renderShell();
    const user = userEvent.setup();

    await user.click(screen.getByRole("link", { name: "Go to B" }));

    expect(await screen.findByText("Page B")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});
