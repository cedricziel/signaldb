import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { MobileSidebarDrawer } from "./MobileSidebarDrawer";

describe("MobileSidebarDrawer", () => {
  it("renders children without a backdrop or close button when closed", () => {
    render(
      <MobileSidebarDrawer open={false} onClose={vi.fn()}>
        <div>sidebar content</div>
      </MobileSidebarDrawer>,
    );
    expect(screen.getByText("sidebar content")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /close/i }),
    ).not.toBeInTheDocument();
  });

  it("shows a close button and backdrop when open, both dismissing", async () => {
    const onClose = vi.fn();
    const { container } = render(
      <MobileSidebarDrawer open onClose={onClose}>
        <div>sidebar content</div>
      </MobileSidebarDrawer>,
    );
    expect(screen.getByText("sidebar content")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /close/i }));
    expect(onClose).toHaveBeenCalledTimes(1);

    const backdrop = container.querySelector(".mobile-sidebar-backdrop");
    expect(backdrop).not.toBeNull();
    await userEvent.click(backdrop!);
    expect(onClose).toHaveBeenCalledTimes(2);
  });
});
