import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  applyPendingUpdate,
  resetUpdateState,
  setUpdateAvailable,
} from "../../lib/pwaUpdate";
import { UpdateBanner } from "./UpdateBanner";

afterEach(() => {
  resetUpdateState();
});

describe("UpdateBanner", () => {
  it("renders nothing while no update is pending", () => {
    render(<UpdateBanner />);
    expect(screen.queryByRole("status")).toBeNull();
  });

  it("appears once an update is available, with a Reload button", () => {
    render(<UpdateBanner />);

    act(() => {
      setUpdateAvailable(vi.fn());
    });

    expect(screen.getByRole("status")).toHaveTextContent(
      "A new version is ready",
    );
    expect(screen.getByRole("button", { name: "Reload" })).toBeInTheDocument();
  });

  it("clicking Reload applies the update and hides the banner", async () => {
    const updateSW = vi.fn().mockResolvedValue(undefined);
    render(<UpdateBanner />);
    act(() => {
      setUpdateAvailable(updateSW);
    });

    await userEvent.click(screen.getByRole("button", { name: "Reload" }));

    expect(updateSW).toHaveBeenCalledWith(true);
    expect(screen.queryByRole("status")).toBeNull();
  });

  it("disappears once the update is applied elsewhere (e.g. auto-apply)", () => {
    render(<UpdateBanner />);
    act(() => {
      setUpdateAvailable(vi.fn());
    });
    expect(screen.getByRole("status")).toBeInTheDocument();

    act(() => {
      applyPendingUpdate();
    });
    expect(screen.queryByRole("status")).toBeNull();
  });
});
