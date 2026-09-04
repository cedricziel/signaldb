import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { createPanelWidth } from "../lib/sidebarWidth";
import { SidebarResizer } from "./SidebarResizer";

const panel = createPanelWidth({
  storageKey: "test.resizer",
  cssVar: "--test-w",
  min: 100,
  max: 400,
  defaultPx: 200,
  grows: "right",
  resizerClassName: "test-resizer",
  resizerLabel: "Resize test panel",
});

afterEach(() => {
  localStorage.clear();
  vi.restoreAllMocks();
});

it("applies the width while dragging and persists once on release", () => {
  const apply = vi.spyOn(panel, "apply");
  const set = vi.spyOn(panel, "set");
  render(<SidebarResizer panel={panel} />);
  fireEvent.mouseDown(screen.getByRole("separator"), { clientX: 10 });
  fireEvent.mouseMove(window, { clientX: 40 });
  fireEvent.mouseMove(window, { clientX: 60 });
  expect(apply).toHaveBeenCalledTimes(2);
  expect(apply).toHaveBeenLastCalledWith(250);
  expect(set).not.toHaveBeenCalled();
  fireEvent.mouseUp(window);
  expect(set).toHaveBeenCalledTimes(1);
  expect(set).toHaveBeenCalledWith(250);
});

it("stops listening when unmounted mid-drag", () => {
  const apply = vi.spyOn(panel, "apply");
  const set = vi.spyOn(panel, "set");
  const { unmount } = render(<SidebarResizer panel={panel} />);
  fireEvent.mouseDown(screen.getByRole("separator"), { clientX: 10 });
  unmount();
  fireEvent.mouseMove(window, { clientX: 80 });
  fireEvent.mouseUp(window);
  expect(apply).not.toHaveBeenCalled();
  expect(set).not.toHaveBeenCalled();
});
