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

it("starts a drag from the drawn width when the CSS caps the saved one", () => {
  localStorage.setItem("test.resizer", "300");
  const apply = vi.spyOn(panel, "apply");
  const set = vi.spyOn(panel, "set");
  render(
    <aside data-testid="pane">
      <SidebarResizer panel={panel} />
    </aside>,
  );
  vi.spyOn(screen.getByTestId("pane"), "getBoundingClientRect").mockReturnValue(
    { width: 150 } as DOMRect,
  );
  fireEvent.mouseDown(screen.getByRole("separator"), { clientX: 10 });
  fireEvent.mouseMove(window, { clientX: 20 });
  expect(apply).toHaveBeenLastCalledWith(160);
  fireEvent.mouseUp(window);
  expect(set).toHaveBeenCalledWith(160);
});

it("does not persist a press that never moved", () => {
  localStorage.setItem("test.resizer", "300");
  const set = vi.spyOn(panel, "set");
  render(<SidebarResizer panel={panel} />);
  fireEvent.mouseDown(screen.getByRole("separator"), { clientX: 10 });
  fireEvent.mouseUp(window);
  expect(set).not.toHaveBeenCalled();
  expect(localStorage.getItem("test.resizer")).toBe("300");
});
