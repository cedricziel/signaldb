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

const handle = () => screen.getByRole("separator");

/** Render inside an `<aside>` drawn `width` wide, whatever the saved width. */
function renderInPane(width: number) {
  render(
    <aside data-testid="pane">
      <SidebarResizer panel={panel} />
    </aside>,
  );
  vi.spyOn(screen.getByTestId("pane"), "getBoundingClientRect").mockReturnValue(
    { width } as DOMRect,
  );
}

it("applies the width while dragging and persists once on release", () => {
  const apply = vi.spyOn(panel, "apply");
  const set = vi.spyOn(panel, "set");
  render(<SidebarResizer panel={panel} />);
  fireEvent.pointerDown(handle(), { clientX: 10 });
  fireEvent.pointerMove(handle(), { clientX: 40 });
  fireEvent.pointerMove(handle(), { clientX: 60 });
  expect(apply).toHaveBeenCalledTimes(2);
  expect(apply).toHaveBeenLastCalledWith(250);
  expect(set).not.toHaveBeenCalled();
  fireEvent.pointerUp(handle());
  expect(set).toHaveBeenCalledTimes(1);
  expect(set).toHaveBeenCalledWith(250);
});

it("resizes with a touch drag and captures the pointer", () => {
  const capture = vi.spyOn(Element.prototype, "setPointerCapture");
  const set = vi.spyOn(panel, "set");
  render(<SidebarResizer panel={panel} />);
  fireEvent.pointerDown(handle(), {
    pointerId: 7,
    pointerType: "touch",
    clientX: 100,
  });
  expect(capture).toHaveBeenCalledWith(7);
  // A second finger landing elsewhere must not steer the drag.
  fireEvent.pointerMove(handle(), { pointerId: 8, clientX: 300 });
  fireEvent.pointerMove(handle(), { pointerId: 7, clientX: 70 });
  fireEvent.pointerUp(handle(), { pointerId: 7 });
  expect(set).toHaveBeenCalledTimes(1);
  expect(set).toHaveBeenCalledWith(170);
  expect(localStorage.getItem("test.resizer")).toBe("170");
});

it("persists what it reached when the browser cancels the drag", () => {
  const set = vi.spyOn(panel, "set");
  render(<SidebarResizer panel={panel} />);
  fireEvent.pointerDown(handle(), { pointerType: "touch", clientX: 10 });
  fireEvent.pointerMove(handle(), { clientX: 30 });
  fireEvent.pointerCancel(handle());
  fireEvent.pointerMove(handle(), { clientX: 90 });
  expect(set).toHaveBeenCalledTimes(1);
  expect(set).toHaveBeenCalledWith(220);
});

it("ignores a secondary-button press", () => {
  const apply = vi.spyOn(panel, "apply");
  render(<SidebarResizer panel={panel} />);
  fireEvent.pointerDown(handle(), { button: 2, clientX: 10 });
  fireEvent.pointerMove(handle(), { clientX: 60 });
  expect(apply).not.toHaveBeenCalled();
});

it("starts a drag from the drawn width when the CSS caps the saved one", () => {
  localStorage.setItem("test.resizer", "300");
  const apply = vi.spyOn(panel, "apply");
  const set = vi.spyOn(panel, "set");
  renderInPane(150);
  fireEvent.pointerDown(handle(), { clientX: 10 });
  fireEvent.pointerMove(handle(), { clientX: 20 });
  expect(apply).toHaveBeenLastCalledWith(160);
  fireEvent.pointerUp(handle());
  expect(set).toHaveBeenCalledWith(160);
});

it("does not persist a press that never moved", () => {
  localStorage.setItem("test.resizer", "300");
  const set = vi.spyOn(panel, "set");
  render(<SidebarResizer panel={panel} />);
  fireEvent.pointerDown(handle(), { clientX: 10 });
  fireEvent.pointerUp(handle());
  expect(set).not.toHaveBeenCalled();
  expect(localStorage.getItem("test.resizer")).toBe("300");
});

it("resizes from the keyboard, starting at the drawn width", () => {
  localStorage.setItem("test.resizer", "300");
  renderInPane(150);
  fireEvent.keyDown(handle(), { key: "ArrowRight" });
  expect(localStorage.getItem("test.resizer")).toBe("166");
  fireEvent.keyDown(handle(), { key: "ArrowLeft", shiftKey: true });
  expect(localStorage.getItem("test.resizer")).toBe("100");
});
