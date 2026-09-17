import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { useRovingFocus } from "./useRovingFocus";

function Group({
  count,
  vertical,
  horizontal,
}: {
  count: number;
  vertical?: (index: number, direction: -1 | 1) => number | null;
  horizontal?: (index: number, direction: -1 | 1) => number | null;
}) {
  const roving = useRovingFocus(count, { vertical, horizontal });
  return (
    <div role="group" aria-label="marks">
      {Array.from({ length: count }, (_, i) => {
        const item = roving.itemProps(i);
        return (
          <button key={i} data-testid={`mark-${i}`} {...item}>
            mark {i}
          </button>
        );
      })}
    </div>
  );
}

describe("useRovingFocus", () => {
  it("gives only the first mark a tab stop", () => {
    render(<Group count={4} />);
    expect(screen.getByTestId("mark-0")).toHaveAttribute("tabindex", "0");
    expect(screen.getByTestId("mark-1")).toHaveAttribute("tabindex", "-1");
    expect(screen.getByTestId("mark-2")).toHaveAttribute("tabindex", "-1");
    expect(screen.getByTestId("mark-3")).toHaveAttribute("tabindex", "-1");
  });

  it("moves the active mark and its tab stop with ArrowRight", () => {
    render(<Group count={4} />);
    const first = screen.getByTestId("mark-0");
    first.focus();
    fireEvent.keyDown(first, { key: "ArrowRight" });
    expect(screen.getByTestId("mark-1")).toHaveFocus();
    expect(screen.getByTestId("mark-1")).toHaveAttribute("tabindex", "0");
    expect(screen.getByTestId("mark-0")).toHaveAttribute("tabindex", "-1");
  });

  it("moves back with ArrowLeft and clamps at the ends", () => {
    render(<Group count={3} />);
    const first = screen.getByTestId("mark-0");
    first.focus();
    fireEvent.keyDown(first, { key: "ArrowLeft" });
    expect(screen.getByTestId("mark-0")).toHaveFocus();
  });

  it("jumps to the first/last mark with Home/End", () => {
    render(<Group count={5} />);
    const first = screen.getByTestId("mark-0");
    first.focus();
    fireEvent.keyDown(first, { key: "End" });
    expect(screen.getByTestId("mark-4")).toHaveFocus();
    fireEvent.keyDown(screen.getByTestId("mark-4"), { key: "Home" });
    expect(screen.getByTestId("mark-0")).toHaveFocus();
  });

  it("uses a supplied vertical stepper for 2-D layouts", () => {
    const vertical = vi.fn((index: number, direction: -1 | 1) =>
      direction === 1 && index === 0 ? 2 : null,
    );
    render(<Group count={4} vertical={vertical} />);
    const first = screen.getByTestId("mark-0");
    first.focus();
    fireEvent.keyDown(first, { key: "ArrowDown" });
    expect(vertical).toHaveBeenCalledWith(0, 1);
    expect(screen.getByTestId("mark-2")).toHaveFocus();
  });

  it("leaves ArrowUp/Down alone when no vertical stepper is supplied", () => {
    render(<Group count={3} />);
    const first = screen.getByTestId("mark-0");
    first.focus();
    const event = fireEvent.keyDown(first, { key: "ArrowDown" });
    // Not swallowed: fireEvent returns true when preventDefault was not called.
    expect(event).toBe(true);
    expect(screen.getByTestId("mark-0")).toHaveFocus();
  });

  it("uses a supplied horizontal stepper instead of the default clamp", () => {
    const horizontal = vi.fn(() => 3);
    render(<Group count={4} horizontal={horizontal} />);
    const first = screen.getByTestId("mark-0");
    first.focus();
    fireEvent.keyDown(first, { key: "ArrowRight" });
    expect(horizontal).toHaveBeenCalledWith(0, 1);
    expect(screen.getByTestId("mark-3")).toHaveFocus();
  });

  it("syncs the active index when focus lands on a mark directly", () => {
    render(<Group count={3} />);
    fireEvent.focus(screen.getByTestId("mark-2"));
    expect(screen.getByTestId("mark-2")).toHaveAttribute("tabindex", "0");
    expect(screen.getByTestId("mark-0")).toHaveAttribute("tabindex", "-1");
  });
});
