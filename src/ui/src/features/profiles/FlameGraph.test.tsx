import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { FlameGraph } from "./FlameGraph";
import type { RenderResponse } from "../../api/pyroscope";

// root (0-100) → childA (0-60), childB (60-100).
const RENDER: RenderResponse = {
  flamebearer: {
    names: ["root", "childA", "childB"],
    levels: [
      [0, 100, 0, 0],
      [0, 60, 60, 1, 0, 40, 40, 2],
    ],
    numTicks: 100,
    maxSelf: 60,
  },
  metadata: {
    format: "single",
    sampleRate: 100,
    units: "samples",
    name: "cpu",
  },
};

function renderFlame() {
  return render(<FlameGraph render={RENDER} unit="samples" />);
}

// A straight five-level chain (root → a → b → c → d), each spanning the
// same full width, so zooming into the deepest frame produces a five-crumb
// breadcrumb (more than four levels deep).
const DEEP_RENDER: RenderResponse = {
  flamebearer: {
    names: ["root", "a", "b", "c", "d"],
    levels: [
      [0, 100, 0, 0],
      [0, 100, 0, 1],
      [0, 100, 0, 2],
      [0, 100, 0, 3],
      [0, 100, 100, 4],
    ],
    numTicks: 100,
    maxSelf: 100,
  },
  metadata: {
    format: "single",
    sampleRate: 100,
    units: "samples",
    name: "cpu",
  },
};

describe("FlameGraph roving focus", () => {
  it("gives only the root frame a tab stop", () => {
    renderFlame();
    expect(screen.getByRole("button", { name: "root" })).toHaveAttribute(
      "tabindex",
      "0",
    );
    expect(screen.getByRole("button", { name: "childA" })).toHaveAttribute(
      "tabindex",
      "-1",
    );
    expect(screen.getByRole("button", { name: "childB" })).toHaveAttribute(
      "tabindex",
      "-1",
    );
  });

  it("moves down into the first child with ArrowDown", () => {
    renderFlame();
    const root = screen.getByRole("button", { name: "root" });
    root.focus();
    fireEvent.keyDown(root, { key: "ArrowDown" });
    const childA = screen.getByRole("button", { name: "childA" });
    expect(childA).toHaveFocus();
    expect(childA).toHaveAttribute("tabindex", "0");
  });

  it("moves across siblings with ArrowRight", () => {
    renderFlame();
    const root = screen.getByRole("button", { name: "root" });
    root.focus();
    fireEvent.keyDown(root, { key: "ArrowDown" });
    const childA = screen.getByRole("button", { name: "childA" });
    fireEvent.keyDown(childA, { key: "ArrowRight" });
    expect(screen.getByRole("button", { name: "childB" })).toHaveFocus();
  });

  it("moves back up to the covering parent with ArrowUp", () => {
    renderFlame();
    const root = screen.getByRole("button", { name: "root" });
    root.focus();
    fireEvent.keyDown(root, { key: "ArrowDown" });
    const childA = screen.getByRole("button", { name: "childA" });
    fireEvent.keyDown(childA, { key: "ArrowUp" });
    expect(root).toHaveFocus();
  });

  it("groups the frame rows for assistive technology", () => {
    renderFlame();
    expect(
      screen.getByRole("group", { name: "Flame graph frames" }),
    ).toBeInTheDocument();
  });

  // Tab should land wherever the pointer last showed detail for, matching
  // `docs/users/explore-ui.md`'s "the last one you pointed at" — not just
  // wherever an arrow key left it.
  it("moves the tab stop to the frame the pointer moves over", () => {
    renderFlame();
    const childB = screen.getByRole("button", { name: "childB" });
    fireEvent.pointerMove(childB);
    expect(childB).toHaveAttribute("tabindex", "0");
    expect(screen.getByRole("button", { name: "root" })).toHaveAttribute(
      "tabindex",
      "-1",
    );
  });
});

describe("FlameGraph breadcrumb", () => {
  function zoomToDeepest() {
    render(<FlameGraph render={DEEP_RENDER} unit="samples" />);
    fireEvent.click(screen.getByRole("button", { name: "d" }));
    return screen.getByLabelText("Zoom path");
  }

  it("collapses the middle crumbs past four levels deep", () => {
    const breadcrumb = zoomToDeepest();
    const crumbs = within(breadcrumb).getAllByRole("button");
    expect(crumbs.map((c) => c.textContent)).toEqual(["root", "…", "c", "d"]);
  });

  it("reveals the full path from the ellipsis crumb", () => {
    const breadcrumb = zoomToDeepest();
    fireEvent.click(within(breadcrumb).getByRole("button", { name: /show full zoom path/i }));
    const crumbs = within(breadcrumb).getAllByRole("button");
    expect(crumbs.map((c) => c.textContent)).toEqual([
      "root",
      "a",
      "b",
      "c",
      "d",
    ]);
  });

  it("does not collapse at four levels or fewer", () => {
    // Zooming to "c" (level 3) makes a four-crumb path: root, a, b, c.
    render(<FlameGraph render={DEEP_RENDER} unit="samples" />);
    fireEvent.click(screen.getByRole("button", { name: "c" }));
    const breadcrumb = screen.getByLabelText("Zoom path");
    const crumbs = within(breadcrumb).getAllByRole("button");
    expect(crumbs.map((c) => c.textContent)).toEqual(["root", "a", "b", "c"]);
  });
});
