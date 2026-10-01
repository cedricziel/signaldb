import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { clippedSides, fitView, ZoomPan } from "./ZoomPan";

afterEach(() => {
  vi.restoreAllMocks();
});

it("fits a wide child by zooming out, and leaves a narrow one alone", () => {
  expect(fitView({ hostW: 800, contentW: 1000 })).toEqual({
    z: 0.8,
    x: 0,
    y: 0,
  });
  expect(fitView({ hostW: 800, contentW: 600 })).toEqual({ z: 1, x: 0, y: 0 });
  expect(fitView({ hostW: 100, contentW: 1000 }).z).toBe(0.5);
  expect(fitView({ hostW: 0, contentW: 1000 }).z).toBe(1);
});

it("reports the sides that clip the child", () => {
  const size = { hostW: 800, contentW: 1000 };
  expect(clippedSides({ z: 1, x: 0, y: 0 }, size)).toEqual({
    left: false,
    right: true,
  });
  expect(clippedSides({ z: 1, x: -200, y: 0 }, size)).toEqual({
    left: true,
    right: false,
  });
  expect(clippedSides(fitView(size), size)).toEqual({
    left: false,
    right: false,
  });
});

function renderSized(hostW: number, contentW: number) {
  vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(hostW);
  vi.spyOn(HTMLElement.prototype, "scrollWidth", "get").mockReturnValue(
    contentW,
  );
  const { container } = render(
    <ZoomPan>
      <div>map</div>
    </ZoomPan>,
  );
  return container.querySelector(".zoompan");
}

it("cues an overflowing child and fits it on FIT", () => {
  const host = renderSized(800, 1000);
  expect(host).toHaveClass("clipped-right");
  expect(host).not.toHaveClass("clipped-left");
  expect(screen.getByText(/FIT to see all/)).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Fit to view" }));
  expect(host).not.toHaveClass("clipped-right");
  expect(screen.getByText(/^80%/)).toBeInTheDocument();
});

it("shows no cue when the child fits", () => {
  expect(renderSized(800, 800)).not.toHaveClass("clipped-right");
  expect(screen.getByRole("button", { name: "Fit to view" })).not.toHaveClass(
    "zoompan-cue",
  );
});
