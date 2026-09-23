import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Sparkline } from "./Sparkline";

const points = [
  { x: 0, v: 1 },
  { x: 60_000, v: 5 },
  { x: 120_000, v: 3 },
];

describe("Sparkline", () => {
  it("draws one polyline point group for a line series", () => {
    render(<Sparkline points={points} />);
    expect(screen.getByRole("img")).toBeInTheDocument();
    expect(document.querySelector("polyline")).toBeInTheDocument();
  });

  it("draws bars for the bar variant", () => {
    render(<Sparkline points={points} variant="bar" />);
    expect(
      document.querySelectorAll("[data-testid='sparkline-bar']"),
    ).toHaveLength(3);
    expect(document.querySelector("polyline")).toBeNull();
  });

  it("shows a VizTooltip with the value on hover", () => {
    render(
      <Sparkline
        points={points}
        formatValue={(v) => `${v} ms`}
        formatLabel={(x) => `t=${x}`}
      />,
    );
    const hit = document.querySelectorAll("[data-testid='sparkline-hit']")[1]!;
    fireEvent.pointerMove(hit, { clientX: 10, clientY: 5 });
    const tip = screen.getByRole("tooltip");
    expect(within(tip).getByText("t=60000")).toBeInTheDocument();
    expect(within(tip).getByTestId("viz-tip-row")).toHaveTextContent("5 ms");
  });

  it("hides the tooltip when the pointer leaves", () => {
    render(<Sparkline points={points} />);
    const hit = document.querySelectorAll("[data-testid='sparkline-hit']")[0]!;
    fireEvent.pointerMove(hit, { clientX: 10, clientY: 5 });
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    fireEvent.pointerLeave(hit);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("suppresses its own tooltip and reports hover instead when asked", () => {
    const onHover = vi.fn();
    render(<Sparkline points={points} showTooltip={false} onHover={onHover} />);
    const hit = document.querySelectorAll("[data-testid='sparkline-hit']")[1]!;
    fireEvent.pointerEnter(hit);
    expect(onHover).toHaveBeenCalledWith(
      expect.objectContaining({ v: 5 }),
      expect.anything(),
    );
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("applies a stroke tone token", () => {
    render(<Sparkline points={points} tone="error" />);
    expect(document.querySelector("polyline")).toHaveClass(
      "sparkline-tone-error",
    );
  });

  it("renders nothing for fewer than two points by default", () => {
    const { container } = render(<Sparkline points={[{ x: 0, v: 1 }]} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("renders an empty-state message when given one and there is no data", () => {
    render(<Sparkline points={[]} emptyText="No data" />);
    expect(screen.getByText("No data")).toBeInTheDocument();
  });
});
