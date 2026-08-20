import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { EntitySparkline } from "./EntitySparkline";

/** A series as the IR envelope carries it: `[timestampNs, value]` pairs. */
function series(values: number[]) {
  return [
    {
      labels: {},
      points: values.map((v, i) => [
        String((1_700_000_000 + i * 60) * 1_000_000_000),
        v,
      ]) as unknown[][],
    },
  ];
}

describe("EntitySparkline", () => {
  it("reports the point under the pointer, for the table's tooltip", () => {
    // The cell sets `overflow: hidden` for its ellipsis, so a tooltip drawn
    // in here would be clipped to 80x18. This reports; the table renders.
    const onHover = vi.fn();
    render(
      <EntitySparkline
        series={series([1, 5, 3])}
        label="system.cpu.time"
        onHover={onHover}
      />,
    );

    fireEvent.pointerEnter(document.querySelectorAll("rect")[1]!);

    expect(onHover).toHaveBeenCalledWith(
      expect.objectContaining({ v: 5, max: 5, stepMs: 60_000 }),
      expect.anything(),
    );
  });

  it("reports the pointer leaving", () => {
    const onHover = vi.fn();
    render(
      <EntitySparkline
        series={series([1, 5, 3])}
        label="system.cpu.time"
        onHover={onHover}
      />,
    );

    fireEvent.pointerLeave(document.querySelectorAll("rect")[1]!);

    expect(onHover).toHaveBeenLastCalledWith(null);
  });

  it("leaves its hit bands unpainted", () => {
    // An SVG rect with no fill defaults to opaque black, which paints over
    // the line and the whole cell with it.
    render(<EntitySparkline series={series([1, 5, 3])} label="x" />);

    for (const rect of document.querySelectorAll("rect")) {
      expect(rect).toHaveClass("entity-sparkline-hit");
      expect(rect.getAttribute("fill")).toBeNull();
    }
    expect(screen.getByRole("img")).toBeInTheDocument();
  });

  it("draws nothing for a row the window holds no points for", () => {
    // Not a flat line at zero: the row's other columns are real measurements
    // and this one must not look like one.
    const { container } = render(<EntitySparkline series={[]} label="x" />);
    expect(container).toBeEmptyDOMElement();
  });

  it("draws nothing for a single point, which has no shape", () => {
    const { container } = render(
      <EntitySparkline series={series([42])} label="x" />,
    );
    expect(container).toBeEmptyDOMElement();
  });
});
