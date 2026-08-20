import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
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
  it("shows the value under the pointer through the shared tooltip", () => {
    // The cell is small enough to read as decoration; the tooltip is what
    // makes it data. Every other panel in the UI reads through this same
    // component, and a chart nobody can interrogate is a picture.
    render(
      <EntitySparkline series={series([1, 5, 3])} label="system.cpu.time" />,
    );

    const marks = screen.getAllByRole("button");
    fireEvent.pointerEnter(marks[1]!);

    const tip = screen.getByRole("tooltip");
    expect(tip).toHaveTextContent("system.cpu.time");
    expect(tip).toHaveTextContent("5");
  });

  it("drops the tooltip when the pointer leaves", () => {
    render(
      <EntitySparkline series={series([1, 5, 3])} label="system.cpu.time" />,
    );

    const marks = screen.getAllByRole("button");
    fireEvent.pointerEnter(marks[1]!);
    expect(screen.getByRole("tooltip")).toBeInTheDocument();

    fireEvent.pointerLeave(marks[1]!);
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
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
