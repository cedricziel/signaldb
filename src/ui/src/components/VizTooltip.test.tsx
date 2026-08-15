import { fireEvent, render, screen, within } from "@testing-library/react";
import { useRef } from "react";
import { describe, expect, it } from "vitest";
import { useVizPointer, VizTooltip } from "./VizTooltip";

const HOST = { width: 1200, height: 800 };
const ROWS = [
  { swatch: "red", label: "error", value: "12 lines" },
  { swatch: "blue", label: "info", value: "525 lines" },
  { label: "gap", value: "–", muted: true },
];

describe("VizTooltip", () => {
  it("renders the title, one row per series with its swatch, and the footer", () => {
    render(
      <VizTooltip
        anchor={{ x: 100, y: 40 }}
        host={HOST}
        title="2024-03-05 14:07"
        rows={ROWS}
        footer={{ label: "total", value: "537 lines" }}
        id="tip"
      />,
    );
    const tip = screen.getByRole("tooltip");
    expect(tip).toHaveAttribute("id", "tip");
    expect(tip).toHaveClass("viz-tip");
    expect(within(tip).getByText("2024-03-05 14:07")).toBeInTheDocument();
    const rows = within(tip).getAllByTestId("viz-tip-row");
    expect(rows).toHaveLength(3);
    expect(within(rows[0]!).getByText("error")).toBeInTheDocument();
    expect(within(rows[0]!).getByText("12 lines")).toBeInTheDocument();
    expect(
      within(rows[0]!).getByTestId("viz-tip-swatch").style.background,
    ).toBe("red");
    expect(rows[2]).toHaveClass("viz-tip-muted");
    const footer = within(tip).getByTestId("viz-tip-footer");
    expect(within(footer).getByText("total")).toBeInTheDocument();
    expect(within(footer).getByText("537 lines")).toBeInTheDocument();
  });

  it("sits to the pointer's right and below it in the top-left quadrant", () => {
    render(
      <VizTooltip anchor={{ x: 100, y: 40 }} host={HOST} title="t" rows={[]} />,
    );
    const tip = screen.getByRole("tooltip");
    expect(tip.style.left).toBe("100px");
    expect(tip.style.top).toBe("40px");
    expect(tip.style.transform).toBe("translate(14px, 12px)");
  });

  it("flips to the pointer's left past the host midline", () => {
    render(
      <VizTooltip anchor={{ x: 900, y: 40 }} host={HOST} title="t" rows={[]} />,
    );
    expect(screen.getByRole("tooltip").style.transform).toBe(
      "translate(calc(-100% - 14px), 12px)",
    );
  });

  it("flips above the pointer near the bottom edge", () => {
    render(
      <VizTooltip
        anchor={{ x: 100, y: 790 }}
        host={HOST}
        title="t"
        rows={ROWS}
      />,
    );
    expect(screen.getByRole("tooltip").style.transform).toBe(
      "translate(14px, calc(-100% - 12px))",
    );
  });

  it("reserves a stable value column width when asked", () => {
    render(
      <VizTooltip
        anchor={{ x: 1, y: 1 }}
        host={HOST}
        title="t"
        rows={ROWS}
        valueWidthCh={13}
      />,
    );
    expect(
      screen.getByRole("tooltip").style.getPropertyValue("--viz-val-ch"),
    ).toBe("13");
  });
});

function PointerHost() {
  const ref = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(ref);
  return (
    <div
      ref={ref}
      data-testid="host"
      onPointerMove={pointer.track}
      onPointerLeave={pointer.clear}
    >
      <button type="button" onFocus={(e) => pointer.anchorTo(e.currentTarget)}>
        mark
      </button>
      {pointer.anchor && (
        <VizTooltip
          anchor={pointer.anchor}
          host={pointer.host}
          title={`${pointer.anchor.x},${pointer.anchor.y}`}
          rows={[]}
        />
      )}
    </div>
  );
}

describe("useVizPointer", () => {
  it("tracks the pointer relative to the host and clears on leave", () => {
    render(<PointerHost />);
    expect(screen.queryByRole("tooltip")).toBeNull();
    // jsdom reports a zero-origin host, so offsets equal the client point.
    fireEvent.pointerMove(screen.getByTestId("host"), {
      clientX: 220,
      clientY: 40,
    });
    expect(screen.getByRole("tooltip")).toHaveTextContent("220,40");
    fireEvent.pointerLeave(screen.getByTestId("host"));
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("anchors under a focused mark's centre without a pointer", () => {
    render(<PointerHost />);
    fireEvent.focus(screen.getByRole("button"));
    // The stubbed rect is 1200×800 at the origin: centre-x 600, bottom 800.
    expect(screen.getByRole("tooltip")).toHaveTextContent("600,800");
  });
});
