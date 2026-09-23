import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { KpiCard, KpiStrip } from "./KpiCard";

describe("KpiCard", () => {
  it("renders a label and value", () => {
    render(<KpiCard label="Requests" value="1,204" unit="req/s" />);
    expect(screen.getByText("Requests")).toBeInTheDocument();
    expect(screen.getByText("1,204")).toBeInTheDocument();
    expect(screen.getByText("req/s")).toBeInTheDocument();
  });

  it("renders a change figure with a tone and direction", () => {
    render(
      <KpiCard
        label="Error rate"
        value="2.1%"
        change={{ text: "+6% vs prev", direction: "up", tone: "bad" }}
      />,
    );
    const change = screen.getByText("+6% vs prev");
    expect(change).toHaveClass("kpi-change-bad");
    expect(change).toHaveClass("kpi-change-up");
  });

  it("renders an optional detail line", () => {
    render(<KpiCard label="p95" value="120ms" detail="last 15 minutes" />);
    expect(screen.getByText("last 15 minutes")).toBeInTheDocument();
  });

  it("applies a value tone", () => {
    render(<KpiCard label="Errors" value="42" valueTone="error" />);
    expect(screen.getByText("42")).toHaveClass("kpi-value-error");
  });

  it("renders a sparkline slot via children", () => {
    render(
      <KpiCard label="Latency" value="12ms">
        <svg data-testid="child-chart" />
      </KpiCard>,
    );
    expect(screen.getByTestId("child-chart")).toBeInTheDocument();
  });
});

describe("KpiStrip", () => {
  it("renders its children in a grid wrapper", () => {
    render(
      <KpiStrip>
        <KpiCard label="A" value="1" />
        <KpiCard label="B" value="2" />
      </KpiStrip>,
    );
    expect(screen.getAllByText(/^[AB]$/)).toHaveLength(2);
  });
});
