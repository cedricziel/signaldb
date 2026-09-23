import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ShareBar } from "./ShareBar";

const segments = [
  { key: "database", value: 300, color: "var(--svc-a)", label: "Database" },
  { key: "http", value: 100, color: "var(--svc-b)", label: "HTTP" },
];

describe("ShareBar", () => {
  it("renders one segment per entry, proportioned by value", () => {
    render(<ShareBar segments={segments} />);
    const segs = screen.getAllByTestId("share-bar-seg");
    expect(segs).toHaveLength(2);
    expect(segs[0]).toHaveStyle({ width: "75%" });
    expect(segs[1]).toHaveStyle({ width: "25%" });
  });

  it("renders a single fill against a track when given a fraction", () => {
    render(<ShareBar fraction={0.4} />);
    expect(screen.getByTestId("share-bar-fill")).toHaveStyle({
      width: "40%",
    });
  });

  it("renders an optional legend", () => {
    render(<ShareBar segments={segments} legend />);
    expect(screen.getByText("Database")).toBeInTheDocument();
    expect(screen.getByText("HTTP")).toBeInTheDocument();
  });

  it("omits the legend by default", () => {
    render(<ShareBar segments={segments} />);
    expect(screen.queryByText("Database")).not.toBeInTheDocument();
  });
});
