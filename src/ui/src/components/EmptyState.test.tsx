import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { EmptyState } from "./EmptyState";

describe("EmptyState", () => {
  it("renders the title as a status region, with no detail line by default", () => {
    render(<EmptyState title="No log lines in this range" />);
    const status = screen.getByRole("status");
    expect(status).toHaveTextContent("No log lines in this range");
    expect(status.querySelector(".empty-state-detail")).toBeNull();
  });

  it("renders an optional detail line below the title", () => {
    render(
      <EmptyState title="No profiles in this range">
        Enable continuous profiling to start collecting them.
      </EmptyState>,
    );
    const status = screen.getByRole("status");
    expect(status).toHaveTextContent("No profiles in this range");
    expect(status).toHaveTextContent(
      "Enable continuous profiling to start collecting them.",
    );
  });
});
