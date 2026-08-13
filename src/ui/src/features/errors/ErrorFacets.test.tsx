import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ErrorFacets } from "./ErrorFacets";
import type { ErrorGroup } from "../../api/errors";
import type { ErrorFilter } from "../../lib/errorFacets";

function group(overrides: Partial<ErrorGroup> = {}): ErrorGroup {
  return {
    source: "traces",
    exceptionType: "std::io::Error",
    exceptionMessage: "boom",
    serviceName: "signaldb",
    escaped: null,
    count: 3,
    firstNs: "1000",
    lastNs: "2000",
    ...overrides,
  };
}

const groups: ErrorGroup[] = [
  group({ exceptionType: "std::io::Error", source: "traces" }),
  group({
    exceptionType: "ValueError",
    serviceName: "signaldb-ui",
    source: "logs",
    count: 9,
  }),
];

describe("ErrorFacets", () => {
  it("lists the facet fields collapsed by default", () => {
    render(
      <ErrorFacets
        groups={groups}
        filters={[]}
        onAddFilter={vi.fn()}
        onRemoveFilter={vi.fn()}
      />,
    );
    expect(screen.getByText("Type")).toBeInTheDocument();
    expect(screen.getByText("Service")).toBeInTheDocument();
    expect(screen.getByText("Source")).toBeInTheDocument();
    expect(screen.queryByText("ValueError")).not.toBeInTheDocument();
  });

  it("expands a field to show its values with counts", async () => {
    render(
      <ErrorFacets
        groups={groups}
        filters={[]}
        onAddFilter={vi.fn()}
        onRemoveFilter={vi.fn()}
      />,
    );
    const user = userEvent.setup();
    await user.click(screen.getByText("Type"));
    expect(screen.getByText("ValueError")).toBeInTheDocument();
    expect(screen.getByText("std::io::Error")).toBeInTheDocument();
    expect(screen.getByText("9")).toBeInTheDocument();
  });

  it("adds a filter when an unselected value is clicked", async () => {
    const onAddFilter = vi.fn();
    render(
      <ErrorFacets
        groups={groups}
        filters={[]}
        onAddFilter={onAddFilter}
        onRemoveFilter={vi.fn()}
      />,
    );
    const user = userEvent.setup();
    await user.click(screen.getByText("Type"));
    await user.click(screen.getByText("ValueError"));
    expect(onAddFilter).toHaveBeenCalledWith({
      field: "exceptionType",
      value: "ValueError",
    });
  });

  it("removes a filter when an already-selected value is clicked", async () => {
    const onRemoveFilter = vi.fn();
    const filters: ErrorFilter[] = [
      { field: "exceptionType", value: "ValueError" },
    ];
    render(
      <ErrorFacets
        groups={groups}
        filters={filters}
        onAddFilter={vi.fn()}
        onRemoveFilter={onRemoveFilter}
      />,
    );
    const user = userEvent.setup();
    await user.click(screen.getByText("Type"));
    await user.click(screen.getByText("ValueError"));
    expect(onRemoveFilter).toHaveBeenCalledWith({
      field: "exceptionType",
      value: "ValueError",
    });
  });
});
