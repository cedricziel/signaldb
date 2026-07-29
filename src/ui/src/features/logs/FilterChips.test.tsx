import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { LabelFilter } from "../../lib/filters";
import { FilterChips } from "./FilterChips";

const filters: LabelFilter[] = [
  { label: "service_name", op: "=", value: "checkout" },
  { label: "level", op: "!=", value: "debug" },
];

describe("FilterChips", () => {
  it("renders one chip per filter", () => {
    render(<FilterChips filters={filters} labels={[]} onChange={() => {}} />);
    expect(screen.getByText("service_name")).toBeInTheDocument();
    expect(screen.getByText("checkout")).toBeInTheDocument();
    expect(screen.getByText("level")).toBeInTheDocument();
  });

  it("removes a chip", async () => {
    const onChange = vi.fn();
    render(<FilterChips filters={filters} labels={[]} onChange={onChange} />);
    await userEvent.click(
      screen.getByRole("button", {
        name: "Remove filter service_name = checkout",
      }),
    );
    expect(onChange).toHaveBeenCalledWith([filters[1]]);
  });

  it("adds a filter through the inline form", async () => {
    const onChange = vi.fn();
    render(<FilterChips filters={[]} labels={["level"]} onChange={onChange} />);
    await userEvent.click(screen.getByRole("button", { name: "+ filter" }));
    await userEvent.type(screen.getByLabelText("Filter label"), "level");
    await userEvent.selectOptions(
      screen.getByLabelText("Filter operator"),
      "=~",
    );
    await userEvent.type(screen.getByLabelText("Filter value"), "err.*");
    await userEvent.click(screen.getByRole("button", { name: "Add" }));
    expect(onChange).toHaveBeenCalledWith([
      { label: "level", op: "=~", value: "err.*" },
    ]);
  });

  it("refuses to add a filter with an invalid label name", async () => {
    const onChange = vi.fn();
    render(<FilterChips filters={[]} labels={[]} onChange={onChange} />);
    await userEvent.click(screen.getByRole("button", { name: "+ filter" }));
    await userEvent.type(screen.getByLabelText("Filter label"), "bad label!");
    await userEvent.click(screen.getByRole("button", { name: "Add" }));
    expect(onChange).not.toHaveBeenCalled();
  });
});
