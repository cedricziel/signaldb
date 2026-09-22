import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TimeRangePicker } from "./TimeRangePicker";

describe("TimeRangePicker", () => {
  it("selects the matching preset for a known relative range", () => {
    render(
      <TimeRangePicker
        range={{ type: "relative", seconds: 3600 }}
        onChange={vi.fn()}
      />,
    );
    expect(screen.getByRole("combobox")).toHaveValue("3600");
  });

  it("renders and selects an extra option for a non-preset relative range", () => {
    render(
      <TimeRangePicker
        range={{ type: "relative", seconds: 1800 }}
        onChange={vi.fn()}
      />,
    );
    const select = screen.getByRole("combobox");
    expect(select).toHaveValue("1800");
    expect(screen.getByRole("option", { name: "Last 30m" })).toBeInTheDocument();
  });

  it("renders and selects an extra option for an absolute range", () => {
    render(
      <TimeRangePicker
        range={{ type: "absolute", fromMs: 0, toMs: 60_000 }}
        onChange={vi.fn()}
      />,
    );
    const select = screen.getByRole("combobox");
    expect(select).toHaveValue("absolute");
  });
});
