import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resetSemanticsCache } from "../../hooks/useSemantics";
import type { LabelFilter } from "../../lib/filters";
import { stubFetchRoutes } from "../../test/render";
import { FilterChips } from "./FilterChips";

afterEach(() => {
  vi.unstubAllGlobals();
  resetSemanticsCache();
});

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

  it("suggests registry keys with briefs merged with observed labels", async () => {
    const hit = (key: string, brief: string) => ({
      key,
      brief,
      type: "string",
      group_id: "registry.http",
      namespace: "otel",
      version: "1.43.0",
      source: "bundled",
    });
    const fetchMock = stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: {
          hits: [
            hit("http.request.method", "HTTP request method."),
            hit("http.response.status_code", "HTTP response status code."),
          ],
        },
      },
    ]);
    const onChange = vi.fn();
    render(
      <FilterChips
        filters={[]}
        labels={["http.request.method", "http.retries", "level"]}
        onChange={onChange}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "+ filter" }));
    await userEvent.type(screen.getByLabelText("Filter label"), "http.re");

    // Observed labels show at once; registry hits merge in when they land.
    await screen.findByText("HTTP request method.");
    const list = screen.getByRole("listbox", { name: "Attribute key suggestions" });
    const options = within(list).getAllByRole("option");
    expect(options.map((o) => o.getAttribute("data-key"))).toEqual([
      "http.request.method",
      "http.response.status_code",
      "http.retries",
    ]);
    expect(options[0]).toHaveTextContent("HTTP request method.");
    expect(options[0]).toHaveTextContent("otel");
    expect(options[0]).toHaveTextContent("seen");
    expect(options[1]).not.toHaveTextContent("seen");
    expect(options[2]).not.toHaveTextContent("otel");
    const url = new URL(
      String((fetchMock.mock.calls.at(-1)![0] as Request).url),
    );
    expect(url.searchParams.get("prefix")).toBe("http.re");

    await userEvent.click(options[1]!);
    expect(screen.getByLabelText("Filter label")).toHaveValue(
      "http.response.status_code",
    );
    expect(
      screen.queryByRole("listbox", { name: "Attribute key suggestions" }),
    ).not.toBeInTheDocument();
  });

  it("falls back to observed labels when the registry search fails", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: { error: "boom" },
        status: 500,
      },
    ]);
    render(
      <FilterChips filters={[]} labels={["level", "line"]} onChange={() => {}} />,
    );
    await userEvent.click(screen.getByRole("button", { name: "+ filter" }));
    await userEvent.type(screen.getByLabelText("Filter label"), "le");
    const list = await screen.findByRole("listbox", { name: "Attribute key suggestions" });
    expect(
      within(list)
        .getAllByRole("option")
        .map((o) => o.getAttribute("data-key")),
    ).toEqual(["level"]);
    expect(screen.queryByText(/boom/)).not.toBeInTheDocument();
  });
});
