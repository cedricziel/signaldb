import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { FieldSidebar } from "./FieldSidebar";

const RANGE = { fromMs: 0, toMs: 1000 };

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("FieldSidebar", () => {
  it("lists labels and filters them by search text", async () => {
    renderWithClient(
      <FieldSidebar
        labels={["service_name", "level", "http_method"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    expect(screen.getByText("service_name")).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("Filter fields"), "lev");
    expect(screen.queryByText("service_name")).not.toBeInTheDocument();
    expect(screen.getByText("level")).toBeInTheDocument();
  });

  it("loads values on expand and adds a filter on click", async () => {
    stubFetchRoutes([
      {
        match: "/loki/api/v1/label/level/values",
        body: { status: "success", data: ["error", "info"] },
      },
    ]);
    const onAddFilter = vi.fn();
    renderWithClient(
      <FieldSidebar
        labels={["level"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={onAddFilter}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "level" }));
    await userEvent.click(await screen.findByRole("button", { name: "error" }));
    expect(onAddFilter).toHaveBeenCalledWith({
      label: "level",
      op: "=",
      value: "error",
    });
  });

  it("shows a failure note when values cannot load", async () => {
    stubFetchRoutes([
      { match: "/values", body: { error: "boom" }, status: 500 },
    ]);
    renderWithClient(
      <FieldSidebar
        labels={["level"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "level" }));
    expect(
      await screen.findByText("Failed to load values"),
    ).toBeInTheDocument();
  });
});
