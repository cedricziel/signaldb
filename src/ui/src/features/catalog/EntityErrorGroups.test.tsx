import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";
import { resolveRange } from "../../lib/time";
import { EntityErrorGroups } from "./EntityErrorGroups";
import * as errorsApi from "../../api/errors";
import type { ErrorGroup, ErrorGroupResult } from "../../api/errors";

vi.mock("../../api/errors", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/errors")>();
  return {
    ...actual,
    fetchErrorGroups: vi.fn(),
    fetchErrorGroupVolume: vi.fn(),
  };
});

const fetchErrorGroups = vi.mocked(errorsApi.fetchErrorGroups);
const fetchErrorGroupVolume = vi.mocked(errorsApi.fetchErrorGroupVolume);

function makeGroup(
  exceptionType: string,
  count: number,
  source: "traces" | "logs" = "traces",
): ErrorGroup {
  return {
    source,
    exceptionType,
    exceptionMessage: `${exceptionType} happened`,
    serviceName: "checkout",
    escaped: "true",
    count,
    firstNs: "1000",
    lastNs: "1700003600000000000",
  };
}

function result(groups: ErrorGroup[]): ErrorGroupResult {
  return { groups, truncated: false };
}

const range = resolveRange({ type: "relative", seconds: 3600 }, Date.now());

beforeEach(() => {
  fetchErrorGroups.mockReset();
  fetchErrorGroupVolume.mockReset();
  fetchErrorGroupVolume.mockResolvedValue([]);
});

afterEach(() => {
  vi.restoreAllMocks();
});

function renderView() {
  const update = vi.fn();
  renderWithClient(
    <EntityErrorGroups
      serviceName="checkout"
      range={range}
      rangeKey="rk"
      update={update}
    />,
  );
  return update;
}

describe("EntityErrorGroups", () => {
  it("fetches error groups filtered to the given service", async () => {
    fetchErrorGroups.mockResolvedValue(result([makeGroup("BoomError", 5)]));
    renderView();
    await waitFor(() =>
      expect(fetchErrorGroups).toHaveBeenCalledWith(range, "checkout"),
    );
  });

  it("shows only the top 5 groups by count", async () => {
    const groups = Array.from({ length: 8 }, (_, i) =>
      makeGroup(`Error${i}`, 100 - i),
    );
    fetchErrorGroups.mockResolvedValue(result(groups));
    renderView();
    const rows = await screen.findAllByTestId("error-groups-row");
    expect(rows).toHaveLength(5);
    expect(within(rows[0]!).getByText("Error0")).toBeInTheDocument();
    expect(within(rows[4]!).getByText("Error4")).toBeInTheDocument();
  });

  it("shows the count, source, and last-seen columns for each group", async () => {
    fetchErrorGroups.mockResolvedValue(
      result([makeGroup("BoomError", 42, "logs")]),
    );
    renderView();
    expect(await screen.findByText("BoomError")).toBeInTheDocument();
    expect(screen.getByText("BoomError happened")).toBeInTheDocument();
    expect(screen.getByText("logs")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
  });

  it("shows the row sparkline as a red line, not bars", async () => {
    fetchErrorGroupVolume.mockResolvedValue([
      {
        key: "s0",
        points: [
          [0, 1],
          [120_000, 3],
        ],
      },
    ]);
    fetchErrorGroups.mockResolvedValue(result([makeGroup("BoomError", 5)]));
    renderView();
    const row = await screen.findByTestId("error-groups-row");
    const line = await within(row).findByRole("img", {
      name: "Occurrences over the last hour",
    });
    expect(line.querySelector("polyline")).toHaveClass("sparkline-tone-error");
    expect(line.querySelector("rect[data-testid='sparkline-bar']")).toBeNull();
  });

  it("shows an empty state naming the service when there are no groups", async () => {
    fetchErrorGroups.mockResolvedValue(result([]));
    renderView();
    expect(
      await screen.findByText("No errors for checkout in this window."),
    ).toBeInTheDocument();
  });

  it('the "All errors" button jumps to the Errors tab filtered to this service', async () => {
    fetchErrorGroups.mockResolvedValue(result([makeGroup("BoomError", 5)]));
    const update = renderView();
    const user = userEvent.setup();
    await user.click(
      await screen.findByRole("button", { name: "All errors for checkout" }),
    );
    expect(update).toHaveBeenCalledWith(
      {
        signal: "errors",
        filters: [{ label: "serviceName", op: "=", value: "checkout" }],
      },
      { push: true },
    );
  });

  it("clicking a row opens that group in the Errors tab", async () => {
    const group = makeGroup("BoomError", 5);
    fetchErrorGroups.mockResolvedValue(result([group]));
    const update = renderView();
    const user = userEvent.setup();
    const row = await screen.findByTestId("error-groups-row");
    await user.click(row);
    expect(update).toHaveBeenCalledWith(
      {
        signal: "errors",
        group: expect.stringContaining("BoomError"),
      },
      { push: true },
    );
  });
});
