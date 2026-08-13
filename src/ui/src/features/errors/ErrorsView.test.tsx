import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE } from "../../lib/urlState";
import { renderWithClient } from "../../test/render";
import { ErrorsView } from "./ErrorsView";
import * as errorsApi from "../../api/errors";
import type { ErrorGroup, ErrorOccurrence } from "../../api/errors";

vi.mock("../../api/errors", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/errors")>();
  return {
    ...actual,
    fetchErrorGroups: vi.fn(),
    fetchErrorOccurrences: vi.fn(),
    fetchErrorGroupVolume: vi.fn(),
  };
});

const fetchErrorGroups = vi.mocked(errorsApi.fetchErrorGroups);
const fetchErrorOccurrences = vi.mocked(errorsApi.fetchErrorOccurrences);
const fetchErrorGroupVolume = vi.mocked(errorsApi.fetchErrorGroupVolume);

afterEach(() => {
  vi.restoreAllMocks();
});

beforeEach(() => {
  fetchErrorGroups.mockReset();
  fetchErrorOccurrences.mockReset();
  fetchErrorGroupVolume.mockReset();
  fetchErrorGroups.mockResolvedValue({ groups: [], truncated: false });
  fetchErrorOccurrences.mockResolvedValue([]);
  fetchErrorGroupVolume.mockResolvedValue([]);
});

function group(overrides: Partial<ErrorGroup> = {}): ErrorGroup {
  return {
    source: "traces",
    exceptionType: "std::io::Error",
    exceptionMessage: "boom",
    serviceName: "signaldb",
    escaped: null,
    count: 3,
    firstNs: "1700000000000000000",
    lastNs: "1700000100000000000",
    ...overrides,
  };
}

function occurrence(overrides: Partial<ErrorOccurrence> = {}): ErrorOccurrence {
  return {
    timestampNs: "1700000100000000000",
    traceId: "abc123",
    stacktrace: "at foo\n at bar",
    ...overrides,
  };
}

function renderView() {
  const update = vi.fn();
  renderWithClient(<ErrorsView state={DEFAULT_STATE} update={update} />);
  return update;
}

describe("ErrorsView", () => {
  it("shows an empty state when there are no captured exceptions", async () => {
    renderView();
    expect(
      await screen.findByText(/No exceptions captured/),
    ).toBeInTheDocument();
  });

  it("lists exception groups ranked by count, across both sources", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [
        group({ source: "traces", exceptionType: "std::io::Error" }),
        group({
          source: "logs",
          exceptionType: "ValueError",
          serviceName: "signaldb-ui",
          count: 9,
        }),
      ],
      truncated: false,
    });
    renderView();
    expect(await screen.findByText("std::io::Error")).toBeInTheDocument();
    expect(screen.getByText("ValueError")).toBeInTheDocument();
    expect(screen.getByText("signaldb-ui")).toBeInTheDocument();
    expect(screen.getByText("traces")).toBeInTheDocument();
    expect(screen.getByText("logs")).toBeInTheDocument();
  });

  it("lists a group's individual occurrences when selected", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [group()],
      truncated: false,
    });
    fetchErrorOccurrences.mockResolvedValue([
      occurrence({ timestampNs: "2000", traceId: "trace-a" }),
      occurrence({ timestampNs: "1000", traceId: "trace-b" }),
    ]);
    renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("std::io::Error"));
    expect(fetchErrorOccurrences).toHaveBeenCalledWith(
      group(),
      expect.anything(),
    );
    // Two distinct occurrences, each with its own trace link.
    const links = await screen.findAllByRole("button", {
      name: /View trace/,
    });
    expect(links).toHaveLength(2);
  });

  it("selects a group via keyboard (focus + Enter on its type button)", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [group()],
      truncated: false,
    });
    fetchErrorOccurrences.mockResolvedValue([occurrence()]);
    renderView();
    const user = userEvent.setup();
    const typeButton = await screen.findByRole("button", {
      name: "std::io::Error",
    });
    typeButton.focus();
    await user.keyboard("{Enter}");
    expect(fetchErrorOccurrences).toHaveBeenCalledWith(
      group(),
      expect.anything(),
    );
  });

  it("expands an occurrence via keyboard (focus + Enter on its time button)", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [group()],
      truncated: false,
    });
    fetchErrorOccurrences.mockResolvedValue([
      occurrence({ stacktrace: "at foo\n at bar" }),
    ]);
    renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("std::io::Error"));
    const timeButton = await screen.findByRole("button", {
      name: /\d/, // the formatted timestamp
    });
    timeButton.focus();
    await user.keyboard("{Enter}");
    expect(await screen.findByText(/at foo/)).toBeInTheDocument();
  });

  it("does not offer a trace link for an occurrence with no active trace", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [group()],
      truncated: false,
    });
    fetchErrorOccurrences.mockResolvedValue([occurrence({ traceId: null })]);
    renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("std::io::Error"));
    await screen.findByTestId("occurrence-row-0");
    expect(
      screen.queryByRole("button", { name: /View trace/ }),
    ).not.toBeInTheDocument();
  });

  it("expands an occurrence to show its own stacktrace", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [group()],
      truncated: false,
    });
    fetchErrorOccurrences.mockResolvedValue([
      occurrence({ stacktrace: "at foo\n at bar" }),
    ]);
    renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("std::io::Error"));
    expect(screen.queryByText(/at foo/)).not.toBeInTheDocument();
    await user.click(await screen.findByTestId("occurrence-row-0"));
    expect(await screen.findByText(/at foo/)).toBeInTheDocument();
  });

  it("clicking a trace link navigates without expanding the row", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [group()],
      truncated: false,
    });
    fetchErrorOccurrences.mockResolvedValue([
      occurrence({ traceId: "abc123", stacktrace: "at foo" }),
    ]);
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("std::io::Error"));
    const link = await screen.findByRole("button", { name: /View trace/ });
    await user.click(link);
    expect(update).toHaveBeenCalledWith(
      { signal: "traces", trace: "abc123" },
      { push: true },
    );
    // The row itself did not also toggle open from the same click.
    expect(screen.queryByText(/at foo/)).not.toBeInTheDocument();
  });

  it("narrows the list via the facet sidebar", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [
        group({ source: "traces", exceptionType: "std::io::Error" }),
        group({
          source: "logs",
          exceptionType: "ValueError",
          serviceName: "signaldb-ui",
          count: 9,
        }),
      ],
      truncated: false,
    });
    renderView();
    const user = userEvent.setup();
    await screen.findByText("std::io::Error");

    // Expand the Source facet and select "logs" (the facet *value* button,
    // not the table's own "logs" source badge for the ValueError row).
    await user.click(screen.getByRole("button", { name: "Source" }));
    await user.click(screen.getByRole("button", { name: /logs/ }));

    expect(screen.queryByText("std::io::Error")).not.toBeInTheDocument();
    expect(screen.getByText("ValueError")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /Remove filter source = logs/ }),
    ).toBeInTheDocument();
  });

  it("shows Handled/Unhandled/— per group's exception.escaped", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [
        group({ exceptionType: "A", escaped: "true" }),
        group({ exceptionType: "B", escaped: "false" }),
        group({ exceptionType: "C", escaped: null }),
      ],
      truncated: false,
    });
    renderView();
    await screen.findByText("A");
    expect(screen.getByText("Unhandled")).toBeInTheDocument();
    // "Handled" also names the column header, so scope to a data row's cell.
    const cRow = screen.getByText("C").closest("tr")!;
    expect(within(cRow).getByText("—")).toBeInTheDocument();
    const bRow = screen.getByText("B").closest("tr")!;
    expect(within(bRow).getByText("Handled")).toBeInTheDocument();
  });

  it("sorts by last-seen when the Last seen header is clicked", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [
        group({ exceptionType: "Older", count: 9, lastNs: "1000" }),
        group({ exceptionType: "Newer", count: 1, lastNs: "2000" }),
      ],
      truncated: false,
    });
    renderView();
    const user = userEvent.setup();
    await screen.findByText("Older");

    // Default sort is by count: "Older" (9) before "Newer" (1).
    let rows = screen.getAllByRole("row").slice(1); // drop header row
    expect(rows[0]).toHaveTextContent("Older");

    await user.click(screen.getByRole("button", { name: "Last seen" }));

    rows = screen.getAllByRole("row").slice(1);
    expect(rows[0]).toHaveTextContent("Newer");
  });

  it("shows a count-over-time sparkline for the selected group", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [group()],
      truncated: false,
    });
    fetchErrorGroupVolume.mockResolvedValue([
      { key: "s0", points: [[1_700_000_000_000, 3]] },
    ]);
    renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("std::io::Error"));
    expect(
      await screen.findByRole("img", { name: /Occurrences over time/ }),
    ).toBeInTheDocument();
    expect(fetchErrorGroupVolume).toHaveBeenCalledWith(
      group(),
      expect.anything(),
      expect.any(String),
    );
  });
});
