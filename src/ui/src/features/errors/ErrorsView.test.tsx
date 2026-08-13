import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE } from "../../lib/urlState";
import { renderWithClient } from "../../test/render";
import { ErrorsView } from "./ErrorsView";
import * as errorsApi from "../../api/errors";
import type { ErrorGroup } from "../../api/errors";

vi.mock("../../api/errors", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/errors")>();
  return {
    ...actual,
    fetchErrorGroups: vi.fn(),
    fetchErrorExample: vi.fn(),
  };
});

const fetchErrorGroups = vi.mocked(errorsApi.fetchErrorGroups);
const fetchErrorExample = vi.mocked(errorsApi.fetchErrorExample);

afterEach(() => {
  vi.restoreAllMocks();
});

beforeEach(() => {
  fetchErrorGroups.mockReset();
  fetchErrorExample.mockReset();
  fetchErrorGroups.mockResolvedValue({ groups: [], truncated: false });
  fetchErrorExample.mockResolvedValue(null);
});

function group(overrides: Partial<ErrorGroup> = {}): ErrorGroup {
  return {
    source: "traces",
    exceptionType: "std::io::Error",
    exceptionMessage: "boom",
    serviceName: "signaldb",
    count: 3,
    firstNs: "1700000000000000000",
    lastNs: "1700000100000000000",
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

  it("fetches and shows a stacktrace when a group is selected", async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [group()],
      truncated: false,
    });
    fetchErrorExample.mockResolvedValue({
      traceId: "abc123",
      stacktrace: "at foo\n at bar",
    });
    renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("std::io::Error"));
    expect(await screen.findByText(/at foo/)).toBeInTheDocument();
    expect(fetchErrorExample).toHaveBeenCalledWith(group(), expect.anything());
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

  it('offers a "View trace" link once the example resolves a trace id', async () => {
    fetchErrorGroups.mockResolvedValue({
      groups: [group()],
      truncated: false,
    });
    fetchErrorExample.mockResolvedValue({
      traceId: "abc123",
      stacktrace: "at foo",
    });
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("std::io::Error"));
    const link = await screen.findByRole("button", { name: /View trace/ });
    await user.click(link);
    expect(update).toHaveBeenCalledWith(
      { signal: "traces", trace: "abc123" },
      { push: true },
    );
  });
});
