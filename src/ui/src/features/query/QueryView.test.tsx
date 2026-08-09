import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { renderWithClient } from "../../test/render";
import { resetApiClient, stubApiFetch } from "../../test/apiClient";
import { QueryView } from "./QueryView";

afterEach(resetApiClient);

describe("QueryView", () => {
  // Task 9.2 — the view is chosen from the declared envelope before results.
  it("selects the view from the declared envelope up front", () => {
    stubApiFetch({});
    renderWithClient(<QueryView />);

    // Default `rows` → list view, no query run yet.
    expect(screen.getByTestId("ir-view-list")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "series" },
    });
    expect(screen.getByTestId("ir-view-chart")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "table" },
    });
    expect(screen.getByTestId("ir-view-table")).toBeInTheDocument();
  });

  // Task 9.1 — the builder emits a valid IR document via the generated client.
  it("emits an IR document to /api/v1/query and renders the rows result", async () => {
    const calls = stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "service_name", type: "string" }],
      rows: [["checkout"]],
    });
    renderWithClient(<QueryView />);

    fireEvent.click(screen.getByText("Run"));

    await waitFor(() => expect(calls.length).toBeGreaterThan(0));
    const first = calls[0]!;
    // The request went to the native IR endpoint via the generated client.
    expect(first.url).toContain("/api/v1/query");
    // The body is a structured IR document (versioned), not a dialect string.
    const doc = first.body as { irVersion?: number; from?: string };
    expect(doc.irVersion).toBe(1);
    expect(doc.from).toBe("logs");

    // The rows envelope renders.
    await screen.findByText("checkout");
  });

  it("selects profile summaries and renders their generic rows envelope", async () => {
    const calls = stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "profile_id", type: "string" }],
      rows: [["profile-1"]],
    });
    renderWithClient(<QueryView />);

    fireEvent.change(screen.getByLabelText("source"), {
      target: { value: "profiles" },
    });
    fireEvent.click(screen.getByText("Run"));

    await waitFor(() => expect(calls.length).toBeGreaterThan(0));
    expect((calls[0]!.body as { from?: string }).from).toBe("profiles");
    await screen.findByText("profile-1");
  });

  it("copies rendered table cells", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "service_name", type: "string" }],
      rows: [["checkout"]],
    });
    renderWithClient(<QueryView />);

    fireEvent.click(screen.getByText("Run"));
    await userEvent.click(
      await screen.findByRole("button", { name: "Copy cell checkout" }),
    );

    expect(writeText).toHaveBeenCalledWith("checkout");
  });

  it("copies rendered series labels", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    stubApiFetch({
      result: "series",
      window: { start_ns: 0, end_ns: 1 },
      series: [{ labels: { service_name: "checkout" }, points: [] }],
    });
    renderWithClient(<QueryView />);

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "series" },
    });
    fireEvent.click(screen.getByText("Run"));
    await userEvent.click(
      await screen.findByRole("button", {
        name: "Copy series service_name=checkout",
      }),
    );

    expect(writeText).toHaveBeenCalledWith("service_name=checkout");
  });
});
