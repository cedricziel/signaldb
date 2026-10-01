import { fireEvent, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as evalSetsApi from "../../api/evalSets";
import { EvalApiError } from "../../api/evalSets";
import * as queryIrApi from "../../api/queryIr";
import { EvalSetsView } from "./EvalSetsView";
import { EVAL_SET_LIST } from "./evalFixtures";
import { mockEvalSetsApi, mockScenarioIr, renderEvalView } from "./testUtils";

vi.mock("../../api/queryIr", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/queryIr")>();
  return { ...actual, runIrQuery: vi.fn() };
});

vi.mock("../../api/evalSets", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/evalSets")>();
  return {
    ...actual,
    listEvalSets: vi.fn(),
    getEvalSet: vi.fn(),
    createEvalSet: vi.fn(),
    addCasesFromTraces: vi.fn(),
  };
});

beforeEach(() => {
  mockScenarioIr();
  mockEvalSetsApi();
});

afterEach(() => {
  vi.mocked(queryIrApi.runIrQuery).mockReset();
  vi.clearAllMocks();
});

const renderView = () =>
  renderEvalView(EvalSetsView, { tenant: "acme", dataset: "production" });

describe("EvalSetsView", () => {
  it("lists each set with what it was built from and its last run", async () => {
    renderView();
    const triage = await screen.findByRole("row", {
      name: /triage-golden-200/,
    });
    expect(
      within(triage).getByRole("link", { name: "triage-golden-200" }),
    ).toHaveAttribute(
      "href",
      "/evals/sets/triage-golden-200?tenant=acme&dataset=production",
    );
    expect(
      await within(triage).findByText("traces 3 · hand-written 9"),
    ).toBeInTheDocument();
    // The newest run of the set is the candidate, v1.8.0.
    expect(await within(triage).findByText("v1.8.0")).toBeInTheDocument();
    expect(within(triage).getByText("Sep 27")).toBeInTheDocument();
    expect(within(triage).getByText(/^\d+%$/)).toBeInTheDocument();

    const edge = screen.getByRole("row", { name: /refund-edge-cases-40/ });
    expect(await within(edge).findByText("JSONL upload")).toBeInTheDocument();
    expect(within(edge).getByText("never run")).toBeInTheDocument();
    expect(within(edge).getByText("—")).toBeInTheDocument();

    const saved = screen.getByRole("row", { name: /regressions-0927/ });
    expect(
      await within(saved).findByText("saved from Compare"),
    ).toBeInTheDocument();
    // "Built from" comes from the list's source counts, not a read per set.
    expect(evalSetsApi.getEvalSet).not.toHaveBeenCalled();
  });

  it("opens the New eval set dialog", async () => {
    renderView();
    fireEvent.click(
      await screen.findByRole("button", { name: "New eval set…" }),
    );
    expect(
      screen.getByRole("dialog", { name: "New eval set" }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("shows the empty state, without a create button for read-only callers", async () => {
    vi.mocked(evalSetsApi.listEvalSets).mockResolvedValue({
      items: [],
      _links: { self: EVAL_SET_LIST._links.self },
    });
    renderView();
    expect(await screen.findByText("No eval sets yet")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "New eval set…" }),
    ).not.toBeInTheDocument();
  });

  it("explains a missing scope and reports other failures", async () => {
    vi.mocked(evalSetsApi.listEvalSets).mockRejectedValue(
      new EvalApiError("missing scope", 403, []),
    );
    renderView();
    expect(
      await screen.findByText("You can't read eval sets in this dataset"),
    ).toBeInTheDocument();
  });

  it("reports a failed listing", async () => {
    vi.mocked(evalSetsApi.listEvalSets).mockRejectedValue(
      new EvalApiError("boom", 500, []),
    );
    renderView();
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Could not load eval sets: boom",
    );
  });
});
