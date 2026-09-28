import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as evalSetsApi from "../../api/evalSets";
import { EvalApiError, type EvalSetResponse } from "../../api/evalSets";
import * as queryIrApi from "../../api/queryIr";
import type { ShellContext } from "../../lib/outletState";
import { EvalSetView } from "./EvalSetView";
import { EVAL_SET_DETAILS } from "./evalFixtures";
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
    deleteEvalSet: vi.fn(),
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

function renderSet(name = "triage-golden-200") {
  const View = (shell: ShellContext) => <EvalSetView {...shell} name={name} />;
  return renderEvalView(View, { tenant: "acme", dataset: "production" });
}

const location = () => screen.getByTestId("location").textContent;

describe("EvalSetView", () => {
  it("shows the cases with their source and score in the newest run", async () => {
    renderSet();
    expect(await screen.findByText("12 cases")).toBeInTheDocument();
    expect(
      await screen.findByText(/Last replayed on v1\.8\.0/),
    ).toBeInTheDocument();

    const regressed = screen.getByRole("row", { name: /case-117/ });
    expect(
      within(regressed).getByText("lookup_order → check_policy → issue_refund"),
    ).toBeInTheDocument();
    expect(
      within(regressed).getByText("Decline the refund, open a warranty claim"),
    ).toBeInTheDocument();
    expect(
      within(regressed).getByRole("link", { name: /^trace 4bf92f35…$/ }),
    ).toHaveAttribute(
      "href",
      expect.stringMatching(/^\/traces\/4bf92f3577b34da6a3ce929d0e0e4700\?/),
    );
    expect(await within(regressed).findByText("3 failing")).toHaveAttribute(
      "title",
      "Failing: Correctness, Groundedness, ToolTrajectory",
    );

    const passing = screen.getByRole("row", { name: /case-001/ });
    expect(within(passing).getByText("hand-written")).toBeInTheDocument();
    expect(within(passing).getByText("pass")).toBeInTheDocument();
    // A judge timeout leaves Correctness without a verdict.
    const timedOut = screen.getByRole("row", { name: /case-003/ });
    expect(within(timedOut).getByText("3 of 4")).toBeInTheDocument();

    expect(screen.getByText("Showing 12 of 12")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "v1.7.3 → v1.8.0 ›" }),
    ).toHaveAttribute(
      "href",
      expect.stringContaining("baseline=run-0923-0915&candidate=run-0927-1004"),
    );
    expect(screen.getByRole("tab", { name: "Runs (2)" })).toHaveAttribute(
      "href",
      expect.stringContaining("set=triage-golden-200"),
    );
  });

  it("exports the cases as JSONL, one case per line", async () => {
    const blobs: Blob[] = [];
    const createObjectURL = vi.fn((b: Blob) => {
      blobs.push(b);
      return "blob:cases";
    });
    const revokeObjectURL = vi.fn();
    vi.stubGlobal("URL", { ...URL, createObjectURL, revokeObjectURL });
    const click = vi
      .spyOn(HTMLAnchorElement.prototype, "click")
      .mockImplementation(() => {});
    renderSet();
    fireEvent.click(
      await screen.findByRole("button", { name: "Export JSONL" }),
    );
    expect(click).toHaveBeenCalled();
    const lines = (await blobs[0]!.text()).trim().split("\n");
    expect(lines).toHaveLength(12);
    expect(JSON.parse(lines[0]!)).toEqual(
      EVAL_SET_DETAILS["triage-golden-200"]!.cases[0],
    );
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:cases");
    click.mockRestore();
    vi.unstubAllGlobals();
  });

  it("adds cases from traces and reports the counts the server returns", async () => {
    vi.mocked(evalSetsApi.addCasesFromTraces).mockResolvedValue({
      matches: 214,
      already_present: 12,
      added: 50,
      added_ids: [],
    });
    renderSet();
    const toggle = await screen.findByRole("button", { name: "Add traces…" });
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    const panel = screen.getByRole("complementary", { name: "Add traces" });
    expect(within(panel).getByLabelText("Agent")).toHaveValue("support-triage");
    fireEvent.change(within(panel).getByLabelText("Window"), {
      target: { value: "30d" },
    });
    const failing = within(panel).getByLabelText("Failing evaluator");
    await within(failing).findByRole("option", { name: "Correctness = fail" });
    fireEvent.change(failing, { target: { value: "Correctness" } });
    expect(
      within(panel).getByText(/These runs failed Correctness/),
    ).toBeInTheDocument();
    fireEvent.change(within(panel).getByLabelText("Sample"), {
      target: { value: "0" },
    });
    expect(
      within(panel).getByRole("button", { name: "Add up to ? cases" }),
    ).toBeDisabled();
    fireEvent.change(within(panel).getByLabelText("Sample"), {
      target: { value: "20" },
    });
    fireEvent.click(
      within(panel).getByRole("checkbox", {
        name: "Use the agent's answer as the reference",
      }),
    );
    fireEvent.click(
      within(panel).getByRole("button", { name: "Add up to 20 cases" }),
    );
    const stats = await within(panel).findByRole("status", {
      name: "Last add",
    });
    expect(stats).toHaveTextContent("Matches 214");
    expect(stats).toHaveTextContent("Already in set 12");
    expect(stats).toHaveTextContent("Added 50");
    expect(evalSetsApi.addCasesFromTraces).toHaveBeenCalledWith(
      "triage-golden-200",
      {
        range: { from: "now-30d", to: "now" },
        agent: "support-triage",
        failing_evaluator: "Correctness",
        sample: 20,
        expected_tools: true,
        reference_from_answer: true,
      },
    );
    fireEvent.click(
      within(panel).getByRole("button", { name: "Close Add traces" }),
    );
    expect(screen.queryByRole("complementary")).not.toBeInTheDocument();
  });

  it("shows a failed add", async () => {
    vi.mocked(evalSetsApi.addCasesFromTraces).mockRejectedValue(
      new EvalApiError("missing scope traces:read", 403, []),
    );
    renderSet();
    fireEvent.click(await screen.findByRole("button", { name: "Add traces…" }));
    fireEvent.click(screen.getByRole("button", { name: "Add up to 50 cases" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "missing scope traces:read",
    );
  });

  it("deletes the set from Settings and returns to the list", async () => {
    vi.mocked(evalSetsApi.deleteEvalSet).mockResolvedValue();
    renderSet();
    fireEvent.click(await screen.findByRole("tab", { name: "Settings" }));
    expect(
      screen.getByText("Core support flows: refunds, tracking, account"),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Delete eval set" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm" }));
    await waitFor(() =>
      expect(location()).toBe("/evals/sets?tenant=acme&dataset=production"),
    );
    expect(evalSetsApi.deleteEvalSet).toHaveBeenCalledWith("triage-golden-200");
  });

  it("reports a refused delete", async () => {
    vi.mocked(evalSetsApi.deleteEvalSet).mockRejectedValue(
      new EvalApiError("forbidden", 403, []),
    );
    renderSet();
    fireEvent.click(await screen.findByRole("tab", { name: "Settings" }));
    fireEvent.click(screen.getByRole("button", { name: "Delete eval set" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("forbidden");
    fireEvent.click(screen.getByRole("tab", { name: "Cases" }));
    expect(screen.getByRole("table")).toBeInTheDocument();
  });

  it("hides writes from a read-only caller", async () => {
    const set = EVAL_SET_DETAILS["refund-edge-cases-40"]!;
    vi.mocked(evalSetsApi.getEvalSet).mockResolvedValue({
      ...set,
      _links: { self: set._links.self },
    } as EvalSetResponse);
    renderSet("refund-edge-cases-40");
    expect(
      await screen.findByRole("button", { name: "Add traces…" }),
    ).toBeDisabled();
    expect(screen.getAllByText("upload")).toHaveLength(3);
    expect(screen.getByText(/Not replayed in the last 30 days/)).toBeTruthy();
    expect(
      screen.getByText("Two runs of this set are needed"),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "Settings" }));
    expect(
      screen.getByRole("button", { name: "Delete eval set" }),
    ).toBeDisabled();
    expect(screen.getByText(/Deleting needs the/)).toBeInTheDocument();
  });

  it("pages a large set", async () => {
    const set = EVAL_SET_DETAILS["refund-edge-cases-40"]!;
    const cases = Array.from({ length: 120 }, (_, i) => ({
      id: `c-${i}`,
      input: `input ${i}`,
    }));
    vi.mocked(evalSetsApi.getEvalSet).mockResolvedValue({
      ...set,
      case_count: 120,
      cases,
    } as EvalSetResponse);
    renderSet("refund-edge-cases-40");
    expect(await screen.findByText("Showing 50 of 120")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Load more" }));
    fireEvent.click(screen.getByRole("button", { name: "Load more" }));
    expect(screen.getByText("Showing 120 of 120")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Load more" }),
    ).not.toBeInTheDocument();
  });

  it("says when the set doesn't exist or can't be read", async () => {
    renderSet("nope");
    expect(
      await screen.findByText("No eval set named nope"),
    ).toBeInTheDocument();
  });

  it("explains a missing read scope and other failures", async () => {
    vi.mocked(evalSetsApi.getEvalSet).mockRejectedValueOnce(
      new EvalApiError("no", 403, []),
    );
    renderSet();
    expect(
      await screen.findByText("You can't read eval sets in this dataset"),
    ).toBeInTheDocument();
  });

  it("reports an unexpected failure", async () => {
    vi.mocked(evalSetsApi.getEvalSet).mockRejectedValueOnce(
      new EvalApiError("down", 503, []),
    );
    renderSet();
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Could not load eval set triage-golden-200: down",
    );
  });

  it("shows an empty set's hint", async () => {
    const set = EVAL_SET_DETAILS["refund-edge-cases-40"]!;
    vi.mocked(evalSetsApi.getEvalSet).mockResolvedValue({
      ...set,
      case_count: 0,
      cases: [],
    } as EvalSetResponse);
    renderSet("refund-edge-cases-40");
    expect(await screen.findByText("No cases yet")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Export JSONL" })).toBeDisabled();
  });
});
