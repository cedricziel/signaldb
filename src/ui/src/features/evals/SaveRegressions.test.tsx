// Compare → "Save N regressed cases as eval set", over the shared scenario:
// triage-golden-200 replayed on v1.7.3 and v1.8.0.
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as evalSetsApi from "../../api/evalSets";
import { EvalApiError, type EvalSetResponse } from "../../api/evalSets";
import { F } from "../../api/evals";
import * as queryIrApi from "../../api/queryIr";
import { CompareView } from "./CompareView";
import {
  EVAL_SET_DETAILS,
  EVAL_SET_LIST,
  evalsIrResponse,
} from "./evalFixtures";
import { FROM_COMPARE_TAG } from "./evalSetModel";
import {
  aggBy,
  mockEvalSetsApi,
  mockIr,
  mockScenarioIr,
  renderEvalView,
  table,
} from "./testUtils";

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

const renderCompare = () =>
  renderEvalView(CompareView, { tenant: "acme", dataset: "production" });

/** The dialog once the source set's cases have loaded. */
async function loadedDialog() {
  await screen.findByLabelText("Name");
  return screen.getByRole("dialog", {
    name: "Save regressed cases as eval set",
  });
}

const saveButton = () =>
  screen.findByRole("button", { name: /regressed cases as eval set$/ });

describe("Save regressed cases as eval set", () => {
  it("creates a set from the regressed cases' originals", async () => {
    vi.mocked(evalSetsApi.createEvalSet).mockResolvedValue(
      {} as EvalSetResponse,
    );
    renderCompare();
    const button = await saveButton();
    expect(button).toHaveTextContent("Save 1 regressed cases as eval set");
    await waitFor(() => expect(button).toBeEnabled());
    fireEvent.click(button);

    const dialog = await loadedDialog();
    expect(within(dialog).getByLabelText("Name")).toHaveValue(
      "regressions-0927",
    );
    expect(within(dialog).getByLabelText("Agent")).toHaveValue(
      "support-triage",
    );
    fireEvent.click(
      within(dialog).getByRole("button", { name: "Create with 1 cases" }),
    );
    await waitFor(() =>
      expect(screen.getByTestId("location").textContent).toBe(
        "/evals/sets/regressions-0927?tenant=acme&dataset=production",
      ),
    );
    const original = EVAL_SET_DETAILS["triage-golden-200"]!.cases.find(
      (c) => c.id === "case-117",
    )!;
    expect(evalSetsApi.createEvalSet).toHaveBeenCalledWith({
      name: "regressions-0927",
      agent: "support-triage",
      description: "Cases that regressed in v1.8.0 against v1.7.3",
      cases: [
        {
          id: "case-117",
          input: original.input,
          expected_tools: original.expected_tools,
          reference: original.reference,
          tags: [FROM_COMPARE_TAG],
          // The scenario's candidate trace ids aren't W3C ids, so the
          // original source stays.
          source: original.source,
        },
      ],
    });
  });

  it("shows why the set can't be created", async () => {
    vi.mocked(evalSetsApi.createEvalSet).mockRejectedValue(
      new EvalApiError("eval set regressions-0927 already exists", 409, []),
    );
    renderCompare();
    const button = await saveButton();
    await waitFor(() => expect(button).toBeEnabled());
    fireEvent.click(button);
    const dialog = await loadedDialog();
    fireEvent.change(within(dialog).getByLabelText("Name"), {
      target: { value: "Bad" },
    });
    expect(
      within(dialog).getByRole("button", { name: "Create with 1 cases" }),
    ).toBeDisabled();
    fireEvent.change(within(dialog).getByLabelText("Name"), {
      target: { value: "regressions-0927" },
    });
    fireEvent.click(
      within(dialog).getByRole("button", { name: "Create with 1 cases" }),
    );
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(
      "already exists",
    );
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("is disabled, with the reason, when the set isn't stored", async () => {
    vi.mocked(evalSetsApi.listEvalSets).mockResolvedValue({
      ...EVAL_SET_LIST,
      items: EVAL_SET_LIST.items.filter((s) => s.name !== "triage-golden-200"),
    } as Awaited<ReturnType<typeof evalSetsApi.listEvalSets>>);
    renderCompare();
    const button = await saveButton();
    await waitFor(() =>
      expect(button).toHaveAttribute(
        "title",
        "triage-golden-200 isn't a stored eval set, so the cases' inputs aren't known",
      ),
    );
    expect(button).toBeDisabled();
    expect(
      screen.getByText(/isn't a stored eval set, so the cases' inputs/, {
        selector: ".evals-note",
      }),
    ).toBeInTheDocument();
    expect(evalSetsApi.getEvalSet).not.toHaveBeenCalled();
  });

  it("loads the set's cases only when the dialog opens", async () => {
    vi.mocked(evalSetsApi.getEvalSet).mockRejectedValue(
      new EvalApiError("database unavailable", 500, []),
    );
    renderCompare();
    const button = await saveButton();
    await waitFor(() => expect(button).toBeEnabled());
    expect(evalSetsApi.getEvalSet).not.toHaveBeenCalled();
    fireEvent.click(button);
    const dialog = screen.getByRole("dialog", {
      name: "Save regressed cases as eval set",
    });
    expect(within(dialog).getByText(/Loading/)).toBeInTheDocument();
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(
      "Could not load triage-golden-200: database unavailable",
    );
    expect(evalSetsApi.getEvalSet).toHaveBeenCalledWith("triage-golden-200");
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("is disabled when the runs name no set", async () => {
    mockIr((doc) => {
      const res = evalsIrResponse(doc) as { rows?: unknown[][] };
      if (!aggBy(doc).includes(F.set)) return res;
      // Blank out the set column (index 1) of every run row.
      return table((res.rows ?? []).map((r) => [r[0], null, ...r.slice(2)]));
    });
    renderCompare();
    const button = await saveButton();
    await waitFor(() =>
      expect(button).toHaveAttribute(
        "title",
        "These runs name no eval set, so the cases' inputs aren't known",
      ),
    );
    expect(button).toBeDisabled();
  });
});
