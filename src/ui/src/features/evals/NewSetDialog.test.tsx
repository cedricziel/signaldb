import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as evalSetsApi from "../../api/evalSets";
import { EvalApiError, type EvalSetResponse } from "../../api/evalSets";
import * as queryIrApi from "../../api/queryIr";
import type { ShellContext } from "../../lib/outletState";
import { NewSetDialog } from "./NewSetDialog";
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

const onClose = vi.fn();

beforeEach(() => {
  mockScenarioIr();
  mockEvalSetsApi();
  vi.mocked(evalSetsApi.createEvalSet).mockResolvedValue({} as EvalSetResponse);
});

afterEach(() => {
  vi.mocked(queryIrApi.runIrQuery).mockReset();
  vi.clearAllMocks();
});

function renderDialog() {
  const View = ({ state }: ShellContext) => (
    <NewSetDialog state={state} onClose={onClose} />
  );
  renderEvalView(View, { tenant: "acme", dataset: "production" });
  return screen.getByRole("dialog", { name: "New eval set" });
}

const location = () => screen.getByTestId("location").textContent;

function fill(dialog: HTMLElement, name: string, agent: string) {
  fireEvent.change(within(dialog).getByLabelText("Name"), {
    target: { value: name },
  });
  fireEvent.change(within(dialog).getByLabelText("Agent"), {
    target: { value: agent },
  });
}

function chooseFile(dialog: HTMLElement, text: string, name = "cases.jsonl") {
  fireEvent.change(within(dialog).getByLabelText("Cases file"), {
    target: { files: [new File([text], name)] },
  });
}

const JSONL = [
  '{"id":"edge-01","input":"Refund order #71002","expected_tools":["lookup_order","check_policy"],"reference":"Decline"}',
  '{"id":"edge-02","input":"Refund half of order #71118","expected_tools":["lookup_order"],"reference":"Partial refund"}',
  '{"id":"edge-03","input":"You refunded me twice"}',
].join("\n");

describe("NewSetDialog", () => {
  it("previews a JSONL file and creates the set with its cases", async () => {
    const dialog = renderDialog();
    fill(dialog, "refund-edge-cases", "support-triage");
    chooseFile(dialog, JSONL);
    expect(await within(dialog).findByText("cases.jsonl")).toBeInTheDocument();
    expect(within(dialog).getByText("3 rows")).toBeInTheDocument();
    expect(
      within(dialog).getByText("lookup_order → check_policy"),
    ).toBeInTheDocument();
    expect(within(dialog).getByText("missing")).toBeInTheDocument();
    expect(
      within(dialog).getByText("1 have no reference answer"),
    ).toBeInTheDocument();

    fireEvent.click(
      within(dialog).getByRole("button", { name: "Create with 3 cases" }),
    );
    await waitFor(() =>
      expect(location()).toBe(
        "/evals/sets/refund-edge-cases?tenant=acme&dataset=production",
      ),
    );
    expect(evalSetsApi.createEvalSet).toHaveBeenCalledWith({
      name: "refund-edge-cases",
      agent: "support-triage",
      cases: [
        expect.objectContaining({ id: "edge-01", source: { kind: "upload" } }),
        expect.objectContaining({ id: "edge-02" }),
        expect.objectContaining({ id: "edge-03" }),
      ],
    });
    expect(onClose).toHaveBeenCalled();
  });

  it("lists unreadable lines and won't create from a broken file", async () => {
    const dialog = renderDialog();
    fill(dialog, "broken", "support-triage");
    chooseFile(dialog, `${JSONL}\n{oops`);
    expect(
      await within(dialog).findByText("line 4: not valid JSON"),
    ).toBeInTheDocument();
    expect(
      within(dialog).getByRole("button", { name: "Create with 3 cases" }),
    ).toBeDisabled();
  });

  it("validates the name", async () => {
    const dialog = renderDialog();
    fill(dialog, "Bad Name", "support-triage");
    expect(within(dialog).getByLabelText("Name")).toHaveAttribute(
      "aria-invalid",
      "true",
    );
    expect(within(dialog).getByText(/Lowercase letters/)).toBeInTheDocument();
  });

  it("creates an empty set", async () => {
    const dialog = renderDialog();
    fill(dialog, "later", "support-triage");
    fireEvent.click(within(dialog).getByRole("radio", { name: /Empty/ }));
    expect(
      within(dialog).getByText(/The set starts with no cases/),
    ).toBeInTheDocument();
    fireEvent.click(
      within(dialog).getByRole("button", { name: "Create empty set" }),
    );
    await waitFor(() =>
      expect(evalSetsApi.createEvalSet).toHaveBeenCalledWith({
        name: "later",
        agent: "support-triage",
        cases: [],
      }),
    );
  });

  it("creates the set, then appends cases from traces", async () => {
    vi.mocked(evalSetsApi.addCasesFromTraces).mockResolvedValue({
      matches: 812,
      already_present: 0,
      added: 40,
      added_ids: [],
    });
    const dialog = renderDialog();
    fill(dialog, "from-prod", "support-triage");
    fireEvent.click(within(dialog).getByRole("radio", { name: /Real traces/ }));
    fireEvent.change(within(dialog).getByLabelText("Sample"), {
      target: { value: "40" },
    });
    fireEvent.click(
      within(dialog).getByRole("checkbox", {
        name: "Use the tools actually called as the expected trajectory",
      }),
    );
    fireEvent.click(
      within(dialog).getByRole("button", {
        name: "Create with up to 40 cases",
      }),
    );
    await waitFor(() =>
      expect(location()).toBe(
        "/evals/sets/from-prod?tenant=acme&dataset=production",
      ),
    );
    expect(evalSetsApi.createEvalSet).toHaveBeenCalledWith({
      name: "from-prod",
      agent: "support-triage",
      cases: [],
    });
    expect(evalSetsApi.addCasesFromTraces).toHaveBeenCalledWith("from-prod", {
      range: { from: "now-7d", to: "now" },
      agent: "support-triage",
      sample: 40,
      expected_tools: false,
      reference_from_answer: false,
    });
  });

  it("says the set exists when appending from traces fails", async () => {
    vi.mocked(evalSetsApi.addCasesFromTraces).mockRejectedValue(
      new EvalApiError("missing scope traces:read", 403, []),
    );
    const dialog = renderDialog();
    fill(dialog, "from-prod", "support-triage");
    fireEvent.click(within(dialog).getByRole("radio", { name: /Real traces/ }));
    fireEvent.click(
      within(dialog).getByRole("button", {
        name: "Create with up to 50 cases",
      }),
    );
    const alert = await within(dialog).findByRole("alert");
    expect(alert).toHaveTextContent(
      "Created from-prod, but adding cases from traces failed: missing scope traces:read",
    );
    fireEvent.click(
      within(alert).getByRole("link", { name: "Open the set ›" }),
    );
    expect(location()).toBe(
      "/evals/sets/from-prod?tenant=acme&dataset=production",
    );
  });

  it("shows why the server refused the set", async () => {
    vi.mocked(evalSetsApi.createEvalSet).mockRejectedValue(
      new EvalApiError("eval set taken already exists", 409, []),
    );
    const dialog = renderDialog();
    fill(dialog, "taken", "support-triage");
    fireEvent.click(within(dialog).getByRole("radio", { name: /Empty/ }));
    fireEvent.click(
      within(dialog).getByRole("button", { name: "Create empty set" }),
    );
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(
      "eval set taken already exists",
    );
    expect(location()).toBe("/");
  });
});
