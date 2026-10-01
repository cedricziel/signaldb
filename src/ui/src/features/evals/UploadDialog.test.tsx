import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as evalSetsApi from "../../api/evalSets";
import { EvalApiError, MAX_BODY_BYTES } from "../../api/evalSets";
import * as queryIrApi from "../../api/queryIr";
import type { ShellContext } from "../../lib/outletState";
import { cliSnippet, UploadDialog } from "./UploadDialog";
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
    uploadResults: vi.fn(),
  };
});

const onClose = vi.fn();

beforeEach(() => {
  mockScenarioIr();
  mockEvalSetsApi();
});

afterEach(() => {
  vi.mocked(queryIrApi.runIrQuery).mockReset();
  vi.clearAllMocks();
});

function renderDialog(initialSet = "triage-golden-200") {
  const View = ({ state }: ShellContext) => (
    <UploadDialog
      state={state}
      initialAgent="support-triage"
      initialSet={initialSet}
      onClose={onClose}
    />
  );
  renderEvalView(View, { tenant: "acme", dataset: "production" });
  return screen.getByRole("dialog", { name: "Upload eval results" });
}

function chooseFile(dialog: HTMLElement, text: string, name: string) {
  fireEvent.change(within(dialog).getByLabelText("Results file"), {
    target: { files: [new File([text], name)] },
  });
}

const TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
const JSONL = [
  `{"case_id":"case-117","name":"ToolTrajectory","score":0.33,"label":"fail","trace_id":"${TRACE}"}`,
  `{"case_id":"case-117","name":"Correctness","label":"fail","trace_id":"${TRACE}"}`,
  `{"case_id":"case-001","name":"Correctness","label":"pass","trace_id":"${TRACE}"}`,
  `{"case_id":"case-999","name":"Correctness","label":"pass"}`,
].join("\n");

describe("UploadDialog", () => {
  it("previews the file, matches case ids and prefills the version", async () => {
    const dialog = renderDialog();
    expect(
      within(dialog).getByRole("button", { name: "Upload results" }),
    ).toBeDisabled();
    chooseFile(dialog, JSONL, "results.jsonl");

    const stats = await within(dialog).findByLabelText("File contents");
    expect(stats).toHaveTextContent("Cases 3");
    expect(stats).toHaveTextContent("Evaluators found 2");
    expect(stats).toHaveTextContent("Linked to a span 3");
    expect(stats).toHaveTextContent("Run-level only 1");
    expect(within(dialog).getByText(/4 rows$/)).toBeInTheDocument();

    // error, explanation, span_id and evaluator aren't in this file.
    expect(within(dialog).getAllByText("not in file")).toHaveLength(4);

    expect(await within(dialog).findByText("2 of 3")).toBeInTheDocument();
    expect(
      within(dialog).getByText(/case IDs match triage-golden-200/),
    ).toBeInTheDocument();
    await waitFor(() =>
      expect(within(dialog).getByLabelText("Agent version")).toHaveValue(
        "v1.8.0",
      ),
    );
    expect(within(dialog).getByText(/service\.version/)).toBeInTheDocument();
    expect(within(dialog).getByRole("status")).toHaveTextContent(
      "1 rows have no trace_id",
    );
  });

  it("uploads and shows the run with its per-evaluator summary", async () => {
    vi.mocked(evalSetsApi.uploadResults).mockResolvedValue({
      run_id: "run-42",
      agent: "support-triage",
      version: "v1.9.0",
      set: "triage-golden-200",
      rows: 4,
      cases: 3,
      span_linked: 3,
      run_level: 1,
      evaluators: [
        {
          name: "Correctness",
          results: 3,
          errors: 0,
          mean: null,
          pass_rate: 0.67,
        },
        {
          name: "ToolTrajectory",
          results: 1,
          errors: 0,
          mean: 0.33,
          pass_rate: 0,
        },
      ],
      _links: {
        query: { href: "/api/v1/query" },
        runs: { href: "/evals/runs" },
      },
    });
    const dialog = renderDialog();
    chooseFile(dialog, JSONL, "results.jsonl");
    await within(dialog).findByLabelText("File contents");
    fireEvent.change(within(dialog).getByLabelText("Agent version"), {
      target: { value: "v1.9.0" },
    });
    fireEvent.click(
      within(dialog).getByRole("button", { name: "Upload 4 results" }),
    );
    expect(await within(dialog).findByText("run-42")).toBeInTheDocument();
    expect(evalSetsApi.uploadResults).toHaveBeenCalledWith(JSONL, {
      agent: "support-triage",
      version: "v1.9.0",
      set: "triage-golden-200",
      runId: expect.any(String),
      format: "jsonl",
    });
    const row = within(dialog).getByRole("row", { name: /ToolTrajectory/ });
    expect(row).toHaveTextContent("0.33");
    expect(row).toHaveTextContent("0%");
    expect(
      within(dialog).getByRole("link", {
        name: "Compare with the previous run ›",
      }),
    ).toHaveAttribute("href", expect.stringContaining("candidate=run-42"));
    fireEvent.click(
      within(dialog).getByRole("link", { name: "Open in Runs ›" }),
    );
    expect(onClose).toHaveBeenCalled();
  });

  it("lists the rows the server rejected", async () => {
    vi.mocked(evalSetsApi.uploadResults).mockRejectedValue(
      new EvalApiError("the results file has 2 invalid rows", 400, [
        { row: 3, column: "score", reason: "not a number" },
        { reason: "file is empty" },
      ]),
    );
    const dialog = renderDialog();
    chooseFile(
      dialog,
      "case_id,name,score\ncase-1,A,0.5\ncase-2,A,high\n",
      "results.csv",
    );
    await within(dialog).findByLabelText("File contents");
    // No trace ids, so nothing to read the version from.
    expect(within(dialog).getByLabelText("Agent version")).toHaveValue("");
    expect(
      within(dialog).getByRole("button", { name: "Upload 2 results" }),
    ).toBeDisabled();
    fireEvent.change(within(dialog).getByLabelText("Agent version"), {
      target: { value: "v2" },
    });
    fireEvent.click(
      within(dialog).getByRole("button", { name: "Upload 2 results" }),
    );
    const alert = await within(dialog).findByRole("alert");
    expect(alert).toHaveTextContent("the results file has 2 invalid rows");
    expect(
      within(alert).getByText("line 3 · score: not a number"),
    ).toBeInTheDocument();
    expect(within(alert).getByText("file is empty")).toBeInTheDocument();
    expect(evalSetsApi.uploadResults).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ format: "csv" }),
    );
  });

  it("flags files without verdict columns, unknown sets and oversized files", async () => {
    const dialog = renderDialog("");
    chooseFile(dialog, '{"case_id":"a","name":"A"}\nnope', "r.jsonl");
    expect(
      await within(dialog).findByText(/this file has none of those columns/),
    ).toBeInTheDocument();
    expect(
      within(dialog).getByText("line 2: not valid JSON"),
    ).toBeInTheDocument();
    fireEvent.change(within(dialog).getByLabelText("Eval set"), {
      target: { value: "ad-hoc-0928" },
    });
    expect(
      within(dialog).getByText(/isn't a stored eval set/),
    ).toBeInTheDocument();

    const big = new File(["x"], "big.jsonl");
    Object.defineProperty(big, "size", { value: MAX_BODY_BYTES + 1 });
    fireEvent.click(within(dialog).getByRole("button", { name: "Replace" }));
    fireEvent.change(within(dialog).getByLabelText("Results file"), {
      target: { files: [big] },
    });
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(
      "uploads are limited to 32 MiB",
    );
    expect(
      within(dialog).getByRole("button", { name: /^Upload/ }),
    ).toBeDisabled();
  });

  it("reads a dropped file", async () => {
    const dialog = renderDialog();
    const zone = within(dialog).getByRole("button", {
      name: /Drop a results file/,
    });
    fireEvent.dragOver(zone);
    expect(zone).toHaveAttribute("data-over", "true");
    fireEvent.dragLeave(zone);
    fireEvent.drop(zone, {
      dataTransfer: { files: [new File([JSONL], "dropped.jsonl")] },
    });
    expect(
      await within(dialog).findByText("dropped.jsonl"),
    ).toBeInTheDocument();
  });

  it("offers the CLI command and the OTLP form with Copy", async () => {
    const dialog = renderDialog();
    fireEvent.click(within(dialog).getByRole("tab", { name: "CLI / CI" }));
    expect(
      within(dialog).getByText(/signaldb-cli evals upload/),
    ).toHaveTextContent("--set triage-golden-200");
    expect(within(dialog).getByRole("button", { name: /Copy/ })).toBeTruthy();
    fireEvent.click(
      within(dialog).getByRole("tab", { name: "Send over OTLP" }),
    );
    expect(
      within(dialog).getByText(/event_name="gen_ai.evaluation.result"/),
    ).toBeInTheDocument();
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(onClose).toHaveBeenCalled();
  });

  it("fills the CLI snippet with defaults", () => {
    expect(cliSnippet("", "")).toContain("--agent support-triage");
    expect(cliSnippet("billing", "golden")).toContain("--set golden");
  });
});
