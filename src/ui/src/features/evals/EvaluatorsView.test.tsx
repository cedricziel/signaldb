import { screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { EvaluatorsView } from "./EvaluatorsView";
import { F } from "../../api/evals";
import * as queryIrApi from "../../api/queryIr";
import { aggBy, mockIr, renderEvalView, rows, table } from "./testUtils";

vi.mock("../../api/queryIr", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/queryIr")>();
  return { ...actual, runIrQuery: vi.fn() };
});

afterEach(() => {
  vi.mocked(queryIrApi.runIrQuery).mockReset();
});

const renderView = () => renderEvalView(EvaluatorsView);

describe("EvaluatorsView", () => {
  it("shows output kind (score / label / score + label) and seen-in", async () => {
    mockIr((doc) => {
      const by = aggBy(doc);
      if (by.includes(F.evaluator)) {
        return table([
          // name, evaluator, n, first, last, offline, scored, labelled, span
          [
            "ToolTrajectory",
            "trajectory-match@2.1.0",
            200,
            1_000_000_000,
            2_000_000_000,
            200,
            200,
            200,
            "span-traj",
          ],
          [
            "Correctness",
            "correctness-judge@1.4",
            150,
            1_000_000_000,
            2_000_000_000,
            100,
            0,
            150,
            "span-corr",
          ],
          [
            "Toxicity",
            "toxicity@1.0",
            50,
            1_000_000_000,
            2_000_000_000,
            0,
            0,
            50,
            "span-tox",
          ],
        ]);
      }
      // fetchSpanOperations
      return rows([
        ["span-traj", "execute_tool"],
        ["span-corr", "chat"],
      ]);
    });
    renderView();

    const toolTrajectoryRow = await screen.findByRole("row", {
      name: /ToolTrajectory/,
    });
    expect(
      within(toolTrajectoryRow).getByText("score + label"),
    ).toBeInTheDocument();
    expect(
      within(toolTrajectoryRow).getByText("offline runs"),
    ).toBeInTheDocument();

    const correctnessRow = screen.getByRole("row", { name: /Correctness/ });
    expect(within(correctnessRow).getByText("label")).toBeInTheDocument();
    expect(
      within(correctnessRow).getByText("offline runs · production"),
    ).toBeInTheDocument();

    const toxicityRow = screen.getByRole("row", { name: /Toxicity/ });
    expect(within(toxicityRow).getByText("production")).toBeInTheDocument();
  });

  it("shows an empty state when nothing has been evaluated", async () => {
    mockIr(() => table([]));
    renderView();
    expect(
      await screen.findByText("No evaluator results in this range"),
    ).toBeInTheDocument();
  });
});
