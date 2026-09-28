import { afterEach, describe, expect, it, vi } from "vitest";
import * as gen from "./gen";
import {
  addCasesFromTraces,
  createEvalSet,
  deleteEvalSet,
  EvalApiError,
  getEvalSet,
  isForbidden,
  isNotFound,
  listEvalSets,
  uploadResults,
} from "./evalSets";

vi.mock("./gen", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./gen")>();
  return {
    ...actual,
    listEvalSets: vi.fn(),
    getEvalSet: vi.fn(),
    createEvalSet: vi.fn(),
    deleteEvalSet: vi.fn(),
    appendEvalCasesFromTraces: vi.fn(),
    uploadEvalResults: vi.fn(),
  };
});

afterEach(() => vi.clearAllMocks());

const ok = (data: unknown, status = 200) =>
  ({ data, response: new Response(null, { status }) }) as never;
const fail = (status: number, error: unknown) =>
  ({ error, response: new Response(null, { status }) }) as never;

describe("eval sets API", () => {
  it("unwraps a successful response", async () => {
    vi.mocked(gen.listEvalSets).mockResolvedValue(
      ok({ items: [], _links: { self: { href: "/api/v1/eval-sets" } } }),
    );
    expect((await listEvalSets()).items).toEqual([]);

    vi.mocked(gen.getEvalSet).mockResolvedValue(ok({ name: "s", cases: [] }));
    expect(await getEvalSet("s")).toMatchObject({ name: "s" });
    expect(gen.getEvalSet).toHaveBeenCalledWith({ path: { name: "s" } });

    vi.mocked(gen.createEvalSet).mockResolvedValue(ok({ name: "n" }, 201));
    await createEvalSet({ name: "n", agent: "a" });
    expect(gen.createEvalSet).toHaveBeenCalledWith({
      body: { name: "n", agent: "a" },
    });

    vi.mocked(gen.deleteEvalSet).mockResolvedValue(ok(undefined, 204));
    await deleteEvalSet("n");

    vi.mocked(gen.appendEvalCasesFromTraces).mockResolvedValue(
      ok({ matches: 3, already_present: 1, added: 2, added_ids: [] }),
    );
    const outcome = await addCasesFromTraces("n", {
      range: { from: "now-7d", to: "now" },
    });
    expect(outcome.added).toBe(2);
  });

  it("keeps the envelope's message, status and row details", async () => {
    vi.mocked(gen.uploadEvalResults).mockResolvedValue(
      fail(400, {
        status: "error",
        errorType: "bad_data",
        error: "2 invalid rows",
        details: [{ row: 3, column: "score", reason: "not a number" }],
      }),
    );
    const err = await uploadResults("{}", {
      agent: "a",
      version: "v1",
      set: "s",
      runId: "r",
      format: "csv",
    }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(EvalApiError);
    expect((err as EvalApiError).message).toBe("2 invalid rows");
    expect((err as EvalApiError).status).toBe(400);
    expect((err as EvalApiError).details).toEqual([
      { row: 3, column: "score", reason: "not a number" },
    ]);
    expect(gen.uploadEvalResults).toHaveBeenCalledWith(
      expect.objectContaining({
        body: "{}",
        query: {
          agent: "a",
          version: "v1",
          set: "s",
          run_id: "r",
          format: "csv",
        },
        headers: { "Content-Type": "text/csv" },
      }),
    );
  });

  it("classifies 404 and 403, and names the call without an envelope", async () => {
    vi.mocked(gen.getEvalSet).mockResolvedValue(fail(404, undefined));
    const missing = await getEvalSet("gone").catch((e: unknown) => e);
    expect(isNotFound(missing)).toBe(true);
    expect(isForbidden(missing)).toBe(false);
    expect((missing as Error).message).toBe("Loading gone failed (404)");

    vi.mocked(gen.listEvalSets).mockResolvedValue(
      fail(403, { error: "missing scope evals:read" }),
    );
    const denied = await listEvalSets().catch((e: unknown) => e);
    expect(isForbidden(denied)).toBe(true);
    expect((denied as EvalApiError).details).toEqual([]);
  });

  it("sends JSONL uploads as NDJSON", async () => {
    vi.mocked(gen.uploadEvalResults).mockResolvedValue(
      ok({ run_id: "r" }, 201),
    );
    await uploadResults("{}", {
      agent: "a",
      version: "v1",
      set: "s",
      runId: "r",
      format: "jsonl",
    });
    expect(gen.uploadEvalResults).toHaveBeenCalledWith(
      expect.objectContaining({
        headers: { "Content-Type": "application/x-ndjson" },
      }),
    );
  });
});
