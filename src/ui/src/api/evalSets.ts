// Eval sets (`/api/v1/eval-sets…`) and results uploads
// (`POST /api/v1/evals/results`), layered over the generated OpenAPI SDK.
// Each call unwraps the SDK envelope into data or an `EvalApiError` that
// keeps the error envelope's `details` (the invalid rows of an upload).
import "./client";

import {
  appendEvalCasesFromTraces,
  createEvalSet as sdkCreateEvalSet,
  deleteEvalSet as sdkDeleteEvalSet,
  getEvalSet as sdkGetEvalSet,
  listEvalSets as sdkListEvalSets,
  uploadEvalResults,
  type ApiErrorDetail,
  type AppendCasesFromTracesOutcome,
  type AppendCasesFromTracesRequest,
  type EvalCase,
  type EvalCaseSourceCounts,
  type EvalResultsFormat,
  type EvalResultsUploadResponse,
  type EvalSetListResponse,
  type EvalSetResponse,
  type EvalSetSpec,
  type EvalSetSummaryResponse,
} from "./gen";
import { ApiError, unwrapSdkResult, type SdkResult } from "./http";

export type {
  ApiErrorDetail,
  AppendCasesFromTracesOutcome,
  AppendCasesFromTracesRequest,
  EvalCase,
  EvalCaseSourceCounts,
  EvalResultsFormat,
  EvalResultsUploadResponse,
  EvalSetListResponse,
  EvalSetResponse,
  EvalSetSpec,
  EvalSetSummaryResponse,
};

/** The server's body limit for eval-set writes and results uploads. */
export const MAX_BODY_BYTES = 32 * 1024 * 1024;

export class EvalApiError extends ApiError {
  readonly details: ApiErrorDetail[];

  constructor(
    message: string,
    status: number,
    details: ApiErrorDetail[],
    retryAfterMs?: number | null,
  ) {
    super(message, status, retryAfterMs);
    this.name = "EvalApiError";
    this.details = details;
  }
}

/** The router's error envelope, as far as these calls read it. */
interface ErrorEnvelope {
  error?: string;
  details?: ApiErrorDetail[] | null;
}

function unwrap<T>(result: SdkResult<T>, what: string): T {
  return unwrapSdkResult(
    result,
    (status) => `${what} failed (${status})`,
    (error) => (error as ErrorEnvelope | undefined)?.error,
    (message, status, error, retryAfterMs) =>
      new EvalApiError(
        message,
        status,
        (error as ErrorEnvelope | undefined)?.details ?? [],
        retryAfterMs,
      ),
  );
}

/** The sets in the dataset; `_links.create` is present only when the
 * caller may create one. */
export const listEvalSets = async (): Promise<EvalSetListResponse> =>
  unwrap(await sdkListEvalSets(), "Listing eval sets");

export const getEvalSet = async (name: string): Promise<EvalSetResponse> =>
  unwrap(await sdkGetEvalSet({ path: { name } }), `Loading ${name}`);

export const createEvalSet = async (
  spec: EvalSetSpec,
): Promise<EvalSetResponse> =>
  unwrap(await sdkCreateEvalSet({ body: spec }), `Creating ${spec.name}`);

export const deleteEvalSet = async (name: string): Promise<void> => {
  unwrap(await sdkDeleteEvalSet({ path: { name } }), `Deleting ${name}`);
};

export const addCasesFromTraces = async (
  name: string,
  request: AppendCasesFromTracesRequest,
): Promise<AppendCasesFromTracesOutcome> =>
  unwrap(
    await appendEvalCasesFromTraces({ path: { name }, body: request }),
    "Adding cases from traces",
  );

export interface UploadRun {
  agent: string;
  version: string;
  set: string;
  runId: string;
  format: EvalResultsFormat;
}

export const uploadResults = async (
  file: string,
  run: UploadRun,
): Promise<EvalResultsUploadResponse> =>
  unwrap(
    await uploadEvalResults({
      body: file,
      query: {
        agent: run.agent,
        version: run.version,
        set: run.set,
        run_id: run.runId,
        format: run.format,
      },
      headers: {
        "Content-Type":
          run.format === "csv" ? "text/csv" : "application/x-ndjson",
      },
    }),
    "Upload",
  );

/** 404 from the eval-set endpoints: the set isn't in this dataset. */
export function isNotFound(error: unknown): boolean {
  return error instanceof ApiError && error.status === 404;
}

/** 403: the credential may not read or change eval sets. */
export function isForbidden(error: unknown): boolean {
  return error instanceof ApiError && error.status === 403;
}
