// Tenant OTTL processors API, layered over the generated OpenAPI SDK. Every
// call delegates to a generated `processors*` operation and unwraps the
// result envelope into data-or-`ApiError` (same contract as
// `features/schema/api.ts`).
import "../../api/client";

import {
  processorsCreate,
  processorsDelete,
  processorsGet,
  processorsList,
  processorsReplace,
  processorsTest,
  processorsValidate,
  type ProcessorResponse,
  type ProcessorSpec,
  type ProcessorWriteResponse,
  type StatementError,
  type TestRequest,
  type TestResponse,
} from "../../api/gen";
import { ApiError, retryAfterMsFrom } from "../../api/http";

export type {
  ProcessorResponse,
  ProcessorSpec,
  ProcessorWriteResponse,
  StatementError,
  TestRequest,
  TestResponse,
};

interface SdkResult<T> {
  data?: T;
  error?: unknown;
  response?: Response;
}

function unwrap<T>(result: SdkResult<T>): T {
  const { error, response } = result;
  if (error !== undefined || !response?.ok) {
    const status = response?.status ?? 0;
    const message =
      (error as { error?: string } | undefined)?.error ??
      `Processor request failed (${status})`;
    throw new ApiError(message, status, retryAfterMsFrom(response));
  }
  return result.data as T;
}

export const listProcessors = async (): Promise<ProcessorResponse[]> =>
  unwrap(await processorsList()).processors;

export const getProcessor = async (name: string): Promise<ProcessorResponse> =>
  unwrap(await processorsGet({ path: { name } }));

export const createProcessor = async (
  spec: ProcessorSpec,
): Promise<ProcessorWriteResponse> =>
  unwrap(await processorsCreate({ body: spec }));

export const replaceProcessor = async (
  name: string,
  spec: ProcessorSpec,
): Promise<ProcessorWriteResponse> =>
  unwrap(await processorsReplace({ path: { name }, body: spec }));

export const deleteProcessor = async (name: string): Promise<void> => {
  unwrap(await processorsDelete({ path: { name } }));
};

export const validateProcessor = async (
  signal: string,
  statements: string[],
): Promise<StatementError[]> =>
  (await unwrap(await processorsValidate({ body: { signal, statements } })))
    .errors ?? [];

export const testProcessor = async (
  request: TestRequest,
): Promise<TestResponse> => unwrap(await processorsTest({ body: request }));
