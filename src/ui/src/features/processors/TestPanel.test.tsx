import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactElement } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { stubFetchRoutes } from "../../test/render";
import { TestPanel } from "./TestPanel";
import { SAMPLE_PAYLOADS } from "./samples";
import type { ProcessorSpec } from "./api";

const SPEC: ProcessorSpec = {
  name: "my-processor",
  description: null,
  signal: "traces",
  dataset: null,
  enabled: true,
  priority: 100,
  error_mode: "ignore",
  statements: ['set(attributes["a"], "1")'],
};

afterEach(() => {
  vi.unstubAllGlobals();
});

function renderWithRerenderableClient(ui: ReactElement) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const { rerender, ...rest } = render(
    <QueryClientProvider client={client}>{ui}</QueryClientProvider>,
  );
  return {
    ...rest,
    rerender: (next: ReactElement) =>
      rerender(<QueryClientProvider client={client}>{next}</QueryClientProvider>),
  };
}

describe("TestPanel", () => {
  it("resets the sample payload when the signal changes", () => {
    const { rerender } = renderWithRerenderableClient(
      <TestPanel signal="traces" dataset={null} spec={SPEC} />,
    );

    expect(
      (screen.getByLabelText("Sample OTLP/JSON payload") as HTMLTextAreaElement)
        .value,
    ).toBe(JSON.stringify(SAMPLE_PAYLOADS.traces, null, 2));

    rerender(<TestPanel signal="logs" dataset={null} spec={SPEC} />);

    expect(
      (screen.getByLabelText("Sample OTLP/JSON payload") as HTMLTextAreaElement)
        .value,
    ).toBe(JSON.stringify(SAMPLE_PAYLOADS.logs, null, 2));
  });

  it("clears a prior result when the signal changes", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/processors:test",
        body: { payload: { a: 1 }, statements: [{ processor: "p", index: 0, matched: 1, errors: 0 }] },
      },
    ]);
    const { rerender } = renderWithRerenderableClient(
      <TestPanel signal="traces" dataset={null} spec={SPEC} />,
    );

    screen.getByRole("button", { name: "Run test" }).click();
    expect(
      await screen.findByText(/statement 0: 1 match, 0 errors/),
    ).toBeInTheDocument();

    rerender(<TestPanel signal="logs" dataset={null} spec={SPEC} />);

    expect(
      screen.queryByText(/statement 0: 1 match, 0 errors/),
    ).not.toBeInTheDocument();
  });
});
