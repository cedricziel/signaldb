import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderWithClient, stubFetchRoutes } from "../test/render";
import { StacktraceLines } from "./StacktraceLines";

afterEach(() => {
  vi.unstubAllGlobals();
});

const STACKTRACE =
  "PaymentError: card declined\n    at src/handler.rs:42:9\n    at <anonymous>\n    at node_modules/framework/run.js:10:1";

function availabilityRoute(overrides: Record<string, unknown> = {}) {
  return {
    match: "/source-context",
    method: "GET",
    body: { configured: true, linked: true, ...overrides },
  };
}

describe("StacktraceLines", () => {
  it("classifies header, frame, and vendor lines", () => {
    const { container } = renderWithClient(
      <StacktraceLines text={STACKTRACE} />,
    );
    expect(
      container.querySelector(".span-event-trace-header"),
    ).toHaveTextContent("PaymentError: card declined");
    expect(
      container.querySelectorAll(".span-event-trace-frame"),
    ).toHaveLength(2);
    expect(
      container.querySelector(".span-event-trace-vendor"),
    ).toHaveTextContent("node_modules/framework/run.js:10:1");
  });

  it("renders the trace variant's wrapper by default", () => {
    const { container } = renderWithClient(
      <StacktraceLines text={STACKTRACE} />,
    );
    const wrapper = container.querySelector(".span-event-trace-lines");
    expect(wrapper?.tagName).toBe("DIV");
  });

  it("renders the error variant's wrapper", () => {
    const { container } = renderWithClient(
      <StacktraceLines text={STACKTRACE} variant="error" />,
    );
    const wrapper = container.querySelector(".errors-stacktrace");
    expect(wrapper?.tagName).toBe("PRE");
    expect(
      container.querySelector(".errors-stacktrace-header"),
    ).toBeInTheDocument();
  });

  it("uses a custom wrapper className when given", () => {
    const { container } = renderWithClient(
      <StacktraceLines text={STACKTRACE} className="custom-wrap" />,
    );
    expect(container.querySelector(".custom-wrap")).toBeInTheDocument();
    expect(
      container.querySelector(".span-event-trace-lines"),
    ).not.toBeInTheDocument();
  });

  it("shows a View source trigger only for a line with a recognizable location, once GitHub is linked", async () => {
    stubFetchRoutes([availabilityRoute()]);
    renderWithClient(
      <StacktraceLines text={STACKTRACE} tenant="acme" />,
    );
    const triggers = await screen.findAllByRole("button", {
      name: "View source",
    });
    // Two frame lines carry a recognizable location; the header and the
    // "at <anonymous>" frame don't.
    expect(triggers).toHaveLength(2);
    expect(triggers[0]).toHaveAttribute("title", "src/handler.rs:42");
  });

  it("shows no trigger when GitHub isn't linked for the tenant", async () => {
    stubFetchRoutes([
      availabilityRoute({ configured: true, linked: false }),
    ]);
    renderWithClient(<StacktraceLines text={STACKTRACE} tenant="acme" />);
    await screen.findByText("PaymentError: card declined");
    expect(
      screen.queryByRole("button", { name: "View source" }),
    ).not.toBeInTheDocument();
  });

  it("shows no trigger without a tenant, even with a recognizable location", () => {
    renderWithClient(<StacktraceLines text={STACKTRACE} />);
    expect(
      screen.queryByRole("button", { name: "View source" }),
    ).not.toBeInTheDocument();
  });

  it("passes repository/ref hints through to the source-context lookup", async () => {
    const fetchMock = stubFetchRoutes([
      availabilityRoute(),
      {
        match: "/source-context",
        method: "POST",
        body: { status: "unavailable", reason: "not_found" },
      },
    ]);
    renderWithClient(
      <StacktraceLines
        text={STACKTRACE}
        tenant="acme"
        hints={{ repository: "acme/api", ref: "main" }}
      />,
    );
    const [trigger] = await screen.findAllByRole("button", {
      name: "View source",
    });
    await userEvent.click(trigger!);
    await screen.findByText(
      "File not found in the linked repositories at that ref.",
    );
    const postCall = fetchMock.mock.calls.find(
      ([input]) => input instanceof Request && input.method === "POST",
    );
    const body = await (postCall![0] as Request).clone().json();
    expect(body).toMatchObject({ repository: "acme/api", ref: "main" });
  });
});
