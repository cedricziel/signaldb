import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderWithClient, stubFetchRoutes } from "../test/render";
import { SourceSnippet } from "./SourceSnippet";

afterEach(() => {
  vi.unstubAllGlobals();
});

function availabilityRoute(overrides: Record<string, unknown> = {}) {
  return {
    match: "/source-context",
    method: "GET",
    body: { configured: true, linked: true, ...overrides },
  };
}

function availableRoute(overrides: Record<string, unknown> = {}) {
  return {
    match: "/source-context",
    method: "POST",
    body: {
      status: "available",
      snippet: {
        repository: "acme/api",
        ref: "main",
        path: "src/handler.rs",
        line: 42,
        start_line: 41,
        lines: ["fn handler() {", "    do_thing();", "}"],
        html_url:
          "https://github.com/acme/api/blob/main/src/handler.rs#L42",
        sha: "deadbeef",
        ...overrides,
      },
    },
  };
}

describe("SourceSnippet", () => {
  it("renders nothing while the tenant's GitHub-linked state is unknown or disabled", () => {
    stubFetchRoutes([availabilityRoute({ configured: false, linked: false })]);
    renderWithClient(
      <SourceSnippet tenant="acme" path="src/handler.rs" line={42} />,
    );
    expect(
      screen.queryByRole("button", { name: "View source" }),
    ).not.toBeInTheDocument();
  });

  it("renders nothing when configured but not yet linked", async () => {
    stubFetchRoutes([availabilityRoute({ configured: true, linked: false })]);
    renderWithClient(
      <SourceSnippet tenant="acme" path="src/handler.rs" line={42} />,
    );
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "View source" }),
      ).not.toBeInTheDocument(),
    );
  });

  it("fetches nothing until clicked", async () => {
    const fetchMock = stubFetchRoutes([availabilityRoute(), availableRoute()]);
    renderWithClient(
      <SourceSnippet tenant="acme" path="src/handler.rs" line={42} />,
    );
    const trigger = await screen.findByRole("button", { name: "View source" });
    expect(trigger).toHaveAttribute("title", "src/handler.rs:42");
    expect(
      fetchMock.mock.calls.some(([input]) =>
        (input instanceof Request ? input.method : "GET") === "POST",
      ),
    ).toBe(false);
  });

  it("shows numbered lines with the target marked and a GitHub link on success", async () => {
    stubFetchRoutes([availabilityRoute(), availableRoute()]);
    renderWithClient(
      <SourceSnippet
        tenant="acme"
        repository="acme/api"
        gitRef="main"
        path="src/handler.rs"
        line={42}
      />,
    );
    await userEvent.click(
      await screen.findByRole("button", { name: "View source" }),
    );

    const link = await screen.findByRole("link", {
      name: "acme/api · src/handler.rs",
    });
    expect(link).toHaveAttribute(
      "href",
      "https://github.com/acme/api/blob/main/src/handler.rs#L42",
    );
    expect(link).toHaveAttribute("target", "_blank");
    expect(link).toHaveAttribute("rel", "noopener noreferrer");

    expect(screen.getByText("@main")).toBeInTheDocument();

    const target = screen.getByText("do_thing();").closest("div")!;
    expect(target).toHaveAttribute("aria-current", "true");
    expect(within(target).getByText("42")).toBeInTheDocument();
    expect(screen.getByText("fn handler() {")).toBeInTheDocument();
    expect(screen.getByText("}")).toBeInTheDocument();
  });

  it("shows the unpinned badge when ref is null", async () => {
    stubFetchRoutes([availabilityRoute(), availableRoute({ ref: null })]);
    renderWithClient(
      <SourceSnippet tenant="acme" path="src/handler.rs" line={42} />,
    );
    await userEvent.click(
      await screen.findByRole("button", { name: "View source" }),
    );
    expect(
      await screen.findByText("default branch (unpinned)"),
    ).toBeInTheDocument();
  });

  it("shortens a full commit SHA ref to its short form", async () => {
    stubFetchRoutes([
      availabilityRoute(),
      availableRoute({ ref: "0123456789abcdef0123456789abcdef01234567" }),
    ]);
    renderWithClient(
      <SourceSnippet tenant="acme" path="src/handler.rs" line={42} />,
    );
    await userEvent.click(
      await screen.findByRole("button", { name: "View source" }),
    );
    expect(await screen.findByText("@0123456")).toBeInTheDocument();
  });

  it.each([
    ["not_configured", "GitHub is not configured on this server."],
    ["no_installation", "No linked GitHub repository covers this file."],
    [
      "not_found",
      "File not found in the linked repositories at that ref.",
    ],
    [
      "not_a_file",
      "This path can't be shown (not a text file, or too large).",
    ],
    [
      "line_out_of_range",
      "The file is shorter than that line at that ref.",
    ],
    ["github_error", "Source is temporarily unavailable."],
  ])("renders a sentence for reason %s", async (reason, sentence) => {
    stubFetchRoutes([
      availabilityRoute(),
      {
        match: "/source-context",
        method: "POST",
        body: { status: "unavailable", reason },
      },
    ]);
    renderWithClient(
      <SourceSnippet tenant="acme" path="src/handler.rs" line={42} />,
    );
    await userEvent.click(
      await screen.findByRole("button", { name: "View source" }),
    );
    expect(await screen.findByText(sentence)).toBeInTheDocument();
  });

  it("renders the caught error message on a thrown request failure", async () => {
    stubFetchRoutes([
      availabilityRoute(),
      {
        match: "/source-context",
        method: "POST",
        status: 403,
        body: { error: "forbidden" },
      },
    ]);
    renderWithClient(
      <SourceSnippet tenant="acme" path="src/handler.rs" line={42} />,
    );
    await userEvent.click(
      await screen.findByRole("button", { name: "View source" }),
    );
    expect(await screen.findByText("forbidden")).toBeInTheDocument();
  });

  it("collapses again on a second click", async () => {
    stubFetchRoutes([availabilityRoute(), availableRoute()]);
    renderWithClient(
      <SourceSnippet tenant="acme" path="src/handler.rs" line={42} />,
    );
    const trigger = await screen.findByRole("button", { name: "View source" });
    await userEvent.click(trigger);
    await waitFor(() =>
      expect(trigger).toHaveAttribute("aria-expanded", "true"),
    );
    await screen.findByText("fn handler() {");

    await userEvent.click(trigger);
    expect(trigger).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByText("fn handler() {")).not.toBeInTheDocument();
  });
});
