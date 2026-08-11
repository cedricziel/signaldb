import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { MemberTable } from "./MemberTable";
import type { TraceGroupMember } from "../../api/traceGroupMembers";

function member(
  traceId: string,
  spanId: string,
  spanName: string,
  serviceName: string,
  startNs: string,
  durationMs: number,
): TraceGroupMember {
  return {
    traceId,
    spanId,
    parentSpanId: null,
    spanName,
    serviceName,
    startNs,
    durationNanos: String(durationMs * 1e6),
    statusCode: "Ok",
  };
}

describe("MemberTable", () => {
  it("shows a busy skeleton while pending", () => {
    render(
      <MemberTable
        members={undefined}
        isError={false}
        identityLabel="Span"
        emptyMessage="No spans."
        onOpenTrace={vi.fn()}
      />,
    );
    expect(screen.getByRole("table")).toHaveAttribute("aria-busy", "true");
  });

  it("shows the error message instead of a table", () => {
    render(
      <MemberTable
        members={undefined}
        isError
        errorMessage="Search failed: boom"
        identityLabel="Span"
        emptyMessage="No spans."
        onOpenTrace={vi.fn()}
      />,
    );
    expect(screen.getByRole("alert")).toHaveTextContent("Search failed: boom");
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("shows the empty message when the query resolved with zero rows", () => {
    render(
      <MemberTable
        members={[]}
        isError={false}
        identityLabel="Span"
        emptyMessage="No spans for this window."
        onOpenTrace={vi.fn()}
      />,
    );
    expect(screen.getByText("No spans for this window.")).toBeInTheDocument();
  });

  it("renders rows and the identity column's header label", () => {
    render(
      <MemberTable
        members={[member("t1", "s1", "GET /pay", "gateway", "1000", 12)]}
        isError={false}
        identityLabel="Span"
        emptyMessage="No spans."
        onOpenTrace={vi.fn()}
      />,
    );
    expect(screen.getByText("Span")).toBeInTheDocument();
    expect(screen.getByText("GET /pay")).toBeInTheDocument();
    expect(screen.getByText("gateway")).toBeInTheDocument();
  });

  it("renders the footnote only when supplied", () => {
    const { rerender } = render(
      <MemberTable
        members={[member("t1", "s1", "GET /pay", "gateway", "1000", 12)]}
        isError={false}
        identityLabel="Span"
        emptyMessage="No spans."
        onOpenTrace={vi.fn()}
      />,
    );
    expect(screen.queryByText(/Showing up to/)).not.toBeInTheDocument();

    rerender(
      <MemberTable
        members={[member("t1", "s1", "GET /pay", "gateway", "1000", 12)]}
        isError={false}
        identityLabel="Span"
        emptyMessage="No spans."
        footnote="Showing up to 500 spans, newest first."
        onOpenTrace={vi.fn()}
      />,
    );
    expect(screen.getByText(/Showing up to 500 spans/)).toBeInTheDocument();
  });

  it("opens the row's trace on click", () => {
    const onOpenTrace = vi.fn();
    render(
      <MemberTable
        members={[member("t1", "s1", "GET /pay", "gateway", "1000", 12)]}
        isError={false}
        identityLabel="Span"
        emptyMessage="No spans."
        onOpenTrace={onOpenTrace}
      />,
    );
    fireEvent.click(screen.getByText("GET /pay"));
    expect(onOpenTrace).toHaveBeenCalledWith("t1");
  });
});
