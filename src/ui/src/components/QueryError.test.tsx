import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { QueryError } from "./QueryError";

describe("QueryError", () => {
  it("names what failed to load and the error's message as an alert", () => {
    render(<QueryError what="logs" error={new Error("boom")} />);
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("Could not load logs: boom");
    expect(alert).toHaveClass("query-error");
  });

  it("stringifies a non-Error value rather than throwing", () => {
    render(<QueryError what="traces" error="plain string" />);
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Could not load traces: plain string",
    );
  });
});
