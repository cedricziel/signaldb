import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { LineDiffView } from "./LineDiffView";
import type { DiffLine } from "../features/processors/lineDiff";

describe("LineDiffView", () => {
  it("prefixes removed and added lines and leaves unchanged lines alone", () => {
    const diff: DiffLine[] = [
      { kind: "same", text: '{"a": 1}' },
      { kind: "removed", text: '"b": 2' },
      { kind: "added", text: '"b": 3' },
    ];
    render(<LineDiffView diff={diff} />);
    const lines = screen.getByLabelText("diff").textContent;
    expect(lines).toContain('  {"a": 1}');
    expect(lines).toContain('- "b": 2');
    expect(lines).toContain('+ "b": 3');
  });
});
