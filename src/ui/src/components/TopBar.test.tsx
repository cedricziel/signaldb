import { screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE } from "../lib/urlState";
import { renderWithClient } from "../test/render";
import { TopBar } from "./TopBar";

function renderTopBar(isDemo: boolean) {
  return renderWithClient(
    <MemoryRouter>
      <TopBar
        state={DEFAULT_STATE}
        update={vi.fn()}
        who={undefined}
        canManage={false}
        isDemo={isDemo}
      />
    </MemoryRouter>,
  );
}

describe("TopBar demo banner", () => {
  it("shows the demo notice as its own strip above the header", () => {
    renderTopBar(true);
    const notice = screen.getByText(/demo · read-only/i);
    expect(
      within(screen.getByRole("banner")).queryByText(/demo · read-only/i),
    ).toBeNull();
    expect(
      notice.compareDocumentPosition(screen.getByRole("banner")) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });

  it("shows no demo notice for a regular account", () => {
    renderTopBar(false);
    expect(screen.queryByText(/demo · read-only/i)).toBeNull();
  });
});
