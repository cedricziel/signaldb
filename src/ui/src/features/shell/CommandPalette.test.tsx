import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import * as rumSessionsApi from "../../api/rumSessions";
import { DEFAULT_STATE } from "../../lib/urlState";
import { renderWithClient } from "../../test/render";
import { CommandPalette } from "./CommandPalette";

vi.mock("../../api/rumSessions", async (orig) => ({
  ...(await orig<typeof import("../../api/rumSessions")>()),
  fetchSessionLookup: vi.fn(),
}));

function renderPalette() {
  return renderWithClient(
    <MemoryRouter>
      <CommandPalette
        state={DEFAULT_STATE}
        canManage={false}
        isDemo={false}
        onClose={() => {}}
      />
    </MemoryRouter>,
  );
}

describe("CommandPalette's session id lookup", () => {
  it("doesn't offer a stale match while a newer keystroke's debounce hasn't settled", async () => {
    const user = userEvent.setup();
    vi.mocked(rumSessionsApi.fetchSessionLookup).mockResolvedValue(
      "storefront-web",
    );
    renderPalette();

    // 13 hex chars: long enough to be session-id-shaped, but not the
    // exact 16 or 32 that would instead match the higher-priority trace/
    // span id check.
    const input = screen.getByRole("searchbox", { name: "Search" });
    await user.type(input, "aaaaaaaaaaaaa");
    expect(
      await screen.findByText(/Open session aaaaaaaaaaaaa/),
    ).toBeInTheDocument();

    // Types one more character without letting the new debounce settle:
    // the stale item (still `data` for the old, already-resolved query)
    // must not linger as a pickable result.
    await user.type(input, "a");
    expect(
      screen.queryByText(/Open session aaaaaaaaaaaaa/),
    ).not.toBeInTheDocument();
    expect(screen.queryByText(/Open session/)).not.toBeInTheDocument();
  });
});
