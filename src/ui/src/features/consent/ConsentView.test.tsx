import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as consentApi from "../../api/consent";
import { renderWithClient } from "../../test/render";
import { ConsentView } from "./ConsentView";

vi.mock("../../api/consent", () => ({
  consentContext: vi.fn(),
  submitConsentDecision: vi.fn(),
}));

const QUERY =
  "client_id=client-1&redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&code_challenge=chal&scope=traces%3Aread";

beforeEach(() => {
  window.history.replaceState({}, "", `/oauth/consent?${QUERY}`);
  vi.mocked(consentApi.consentContext).mockResolvedValue({
    client_name: "Claude",
    tenants: [
      { id: "acme", role: "member" },
      { id: "globex", role: "admin" },
    ],
  });
  vi.mocked(consentApi.submitConsentDecision).mockResolvedValue(
    "https://claude.ai/cb?code=abc",
  );
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("ConsentView", () => {
  it("shows the client, the requested read scope, and only the user's tenants", async () => {
    renderWithClient(<ConsentView />);

    expect(await screen.findByText("Claude")).toBeInTheDocument();
    // scope=traces:read only → logs/metrics are not offered
    expect(screen.getByText("Read traces")).toBeInTheDocument();
    expect(screen.queryByText("Read logs")).not.toBeInTheDocument();
    // Exactly the tenants the context returned are selectable
    expect(screen.getByRole("radio", { name: /acme/ })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: /globex/ })).toBeInTheDocument();
  });

  it("approves with the selected tenant", async () => {
    renderWithClient(<ConsentView />);
    await screen.findByText("Claude");

    await userEvent.click(screen.getByRole("radio", { name: /globex/ }));
    await userEvent.click(screen.getByRole("button", { name: "Approve" }));

    await waitFor(() =>
      expect(consentApi.submitConsentDecision).toHaveBeenCalled(),
    );
    expect(consentApi.submitConsentDecision).toHaveBeenCalledWith(
      expect.objectContaining({
        client_id: "client-1",
        tenant: "globex",
        approved: true,
      }),
    );
  });

  it("denies (approved=false) so the client learns of the refusal", async () => {
    renderWithClient(<ConsentView />);
    await screen.findByText("Claude");

    await userEvent.click(screen.getByRole("button", { name: "Deny" }));

    await waitFor(() =>
      expect(consentApi.submitConsentDecision).toHaveBeenCalledWith(
        expect.objectContaining({ approved: false }),
      ),
    );
  });
});
