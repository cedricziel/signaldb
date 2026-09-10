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

const ACME_DATASETS = [
  { id: "production", name: "production" },
  { id: "staging", name: "staging" },
];
const GLOBEX_DATASETS = [{ id: "prod", name: "prod" }];

beforeEach(() => {
  window.history.replaceState({}, "", `/oauth/consent?${QUERY}`);
  vi.mocked(consentApi.consentContext).mockResolvedValue({
    client_name: "Claude",
    tenants: [
      { id: "acme", role: "member", datasets: ACME_DATASETS },
      { id: "globex", role: "admin", datasets: GLOBEX_DATASETS },
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

    expect(await screen.findByRole("heading", { name: /Claude/ })).toBeInTheDocument();
    // scope=traces:read only → logs/metrics are not offered
    expect(screen.getByText("Read traces")).toBeInTheDocument();
    expect(screen.queryByText("Read logs")).not.toBeInTheDocument();
    // Exactly the tenants the context returned are selectable. Anchored so
    // this doesn't also match the "All datasets in acme" dataset-mode radio.
    expect(screen.getByRole("radio", { name: /^acme/ })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: /^globex/ })).toBeInTheDocument();
  });

  it("approves with the selected tenant", async () => {
    renderWithClient(<ConsentView />);
    await screen.findByRole("heading", { name: /Claude/ });

    await userEvent.click(screen.getByRole("radio", { name: /globex/ }));
    await userEvent.click(screen.getByRole("button", { name: "Authorize" }));

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
    await screen.findByRole("heading", { name: /Claude/ });

    await userEvent.click(screen.getByRole("button", { name: "Deny" }));

    await waitFor(() =>
      expect(consentApi.submitConsentDecision).toHaveBeenCalledWith(
        expect.objectContaining({ approved: false }),
      ),
    );
  });

  it("defaults to 'all datasets' and sends no dataset_ids", async () => {
    renderWithClient(<ConsentView />);
    await screen.findByRole("heading", { name: /Claude/ });

    expect(
      screen.getByRole("radio", { name: /All datasets/ }),
    ).toBeChecked();
    // The checklist only renders in the "only these" state.
    expect(
      screen.queryByRole("checkbox", { name: "production" }),
    ).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Authorize" }));

    await waitFor(() =>
      expect(consentApi.submitConsentDecision).toHaveBeenCalledWith(
        expect.not.objectContaining({ dataset_ids: expect.anything() }),
      ),
    );
  });

  it("restricts the grant to the checked datasets once 'only these datasets' is chosen", async () => {
    renderWithClient(<ConsentView />);
    await screen.findByRole("heading", { name: /Claude/ });

    await userEvent.click(
      screen.getByRole("radio", { name: /Only these datasets/ }),
    );
    // Nothing checked yet: submit is disabled rather than granting everything.
    expect(screen.getByRole("button", { name: "Authorize" })).toBeDisabled();

    await userEvent.click(screen.getByRole("checkbox", { name: "production" }));
    expect(screen.getByRole("button", { name: "Authorize" })).toBeEnabled();

    await userEvent.click(screen.getByRole("button", { name: "Authorize" }));

    await waitFor(() =>
      expect(consentApi.submitConsentDecision).toHaveBeenCalledWith(
        expect.objectContaining({
          tenant: "acme",
          dataset_ids: ["production"],
        }),
      ),
    );
  });

  it("resets the dataset choice to 'all datasets' when the selected tenant changes", async () => {
    renderWithClient(<ConsentView />);
    await screen.findByRole("heading", { name: /Claude/ });

    await userEvent.click(
      screen.getByRole("radio", { name: /Only these datasets/ }),
    );
    await userEvent.click(screen.getByRole("checkbox", { name: "production" }));

    await userEvent.click(screen.getByRole("radio", { name: /globex/ }));

    expect(
      screen.getByRole("radio", { name: /All datasets/ }),
    ).toBeChecked();
    expect(
      screen.queryByRole("checkbox", { name: "production" }),
    ).not.toBeInTheDocument();
    // The reset tenant's grant is immediately valid again (unrestricted).
    expect(screen.getByRole("button", { name: "Authorize" })).toBeEnabled();

    await userEvent.click(screen.getByRole("button", { name: "Authorize" }));
    await waitFor(() =>
      expect(consentApi.submitConsentDecision).toHaveBeenCalledWith(
        expect.not.objectContaining({ dataset_ids: expect.anything() }),
      ),
    );
  });

  it("shows the all-vs-restricted dataset choice even for a lone tenant", async () => {
    vi.mocked(consentApi.consentContext).mockResolvedValueOnce({
      client_name: "Claude",
      tenants: [{ id: "acme", role: "member", datasets: ACME_DATASETS }],
    });
    renderWithClient(<ConsentView />);
    await screen.findByRole("heading", { name: /Claude/ });

    expect(
      screen.getByRole("radio", { name: /All datasets/ }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("radio", { name: /Only these datasets/ }),
    ).toBeInTheDocument();
  });
});
