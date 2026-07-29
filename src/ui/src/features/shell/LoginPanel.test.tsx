import { useQuery } from "@tanstack/react-query";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/http";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { LoginGate, LoginPanel } from "./LoginPanel";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("LoginPanel", () => {
  it("POSTs the entered credentials and reports success", async () => {
    const fetchFn = stubFetchRoutes([{ match: "/ui/session", body: null }]);
    const onSuccess = vi.fn();
    renderWithClient(<LoginPanel tenant="acme" onSuccess={onSuccess} />);

    await userEvent.type(screen.getByLabelText("API key"), "sk-secret");
    await userEvent.type(screen.getByLabelText("Login dataset"), "prod");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    await waitFor(() => expect(onSuccess).toHaveBeenCalled());
    expect(onSuccess).toHaveBeenCalledWith({ tenant: "acme", dataset: "prod" });
    const call = fetchFn.mock.calls.find((c) =>
      String(c[0]).includes("/ui/session"),
    );
    const init = call?.[1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({
      api_key: "sk-secret",
      tenant: "acme",
      dataset: "prod",
    });
  });

  it("shows the server's error message on rejected credentials", async () => {
    stubFetchRoutes([
      {
        match: "/ui/session",
        body: { error: "Invalid or revoked API key" },
        status: 401,
      },
    ]);
    const onSuccess = vi.fn();
    renderWithClient(<LoginPanel tenant="acme" onSuccess={onSuccess} />);

    await userEvent.type(screen.getByLabelText("API key"), "bad-key");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Invalid or revoked API key",
    );
    expect(onSuccess).not.toHaveBeenCalled();
  });
});

describe("LoginGate", () => {
  function Probe({ queryFn }: { queryFn: () => Promise<string> }) {
    const query = useQuery({ queryKey: ["probe"], queryFn, retry: false });
    return <div data-testid="probe">{query.data ?? "pending"}</div>;
  }

  it("appears on a 401 query failure and retries after login", async () => {
    stubFetchRoutes([{ match: "/ui/session", body: null }]);
    // First query fails as unauthenticated; after login it succeeds.
    let calls = 0;
    const queryFn = vi.fn().mockImplementation(() => {
      calls += 1;
      return calls === 1
        ? Promise.reject(new ApiError("Loki API failed (401)", 401))
        : Promise.resolve("data");
    });

    renderWithClient(
      <>
        <Probe queryFn={queryFn} />
        <LoginGate tenant="acme" />
      </>,
    );

    // The 401 surfaces the login dialog.
    const dialog = await screen.findByRole("dialog", { name: "Sign in" });
    expect(dialog).toBeInTheDocument();

    await userEvent.type(screen.getByLabelText("API key"), "sk-secret");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    // Login hides the dialog and invalidation retries the query.
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Sign in" }),
      ).not.toBeInTheDocument(),
    );
    await waitFor(() =>
      expect(screen.getByTestId("probe")).toHaveTextContent("data"),
    );
    expect(queryFn).toHaveBeenCalledTimes(2);
  });

  it("stays hidden for non-auth query failures", async () => {
    const queryFn = vi
      .fn()
      .mockRejectedValue(new ApiError("Loki API failed (500)", 500));
    renderWithClient(
      <>
        <Probe queryFn={queryFn} />
        <LoginGate />
      </>,
    );
    await waitFor(() => expect(queryFn).toHaveBeenCalled());
    expect(
      screen.queryByRole("dialog", { name: "Sign in" }),
    ).not.toBeInTheDocument();
  });
});
