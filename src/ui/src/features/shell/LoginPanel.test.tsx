import { useQuery } from "@tanstack/react-query";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, setTenantContext, tenantHeaders } from "../../api/http";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { LoginGate, LoginPanel } from "./LoginPanel";

afterEach(() => {
  vi.unstubAllGlobals();
});

const acmeMembership = {
  tenant_id: "acme",
  name: "Acme Inc",
  role: "admin" as const,
};
const globexMembership = {
  tenant_id: "globex",
  name: "Globex Corp",
  role: "member" as const,
};

describe("LoginPanel", () => {
  it("POSTs email and password only and reports the resolved tenant", async () => {
    const fetchFn = stubFetchRoutes([
      {
        match: "/ui/session",
        body: {
          tenant: "acme",
          dataset: "prod",
          memberships: [acmeMembership],
        },
      },
    ]);
    const onSuccess = vi.fn();
    renderWithClient(<LoginPanel onSuccess={onSuccess} />);

    expect(screen.queryByLabelText("Login tenant")).not.toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("Email"), "alice@example.com");
    await userEvent.type(screen.getByLabelText("Password"), "secret");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    await waitFor(() => expect(onSuccess).toHaveBeenCalled());
    expect(onSuccess).toHaveBeenCalledWith({ tenant: "acme", dataset: "prod" });
    const call = fetchFn.mock.calls.find((c) =>
      String(c[0]).includes("/ui/session"),
    );
    const init = call?.[1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({
      email: "alice@example.com",
      password: "secret",
    });
  });

  it("offers a tenant selector when the account spans multiple tenants", async () => {
    const fetchFn = stubFetchRoutes([
      {
        match: "/ui/session",
        body: {
          tenant: null,
          dataset: null,
          memberships: [acmeMembership, globexMembership],
        },
      },
      {
        match: "/api/v1/whoami",
        body: {
          memberships: [acmeMembership],
          tenant: { id: "globex", slug: "globex", name: "Globex Corp" },
          datasets: [{ id: "main", slug: "main", is_default: true }],
          default_dataset: "main",
        },
      },
    ]);
    const onSuccess = vi.fn();
    renderWithClient(<LoginPanel onSuccess={onSuccess} />);

    await userEvent.type(screen.getByLabelText("Email"), "alice@example.com");
    await userEvent.type(screen.getByLabelText("Password"), "secret");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    // The selector lists each membership as a button; no free-text entry.
    const dialog = await screen.findByRole("dialog", { name: "Choose tenant" });
    expect(dialog).toBeInTheDocument();
    expect(screen.queryByLabelText("Login tenant")).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /Acme Inc/ }),
    ).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /Globex Corp/ }));

    await waitFor(() => expect(onSuccess).toHaveBeenCalled());
    expect(onSuccess).toHaveBeenCalledWith({
      tenant: "globex",
      dataset: "main",
    });
    // The default dataset came from whoami scoped to the picked tenant.
    const whoamiCall = fetchFn.mock.calls.find((c) =>
      String(c[0]).includes("/api/v1/whoami"),
    );
    const init = whoamiCall?.[1] as RequestInit;
    expect((init.headers as Record<string, string>)["X-Tenant-ID"]).toBe(
      "globex",
    );
  });

  it("shows the server's error message on rejected credentials", async () => {
    stubFetchRoutes([
      {
        match: "/ui/session",
        body: { error: "Invalid email or password" },
        status: 401,
      },
    ]);
    const onSuccess = vi.fn();
    renderWithClient(<LoginPanel onSuccess={onSuccess} />);

    await userEvent.type(screen.getByLabelText("Email"), "alice@example.com");
    await userEvent.type(screen.getByLabelText("Password"), "bad-password");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Invalid email or password",
    );
    expect(onSuccess).not.toHaveBeenCalled();
  });
});

describe("LoginGate", () => {
  function Probe({
    id,
    queryFn,
  }: {
    id: string;
    queryFn: () => Promise<string>;
  }) {
    const query = useQuery({ queryKey: [id], queryFn, retry: false });
    return <div data-testid={id}>{query.data ?? "pending"}</div>;
  }

  it("appears on a 401 query failure and retries after login", async () => {
    stubFetchRoutes([
      {
        match: "/ui/session",
        body: {
          tenant: "acme",
          dataset: "production",
          memberships: [acmeMembership],
        },
      },
    ]);
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
        <Probe id="probe" queryFn={queryFn} />
        <LoginGate />
      </>,
    );

    // The 401 surfaces the login dialog.
    const dialog = await screen.findByRole("dialog", { name: "Sign in" });
    expect(dialog).toBeInTheDocument();

    await userEvent.type(screen.getByLabelText("Email"), "alice@example.com");
    await userEvent.type(screen.getByLabelText("Password"), "secret");
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

  it("does not reappear when a pre-login query settles 401 late", async () => {
    stubFetchRoutes([
      {
        match: "/ui/session",
        body: {
          tenant: "acme",
          dataset: "production",
          memberships: [acmeMembership],
        },
      },
    ]);

    // "fast" opens the gate immediately; "slow" started before login and
    // its unauthenticated 401 lands only after the login succeeded. That
    // stale failure must not re-open the gate (the double-login bug).
    let fastCalls = 0;
    const fastFn = vi.fn().mockImplementation(() => {
      fastCalls += 1;
      return fastCalls === 1
        ? Promise.reject(new ApiError("Loki API failed (401)", 401))
        : Promise.resolve("fast-data");
    });
    let slowCalls = 0;
    let rejectSlow: ((err: unknown) => void) | undefined;
    const slowFn = vi.fn().mockImplementation(() => {
      slowCalls += 1;
      if (slowCalls === 1) {
        return new Promise<string>((_, reject) => {
          rejectSlow = reject;
        });
      }
      return Promise.resolve("slow-data");
    });

    renderWithClient(
      <>
        <Probe id="fast" queryFn={fastFn} />
        <Probe id="slow" queryFn={slowFn} />
        <LoginGate />
      </>,
    );

    await screen.findByRole("dialog", { name: "Sign in" });
    await userEvent.type(screen.getByLabelText("Email"), "alice@example.com");
    await userEvent.type(screen.getByLabelText("Password"), "secret");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Sign in" }),
      ).not.toBeInTheDocument(),
    );

    // The pre-login fetch now settles with its stale 401.
    rejectSlow?.(new ApiError("Loki API failed (401)", 401));

    await waitFor(() =>
      expect(screen.getByTestId("slow")).toHaveTextContent("slow-data"),
    );
    await waitFor(() =>
      expect(screen.getByTestId("fast")).toHaveTextContent("fast-data"),
    );
    expect(
      screen.queryByRole("dialog", { name: "Sign in" }),
    ).not.toBeInTheDocument();
  });

  it("retries with the logged-in tenant context before React re-renders", async () => {
    setTenantContext({ tenant: "", dataset: "" });
    stubFetchRoutes([
      {
        match: "/ui/session",
        body: {
          tenant: "acme",
          dataset: "production",
          memberships: [acmeMembership],
        },
      },
    ]);
    // The refetch after login must already carry the resolved tenant —
    // refetching under the stale empty context yields a fresh 401 that
    // re-opens the gate (the second half of the double-login bug).
    let calls = 0;
    const queryFn = vi.fn().mockImplementation(() => {
      calls += 1;
      if (calls === 1) {
        return Promise.reject(new ApiError("Loki API failed (401)", 401));
      }
      const tenant = tenantHeaders()["X-Tenant-ID"];
      return tenant
        ? Promise.resolve(`tenant:${tenant}`)
        : Promise.reject(new ApiError("Missing tenant (401)", 401));
    });

    renderWithClient(
      <>
        <Probe id="probe" queryFn={queryFn} />
        <LoginGate />
      </>,
    );

    await screen.findByRole("dialog", { name: "Sign in" });
    await userEvent.type(screen.getByLabelText("Email"), "alice@example.com");
    await userEvent.type(screen.getByLabelText("Password"), "secret");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));

    await waitFor(() =>
      expect(screen.getByTestId("probe")).toHaveTextContent("tenant:acme"),
    );
    expect(
      screen.queryByRole("dialog", { name: "Sign in" }),
    ).not.toBeInTheDocument();
  });

  it("stays hidden for non-auth query failures", async () => {
    const queryFn = vi
      .fn()
      .mockRejectedValue(new ApiError("Loki API failed (500)", 500));
    renderWithClient(
      <>
        <Probe id="probe" queryFn={queryFn} />
        <LoginGate />
      </>,
    );
    await waitFor(() => expect(queryFn).toHaveBeenCalled());
    expect(
      screen.queryByRole("dialog", { name: "Sign in" }),
    ).not.toBeInTheDocument();
  });
});
