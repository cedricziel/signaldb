import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { LoginMethods } from "./LoginMethods";

const passwordOnly = { password_enabled: true, oidc: null, demo: null };
const both = {
  password_enabled: true,
  oidc: { name: "Acme SSO" },
  demo: null,
};
const ssoOnly = {
  password_enabled: false,
  oidc: { name: "Acme SSO" },
  demo: null,
};
const neither = { password_enabled: false, oidc: null, demo: null };

describe("LoginMethods", () => {
  it("probe pending (config undefined): shows the checking hint, no form or SSO control", () => {
    renderWithClient(
      <LoginMethods
        config={undefined}
        redirect="/logs"
        onAuthenticated={vi.fn()}
      />,
    );
    expect(screen.getByText("Checking sign-in options…")).toBeInTheDocument();
    expect(screen.queryByLabelText("Email")).not.toBeInTheDocument();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });

  it("password only: shows the form, no SSO control, primary submit", () => {
    renderWithClient(
      <LoginMethods
        config={passwordOnly}
        redirect="/logs"
        onAuthenticated={vi.fn()}
      />,
    );
    expect(screen.getByLabelText("Email")).toBeInTheDocument();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Sign in" })).toHaveClass(
      "btn-primary",
    );
  });

  it("both methods: SSO first, then a divider, then a soft password form", () => {
    renderWithClient(
      <LoginMethods config={both} redirect="/logs" onAuthenticated={vi.fn()} />,
    );
    expect(
      screen.getByRole("link", { name: "Continue with Acme SSO" }),
    ).toBeInTheDocument();
    expect(screen.getByText("or")).toBeInTheDocument();
    expect(screen.getByLabelText("Email")).toBeInTheDocument();
    const submit = screen.getByRole("button", { name: "Sign in" });
    expect(submit).toHaveClass("btn");
    expect(submit).not.toHaveClass("btn-primary");
  });

  it("SSO only: no password form, and a hint that password sign-in is off", () => {
    renderWithClient(
      <LoginMethods
        config={ssoOnly}
        redirect="/logs"
        onAuthenticated={vi.fn()}
      />,
    );
    expect(
      screen.getByRole("link", { name: "Continue with Acme SSO" }),
    ).toBeInTheDocument();
    expect(screen.queryByLabelText("Email")).not.toBeInTheDocument();
    expect(
      screen.getByText("Password sign-in is off on this instance."),
    ).toBeInTheDocument();
  });

  it("degenerate config (password disabled, no oidc): shows the password form anyway", () => {
    renderWithClient(
      <LoginMethods
        config={neither}
        redirect="/logs"
        onAuthenticated={vi.fn()}
      />,
    );
    expect(screen.getByLabelText("Email")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Sign in" })).toHaveClass(
      "btn-primary",
    );
  });

  it("probe unavailable: shows the password form, a notice, and no SSO control", () => {
    renderWithClient(
      <LoginMethods
        config="unavailable"
        redirect="/logs"
        onAuthenticated={vi.fn()}
      />,
    );
    expect(screen.getByLabelText("Email")).toBeInTheDocument();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
    expect(
      screen.getByText(
        "Couldn't load sign-in options — password sign-in is shown as a fallback.",
      ),
    ).toBeInTheDocument();
  });

  it("SSO control carries the redirect target, URL-encoded", () => {
    renderWithClient(
      <LoginMethods
        config={both}
        redirect="/traces?range=15m"
        onAuthenticated={vi.fn()}
      />,
    );
    expect(
      screen.getByRole("link", { name: "Continue with Acme SSO" }),
    ).toHaveAttribute(
      "href",
      "/ui/session/oidc/start?redirect=%2Ftraces%3Frange%3D15m",
    );
  });

  it("focuses the SSO link first when one is offered", () => {
    renderWithClient(
      <LoginMethods config={both} redirect="/logs" onAuthenticated={vi.fn()} />,
    );
    expect(
      screen.getByRole("link", { name: "Continue with Acme SSO" }),
    ).toHaveFocus();
  });

  it("focuses the email field when there is no SSO control", () => {
    renderWithClient(
      <LoginMethods
        config={passwordOnly}
        redirect="/logs"
        onAuthenticated={vi.fn()}
      />,
    );
    expect(screen.getByLabelText("Email")).toHaveFocus();
  });

  it("demo config: shows the demo button above a divider, ahead of the password form", () => {
    renderWithClient(
      <LoginMethods
        config={{
          password_enabled: true,
          oidc: null,
          demo: { username: "demo", password: "demo" },
        }}
        redirect="/logs"
        onAuthenticated={vi.fn()}
      />,
    );
    expect(
      screen.getByRole("button", { name: "Explore the demo" }),
    ).toBeInTheDocument();
    expect(screen.getByText("demo / demo")).toBeInTheDocument();
    expect(screen.getByText("or")).toBeInTheDocument();
    expect(screen.getByLabelText("Email")).toBeInTheDocument();
  });

  it("demo button signs in with the demo credentials", async () => {
    stubFetchRoutes([
      {
        match: "/ui/session",
        body: { tenant: "demo", dataset: "otel-demo", memberships: [] },
      },
    ]);
    const onAuthenticated = vi.fn();
    renderWithClient(
      <LoginMethods
        config={{
          password_enabled: true,
          oidc: null,
          demo: { username: "demo", password: "demo" },
        }}
        redirect="/logs"
        onAuthenticated={onAuthenticated}
      />,
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Explore the demo" }),
    );
    await screen.findByRole("button", { name: "Explore the demo" });
    expect(onAuthenticated).toHaveBeenCalledWith({
      tenant: "demo",
      dataset: "otel-demo",
      memberships: [],
    });
  });

  it("POSTs credentials to /ui/session and reports the response", async () => {
    stubFetchRoutes([
      {
        match: "/ui/session",
        body: { tenant: "acme", dataset: "prod", memberships: [] },
      },
    ]);
    const onAuthenticated = vi.fn();
    renderWithClient(
      <LoginMethods
        config={passwordOnly}
        redirect="/logs"
        onAuthenticated={onAuthenticated}
      />,
    );
    await userEvent.type(screen.getByLabelText("Email"), "alice@example.com");
    await userEvent.type(screen.getByLabelText("Password"), "secret");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("button", { name: "Sign in" });
    expect(onAuthenticated).toHaveBeenCalledWith({
      tenant: "acme",
      dataset: "prod",
      memberships: [],
    });
  });
});
