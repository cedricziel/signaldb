import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { Instrumentation } from "./Instrumentation";

/** Locate the <code> element inside the "Configuration snippet" panel. */
function getCodeBlock(): HTMLElement {
  const header = screen.getByText("Configuration snippet");
  const container = header.closest(".code-snippet");
  const code = container?.querySelector("code");
  if (!code) throw new Error("code element not found in snippet container");
  return code as HTMLElement;
}

function renderInstrumentation(props: {
  state: { tenant: string; dataset: string };
}) {
  return renderWithClient(
    <MemoryRouter>
      {/* Instrumentation expects state prop */}
      <Instrumentation state={props.state} />
    </MemoryRouter>,
  );
}

const WHOAMI = {
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [
    { id: "production", slug: "production", is_default: true },
    { id: "staging", slug: "staging", is_default: false },
  ],
  default_dataset: "production",
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Instrumentation page", () => {
  it("shows source selector with all 6 sources", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByText("OTel SDK")).toBeInTheDocument();
      expect(screen.getByText("OTel Collector")).toBeInTheDocument();
      expect(screen.getByText("Kubernetes")).toBeInTheDocument();
      expect(screen.getByText("Docker")).toBeInTheDocument();
      expect(screen.getByText("journald")).toBeInTheDocument();
      expect(screen.getByText("Prometheus")).toBeInTheDocument();
    });
  });

  it("OTel SDK selected by default", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByText("OTel SDK")).toHaveClass("selected");
      // Ensure content for OTel SDK appears
      expect(screen.getByText("OpenTelemetry SDK")).toBeInTheDocument();
    });
  });

  it("shows code snippet for selected source", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      // Should include tenant and dataset placeholders
      const codeBlock = getCodeBlock();
      expect(codeBlock).toBeInTheDocument();
      expect(codeBlock.textContent).toContain("acme");
      expect(codeBlock.textContent).toContain("production");
    });
  });

  it("clicking a source switches content", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByText("OpenTelemetry SDK")).toBeInTheDocument();
    });

    await userEvent.click(screen.getByRole("button", { name: "Kubernetes" }));
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Kubernetes" })).toHaveClass(
        "selected",
      );
      // Kubernetes content appears
      expect(
        screen.getByText(/Instrument Kubernetes workloads/i),
      ).toBeInTheDocument();
    });
  });

  it("shows verification status section", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      expect(screen.getByText("Verification")).toBeInTheDocument();
      expect(screen.getByText("Traces")).toBeInTheDocument();
      expect(screen.getByText("Logs")).toBeInTheDocument();
      expect(screen.getByText("Metrics")).toBeInTheDocument();
      expect(screen.getByText("Profiles")).toBeInTheDocument();
    });
  });

  it("code snippets include current tenant from whoami", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderInstrumentation({ state: { tenant: "acme", dataset: "production" } });

    await waitFor(() => {
      const code = getCodeBlock();
      expect(code.textContent).toMatch(/X-Tenant-ID.*acme/);
      expect(code.textContent).toMatch(/X-Dataset-ID.*production/);
    });
  });
});
