import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router";
import { setTenantContext } from "../../api/http";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { ProcessorList } from "./ProcessorList";
import {
  PROCESSORS_LIST,
  shellOutlet,
  WHOAMI_MEMBER,
  WHOAMI_TENANT_ADMIN,
} from "./testFixtures";

function renderList(tenant = "acme") {
  setTenantContext({ tenant, dataset: "" });
  return renderWithClient(
    <MemoryRouter initialEntries={["/processors"]}>
      <Routes>
        <Route element={shellOutlet(tenant)}>
          <Route path="/processors" element={<ProcessorList />} />
          <Route path="/processors/new" element={<div>New page</div>} />
          <Route
            path="/processors/:name/edit"
            element={<div>Edit page</div>}
          />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  setTenantContext({ tenant: "", dataset: "" });
});

describe("ProcessorList", () => {
  it("renders name, signal, dataset, enabled, priority, status, and updated", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/processors", body: PROCESSORS_LIST },
    ]);
    renderList();

    const okRow = (await screen.findByText("redact-emails")).closest("tr")!;
    expect(okRow).toHaveTextContent("logs");
    expect(okRow).toHaveTextContent("production");
    expect(okRow).toHaveTextContent("yes");
    expect(okRow).toHaveTextContent("100");
    expect(okRow).toHaveTextContent("ok");

    const invalidRow = screen.getByText("strip-query").closest("tr")!;
    expect(invalidRow).toHaveTextContent("traces");
    expect(invalidRow).toHaveTextContent("all datasets");
    expect(invalidRow).toHaveTextContent("no");
    expect(invalidRow).toHaveTextContent("invalid");
    expect(invalidRow.className).toContain("processors-row-disabled");
    expect(okRow.className).not.toContain("processors-row-disabled");
  });

  it("hides create/edit/delete affordances from non-admins", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/processors", body: PROCESSORS_LIST },
    ]);
    renderList();
    await screen.findByText("redact-emails");

    expect(screen.queryByRole("link", { name: "New" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Delete" })).toBeNull();
    // Names are plain text, not links, for a read-only viewer.
    expect(
      screen.queryByRole("link", { name: "redact-emails" }),
    ).toBeNull();
  });

  it("shows create/edit/delete affordances for tenant admins", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/processors", body: PROCESSORS_LIST },
    ]);
    renderList();
    await screen.findByText("redact-emails");

    expect(
      await screen.findByRole("link", { name: "New" }),
    ).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Delete" })).toHaveLength(2);
    expect(
      screen.getByRole("link", { name: "redact-emails" }),
    ).toBeInTheDocument();
  });
});
