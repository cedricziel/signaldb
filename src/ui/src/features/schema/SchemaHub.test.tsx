import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BrowserRouter, Route, Routes } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { schemaRoutes } from "./routes";
import {
  REGISTRIES,
  WHOAMI_INSTANCE_ADMIN,
  WHOAMI_MEMBER,
} from "./testFixtures";

const SCHEMA = {
  logical_schema_version: "otel-2026-08",
  logical: [],
  physical: [],
};

function renderHub(path: string) {
  window.history.replaceState(null, "", path);
  return renderWithClient(
    <BrowserRouter>
      <Routes>
        {schemaRoutes()}
        <Route path="/logs" element={<div>Logs page</div>} />
      </Routes>
    </BrowserRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
});

describe("SchemaHub", () => {
  it("shows the Conventions tab to a tenant member and hides Storage", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
    ]);
    renderHub("/schema");

    const conventions = await screen.findByRole("tab", { name: "Conventions" });
    expect(conventions).toHaveAttribute("aria-selected", "true");
    expect(screen.queryByRole("tab", { name: "Storage" })).toBeNull();
    // The Conventions tab lists registries.
    expect(await screen.findByText("otel")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/schema/conventions");
  });

  it("offers the Storage tab to an instance admin and switches on click", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_INSTANCE_ADMIN },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
      { match: "/api/v1/manage/schema", body: SCHEMA },
    ]);
    renderHub("/schema");
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "Storage" }));
    expect(await screen.findByText("Logical schema")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/schema/storage");
    expect(screen.getByRole("tab", { name: "Storage" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
  });

  it("deep-links /schema/storage straight to the storage explorer", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_INSTANCE_ADMIN },
      { match: "/api/v1/manage/schema", body: SCHEMA },
    ]);
    renderHub("/schema/storage");

    expect(await screen.findByText("Logical schema")).toBeInTheDocument();
    expect(screen.getByText("otel-2026-08")).toBeInTheDocument();
  });

  it("returns to the previous tab on browser back", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_INSTANCE_ADMIN },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
      { match: "/api/v1/manage/schema", body: SCHEMA },
    ]);
    renderHub("/schema/conventions");
    const user = userEvent.setup();
    await screen.findByText("otel");

    await user.click(screen.getByRole("tab", { name: "Storage" }));
    await screen.findByText("Logical schema");

    window.history.back();
    await waitFor(() =>
      expect(window.location.pathname).toBe("/schema/conventions"),
    );
    expect(await screen.findByText("otel")).toBeInTheDocument();
  });
});
