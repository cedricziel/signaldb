import { fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router";
import { setTenantContext } from "../../api/http";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { ProcessorEditor } from "./ProcessorEditor";
import {
  TEST_RESPONSE,
  VALIDATION_ERROR,
  VALIDATION_OK,
  shellOutlet,
  WHOAMI_TENANT_ADMIN,
} from "./testFixtures";

function renderEditor() {
  setTenantContext({ tenant: "acme", dataset: "" });
  return renderWithClient(
    <MemoryRouter initialEntries={["/processors/new"]}>
      <Routes>
        <Route element={shellOutlet("acme")}>
          <Route path="/processors/new" element={<ProcessorEditor />} />
          <Route path="/processors" element={<div>List page</div>} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  setTenantContext({ tenant: "", dataset: "" });
});

describe("ProcessorEditor", () => {
  it("annotates an invalid line and disables Save", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/processors:validate", body: VALIDATION_ERROR },
    ]);
    renderEditor();
    const user = userEvent.setup();

    await user.type(await screen.findByLabelText("Name"), "my-processor");
    fireEvent.change(
      screen.getByLabelText("Statements (one per line)"),
      {
        target: {
          value:
            'set(attributes["a"], "1")\nmerge_maps(attributes, resource.attributes, "upsert")',
        },
      },
    );
    await user.click(screen.getByRole("button", { name: "Validate" }));

    expect(
      await screen.findByText(/Line 2.*merge_maps is not supported/),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();
  });

  it("enables Save once validation passes", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/processors:validate", body: VALIDATION_OK },
    ]);
    renderEditor();
    const user = userEvent.setup();

    await user.type(await screen.findByLabelText("Name"), "my-processor");
    fireEvent.change(screen.getByLabelText("Statements (one per line)"), {
      target: { value: 'set(attributes["a"], "1")' },
    });
    expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Validate" }));

    expect(
      await screen.findByRole("button", { name: "Save" }),
    ).not.toBeDisabled();
  });

  it("disables Save again after the signal changes post-validation", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/processors:validate", body: VALIDATION_OK },
    ]);
    renderEditor();
    const user = userEvent.setup();

    await user.type(await screen.findByLabelText("Name"), "my-processor");
    fireEvent.change(screen.getByLabelText("Statements (one per line)"), {
      target: { value: 'set(attributes["a"], "1")' },
    });
    await user.click(screen.getByRole("button", { name: "Validate" }));
    expect(
      await screen.findByRole("button", { name: "Save" }),
    ).not.toBeDisabled();

    await user.selectOptions(screen.getByLabelText("Signal"), "logs");

    expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();
  });

  it("annotates an error after a blank line on the correct source line", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      {
        match: "/api/v1/processors:validate",
        body: {
          errors: [{ statement: 1, column: 0, message: "boom" }],
        },
      },
    ]);
    renderEditor();
    const user = userEvent.setup();

    await user.type(await screen.findByLabelText("Name"), "my-processor");
    fireEvent.change(screen.getByLabelText("Statements (one per line)"), {
      target: {
        value: 'set(attributes["a"], "1")\n\nmerge_maps(attributes, resource.attributes, "upsert")',
      },
    });
    await user.click(screen.getByRole("button", { name: "Validate" }));

    // Filtered statement index 1 ("merge_maps...") is on textarea line 3,
    // not line 2 — the blank line at textarea index 1 must not shift it.
    expect(await screen.findByText(/Line 3.*boom/)).toBeInTheDocument();
  });

  it("renders the test panel diff and per-statement counts", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/processors:test", body: TEST_RESPONSE },
    ]);
    renderEditor();
    const user = userEvent.setup();

    await screen.findByLabelText("Name");
    await user.selectOptions(screen.getByLabelText("Signal"), "logs");
    await user.click(screen.getByRole("button", { name: "Run test" }));

    expect(
      await screen.findByText(/statement 0: 1 match, 0 errors/),
    ).toBeInTheDocument();
    expect(screen.getByLabelText("payload diff")).toHaveTextContent(
      "[redacted]",
    );
  });
});
