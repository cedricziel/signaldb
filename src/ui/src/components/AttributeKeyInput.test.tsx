import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { resetSemanticsCache } from "../hooks/useSemantics";
import { stubFetchRoutes } from "../test/render";
import { AttributeKeyInput } from "./AttributeKeyInput";

// The registry search is covered by FilterChips.test.tsx; fail it here so
// the test exercises the observed-keys fallback deterministically.
beforeEach(() => {
  stubFetchRoutes([
    {
      match: "/api/v1/schema/attributes",
      body: { error: "boom" },
      status: 500,
    },
  ]);
});

afterEach(() => {
  vi.unstubAllGlobals();
  resetSemanticsCache();
});

// Mirrors the real callers: picking writes the key back into the controlled
// value, which is what closes the list (picked === value).
function Harness({ onPick }: { onPick: (key: string) => void }) {
  const [value, setValue] = useState("");
  return (
    <AttributeKeyInput
      value={value}
      onChange={setValue}
      onPick={(key) => {
        setValue(key);
        onPick(key);
      }}
      observed={["level", "line"]}
      ariaLabel="Attribute key"
    />
  );
}

it("suggests observed keys, reports a pick, and closes the list", async () => {
  const onPick = vi.fn();
  render(<Harness onPick={onPick} />);
  await userEvent.type(screen.getByLabelText("Attribute key"), "le");
  const list = await screen.findByRole("listbox", {
    name: "Attribute key suggestions",
  });
  expect(list).toBeInTheDocument();
  await userEvent.click(screen.getByRole("option", { name: /level/ }));
  expect(onPick).toHaveBeenCalledWith("level");
  expect(
    screen.queryByRole("listbox", { name: "Attribute key suggestions" }),
  ).not.toBeInTheDocument();
});

it("moves the highlight with the arrow keys and picks it with Enter", async () => {
  const onPick = vi.fn();
  render(<Harness onPick={onPick} />);
  const input = screen.getByLabelText("Attribute key");
  await userEvent.type(input, "l");
  await screen.findByRole("listbox", { name: "Attribute key suggestions" });
  expect(input).not.toHaveAttribute("aria-activedescendant");

  await userEvent.keyboard("{ArrowDown}");
  const first = screen.getByRole("option", { name: /level/ });
  expect(first).toHaveAttribute("aria-selected", "true");
  expect(input).toHaveAttribute("aria-activedescendant", first.id);

  await userEvent.keyboard("{ArrowDown}");
  const second = screen.getByRole("option", { name: /line/ });
  expect(second).toHaveAttribute("aria-selected", "true");
  expect(first).toHaveAttribute("aria-selected", "false");

  // Wraps around from the last option back to the first.
  await userEvent.keyboard("{ArrowDown}");
  expect(first).toHaveAttribute("aria-selected", "true");

  await userEvent.keyboard("{ArrowUp}");
  expect(second).toHaveAttribute("aria-selected", "true");

  await userEvent.keyboard("{Enter}");
  expect(onPick).toHaveBeenCalledWith("line");
  expect(
    screen.queryByRole("listbox", { name: "Attribute key suggestions" }),
  ).not.toBeInTheDocument();
});

it("leaves Enter to the caller when nothing is highlighted", async () => {
  const onPick = vi.fn();
  render(<Harness onPick={onPick} />);
  await userEvent.type(screen.getByLabelText("Attribute key"), "le");
  await screen.findByRole("listbox", { name: "Attribute key suggestions" });
  await userEvent.keyboard("{Enter}");
  expect(onPick).not.toHaveBeenCalled();
  expect(
    screen.getByRole("listbox", { name: "Attribute key suggestions" }),
  ).toBeInTheDocument();
});

it("closes the list on Escape without picking", async () => {
  const onPick = vi.fn();
  render(<Harness onPick={onPick} />);
  const input = screen.getByLabelText("Attribute key");
  await userEvent.type(input, "le");
  await screen.findByRole("listbox", { name: "Attribute key suggestions" });
  await userEvent.keyboard("{Escape}");
  expect(onPick).not.toHaveBeenCalled();
  expect(
    screen.queryByRole("listbox", { name: "Attribute key suggestions" }),
  ).not.toBeInTheDocument();
  expect(input).toHaveValue("le");
});
