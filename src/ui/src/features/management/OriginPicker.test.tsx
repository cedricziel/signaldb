import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { anyDirty, resetDirtyForms } from "../../lib/dirtyForms";
import { OriginPicker } from "./OriginPicker";

afterEach(() => {
  resetDirtyForms();
});

function Harness({ initial = [] as string[] }) {
  const [origins, setOrigins] = useState(initial);
  return (
    <OriginPicker idPrefix="test" origins={origins} onChange={setOrigins} />
  );
}

describe("OriginPicker dirty tracking", () => {
  it("is not dirty with no pending text and no chips", () => {
    render(<Harness />);
    expect(anyDirty()).toBe(false);
  });

  it("becomes dirty once text is typed into the add field", async () => {
    render(<Harness />);
    await userEvent.type(
      screen.getByLabelText("Add allowed origin"),
      "https://app.example.com",
    );
    expect(anyDirty()).toBe(true);
  });

  it("becomes dirty once an origin chip is added, even after the input clears", async () => {
    render(<Harness />);
    await userEvent.type(
      screen.getByLabelText("Add allowed origin"),
      "https://app.example.com",
    );
    await userEvent.click(screen.getByText("Add"));

    expect(screen.getByLabelText("Add allowed origin")).toHaveValue("");
    expect(anyDirty()).toBe(true);
  });

  it("clears once every chip is removed and the input is empty", async () => {
    render(<Harness initial={["https://app.example.com"]} />);
    expect(anyDirty()).toBe(true);

    await userEvent.click(
      screen.getByRole("button", { name: "Remove https://app.example.com" }),
    );

    expect(anyDirty()).toBe(false);
  });

  it("clears on unmount", () => {
    const { unmount } = render(<Harness initial={["https://a.example.com"]} />);
    expect(anyDirty()).toBe(true);
    unmount();
    expect(anyDirty()).toBe(false);
  });
});
