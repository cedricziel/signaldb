import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AttributeValue } from "./AttributeValue";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("AttributeValue", () => {
  it("renders ordinary strings without a JSON disclosure control", () => {
    render(<AttributeValue value="checkout" label="value for service" />);

    expect(screen.getByText("checkout")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Expand JSON" })).toBeNull();
  });

  it("renders object JSON as a compact preview that can be expanded", async () => {
    const user = userEvent.setup();
    render(
      <AttributeValue
        value={'{"service":"checkout","retries":2,"failed":false,"cause":null}'}
        label="value for attributes"
      />,
    );

    const toggle = screen.getByRole("button", { name: "Expand JSON" });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(
      screen.getByText(
        '{"service":"checkout","retries":2,"failed":false,"cause":null}',
      ),
    ).toBeInTheDocument();

    await user.click(toggle);

    expect(toggle).toHaveAccessibleName("Collapse JSON");
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText('"service"')).toHaveClass("json-key");
    expect(screen.getByText('"checkout"')).toHaveClass("json-string");
    expect(screen.getByText("2")).toHaveClass("json-number");
    expect(screen.getByText("false")).toHaveClass("json-boolean");
    expect(screen.getByText("null")).toHaveClass("json-null");
  });

  it("detects arrays as expandable JSON", () => {
    render(<AttributeValue value="[1,2,3]" label="value for samples" />);

    expect(screen.getByRole("button", { name: "Expand JSON" })).toHaveAttribute(
      "aria-expanded",
      "false",
    );
  });

  it("copies the original JSON string rather than the formatted value", async () => {
    const user = userEvent.setup();
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    const value = '{"service":"checkout", "retries":2}';
    render(<AttributeValue value={value} label="value for attributes" />);

    await user.click(
      screen.getByRole("button", { name: "Copy value for attributes" }),
    );

    expect(writeText).toHaveBeenCalledWith(value);
  });

  it("keeps the copy control available when clipboard access is denied", async () => {
    const user = userEvent.setup();
    vi.stubGlobal("navigator", {
      clipboard: { writeText: vi.fn().mockRejectedValue(new Error("denied")) },
    });
    render(<AttributeValue value="checkout" label="value for service" />);

    const button = screen.getByRole("button", {
      name: "Copy value for service",
    });
    await user.click(button);

    expect(button).toHaveAccessibleName("Copy value for service");
    expect(button).not.toHaveAttribute("data-copied");
  });
});
