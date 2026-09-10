import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import { ConfirmButton } from "./ConfirmButton";
import { Dialog } from "./Dialog";

it("shows the prompt on click", async () => {
  render(
    <ConfirmButton
      label="Delete"
      prompt="Delete dataset staging?"
      onConfirm={vi.fn()}
    />,
  );
  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  expect(screen.getByText("Delete dataset staging?")).toBeInTheDocument();
});

it("fires onConfirm once and resets", async () => {
  const onConfirm = vi.fn();
  render(
    <ConfirmButton
      label="Delete"
      prompt="Delete dataset staging?"
      onConfirm={onConfirm}
    />,
  );
  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  await userEvent.click(screen.getByRole("button", { name: "Confirm" }));
  expect(onConfirm).toHaveBeenCalledTimes(1);
  expect(screen.queryByText("Delete dataset staging?")).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument();
});

it("Cancel resets without firing onConfirm", async () => {
  const onConfirm = vi.fn();
  render(
    <ConfirmButton
      label="Delete"
      prompt="Delete dataset staging?"
      onConfirm={onConfirm}
    />,
  );
  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(onConfirm).not.toHaveBeenCalled();
  expect(screen.queryByText("Delete dataset staging?")).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument();
});

it("Escape resets without firing onConfirm", async () => {
  const onConfirm = vi.fn();
  render(
    <ConfirmButton
      label="Delete"
      prompt="Delete dataset staging?"
      onConfirm={onConfirm}
    />,
  );
  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  await userEvent.keyboard("{Escape}");
  expect(onConfirm).not.toHaveBeenCalled();
  expect(screen.queryByText("Delete dataset staging?")).not.toBeInTheDocument();
});

it("moves focus onto Confirm when armed and back to the button on Cancel", async () => {
  render(
    <ConfirmButton label="Delete" prompt="Delete it?" onConfirm={vi.fn()} />,
  );
  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  expect(screen.getByRole("button", { name: "Confirm" })).toHaveFocus();
  await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(screen.getByRole("button", { name: "Delete" })).toHaveFocus();
});

it("Escape while armed cancels the confirmation without closing a Dialog around it", async () => {
  const onClose = vi.fn();
  const onConfirm = vi.fn();
  render(
    <Dialog label="Manage" onClose={onClose}>
      <ConfirmButton label="Delete" prompt="Delete it?" onConfirm={onConfirm} />
    </Dialog>,
  );
  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  await userEvent.keyboard("{Escape}");
  expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument();
  expect(onConfirm).not.toHaveBeenCalled();
  expect(onClose).not.toHaveBeenCalled();
  // With nothing armed, Escape reaches the dialog as before.
  await userEvent.keyboard("{Escape}");
  expect(onClose).toHaveBeenCalledTimes(1);
});

it("one Escape cancels every armed confirmation, not just the first-armed", async () => {
  render(
    <>
      <ConfirmButton label="Delete A" prompt="A?" onConfirm={vi.fn()} />
      <ConfirmButton label="Delete B" prompt="B?" onConfirm={vi.fn()} />
    </>,
  );
  await userEvent.click(screen.getByRole("button", { name: "Delete A" }));
  await userEvent.click(screen.getByRole("button", { name: "Delete B" }));
  expect(screen.getAllByRole("button", { name: "Confirm" })).toHaveLength(2);
  await userEvent.keyboard("{Escape}");
  expect(
    screen.queryByRole("button", { name: "Confirm" }),
  ).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Delete A" })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Delete B" })).toBeInTheDocument();
});

it("Escape from a sibling control still cancels the confirmation before the Dialog", async () => {
  const onClose = vi.fn();
  render(
    <Dialog label="Manage" onClose={onClose}>
      <button>Other</button>
      <ConfirmButton label="Delete" prompt="Delete it?" onConfirm={vi.fn()} />
    </Dialog>,
  );
  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  screen.getByRole("button", { name: "Other" }).focus();
  await userEvent.keyboard("{Escape}");
  expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument();
  expect(onClose).not.toHaveBeenCalled();
});
