import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import { Dialog } from "./Dialog";

it("calls onClose on Escape", async () => {
  const onClose = vi.fn();
  render(
    <Dialog label="Test dialog" onClose={onClose}>
      <button>First</button>
      <button>Last</button>
    </Dialog>,
  );
  await userEvent.keyboard("{Escape}");
  expect(onClose).toHaveBeenCalledTimes(1);
});

it("closes on a backdrop click but not on a click inside the panel", async () => {
  const onClose = vi.fn();
  const { container } = render(
    <Dialog label="Test dialog" onClose={onClose}>
      <button>Inside</button>
    </Dialog>,
  );

  await userEvent.click(screen.getByText("Inside"));
  expect(onClose).not.toHaveBeenCalled();

  // A click on the panel itself (the "dialog" role element) must not
  // close it — only a click that lands on the backdrop element directly.
  await userEvent.click(screen.getByRole("dialog", { name: "Test dialog" }));
  expect(onClose).not.toHaveBeenCalled();

  const backdrop = container.querySelector(".dialog-backdrop");
  expect(backdrop).not.toBeNull();
  await userEvent.click(backdrop as Element);
  expect(onClose).toHaveBeenCalledTimes(1);
});

it("wraps focus from the last focusable element back to the first on Tab", async () => {
  render(
    <Dialog label="Test dialog">
      <button>First</button>
      <button>Last</button>
    </Dialog>,
  );
  const first = screen.getByText("First");
  const last = screen.getByText("Last");
  last.focus();
  expect(last).toHaveFocus();
  await userEvent.tab();
  expect(first).toHaveFocus();
});

it("restores focus to the previously focused element on unmount", async () => {
  function Harness({ open }: { open: boolean }) {
    return (
      <div>
        <button>Opener</button>
        {open && (
          <Dialog label="Test dialog">
            <button>Inside</button>
          </Dialog>
        )}
      </div>
    );
  }
  const { rerender } = render(<Harness open={false} />);
  const opener = screen.getByText("Opener");
  opener.focus();
  expect(opener).toHaveFocus();

  rerender(<Harness open={true} />);
  expect(screen.getByText("Inside")).toHaveFocus();

  rerender(<Harness open={false} />);
  expect(opener).toHaveFocus();
});

it("does nothing on Escape when no onClose is given", async () => {
  render(
    <Dialog label="Test dialog">
      <button>Inside</button>
    </Dialog>,
  );
  // No onClose to call; this only asserts it doesn't throw and the dialog
  // stays mounted.
  await userEvent.keyboard("{Escape}");
  expect(
    screen.getByRole("dialog", { name: "Test dialog" }),
  ).toBeInTheDocument();
});

it("keeps focus on the panel when the dialog has nothing focusable", async () => {
  render(
    <Dialog label="Message only">
      <p>Nothing to press here.</p>
    </Dialog>,
  );
  const panel = screen.getByRole("dialog");
  expect(panel).toHaveFocus();
  await userEvent.tab();
  expect(panel).toHaveFocus();
  await userEvent.tab({ shift: true });
  expect(panel).toHaveFocus();
});
