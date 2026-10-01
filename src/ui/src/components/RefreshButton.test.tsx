import { useQuery } from "@tanstack/react-query";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../test/render";
import { RefreshButton } from "./RefreshButton";

function Consumer({
  queryKey,
  queryFn,
}: {
  queryKey: string[];
  queryFn: () => Promise<string>;
}) {
  useQuery({ queryKey, queryFn });
  return null;
}

function setup(queryKey: string[], queryFn: () => Promise<string>) {
  renderWithClient(
    <>
      <RefreshButton />
      <Consumer queryKey={queryKey} queryFn={queryFn} />
    </>,
  );
  return screen.getByRole("button", { name: "Refresh" });
}

describe("RefreshButton", () => {
  it("animates while queries are fetching and stops once they settle", async () => {
    let resolve!: (v: string) => void;
    const button = setup(["data"], () => new Promise((r) => (resolve = r)));
    await waitFor(() => expect(button).toHaveAttribute("aria-busy", "true"));
    resolve("done");
    await waitFor(() => expect(button).not.toHaveAttribute("aria-busy"));
  });

  it("ignores queries that don't depend on the time range", async () => {
    const button = setup(["whoami"], () => new Promise(() => {}));
    await new Promise((r) => setTimeout(r, 0));
    expect(button).not.toHaveAttribute("aria-busy");
  });

  it("refetches active range queries on click", async () => {
    const queryFn = vi.fn().mockResolvedValue("ok");
    const button = setup(["data"], queryFn);
    await waitFor(() => expect(queryFn).toHaveBeenCalledTimes(1));
    await userEvent.click(button);
    await waitFor(() => expect(queryFn).toHaveBeenCalledTimes(2));
  });

  it("leaves non-range queries alone on click", async () => {
    const queryFn = vi.fn().mockResolvedValue("ok");
    const button = setup(["whoami"], queryFn);
    await waitFor(() => expect(queryFn).toHaveBeenCalledTimes(1));
    await userEvent.click(button);
    await new Promise((r) => setTimeout(r, 20));
    expect(queryFn).toHaveBeenCalledTimes(1);
  });
});
