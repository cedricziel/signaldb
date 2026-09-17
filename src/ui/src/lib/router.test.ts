import { afterEach, describe, expect, it, vi } from "vitest";
import type { NavigateFunction } from "react-router";
import { goBackOr } from "./router";

/** Simulates the entry `window.history.state.idx` react-router leaves
 * behind after `idx` navigations (a `push` each) from a fresh session. */
function setHistoryIdx(idx: number): void {
  window.history.replaceState({ idx }, "");
}

afterEach(() => {
  // Isolate each test's simulated history depth from the next.
  window.history.replaceState(null, "");
});

describe("goBackOr", () => {
  it("navigates back when there is an in-app entry behind this one", () => {
    setHistoryIdx(1);
    const navigate = vi.fn() as unknown as NavigateFunction;
    const fallback = vi.fn();
    goBackOr(navigate, fallback);
    expect(navigate).toHaveBeenCalledWith(-1);
    expect(fallback).not.toHaveBeenCalled();
  });

  it("runs the fallback when this is the first entry in the session", () => {
    setHistoryIdx(0);
    const navigate = vi.fn() as unknown as NavigateFunction;
    const fallback = vi.fn();
    goBackOr(navigate, fallback);
    expect(navigate).not.toHaveBeenCalled();
    expect(fallback).toHaveBeenCalledTimes(1);
  });

  it("runs the fallback when idx is 0 even though a replace navigation gave the entry a fresh key", () => {
    // The bug this guards against: a page reached via a `replace` (e.g. from
    // /login) gets a non-"default" `location.key`, but `replace` overwrites
    // the current entry rather than adding one, so `idx` stays 0 — there is
    // still nothing in-app behind it.
    setHistoryIdx(0);
    const navigate = vi.fn() as unknown as NavigateFunction;
    const fallback = vi.fn();
    goBackOr(navigate, fallback);
    expect(navigate).not.toHaveBeenCalled();
    expect(fallback).toHaveBeenCalledTimes(1);
  });

  it("runs the fallback when history.state carries no idx at all", () => {
    window.history.replaceState(null, "");
    const navigate = vi.fn() as unknown as NavigateFunction;
    const fallback = vi.fn();
    goBackOr(navigate, fallback);
    expect(navigate).not.toHaveBeenCalled();
    expect(fallback).toHaveBeenCalledTimes(1);
  });
});
