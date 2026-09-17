import { describe, expect, it, vi } from "vitest";
import type { Location, NavigateFunction } from "react-router";
import { goBackOr } from "./router";

function locationWithKey(key: string): Location {
  return { key } as Location;
}

describe("goBackOr", () => {
  it("navigates back when there is in-app history", () => {
    const navigate = vi.fn() as unknown as NavigateFunction;
    const fallback = vi.fn();
    goBackOr(navigate, locationWithKey("abc123"), fallback);
    expect(navigate).toHaveBeenCalledWith(-1);
    expect(fallback).not.toHaveBeenCalled();
  });

  it("runs the fallback when the location is the first history entry", () => {
    const navigate = vi.fn() as unknown as NavigateFunction;
    const fallback = vi.fn();
    goBackOr(navigate, locationWithKey("default"), fallback);
    expect(navigate).not.toHaveBeenCalled();
    expect(fallback).toHaveBeenCalledTimes(1);
  });
});
