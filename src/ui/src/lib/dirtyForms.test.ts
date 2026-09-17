import { renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  anyDirty,
  markDirty,
  resetDirtyForms,
  subscribe,
  useDirtyForm,
} from "./dirtyForms";

afterEach(() => {
  resetDirtyForms();
});

describe("markDirty / anyDirty", () => {
  it("is false when nothing is registered", () => {
    expect(anyDirty()).toBe(false);
  });

  it("becomes true once any id is marked dirty", () => {
    markDirty("a", true);
    expect(anyDirty()).toBe(true);
  });

  it("stays true while at least one id is dirty, even after others clear", () => {
    markDirty("a", true);
    markDirty("b", true);
    markDirty("a", false);
    expect(anyDirty()).toBe(true);
    markDirty("b", false);
    expect(anyDirty()).toBe(false);
  });

  it("re-marking the same id with the same value is a no-op", () => {
    const listener = vi.fn();
    const unsubscribe = subscribe(listener);
    markDirty("a", true);
    expect(listener).toHaveBeenCalledTimes(1);
    markDirty("a", true);
    expect(listener).toHaveBeenCalledTimes(1);
    unsubscribe();
  });
});

describe("subscribe", () => {
  it("notifies listeners on every dirty-set change", () => {
    const listener = vi.fn();
    const unsubscribe = subscribe(listener);

    markDirty("a", true);
    markDirty("a", false);
    expect(listener).toHaveBeenCalledTimes(2);

    unsubscribe();
    markDirty("a", true);
    expect(listener).toHaveBeenCalledTimes(2);
  });
});

describe("useDirtyForm", () => {
  it("registers the id as dirty while isDirty is true", () => {
    const { rerender, unmount } = renderHook(
      ({ isDirty }: { isDirty: boolean }) => useDirtyForm("form-1", isDirty),
      { initialProps: { isDirty: false } },
    );
    expect(anyDirty()).toBe(false);

    rerender({ isDirty: true });
    expect(anyDirty()).toBe(true);

    rerender({ isDirty: false });
    expect(anyDirty()).toBe(false);

    unmount();
    expect(anyDirty()).toBe(false);
  });

  it("clears its id on unmount even if it was dirty", () => {
    const { unmount } = renderHook(() => useDirtyForm("form-2", true));
    expect(anyDirty()).toBe(true);

    unmount();
    expect(anyDirty()).toBe(false);
  });
});
