import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { useMobileSidebar } from "./useMobileSidebar";

describe("useMobileSidebar", () => {
  it("starts closed and toggles open/closed", () => {
    const { result } = renderHook(() => useMobileSidebar());
    expect(result.current.open).toBe(false);

    act(() => result.current.toggle());
    expect(result.current.open).toBe(true);

    act(() => result.current.toggle());
    expect(result.current.open).toBe(false);
  });

  it("close() closes regardless of current state", () => {
    const { result } = renderHook(() => useMobileSidebar());
    act(() => result.current.toggle());
    expect(result.current.open).toBe(true);

    act(() => result.current.close());
    expect(result.current.open).toBe(false);
  });

  it("closes on Escape while open", () => {
    const { result } = renderHook(() => useMobileSidebar());
    act(() => result.current.toggle());
    expect(result.current.open).toBe(true);

    act(() => {
      window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    });
    expect(result.current.open).toBe(false);
  });

  it("ignores Escape while closed", () => {
    const { result } = renderHook(() => useMobileSidebar());
    act(() => {
      window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    });
    expect(result.current.open).toBe(false);
  });
});
