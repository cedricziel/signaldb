import { renderHook } from "@testing-library/react";
import { useRef } from "react";
import { describe, expect, it } from "vitest";
import { useContainerWidth } from "./useContainerWidth";

describe("useContainerWidth", () => {
  it("returns the fallback until the ref is attached", () => {
    const { result } = renderHook(() => {
      const ref = useRef<HTMLDivElement | null>(null);
      return useContainerWidth(ref, 720);
    });
    expect(result.current).toBe(720);
  });

  it("reports the observed element's width once attached", () => {
    const div = document.createElement("div");
    Object.defineProperty(div, "getBoundingClientRect", {
      value: () => ({
        width: 480,
        height: 64,
        top: 0,
        left: 0,
        bottom: 0,
        right: 0,
        x: 0,
        y: 0,
        toJSON: () => ({}),
      }),
    });
    document.body.appendChild(div);

    const { result } = renderHook(() => {
      const ref = useRef<HTMLDivElement | null>(div);
      return useContainerWidth(ref, 720);
    });

    expect(result.current).toBe(480);
    document.body.removeChild(div);
  });
});
