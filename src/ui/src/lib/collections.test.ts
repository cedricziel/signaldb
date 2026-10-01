import { describe, expect, it } from "vitest";
import { toggleInSet } from "./collections";

describe("toggleInSet", () => {
  it("adds an absent item", () => {
    const out = toggleInSet(new Set(["a"]), "b");
    expect(out).toEqual(new Set(["a", "b"]));
  });

  it("removes a present item", () => {
    const out = toggleInSet(new Set(["a", "b"]), "b");
    expect(out).toEqual(new Set(["a"]));
  });

  it("never mutates the input set", () => {
    const input = new Set(["a"]);
    toggleInSet(input, "b");
    expect(input).toEqual(new Set(["a"]));
  });

  it("returns a new Set instance even when nothing changes conceptually", () => {
    const input = new Set(["a"]);
    expect(toggleInSet(input, "a")).not.toBe(input);
  });
});
