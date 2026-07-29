import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { App } from "./App";

describe("App", () => {
  it("renders the shell with the product mark", () => {
    render(<App />);
    expect(screen.getByRole("banner")).toHaveTextContent(/signaldb/i);
  });
});
