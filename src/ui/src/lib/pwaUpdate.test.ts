import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { schedulePeriodicUpdateCheck } from "./pwaUpdate";

function setVisibility(state: DocumentVisibilityState) {
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    get: () => state,
  });
}

describe("schedulePeriodicUpdateCheck", () => {
  let registration: ServiceWorkerRegistration;

  beforeEach(() => {
    vi.useFakeTimers();
    registration = { update: vi.fn() } as unknown as ServiceWorkerRegistration;
    setVisibility("visible");
  });

  afterEach(() => {
    vi.useRealTimers();
    setVisibility("visible");
  });

  it("polls registration.update() on the given interval while visible", () => {
    schedulePeriodicUpdateCheck(registration, 1000);

    expect(registration.update).not.toHaveBeenCalled();

    vi.advanceTimersByTime(3000);

    expect(registration.update).toHaveBeenCalledTimes(3);
  });

  it("defaults to an hourly interval", () => {
    schedulePeriodicUpdateCheck(registration);

    vi.advanceTimersByTime(60 * 60 * 1000);

    expect(registration.update).toHaveBeenCalledTimes(1);
  });

  it("skips the check while the tab is hidden", () => {
    setVisibility("hidden");

    schedulePeriodicUpdateCheck(registration, 1000);
    vi.advanceTimersByTime(3000);

    expect(registration.update).not.toHaveBeenCalled();
  });

  it("checks immediately on regaining visibility once the interval has elapsed", () => {
    setVisibility("hidden");
    schedulePeriodicUpdateCheck(registration, 1000);
    vi.advanceTimersByTime(5000);
    expect(registration.update).not.toHaveBeenCalled();

    setVisibility("visible");
    document.dispatchEvent(new Event("visibilitychange"));

    expect(registration.update).toHaveBeenCalledTimes(1);
  });

  it("returns a disposer that stops further checks", () => {
    const stop = schedulePeriodicUpdateCheck(registration, 1000);

    stop();
    vi.advanceTimersByTime(5000);
    document.dispatchEvent(new Event("visibilitychange"));

    expect(registration.update).not.toHaveBeenCalled();
  });
});
