import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { markDirty, resetDirtyForms } from "./dirtyForms";
import {
  applyPendingUpdate,
  getUpdateState,
  maybeAutoApplyUpdate,
  resetUpdateState,
  schedulePeriodicUpdateCheck,
  setUpdateAvailable,
  subscribeUpdateState,
} from "./pwaUpdate";

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

describe("update-available store", () => {
  afterEach(() => {
    resetUpdateState();
    resetDirtyForms();
  });

  it("starts with no update pending", () => {
    expect(getUpdateState().updateSW).toBeNull();
  });

  it("records the updateSW callback and notifies subscribers", () => {
    const listener = vi.fn();
    const unsubscribe = subscribeUpdateState(listener);
    const updateSW = vi.fn();

    setUpdateAvailable(updateSW);

    expect(getUpdateState().updateSW).toBe(updateSW);
    expect(listener).toHaveBeenCalledTimes(1);
    unsubscribe();
  });

  describe("applyPendingUpdate", () => {
    it("invokes the stored callback with reloadPage: true and clears the pending state", () => {
      const updateSW = vi.fn().mockResolvedValue(undefined);
      setUpdateAvailable(updateSW);

      applyPendingUpdate();

      expect(updateSW).toHaveBeenCalledWith(true);
      expect(getUpdateState().updateSW).toBeNull();
    });

    it("is a no-op when no update is pending", () => {
      expect(() => applyPendingUpdate()).not.toThrow();
    });
  });

  describe("maybeAutoApplyUpdate", () => {
    it("applies the pending update when no form is dirty", () => {
      const updateSW = vi.fn().mockResolvedValue(undefined);
      setUpdateAvailable(updateSW);

      maybeAutoApplyUpdate();

      expect(updateSW).toHaveBeenCalledWith(true);
      expect(getUpdateState().updateSW).toBeNull();
    });

    it("never applies the update while a form is dirty", () => {
      const updateSW = vi.fn().mockResolvedValue(undefined);
      setUpdateAvailable(updateSW);
      markDirty("some-form", true);

      maybeAutoApplyUpdate();

      expect(updateSW).not.toHaveBeenCalled();
      expect(getUpdateState().updateSW).toBe(updateSW);

      markDirty("some-form", false);
    });

    it("does nothing when no update is pending, dirty or not", () => {
      expect(() => maybeAutoApplyUpdate()).not.toThrow();
    });
  });
});
