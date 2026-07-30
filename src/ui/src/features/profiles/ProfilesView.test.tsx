import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { ProfilesView } from "./ProfilesView";

afterEach(() => {
  vi.unstubAllGlobals();
});

const TYPES = [
  {
    ID: "cpu:nanoseconds",
    name: "cpu",
    sampleType: "cpu",
    sampleUnit: "nanoseconds",
  },
];
const SERVICES = { names: ["signaldb-router", "signaldb-querier"] };
const RENDER = {
  flamebearer: {
    names: ["total", "main", "work"],
    levels: [
      [0, 100, 0, 0],
      [0, 100, 20, 1],
      [0, 80, 80, 2],
    ],
    numTicks: 100,
    maxSelf: 80,
  },
  metadata: {
    format: "single",
    sampleRate: 100,
    units: "samples",
    name: "cpu",
  },
};

function state(overrides: Partial<ExploreState> = {}): ExploreState {
  return { ...DEFAULT_STATE, signal: "profiles", ...overrides };
}

describe("ProfilesView", () => {
  it("populates selectors and renders a flame graph", async () => {
    stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: TYPES },
      { match: "/pyroscope/label-values", body: SERVICES },
      { match: "/pyroscope/render", body: RENDER },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);

    // Service and profile-type selectors fill from the discovery endpoints.
    expect(
      await screen.findByRole("option", { name: "signaldb-router" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "cpu · nanoseconds" }),
    ).toBeInTheDocument();

    // Flame frames render with their names.
    expect(await screen.findByText("main")).toBeInTheDocument();
    expect(screen.getByText("work")).toBeInTheDocument();
  });

  it("zooms into a frame on click and back via reset", async () => {
    stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: TYPES },
      { match: "/pyroscope/label-values", body: SERVICES },
      { match: "/pyroscope/render", body: RENDER },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);

    const work = await screen.findByRole("button", { name: /work/ });
    await userEvent.click(work);

    // Breadcrumb now leads to the focused frame and offers a reset.
    const crumbs = screen.getByText("reset zoom");
    expect(crumbs).toBeInTheDocument();
    await userEvent.click(crumbs);
    expect(screen.queryByText("reset zoom")).not.toBeInTheDocument();
  });

  it("updates URL state when a service is chosen", async () => {
    stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: TYPES },
      { match: "/pyroscope/label-values", body: SERVICES },
      { match: "/pyroscope/render", body: RENDER },
    ]);
    const update = vi.fn();

    renderWithClient(<ProfilesView state={state()} update={update} />);
    await screen.findByRole("option", { name: "signaldb-router" });

    await userEvent.selectOptions(
      screen.getByLabelText("Profile service"),
      "signaldb-querier",
    );
    expect(update).toHaveBeenCalledWith({ profileService: "signaldb-querier" });
  });

  it("highlights matching frames and reports the matched share", async () => {
    stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: TYPES },
      { match: "/pyroscope/label-values", body: SERVICES },
      { match: "/pyroscope/render", body: RENDER },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
    await screen.findByText("work");

    await userEvent.type(screen.getByLabelText("Highlight frames"), "work");

    // "work" (self 80 of 100 ticks) is the only match.
    expect(screen.getByText(/80\.0% matched/)).toBeInTheDocument();
    // Non-matching frames are dimmed; the match is not.
    expect(screen.getByRole("button", { name: /main/ })).toHaveClass("dim");
    expect(screen.getByRole("button", { name: /work/ })).not.toHaveClass("dim");
  });

  it("shows an empty state when the profile has no frames", async () => {
    stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: TYPES },
      { match: "/pyroscope/label-values", body: SERVICES },
      {
        match: "/pyroscope/render",
        body: {
          flamebearer: {
            names: ["total"],
            levels: [[0, 0, 0, 0]],
            numTicks: 0,
            maxSelf: 0,
          },
          metadata: {
            format: "single",
            sampleRate: 100,
            units: "samples",
            name: "cpu",
          },
        },
      },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
    expect(
      await screen.findByText(/No profiles in the selected range for this/),
    ).toBeInTheDocument();
  });

  it("prompts to enable self-profiling when no types exist", async () => {
    stubFetchRoutes([
      { match: "/pyroscope/profile-types", body: [] },
      { match: "/pyroscope/label-values", body: { names: [] } },
    ]);

    renderWithClient(<ProfilesView state={state()} update={vi.fn()} />);
    expect(await screen.findByText(/profiles_enabled/)).toBeInTheDocument();
  });
});
