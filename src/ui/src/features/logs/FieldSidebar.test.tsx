import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resetSemanticsCache } from "../../hooks/useSemantics";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { FieldSidebar } from "./FieldSidebar";

const RANGE = { fromMs: 0, toMs: 1000 };

afterEach(() => {
  vi.unstubAllGlobals();
  resetSemanticsCache();
});

const POD_UID = {
  key: "k8s.pod.uid",
  brief: "The UID of the Pod.",
  type: "string",
  group_id: "registry.k8s.pod",
  group_display_name: "Kubernetes Attributes",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
};

const NODE_NAME = {
  key: "k8s.node.name",
  brief: "The name of the Node.",
  type: "string",
  group_id: "registry.k8s.node",
  group_display_name: "Kubernetes Attributes",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
};

const CLOUD_REGION = {
  key: "cloud.region",
  brief: "The cloud region.",
  type: "string",
  group_id: "registry.cloud",
  group_display_name: "Cloud Attributes",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
};

const OLD_KEY = {
  key: "old.key",
  brief: "A deprecated key.",
  type: "string",
  group_id: "registry.old",
  group_display_name: "Cloud Attributes",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
  deprecated: { renamed_to: "cloud.region" },
};

/** Stub `/schema/attributes` resolving each key in `known` to its fixture,
 * and any other requested key to "unknown". */
function stubResolve(known: Record<string, unknown>) {
  return stubFetchRoutes([
    {
      match: "/api/v1/schema/attributes",
      body: {
        hits: [],
        resolutions: Object.entries(known).map(([key, hit]) => ({
          key,
          hits: [hit],
          primary: hit,
        })),
      },
    },
  ]);
}

describe("FieldSidebar", () => {
  it("lists labels and filters them by search text", async () => {
    renderWithClient(
      <FieldSidebar
        labels={["service_name", "level", "http_method"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    expect(screen.getByText("service_name")).toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("Filter fields"), "lev");
    expect(screen.queryByText("service_name")).not.toBeInTheDocument();
    expect(screen.getByText("level")).toBeInTheDocument();
  });

  it("loads values on expand and adds a filter on click", async () => {
    stubFetchRoutes([
      {
        match: "/loki/api/v1/label/level/values",
        body: { status: "success", data: ["error", "info"] },
      },
    ]);
    const onAddFilter = vi.fn();
    renderWithClient(
      <FieldSidebar
        labels={["level"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={onAddFilter}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "level" }));
    await userEvent.click(await screen.findByRole("button", { name: "error" }));
    expect(onAddFilter).toHaveBeenCalledWith({
      label: "level",
      op: "=",
      value: "error",
    });
  });

  it("shows a failure note when values cannot load", async () => {
    stubFetchRoutes([
      { match: "/values", body: { error: "boom" }, status: 500 },
    ]);
    renderWithClient(
      <FieldSidebar
        labels={["level"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "level" }));
    expect(
      await screen.findByText("Could not load values"),
    ).toBeInTheDocument();
  });

  it("adds an info glyph with the registry tooltip to fields it knows", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: {
          hits: [],
          resolutions: [
            { key: "k8s.pod.uid", hits: [POD_UID], primary: POD_UID },
            { key: "level", hits: [] },
          ],
        },
      },
    ]);
    renderWithClient(
      <FieldSidebar
        labels={["k8s.pod.uid", "level"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    const info = await screen.findByLabelText("About k8s.pod.uid");
    expect(screen.queryByLabelText("About level")).not.toBeInTheDocument();
    // The toggle button keeps its plain accessible name.
    expect(
      screen.getByRole("button", { name: "k8s.pod.uid" }),
    ).toBeInTheDocument();
    await userEvent.hover(info);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "The UID of the Pod.",
    );
  });

  it("renders no group headers when semantics never resolve", async () => {
    renderWithClient(
      <FieldSidebar
        labels={["b_field", "a_field"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    expect(screen.getByText("a_field")).toBeInTheDocument();
    expect(screen.getByText("b_field")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /Kubernetes|Cloud|Other/ }),
    ).not.toBeInTheDocument();
  });

  it("groups fields with titles and counts once semantics resolve", async () => {
    stubResolve({
      "k8s.pod.uid": POD_UID,
      "k8s.node.name": NODE_NAME,
      "cloud.region": CLOUD_REGION,
    });
    renderWithClient(
      <FieldSidebar
        labels={["k8s.pod.uid", "k8s.node.name", "cloud.region", "level"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    const kubernetesHead = await screen.findByRole("button", {
      name: /Kubernetes/,
    });
    expect(kubernetesHead).toHaveTextContent("2");
    const cloudHead = screen.getByRole("button", { name: /Cloud/ });
    expect(cloudHead).toHaveTextContent("1");
    expect(screen.getByRole("button", { name: /^Line/ })).toHaveTextContent(
      "1",
    );
    expect(
      screen.getByRole("button", { name: "k8s.pod.uid" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "k8s.node.name" }),
    ).toBeInTheDocument();
  });

  it("collapsing a group hides its fields", async () => {
    stubResolve({ "cloud.region": CLOUD_REGION });
    renderWithClient(
      <FieldSidebar
        labels={["cloud.region"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    const cloudHead = await screen.findByRole("button", { name: /Cloud/ });
    expect(
      screen.getByRole("button", { name: "cloud.region" }),
    ).toBeInTheDocument();
    expect(cloudHead).toHaveAttribute("aria-expanded", "true");
    await userEvent.click(cloudHead);
    expect(cloudHead).toHaveAttribute("aria-expanded", "false");
    expect(
      screen.queryByRole("button", { name: "cloud.region" }),
    ).not.toBeInTheDocument();
  });

  it("filter text hides groups with no matching label, and expands the rest", async () => {
    stubResolve({
      "k8s.pod.uid": POD_UID,
      "cloud.region": CLOUD_REGION,
    });
    renderWithClient(
      <FieldSidebar
        labels={["k8s.pod.uid", "cloud.region"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    await screen.findByRole("button", { name: /Kubernetes/ });
    await userEvent.type(screen.getByLabelText("Filter fields"), "pod");
    expect(
      screen.getByRole("button", { name: "k8s.pod.uid" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /Cloud/ }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "cloud.region" }),
    ).not.toBeInTheDocument();
  });

  it("shows a deprecated key struck through with its replacement", async () => {
    stubResolve({
      "cloud.region": CLOUD_REGION,
      "old.key": OLD_KEY,
    });
    renderWithClient(
      <FieldSidebar
        labels={["cloud.region", "old.key"]}
        range={RANGE}
        rangeKey="1h"
        onAddFilter={() => {}}
      />,
    );
    const deprecatedHead = await screen.findByRole("button", {
      name: /Deprecated/,
    });
    expect(deprecatedHead).toHaveTextContent("1");
    expect(screen.getByText("old.key").tagName).toBe("S");
    expect(screen.getByText("→ cloud.region")).toBeInTheDocument();
  });
});
