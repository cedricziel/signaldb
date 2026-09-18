import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { AttributeHit } from "../api/gen";
import { semanticsFromResolution, type SemanticsMap } from "../lib/semantics";
import { AttributeTable } from "./AttributeTable";

const hit = (over: Partial<AttributeHit> = {}): AttributeHit => ({
  key: "k8s.pod.uid",
  brief: "The UID of the Pod.",
  type: "string",
  group_id: "registry.k8s.pod",
  group_display_name: "Kubernetes Attributes",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
  stability: "development",
  ...over,
});

function semanticsOf(entries: [string, AttributeHit[]][]): SemanticsMap {
  const map = new Map();
  for (const [key, hits] of entries) {
    const sem = semanticsFromResolution({ key, hits, primary: hits[0] });
    if (sem) map.set(key, sem);
  }
  return map;
}

describe("AttributeTable", () => {
  it("shows only the key and value for a resolved row — no brief, no namespace chip on the row", () => {
    const semantics = semanticsOf([["k8s.pod.uid", [hit()]]]);
    render(
      <AttributeTable
        entries={[["k8s.pod.uid", "275ecb36"]]}
        semantics={semantics}
        layout="grid"
        showDescriptions={false}
      />,
    );
    const row = screen
      .getByText("275ecb36")
      .closest<HTMLElement>(".attrtable-row")!;
    expect(within(row).queryByText("The UID of the Pod.")).toBeNull();
    expect(within(row).queryByText("otel")).toBeNull();
  });

  it("states the group's namespace and entity once on the heading", () => {
    const semantics = semanticsOf([
      [
        "k8s.pod.uid",
        [
          hit({
            entity_roles: [
              { namespace: "otel", entity: "k8s.pod", role: "identifying" },
            ],
          }),
        ],
      ],
      [
        "k8s.pod.name",
        [
          hit({
            key: "k8s.pod.name",
            entity_roles: [
              { namespace: "otel", entity: "k8s.pod", role: "identifying" },
            ],
          }),
        ],
      ],
    ]);
    render(
      <AttributeTable
        entries={[
          ["k8s.pod.name", "web-1"],
          ["k8s.pod.uid", "275ecb36"],
        ]}
        semantics={semantics}
        layout="grid"
        showDescriptions={false}
      />,
    );
    const heading = screen
      .getByText("Kubernetes Attributes")
      .closest<HTMLElement>(".attrtable-group")!;
    expect(within(heading).getByText("otel")).toBeInTheDocument();
    expect(within(heading).getByText("◆ k8s.pod")).toBeInTheDocument();
    // Stated once for the whole group, not per row.
    expect(screen.getAllByText("otel")).toHaveLength(1);
  });

  it("strikes a deprecated key and shows its replacement inline", () => {
    const semantics = semanticsOf([
      [
        "http.status_code",
        [
          hit({
            key: "http.status_code",
            deprecated: { renamed_to: "http.response.status_code" },
          }),
        ],
      ],
    ]);
    render(
      <AttributeTable
        entries={[["http.status_code", "200"]]}
        semantics={semantics}
        layout="grid"
        showDescriptions={false}
      />,
    );
    const key = screen.getByText("http.status_code", { selector: "s" });
    expect(key.closest(".semkey-name")).not.toBeNull();
    expect(screen.getByText("→ http.response.status_code")).toBeInTheDocument();
  });

  it("adds the plain-text brief only when showDescriptions is on", () => {
    const semantics = semanticsOf([["k8s.pod.uid", [hit()]]]);
    const { rerender } = render(
      <AttributeTable
        entries={[["k8s.pod.uid", "275ecb36"]]}
        semantics={semantics}
        layout="grid"
        showDescriptions={false}
      />,
    );
    expect(screen.queryByText(/The UID of the Pod\./)).toBeNull();
    rerender(
      <AttributeTable
        entries={[["k8s.pod.uid", "275ecb36"]]}
        semantics={semantics}
        layout="grid"
        showDescriptions
      />,
    );
    expect(screen.getByText(/The UID of the Pod\./)).toBeInTheDocument();
  });

  it("renders and fires per-row actions", async () => {
    const onClick = vi.fn();
    render(
      <AttributeTable
        entries={[["service.name", "checkout"]]}
        semantics={new Map()}
        layout="grid"
        showDescriptions={false}
        actions={(key, value) => [
          {
            label: "+ filter",
            ariaLabel: `Filter for ${key} = ${value}`,
            onClick,
          },
        ]}
      />,
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Filter for service.name = checkout" }),
    );
    expect(onClick).toHaveBeenCalled();
  });

  it("renders no group heading when nothing in the input resolved", () => {
    const { container } = render(
      <AttributeTable
        entries={[["app.order.id", "o-1"]]}
        semantics={new Map()}
        layout="grid"
        showDescriptions={false}
      />,
    );
    expect(container.querySelector(".attrtable-group")).toBeNull();
    expect(screen.getByText("app.order.id")).toBeInTheDocument();
  });

  it("states no namespace or entity on the Other group", () => {
    // Two Kubernetes keys keep their heading; the lone host key folds into
    // Other next to an unknown key, so nothing is shared across that group.
    const semantics = semanticsOf([
      ["k8s.pod.uid", [hit()]],
      ["k8s.pod.name", [hit({ key: "k8s.pod.name" })]],
      [
        "host.name",
        [
          hit({
            key: "host.name",
            group_id: "registry.host",
            group_display_name: "Host Attributes",
            entity_roles: [{ namespace: "otel", entity: "host", role: "identifying" }],
          }),
        ],
      ],
    ]);
    const { container } = render(
      <AttributeTable
        entries={[
          ["app.order.id", "o-1"],
          ["host.name", "web-1"],
          ["k8s.pod.name", "pod-a"],
          ["k8s.pod.uid", "uid-a"],
        ]}
        semantics={semantics}
        layout="grid"
        showDescriptions={false}
      />,
    );
    const headings = [...container.querySelectorAll(".attrtable-group")];
    expect(headings.map((h) => h.querySelector(".attrtable-title")?.textContent)).toEqual([
      "Kubernetes Attributes",
      "Other",
    ]);
    const other = headings[1]!;
    expect(other.querySelector(".attrtable-ns")).toBeNull();
    expect(other.querySelector(".attrtable-entity")).toBeNull();
  });

  it("states no namespace on the heading when the group's resolved rows disagree", () => {
    // Same title (so they land in the same group), different definitions —
    // a tenant override for one key of the pair, the bundled otel one for
    // the other. The heading must not assert a shared source neither row
    // alone stands for.
    const semantics = semanticsOf([
      ["k8s.pod.uid", [hit()]],
      [
        "k8s.pod.name",
        [
          hit({
            key: "k8s.pod.name",
            namespace: "acme",
            source: "custom",
          }),
        ],
      ],
    ]);
    render(
      <AttributeTable
        entries={[
          ["k8s.pod.name", "web-1"],
          ["k8s.pod.uid", "275ecb36"],
        ]}
        semantics={semantics}
        layout="grid"
        showDescriptions={false}
      />,
    );
    const heading = screen
      .getByText("Kubernetes Attributes")
      .closest<HTMLElement>(".attrtable-group")!;
    expect(within(heading).queryByText("otel")).toBeNull();
    expect(within(heading).queryByText("acme")).toBeNull();
  });

  it("shows the descriptive glyph when not every resolved row identifies the shared entity", () => {
    const semantics = semanticsOf([
      [
        "k8s.pod.uid",
        [
          hit({
            entity_roles: [
              { namespace: "otel", entity: "k8s.pod", role: "identifying" },
            ],
          }),
        ],
      ],
      [
        "k8s.pod.start_time",
        [
          hit({
            key: "k8s.pod.start_time",
            entity_roles: [
              { namespace: "otel", entity: "k8s.pod", role: "descriptive" },
            ],
          }),
        ],
      ],
    ]);
    render(
      <AttributeTable
        entries={[
          ["k8s.pod.start_time", "2024-01-01T00:00:00Z"],
          ["k8s.pod.uid", "275ecb36"],
        ]}
        semantics={semantics}
        layout="grid"
        showDescriptions={false}
      />,
    );
    const heading = screen
      .getByText("Kubernetes Attributes")
      .closest<HTMLElement>(".attrtable-group")!;
    expect(within(heading).getByText("○ k8s.pod")).toBeInTheDocument();
    expect(within(heading).queryByText("◆ k8s.pod")).toBeNull();
  });

  it("keys each action button by its (unique) aria-label rather than its (possibly shared) visible label", async () => {
    render(
      <AttributeTable
        entries={[["service.name", "checkout"]]}
        semantics={new Map()}
        layout="grid"
        showDescriptions={false}
        actions={(key, value) => [
          { label: "↗", ariaLabel: `Traces with ${key} = ${value}`, onClick: () => {} },
          { label: "↗", ariaLabel: `Open service ${value} in the catalog`, onClick: () => {} },
        ]}
      />,
    );
    expect(
      screen.getByRole("button", { name: `Traces with service.name = checkout` }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Open service checkout in the catalog" }),
    ).toBeInTheDocument();
  });

  it("wraps each group's rows in its own <dl>, headed by a heading div the <dl> is aria-labelledby", () => {
    const semantics = semanticsOf([
      ["k8s.pod.uid", [hit()]],
      ["k8s.pod.name", [hit({ key: "k8s.pod.name" })]],
    ]);
    const { container } = render(
      <AttributeTable
        entries={[
          ["k8s.pod.name", "web-1"],
          ["k8s.pod.uid", "275ecb36"],
        ]}
        semantics={semantics}
        layout="grid"
        showDescriptions={false}
      />,
    );
    const heading = container.querySelector(".attrtable-group")!;
    const list = container.querySelector("dl.attrtable-list")!;
    expect(heading.id).not.toBe("");
    expect(list).toHaveAttribute("aria-labelledby", heading.id);
  });

  it("gives the untitled group's <dl> no aria-labelledby", () => {
    const { container } = render(
      <AttributeTable
        entries={[["app.order.id", "o-1"]]}
        semantics={new Map()}
        layout="grid"
        showDescriptions={false}
      />,
    );
    const list = container.querySelector("dl.attrtable-list")!;
    expect(list).not.toHaveAttribute("aria-labelledby");
  });

  it("inserts a <wbr> after each dot in a dotted key", () => {
    const { container } = render(
      <AttributeTable
        entries={[["cloud.region", "us-east-1"]]}
        semantics={new Map()}
        layout="grid"
        showDescriptions={false}
      />,
    );
    expect(container.querySelector("dt wbr")).not.toBeNull();
  });
});
