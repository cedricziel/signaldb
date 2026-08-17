import { render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { AttributeHit } from "../api/gen";
import { semanticsFromResolution } from "../lib/semantics";
import { SemanticInfo, SemanticKey } from "./SemanticKey";

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
  examples: ["275ecb36-5aa8-4c2a-9c47-d8bb681b9aff"],
  ...over,
});

const semOf = (hits: AttributeHit[]) =>
  semanticsFromResolution({ key: hits[0]!.key, hits, primary: hits[0] });

describe("SemanticKey", () => {
  it("renders the bare key when the registry does not know it", () => {
    const { container } = render(
      <dt>
        <SemanticKey name="app.order.id" semantics={undefined} />
      </dt>,
    );
    expect(container.querySelector("dt")!.innerHTML).toBe("app.order.id");
  });

  it("shows key, brief, title and namespace tag for a registered key", () => {
    render(
      <SemanticKey name="k8s.pod.uid" semantics={semOf([hit()])} showTitle />,
    );
    expect(screen.getByText("k8s.pod.uid")).toBeInTheDocument();
    expect(screen.getByText("The UID of the Pod.")).toBeInTheDocument();
    expect(screen.getByText("Kubernetes Attributes")).toBeInTheDocument();
    expect(screen.getByText("otel")).toBeInTheDocument();
  });

  it("marks a deprecated key with its replacement", () => {
    render(
      <SemanticKey
        name="http.status_code"
        semantics={semOf([
          hit({
            key: "http.status_code",
            deprecated: { renamed_to: "http.response.status_code" },
          }),
        ])}
      />,
    );
    expect(
      screen.getByText("⚠ deprecated → http.response.status_code"),
    ).toBeInTheDocument();
  });

  it("shows entity roles with the identifying/descriptive glyphs", () => {
    render(
      <SemanticKey
        name="k8s.pod.uid"
        semantics={semOf([
          hit({
            entity_roles: [
              { namespace: "otel", entity: "k8s.pod", role: "identifying" },
            ],
          }),
        ])}
      />,
    );
    expect(screen.getByText("◆ identifying · k8s.pod")).toBeInTheDocument();
  });

  it("tags the tenant's definition as primary and offers otel in the tooltip", async () => {
    const custom = hit({
      key: "service.name",
      brief: "Our service registry name.",
      namespace: "acme",
      version: "1.0.0",
      source: "custom",
      group_display_name: "Acme Service",
    });
    const otel = hit({
      key: "service.name",
      brief: "Logical name of the service.",
    });
    render(
      <SemanticKey name="service.name" semantics={semOf([custom, otel])} />,
    );
    expect(screen.getByText("Our service registry name.")).toBeInTheDocument();
    expect(screen.getByText("acme")).toBeInTheDocument();
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();

    const label = screen.getByText("service.name", {
      selector: ".semkey-name",
    });
    await userEvent.hover(label);
    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("acme@1.0.0");
    expect(tip).toHaveTextContent("Also defined in: otel@1.43.0");
    expect(tip).toHaveTextContent("Acme Service · string · development");
    expect(tip).toHaveTextContent("e.g. 275ecb36-5aa8-4c2a-9c47-d8bb681b9aff");

    await userEvent.unhover(label);
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("links every registry mention in the tooltip to its definition page in the schema hub", async () => {
    const custom = hit({
      key: "service.name",
      namespace: "acme",
      version: "1.0.0",
      source: "custom",
      entity_roles: [
        { namespace: "otel", entity: "service", role: "identifying" },
      ],
    });
    const otel = hit({ key: "service.name" });
    render(
      <MemoryRouter>
        <SemanticKey name="service.name" semantics={semOf([custom, otel])} />
      </MemoryRouter>,
    );
    await userEvent.hover(
      screen.getByText("service.name", { selector: ".semkey-name" }),
    );
    const tip = await screen.findByRole("tooltip");
    expect(
      within(tip).getByRole("link", { name: "acme@1.0.0" }),
    ).toHaveAttribute(
      "href",
      "/schema/conventions/acme/1.0.0/attributes/service.name",
    );
    expect(
      within(tip).getByRole("link", { name: "otel@1.43.0" }),
    ).toHaveAttribute(
      "href",
      "/schema/conventions/otel/1.43.0/attributes/service.name",
    );
    expect(within(tip).getByRole("link", { name: "service" })).toHaveAttribute(
      "href",
      "/schema/conventions/otel/latest/entities/service",
    );
  });

  it("degrades registry mentions to plain anchors outside a router", async () => {
    render(<SemanticKey name="k8s.pod.uid" semantics={semOf([hit()])} />);
    await userEvent.hover(
      screen.getByText("k8s.pod.uid", { selector: ".semkey-name" }),
    );
    const tip = await screen.findByRole("tooltip");
    expect(
      within(tip).getByRole("link", { name: "otel@1.43.0" }),
    ).toHaveAttribute(
      "href",
      "/schema/conventions/otel/1.43.0/attributes/k8s.pod.uid",
    );
  });
});

describe("SemanticInfo", () => {
  it("renders nothing for an unknown key", () => {
    const { container } = render(
      <SemanticInfo name="level" semantics={undefined} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("opens the tooltip on focus for a known key", async () => {
    render(<SemanticInfo name="k8s.pod.uid" semantics={semOf([hit()])} />);
    const glyph = screen.getByLabelText("About k8s.pod.uid");
    await userEvent.hover(glyph);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "The UID of the Pod.",
    );
  });
});

describe("SemanticKey tooltip placement", () => {
  it("renders the tooltip in a body portal, positioned from the trigger", async () => {
    // Facet sidebars and attribute tables scroll (`overflow: auto`), which
    // would clip a tooltip positioned inside them at the pane edge — so the
    // tooltip escapes to <body> and is placed with fixed coordinates.
    const { container } = render(
      <SemanticInfo name="k8s.pod.uid" semantics={semOf([hit()])} />,
    );
    const glyph = screen.getByLabelText("About k8s.pod.uid");
    const trigger = glyph.parentElement as HTMLElement;
    trigger.getBoundingClientRect = () =>
      ({
        left: 40,
        right: 60,
        top: 100,
        bottom: 116,
        width: 20,
        height: 16,
      }) as DOMRect;

    await userEvent.hover(glyph);
    const tip = await screen.findByRole("tooltip");
    expect(container.contains(tip)).toBe(false);
    expect(tip.parentElement).toBe(document.body);
    expect(tip.style.position).toBe("fixed");
    expect(tip.style.left).toBe("40px");
    expect(tip.style.top).toBe("120px");
  });

  it("stays open while the pointer moves from the trigger into the tooltip", async () => {
    render(<SemanticInfo name="k8s.pod.uid" semantics={semOf([hit()])} />);
    const glyph = screen.getByLabelText("About k8s.pod.uid");
    await userEvent.hover(glyph);
    const tip = await screen.findByRole("tooltip");

    await userEvent.hover(tip);
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    await userEvent.unhover(tip);
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });
});
