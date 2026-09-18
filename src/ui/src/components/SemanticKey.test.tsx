import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AttributeHit } from "../api/gen";
import { semanticsFromResolution } from "../lib/semantics";
import { SemanticInfo, SemanticKeyLabel } from "./SemanticKey";

afterEach(() => vi.unstubAllGlobals());

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

describe("SemanticKeyLabel", () => {
  it("renders the bare key when the registry does not know it", () => {
    const { container } = render(
      <dt>
        <SemanticKeyLabel name="app.order.id" semantics={undefined} />
      </dt>,
    );
    expect(container.querySelector("dt")!.textContent).toBe("app.order.id");
  });

  it("marks a deprecated key with its replacement", () => {
    render(
      <SemanticKeyLabel
        name="http.status_code"
        semantics={semOf([
          hit({
            key: "http.status_code",
            deprecated: { renamed_to: "http.response.status_code" },
          }),
        ])}
      />,
    );
    const struck = screen.getByText("http.status_code", { selector: "s" });
    expect(struck.closest(".semkey-name")).not.toBeNull();
    expect(
      screen.getByText("→ http.response.status_code"),
    ).toBeInTheDocument();
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
      <SemanticKeyLabel name="service.name" semantics={semOf([custom, otel])} />,
    );
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();

    const label = screen.getByText("service.name", {
      selector: ".semkey-name",
    });
    await userEvent.hover(label);
    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("Our service registry name.");
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
        <SemanticKeyLabel name="service.name" semantics={semOf([custom, otel])} />
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
    render(<SemanticKeyLabel name="k8s.pod.uid" semantics={semOf([hit()])} />);
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

describe("SemanticInfo tooltip placement", () => {
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

  it("flips above the trigger near the bottom edge and bridges the gap on that side", async () => {
    render(<SemanticInfo name="k8s.pod.uid" semantics={semOf([hit()])} />);
    const glyph = screen.getByLabelText("About k8s.pod.uid");
    const trigger = glyph.parentElement as HTMLElement;
    trigger.getBoundingClientRect = () =>
      ({
        left: 40,
        right: 60,
        top: window.innerHeight - 20,
        bottom: window.innerHeight - 4,
        width: 20,
        height: 16,
      }) as DOMRect;

    await userEvent.hover(glyph);
    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveAttribute("data-placement", "above");
    expect(tip.style.bottom).not.toBe("");
    expect(tip.style.top).toBe("");
    // The bridge sits on the trigger-facing (bottom) side of the tip, not
    // the top, which is where it sat when the tip was always below.
    const bridge = tip.firstElementChild as HTMLElement;
    expect(bridge).toHaveAttribute("aria-hidden", "true");
    expect(bridge.style.bottom).not.toBe("");
    expect(bridge.style.top).toBe("");
  });

  it("clamps the tooltip's left edge to stay on screen on a narrow viewport", async () => {
    vi.stubGlobal("innerWidth", 320);
    render(<SemanticInfo name="k8s.pod.uid" semantics={semOf([hit()])} />);
    const glyph = screen.getByLabelText("About k8s.pod.uid");
    const trigger = glyph.parentElement as HTMLElement;
    // Near the left edge, but right-alignment still kicks in because the
    // tip's max-width alone exceeds this viewport.
    trigger.getBoundingClientRect = () =>
      ({ left: 10, right: 30, top: 100, bottom: 116, width: 20, height: 16 }) as DOMRect;

    await userEvent.hover(glyph);
    const tip = await screen.findByRole("tooltip");
    expect(parseFloat(tip.style.left)).toBeGreaterThanOrEqual(8);
  });

  it("caps the tooltip's own width to the viewport on a narrow screen", async () => {
    // The clamped `left` alone doesn't stop the tip's own 360px CSS
    // max-width from running past a viewport narrower than that.
    vi.stubGlobal("innerWidth", 320);
    render(<SemanticInfo name="k8s.pod.uid" semantics={semOf([hit()])} />);
    const glyph = screen.getByLabelText("About k8s.pod.uid");
    const trigger = glyph.parentElement as HTMLElement;
    trigger.getBoundingClientRect = () =>
      ({ left: 10, right: 30, top: 100, bottom: 116, width: 20, height: 16 }) as DOMRect;

    await userEvent.hover(glyph);
    const tip = await screen.findByRole("tooltip");
    expect(tip.style.maxWidth).toBe("calc(100vw - 16px)");
  });

  it("keeps the tooltip open while focus tabs into its links, closing once focus leaves both", async () => {
    const custom = hit({
      key: "service.name",
      namespace: "acme",
      version: "1.0.0",
      source: "custom",
    });
    const otel = hit({ key: "service.name" });
    render(
      <MemoryRouter>
        <SemanticKeyLabel name="service.name" semantics={semOf([custom, otel])} />
      </MemoryRouter>,
    );
    const trigger = screen.getByText("service.name", {
      selector: ".semkey-name",
    });
    trigger.focus();
    const tip = await screen.findByRole("tooltip");
    const link = within(tip).getByRole("link", { name: "acme@1.0.0" });

    await userEvent.tab();
    expect(link).toHaveFocus();
    expect(screen.getByRole("tooltip")).toBeInTheDocument();

    // Focus leaving the tooltip (and the trigger) for anywhere else closes it.
    fireEvent.focusOut(link, { relatedTarget: document.body });
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });
});

describe("SemanticKeyLabel tooltip keyboard access", () => {
  function renderTrigger() {
    render(
      <MemoryRouter>
        <SemanticKeyLabel
          name="service.name"
          semantics={semOf([hit({ key: "service.name" })])}
        />
      </MemoryRouter>,
    );
    return screen.getByText("service.name", { selector: ".semkey-name" });
  }

  it("shows the tooltip when the trigger receives focus", async () => {
    const trigger = renderTrigger();
    trigger.focus();
    expect(await screen.findByRole("tooltip")).toBeInTheDocument();
  });

  it("hides the tooltip on Escape without letting the row/drawer also react", async () => {
    const trigger = renderTrigger();
    const onKeyDown = vi.fn();
    document.body.addEventListener("keydown", onKeyDown);
    trigger.focus();
    await screen.findByRole("tooltip");

    fireEvent.keyDown(trigger, { key: "Escape" });
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(onKeyDown).not.toHaveBeenCalled();
    document.body.removeEventListener("keydown", onKeyDown);
  });

  it("moves focus to the tooltip's first link on Tab, keeping the tooltip open", async () => {
    const trigger = renderTrigger();
    trigger.focus();
    const tip = await screen.findByRole("tooltip");

    await userEvent.tab();
    const link = within(tip).getByRole("link", { name: "otel@1.43.0" });
    expect(link).toHaveFocus();
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
  });

  it("returns focus to the trigger on Shift+Tab from the tooltip's first link", async () => {
    const trigger = renderTrigger();
    trigger.focus();
    await screen.findByRole("tooltip");
    await userEvent.tab();

    await userEvent.tab({ shift: true });
    expect(trigger).toHaveFocus();
  });
});
