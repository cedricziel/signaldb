// Click events via the SDK's `UserActionInstrumentation`, retargeting clicks
// on SVG icons to their closest HTMLElement ancestor. SDK 0.7's `onClick`
// drops any target that isn't an `HTMLElement`, and it is `private` with no
// config hook, so this subclass installs its own listener and calls it with a
// retargeted event. Pinned to 0.7 internals; `userAction.test.ts` guards it.

import { UserActionInstrumentation } from "@opentelemetry/browser-instrumentation/experimental/user-action";

type WithOnClick = { onClick(event: MouseEvent): void };

function closestHtmlElement(target: EventTarget | null): HTMLElement | null {
  let node: Element | null = target instanceof Element ? target : null;
  while (node !== null && !(node instanceof HTMLElement)) {
    node = node.parentElement;
  }
  return node;
}

/** Native getters like `pageX` must run against the real event, hence the
 * explicit receiver. */
function retargetEvent(
  event: MouseEvent,
  resolvedTarget: HTMLElement,
): MouseEvent {
  return new Proxy(event, {
    get(realEvent, prop, _receiver) {
      if (prop === "target") return resolvedTarget;
      return Reflect.get(realEvent, prop, realEvent);
    },
  });
}

export class SvgAwareUserActionInstrumentation extends UserActionInstrumentation {
  private clickListener: ((event: MouseEvent) => void) | undefined;

  enable(): void {
    if (this.clickListener) return;
    this.clickListener = (event: MouseEvent) => {
      const element = closestHtmlElement(event.target);
      if (!element) return;
      (this as unknown as WithOnClick).onClick(retargetEvent(event, element));
    };
    document.addEventListener("click", this.clickListener, true);
  }

  disable(): void {
    if (!this.clickListener) return;
    document.removeEventListener("click", this.clickListener, true);
    this.clickListener = undefined;
  }
}
