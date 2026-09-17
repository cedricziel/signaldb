import { useCallback, useRef, useState, type KeyboardEvent } from "react";

export interface RovingFocusItemProps {
  tabIndex: 0 | -1;
  onKeyDown: (e: KeyboardEvent<HTMLOrSVGElement>) => void;
  onFocus: () => void;
  /**
   * Registers the mark's DOM node so an arrow key can move real focus to it.
   * `HTMLOrSVGElement` (not `HTMLElement`) so this assigns to both HTML
   * marks (buttons, spans) and SVG ones (the `<rect>` hit targets an inline
   * chart draws).
   */
  ref: (el: HTMLOrSVGElement | null) => void;
}

export interface UseRovingFocusOptions {
  /**
   * Override left/right stepping for a 2-D layout (a heatmap row, a flame
   * graph level): given the current index and a direction, return the index
   * to move to, or `null` when there's nothing that way. Omit for a flat
   * list, where left/right simply clamp to the adjacent index.
   */
  horizontal?: (index: number, direction: -1 | 1) => number | null;
  /**
   * Up/Down stepping for a 2-D layout (heatmap rows, flame graph levels).
   * Omitted entirely (rather than a no-op) for a 1-D list, so up/down are
   * left for the page to handle (e.g. scrolling) instead of swallowed.
   */
  vertical?: (index: number, direction: -1 | 1) => number | null;
}

export interface RovingFocus {
  activeIndex: number;
  setActiveIndex: (index: number) => void;
  itemProps: (index: number) => RovingFocusItemProps;
}

/**
 * Roving tabindex for a group of same-role data marks (histogram bars,
 * heatmap cells, flame frames, dependency segments, …): the group is one tab
 * stop — only the active mark has `tabIndex={0}`, the rest `-1` — and arrow
 * keys move which mark is active, `Home`/`End` jump to the first/last.
 *
 * Marks driven by this hook keep showing their data through the shared
 * `VizTooltip` (see `.claude/skills/frontend-instrumentation`); this only
 * changes how keyboard focus reaches them, not how their detail is shown.
 */
export function useRovingFocus(
  count: number,
  options: UseRovingFocusOptions = {},
): RovingFocus {
  const [activeIndex, setActiveIndexState] = useState(0);
  const itemRefs = useRef<(HTMLOrSVGElement | null)[]>([]);
  const { horizontal, vertical } = options;

  const moveFocusTo = useCallback(
    (index: number | null) => {
      if (index === null || index < 0 || index >= count) return;
      setActiveIndexState(index);
      itemRefs.current[index]?.focus();
    },
    [count],
  );

  const setActiveIndex = useCallback(
    (index: number) => {
      if (index >= 0 && index < count) setActiveIndexState(index);
    },
    [count],
  );

  const itemProps = useCallback(
    (index: number): RovingFocusItemProps => ({
      tabIndex: index === activeIndex ? 0 : -1,
      ref: (el) => {
        itemRefs.current[index] = el;
      },
      onFocus: () => setActiveIndexState(index),
      onKeyDown: (e) => {
        switch (e.key) {
          case "ArrowRight":
            e.preventDefault();
            moveFocusTo(
              horizontal
                ? horizontal(index, 1)
                : Math.min(count - 1, index + 1),
            );
            break;
          case "ArrowLeft":
            e.preventDefault();
            moveFocusTo(
              horizontal ? horizontal(index, -1) : Math.max(0, index - 1),
            );
            break;
          case "ArrowDown":
            if (!vertical) break;
            e.preventDefault();
            moveFocusTo(vertical(index, 1));
            break;
          case "ArrowUp":
            if (!vertical) break;
            e.preventDefault();
            moveFocusTo(vertical(index, -1));
            break;
          case "Home":
            e.preventDefault();
            moveFocusTo(0);
            break;
          case "End":
            e.preventDefault();
            moveFocusTo(count - 1);
            break;
          default:
            break;
        }
      },
    }),
    [activeIndex, count, horizontal, moveFocusTo, vertical],
  );

  return { activeIndex, setActiveIndex, itemProps };
}
