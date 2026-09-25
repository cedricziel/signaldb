// Which edges of a horizontally-scrolled element currently have content
// hidden off-screen — drives the CSS mask-image fade on the signal tab strip
// (see ExploreView) so a phone user sees that more tabs exist off-screen.
export interface EdgeOverflow {
  left: boolean;
  right: boolean;
}

export interface ScrollMetrics {
  scrollLeft: number;
  scrollWidth: number;
  clientWidth: number;
}

// A 1px slop absorbs sub-pixel rounding from browser zoom/DPI so a fully
// scrolled-to-edge strip doesn't flicker a fade on the edge it just reached.
const SLOP = 1;

export function computeEdgeOverflow(el: ScrollMetrics): EdgeOverflow {
  return {
    left: el.scrollLeft > SLOP,
    right: el.scrollLeft + el.clientWidth < el.scrollWidth - SLOP,
  };
}
