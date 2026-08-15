/**
 * The one tooltip every visualization panel renders its data points through.
 *
 * The primitive knows nothing about data: a panel resolves pointer → datum
 * itself and hands over a title (the x position), rows (one per series, with
 * swatch, label, value) and an optional aggregate footer. The tooltip follows
 * the anchor, flips left past the host's midline and up near its bottom edge,
 * and never intercepts the pointer. Hosts must be `position: relative`.
 */
import {
  useCallback,
  useLayoutEffect,
  useRef,
  useState,
  type CSSProperties,
  type RefObject,
} from "react";

export interface VizTooltipRow {
  /** CSS colour matching the drawn series; omitted for aggregate rows. */
  swatch?: string;
  label: string;
  value: string;
  /** A row for a series with no sample at this x (`–`). */
  muted?: boolean;
}

export interface VizTooltipProps {
  /** Pointer or focus position, in px relative to the host. */
  anchor: { x: number; y: number };
  host: { width: number; height: number };
  title: string;
  rows: VizTooltipRow[];
  /** Aggregate line under the rows: a plain string or a label/value pair. */
  footer?: string | { label: string; value: string };
  id?: string;
  /**
   * Reserve this many `ch` for the value column so the tooltip keeps one width
   * while the pointer moves between buckets instead of jittering.
   */
  valueWidthCh?: number;
}

/** Distance from the anchor to the tooltip's near edge. */
const OFFSET_X = 14;
const OFFSET_Y = 12;

/** Height guess used before the first paint has measured the element. */
function estimateHeight(rows: number, footer: boolean): number {
  return 12 + 16 + rows * 15 + (footer ? 22 : 0);
}

export function VizTooltip({
  anchor,
  host,
  title,
  rows,
  footer,
  id,
  valueWidthCh,
}: VizTooltipProps) {
  const ref = useRef<HTMLDivElement>(null);
  const [measured, setMeasured] = useState(0);
  useLayoutEffect(() => {
    const h = ref.current?.offsetHeight ?? 0;
    if (h > 0 && h !== measured) setMeasured(h);
  });

  const height =
    measured > 0 ? measured : estimateHeight(rows.length, !!footer);
  const flipX = anchor.x > host.width / 2;
  const flipY = anchor.y + OFFSET_Y + height > host.height;
  const tx = flipX ? `calc(-100% - ${OFFSET_X}px)` : `${OFFSET_X}px`;
  const ty = flipY ? `calc(-100% - ${OFFSET_Y}px)` : `${OFFSET_Y}px`;

  const style: CSSProperties & { "--viz-val-ch"?: string } = {
    left: `${anchor.x}px`,
    top: `${anchor.y}px`,
    transform: `translate(${tx}, ${ty})`,
  };
  if (valueWidthCh !== undefined) style["--viz-val-ch"] = String(valueWidthCh);

  return (
    <div className="viz-tip" role="tooltip" id={id} ref={ref} style={style}>
      <div className="viz-tip-t">{title}</div>
      {rows.map((row, i) => (
        <div
          className={row.muted ? "viz-tip-row viz-tip-muted" : "viz-tip-row"}
          data-testid="viz-tip-row"
          key={`${row.label}-${i}`}
        >
          <i
            data-testid="viz-tip-swatch"
            style={{
              background: row.swatch,
              visibility: row.swatch ? undefined : "hidden",
            }}
          />
          <span>{row.label}</span>
          <b>{row.value}</b>
        </div>
      ))}
      {footer !== undefined &&
        (typeof footer === "string" ? (
          <div className="viz-tip-footer" data-testid="viz-tip-footer">
            <span>{footer}</span>
          </div>
        ) : (
          <div className="viz-tip-footer" data-testid="viz-tip-footer">
            <span>{footer.label}</span>
            <b>{footer.value}</b>
          </div>
        ))}
    </div>
  );
}

export interface VizPointer {
  /** Where the tooltip should anchor, or null when nothing is pointed at. */
  anchor: { x: number; y: number } | null;
  /** The host's size, measured alongside the anchor. */
  host: { width: number; height: number };
  /** Follow a pointer event. */
  track: (e: { clientX: number; clientY: number }) => void;
  /** Keyboard focus has no pointer: anchor under the mark's centre instead. */
  anchorTo: (el: Element) => void;
  clear: () => void;
}

/**
 * Pointer/focus tracking for SVG and DOM panels, in host-relative pixels.
 * Wire `track` to `onPointerMove`, `clear` to `onPointerLeave`/`onBlur`, and
 * `anchorTo` to `onFocus` of a data mark.
 */
export function useVizPointer(
  hostRef: RefObject<HTMLElement | null>,
): VizPointer {
  const [anchor, setAnchor] = useState<{ x: number; y: number } | null>(null);
  const [host, setHost] = useState({ width: 0, height: 0 });

  const measure = useCallback(() => {
    const rect = hostRef.current?.getBoundingClientRect();
    if (rect) setHost({ width: rect.width, height: rect.height });
    return rect;
  }, [hostRef]);

  const track = useCallback(
    (e: { clientX: number; clientY: number }) => {
      const rect = measure();
      if (!rect) return;
      setAnchor({ x: e.clientX - rect.left, y: e.clientY - rect.top });
    },
    [measure],
  );

  const anchorTo = useCallback(
    (el: Element) => {
      const rect = measure();
      if (!rect) return;
      const box = el.getBoundingClientRect();
      setAnchor({
        x: box.left - rect.left + box.width / 2,
        y: box.bottom - rect.top,
      });
    },
    [measure],
  );

  const clear = useCallback(() => setAnchor(null), []);

  return { anchor, host, track, anchorTo, clear };
}
