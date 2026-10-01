// Paging a `rows` result through the IR endpoint's `page` (irVersion 14):
// the view asks for one page, then follows `page.next_cursor` on "Load more".
import type { QueryIrRequest, QueryIrResponse } from "../../api/gen";

/** Rows per page of a `rows` result. */
export const ROWS_PAGE_SIZE = 500;

/** The lowest irVersion that carries a document-level `page`. */
const PAGE_IR_VERSION = 14;

/** `doc` asking for one page, continuing after `cursor` when given. */
export function pagedDocument(
  doc: QueryIrRequest,
  cursor?: string,
): QueryIrRequest {
  return {
    ...doc,
    irVersion: Math.max(doc.irVersion, PAGE_IR_VERSION),
    page: cursor ? { size: ROWS_PAGE_SIZE, cursor } : { size: ROWS_PAGE_SIZE },
  };
}

/** The pages fetched so far as one response: their rows in order, and the
 * last page's cursor. */
export function mergePages(
  pages: QueryIrResponse[],
): QueryIrResponse | undefined {
  const [first] = pages;
  if (!first) return undefined;
  return {
    ...first,
    rows: pages.flatMap((p) => p.rows ?? []),
    page: pages[pages.length - 1]!.page,
  };
}
