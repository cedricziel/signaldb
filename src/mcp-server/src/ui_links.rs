//! Deep links from MCP tool results into the SignalDB UI.
//!
//! Each function is pure and returns `None` whenever it cannot produce a
//! link — `base` unset, or a malformed configured `ui_base_url` — so a
//! misconfigured deployment degrades to "no link" rather than a broken tool
//! call. Callers pass `self.ui_base_url.as_deref()` straight through.

use url::Url;

/// Wrap a built link as the `_links.ui` payload `json_result_ext` merges into
/// a tool result — the one place the `"ui"` key name is spelled out, rather
/// than repeating it at each call site.
pub fn as_link(url: Option<String>) -> Option<serde_json::Value> {
    url.map(|url| serde_json::json!({ "ui": url }))
}

/// Append `tenant`, `dataset`, and an optional non-empty `q`, shared by every
/// search-view link below.
fn append_search_params(url: &mut Url, tenant: &str, dataset: &str, query: Option<&str>) {
    let mut pairs = url.query_pairs_mut();
    pairs
        .append_pair("tenant", tenant)
        .append_pair("dataset", dataset);
    if let Some(q) = query.filter(|q| !q.is_empty()) {
        pairs.append_pair("q", q);
    }
}

/// Deep link into the UI's single-trace view.
///
/// `{base}/traces/{trace_id}?tenant={tenant}&dataset={dataset}`
pub fn trace_url(
    base: Option<&str>,
    tenant: &str,
    dataset: &str,
    trace_id: &str,
) -> Option<String> {
    let mut url = Url::parse(base?).ok()?;
    url.path_segments_mut().ok()?.push("traces").push(trace_id);
    url.query_pairs_mut()
        .append_pair("tenant", tenant)
        .append_pair("dataset", dataset);
    Some(url.to_string())
}

/// Deep link into the UI's trace search view.
///
/// `{base}/traces?tenant={tenant}&dataset={dataset}[&q={query}][&range={fromMs}-{toMs}]`
///
/// `q` is included only when `query` is `Some` and non-empty. `range` is
/// included only when both `start` and `end` (unix seconds) are `Some`,
/// converted to the UI's absolute-range millisecond format
/// (`src/ui/src/lib/time.ts`'s `rangeToParam`).
pub fn traces_search_url(
    base: Option<&str>,
    tenant: &str,
    dataset: &str,
    query: Option<&str>,
    start: Option<i64>,
    end: Option<i64>,
) -> Option<String> {
    let mut url = Url::parse(base?).ok()?;
    url.path_segments_mut().ok()?.push("traces");
    append_search_params(&mut url, tenant, dataset, query);
    if let (Some(start), Some(end)) = (start, end) {
        url.query_pairs_mut()
            .append_pair("range", &format!("{}-{}", start * 1000, end * 1000));
    }
    Some(url.to_string())
}

/// Deep link into the UI's log search view.
///
/// `{base}/logs?tenant={tenant}&dataset={dataset}[&q={query}]`
///
/// No `range` param: `search_logs`'s `start`/`end` are free-form strings
/// (unix ns/s or RFC3339), not reliably convertible to the UI's
/// `fromMs-toMs` integer-millisecond format without a parser this crate
/// doesn't have — deliberately out of scope.
pub fn logs_search_url(
    base: Option<&str>,
    tenant: &str,
    dataset: &str,
    query: Option<&str>,
) -> Option<String> {
    let mut url = Url::parse(base?).ok()?;
    url.path_segments_mut().ok()?.push("logs");
    append_search_params(&mut url, tenant, dataset, query);
    Some(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_url_percent_encodes_path_and_query_segments() {
        let url = trace_url(
            Some("https://ui.example.com"),
            "acme & co",
            "prod?ds",
            "trace 1",
        )
        .expect("base is set");
        assert_eq!(
            url,
            "https://ui.example.com/traces/trace%201?tenant=acme+%26+co&dataset=prod%3Fds"
        );
    }

    /// A `/` in `trace_id` must not be able to inject an extra path segment —
    /// `path_segments_mut` is relied on to percent-encode it as `%2F`.
    #[test]
    fn trace_url_percent_encodes_a_slash_in_the_trace_id() {
        let url = trace_url(Some("https://ui.example.com"), "acme", "prod", "a/../b")
            .expect("base is set");
        assert_eq!(
            url,
            "https://ui.example.com/traces/a%2F..%2Fb?tenant=acme&dataset=prod"
        );
    }

    #[test]
    fn traces_search_url_omits_q_when_query_is_none_or_empty() {
        let none = traces_search_url(
            Some("https://ui.example.com"),
            "acme",
            "prod",
            None,
            None,
            None,
        )
        .expect("base is set");
        assert_eq!(
            none,
            "https://ui.example.com/traces?tenant=acme&dataset=prod"
        );

        let empty = traces_search_url(
            Some("https://ui.example.com"),
            "acme",
            "prod",
            Some(""),
            None,
            None,
        )
        .expect("base is set");
        assert_eq!(
            empty,
            "https://ui.example.com/traces?tenant=acme&dataset=prod"
        );
    }

    #[test]
    fn traces_search_url_includes_q_and_range_when_present() {
        let url = traces_search_url(
            Some("https://ui.example.com"),
            "acme",
            "prod",
            Some("{ status = error }"),
            Some(1_700_000_000),
            Some(1_700_000_060),
        )
        .expect("base is set");
        assert_eq!(
            url,
            "https://ui.example.com/traces?tenant=acme&dataset=prod&q=%7B+status+%3D+error+%7D&range=1700000000000-1700000060000"
        );
    }

    #[test]
    fn traces_search_url_omits_range_unless_both_start_and_end_are_set() {
        let start_only = traces_search_url(
            Some("https://ui.example.com"),
            "acme",
            "prod",
            None,
            Some(1_700_000_000),
            None,
        )
        .expect("base is set");
        assert!(!start_only.contains("range="));

        let end_only = traces_search_url(
            Some("https://ui.example.com"),
            "acme",
            "prod",
            None,
            None,
            Some(1_700_000_060),
        )
        .expect("base is set");
        assert!(!end_only.contains("range="));
    }

    #[test]
    fn logs_search_url_includes_q_but_never_range() {
        let url = logs_search_url(
            Some("https://ui.example.com"),
            "acme",
            "prod",
            Some("{service_name=\"api\"}"),
        )
        .expect("base is set");
        assert_eq!(
            url,
            "https://ui.example.com/logs?tenant=acme&dataset=prod&q=%7Bservice_name%3D%22api%22%7D"
        );
        assert!(!url.contains("range="));
    }

    #[test]
    fn logs_search_url_omits_q_when_query_is_none_or_empty() {
        let none = logs_search_url(Some("https://ui.example.com"), "acme", "prod", None)
            .expect("base is set");
        assert_eq!(none, "https://ui.example.com/logs?tenant=acme&dataset=prod");

        let empty = logs_search_url(Some("https://ui.example.com"), "acme", "prod", Some(""))
            .expect("base is set");
        assert_eq!(
            empty,
            "https://ui.example.com/logs?tenant=acme&dataset=prod"
        );
    }

    #[test]
    fn all_functions_return_none_when_base_is_unset() {
        assert_eq!(trace_url(None, "acme", "prod", "abc"), None);
        assert_eq!(
            traces_search_url(None, "acme", "prod", None, None, None),
            None
        );
        assert_eq!(logs_search_url(None, "acme", "prod", None), None);
    }

    #[test]
    fn all_functions_return_none_for_a_malformed_base() {
        let base = Some("not a url");
        assert_eq!(trace_url(base, "acme", "prod", "abc"), None);
        assert_eq!(
            traces_search_url(base, "acme", "prod", None, None, None),
            None
        );
        assert_eq!(logs_search_url(base, "acme", "prod", None), None);
    }
}
