//! # Skill resources
//!
//! Longer-form, on-demand guidance surfaced as MCP resources (`skill://`
//! URIs) rather than crammed into [`ServerInfo::instructions`], which is sent
//! on every session and should stay short. A client reads one of these only
//! when it decides the topic is relevant — the same pattern other MCP
//! servers use for procedural "skill" documents.
//!
//! [`ServerInfo::instructions`]: rmcp::model::ServerInfo

use rmcp::model::{Resource, ResourceContents};

/// MIME type for the skill documents below.
const SKILL_MIME_TYPE: &str = "text/markdown";

/// URI of the Query IR skill: when to reach for `query_ir` instead of the
/// signal-specific query tools, and how to build a document.
pub const QUERY_IR_SKILL_URI: &str = "skill://signaldb/query-ir";

/// The canonical Query IR reference (`docs/users/querying-ir.md`), the same
/// document users read — kept as the single source of truth rather than a
/// second, drift-prone copy — with its docs-pipeline frontmatter stripped and
/// an MCP-specific "when to use this" framing prepended. `resources/read` is
/// not a hot path, so this is rebuilt per call rather than cached.
fn query_ir_skill_text() -> String {
    const RAW: &str = include_str!("../../../docs/users/querying-ir.md");
    let reference = strip_frontmatter(RAW);
    format!(
        "# When to reach for `query_ir`\n\n\
         `search_traces`, `search_logs`, and `query_metrics` cover the common case: one query, \
         in that signal's own dialect, against one signal. Reach for the `query_ir` tool instead \
         when you need a pipeline stage those dialects can't express (`topk`/`bottomk`, `extract` \
         on logs, a multi-stage `aggregate` with `step`), or you're already working from \
         `discover_sources` / `discover_fields` / `discover_field_values` and have the document \
         half-built. `get_profile` already returns query_ir's `flamegraph` envelope directly, so \
         profiles never need this tool. The reference below is the full IR document shape, \
         pipeline stages, and predicate grammar.\n\n\
         ---\n\n{reference}"
    )
}

/// Strip a doc's YAML frontmatter (`---\n...\n---\n`), if present.
fn strip_frontmatter(doc: &str) -> &str {
    let Some(rest) = doc.strip_prefix("---\n") else {
        return doc;
    };
    rest.find("\n---\n")
        .map(|end| rest[end + "\n---\n".len()..].trim_start())
        .unwrap_or(doc)
}

/// The skill resources this server exposes, for `resources/list`.
pub fn skill_resources() -> Vec<Resource> {
    let text = query_ir_skill_text();
    vec![
        Resource::new(QUERY_IR_SKILL_URI, "query-ir")
            .with_title("Query IR: when and how")
            .with_description(
                "When the native query_ir tool covers more than search_traces/search_logs/\
                 query_metrics, and the full IR document reference (sources, pipeline stages, \
                 predicates, result envelopes).",
            )
            .with_mime_type(SKILL_MIME_TYPE)
            .with_size(text.len() as u64),
    ]
}

/// Resolve a `skill://` URI to its contents for `resources/read`, or `None`
/// when no skill is registered under that URI.
pub fn read_skill_resource(uri: &str) -> Option<ResourceContents> {
    (uri == QUERY_IR_SKILL_URI).then(|| {
        ResourceContents::text(query_ir_skill_text(), QUERY_IR_SKILL_URI)
            .with_mime_type(SKILL_MIME_TYPE)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontmatter_is_stripped() {
        let doc = "---\naudience: user\n---\n# Title\n\nbody";
        assert_eq!(strip_frontmatter(doc), "# Title\n\nbody");
    }

    #[test]
    fn missing_frontmatter_is_left_alone() {
        let doc = "# Title\n\nbody";
        assert_eq!(strip_frontmatter(doc), doc);
    }

    #[test]
    fn query_ir_skill_is_listed_with_the_skill_mime_type() {
        let resources = skill_resources();
        let skill = resources
            .iter()
            .find(|r| r.uri == QUERY_IR_SKILL_URI)
            .expect("query-ir skill is listed");
        assert_eq!(skill.mime_type.as_deref(), Some(SKILL_MIME_TYPE));
        assert_eq!(skill.name, "query-ir");
        assert!(skill.size.is_some_and(|size| size > 0));
    }

    #[test]
    fn reading_the_query_ir_skill_returns_guidance_and_reference() {
        let contents = read_skill_resource(QUERY_IR_SKILL_URI).expect("query-ir skill is readable");
        let ResourceContents::TextResourceContents {
            uri,
            mime_type,
            text,
            ..
        } = contents
        else {
            panic!("skill resources are served as text, not blobs");
        };
        assert_eq!(uri, QUERY_IR_SKILL_URI);
        assert_eq!(mime_type.as_deref(), Some(SKILL_MIME_TYPE));
        assert!(text.contains("Reach for the `query_ir` tool"));
        assert!(
            text.contains("Pipeline stages"),
            "reference body must follow the framing"
        );
        assert!(!text.starts_with("---\n"), "frontmatter must be stripped");
    }

    #[test]
    fn unknown_skill_uri_is_not_served() {
        assert!(read_skill_resource("skill://signaldb/nope").is_none());
        assert!(read_skill_resource("file:///etc/passwd").is_none());
    }
}
