//! # Skill resources
//!
//! Longer-form, on-demand guidance surfaced as MCP resources (`skill://`
//! URIs, following the `skill://<name>/SKILL.md` convention other MCP
//! servers use) and as the `list_skills`/`get_skill` tools in
//! [`crate::server`], for clients that don't read MCP resources on their
//! own. Each skill's document is longer than belongs in
//! [`ServerInfo::instructions`], which is sent on every session and should
//! stay short; a client reaches for one only when it decides the topic is
//! relevant.
//!
//! [`ServerInfo::instructions`]: rmcp::model::ServerInfo

use rmcp::model::{Resource, ResourceContents};

/// MIME type for the skill documents below.
const SKILL_MIME_TYPE: &str = "text/markdown";

/// MIME type for [`INDEX_URI`].
const INDEX_MIME_TYPE: &str = "application/json";

/// URI of the skill discovery index, listing every registered skill.
pub const INDEX_URI: &str = "skill://index.json";

/// One skill: a name, the metadata surfaced in `resources/list`/`list_skills`,
/// and the builder for its Markdown body.
struct Skill {
    /// Short identifier, e.g. `"query-ir"` — the `get_skill` tool's `name`
    /// argument and the first path segment of [`Self::uri`].
    name: &'static str,
    title: &'static str,
    description: &'static str,
    text: fn() -> String,
}

impl Skill {
    /// This skill's canonical `skill://<name>/SKILL.md` URI.
    fn uri(&self) -> String {
        format!("skill://{}/SKILL.md", self.name)
    }
}

/// The registry: add an entry here to register a new skill.
const SKILLS: &[Skill] = &[Skill {
    name: "query-ir",
    title: "Query IR: when and how",
    description: "When the native query_ir tool covers more than search_traces/search_logs/\
                  query_metrics, and the full IR document reference (sources, pipeline stages, \
                  predicates, result envelopes).",
    text: query_ir_skill_text,
}];

/// The canonical Query IR reference (`docs/users/querying-ir.md`), the same
/// document users read — kept as the single source of truth rather than a
/// second, drift-prone copy — with its docs-pipeline frontmatter stripped and
/// an MCP-specific "when to use this" framing prepended. `resources/read` and
/// `get_skill` are not hot paths, so this is rebuilt per call rather than
/// cached.
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

/// One [`SKILLS`] entry, as returned by `list_skills` and the
/// `skill://index.json` resource.
#[derive(serde::Serialize)]
pub struct SkillSummary {
    pub name: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub uri: String,
}

/// The registered skills, summarized for discovery (`list_skills`,
/// `skill://index.json`).
pub fn skill_summaries() -> Vec<SkillSummary> {
    SKILLS
        .iter()
        .map(|skill| SkillSummary {
            name: skill.name,
            title: skill.title,
            description: skill.description,
            uri: skill.uri(),
        })
        .collect()
}

/// A registered skill's Markdown body by name (the `get_skill` tool's
/// argument), or `None` when `name` isn't registered.
pub fn skill_text(name: &str) -> Option<String> {
    SKILLS
        .iter()
        .find(|skill| skill.name == name)
        .map(|skill| (skill.text)())
}

/// The names of every registered skill, for the `get_skill` error message.
pub fn skill_names() -> Vec<&'static str> {
    SKILLS.iter().map(|skill| skill.name).collect()
}

/// The skill resources this server exposes, for `resources/list`: each
/// registered skill's `SKILL.md` plus the discovery index.
pub fn skill_resources() -> Vec<Resource> {
    let mut resources: Vec<Resource> = SKILLS
        .iter()
        .map(|skill| {
            let text = (skill.text)();
            Resource::new(skill.uri(), skill.name)
                .with_title(skill.title.to_string())
                .with_description(skill.description.to_string())
                .with_mime_type(SKILL_MIME_TYPE)
                .with_size(text.len() as u64)
        })
        .collect();
    let index = index_json();
    resources.push(
        Resource::new(INDEX_URI, "index")
            .with_title("Skill index")
            .with_description("Discovery index of every skill this server exposes.")
            .with_mime_type(INDEX_MIME_TYPE)
            .with_size(index.len() as u64),
    );
    resources
}

/// Serialize [`skill_summaries`] for [`INDEX_URI`].
fn index_json() -> String {
    serde_json::to_string_pretty(&skill_summaries()).unwrap_or_else(|_| "[]".to_string())
}

/// Resolve a `skill://` URI to its contents for `resources/read`, or `None`
/// when no skill is registered under that URI.
pub fn read_skill_resource(uri: &str) -> Option<ResourceContents> {
    if uri == INDEX_URI {
        return Some(
            ResourceContents::text(index_json(), INDEX_URI).with_mime_type(INDEX_MIME_TYPE),
        );
    }
    let name = uri.strip_prefix("skill://")?.strip_suffix("/SKILL.md")?;
    skill_text(name)
        .map(|text| ResourceContents::text(text, uri.to_string()).with_mime_type(SKILL_MIME_TYPE))
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUERY_IR_SKILL_URI: &str = "skill://query-ir/SKILL.md";

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
    fn index_is_listed_with_the_json_mime_type() {
        let resources = skill_resources();
        let index = resources
            .iter()
            .find(|r| r.uri == INDEX_URI)
            .expect("skill index is listed");
        assert_eq!(index.mime_type.as_deref(), Some(INDEX_MIME_TYPE));
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
    fn index_json_lists_registered_skills() {
        let contents = read_skill_resource(INDEX_URI).expect("index is readable");
        let ResourceContents::TextResourceContents { text, .. } = contents else {
            panic!("index is served as text, not a blob");
        };
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(&text).expect("index is valid JSON");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["name"], "query-ir");
        assert_eq!(parsed[0]["uri"], QUERY_IR_SKILL_URI);
    }

    #[test]
    fn unknown_skill_uri_is_not_served() {
        assert!(read_skill_resource("skill://nope/SKILL.md").is_none());
        assert!(read_skill_resource("file:///etc/passwd").is_none());
    }

    #[test]
    fn skill_text_resolves_registered_skills_by_name() {
        assert!(skill_text("query-ir").is_some_and(|t| t.contains("Reach for the `query_ir`")));
        assert!(skill_text("nope").is_none());
    }

    #[test]
    fn skill_names_lists_registered_skills() {
        assert_eq!(skill_names(), vec!["query-ir"]);
    }

    #[test]
    fn skill_summaries_match_the_registry() {
        let summaries = skill_summaries();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].name, "query-ir");
        assert_eq!(summaries[0].uri, QUERY_IR_SKILL_URI);
    }
}
