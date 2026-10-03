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

use std::sync::LazyLock;

use rmcp::model::{Resource, ResourceContents};

/// MIME type for the skill documents below.
const SKILL_MIME_TYPE: &str = "text/markdown";

/// MIME type for [`INDEX_URI`].
const INDEX_MIME_TYPE: &str = "application/json";

/// URI of the skill discovery index, listing every registered skill.
pub const INDEX_URI: &str = "skill://index.json";

/// One skill: a name, the metadata surfaced in `resources/list`/`list_skills`,
/// and its Markdown body.
struct Skill {
    /// Short identifier, e.g. `"query-ir"` or `"query-ir/aggregate"` — the
    /// `get_skill` tool's `name` argument and the path of [`Self::uri`].
    name: String,
    title: String,
    description: String,
    text: String,
}

impl Skill {
    /// This skill's canonical `skill://<name>/SKILL.md` URI.
    fn uri(&self) -> String {
        format!("skill://{}/SKILL.md", self.name)
    }
}

/// The canonical Query IR reference (`docs/users/querying-ir.md`), the same
/// document users read — kept as the single source of truth rather than a
/// second, drift-prone copy.
const QUERY_IR_REFERENCE: &str = include_str!("../../../docs/users/querying-ir.md");

/// One fetchable section of the Query IR reference: `query-ir/<slug>` runs
/// from the heading `starts_at` to the next section's heading. The whole
/// reference overflows an agent's tool-result budget (#2173), so the
/// `query-ir` skill is an index and each section is read on its own.
struct QueryIrSection {
    slug: &'static str,
    title: &'static str,
    description: &'static str,
    /// The heading line that opens this section.
    starts_at: &'static str,
}

/// The sections, in document order. Their headings must exist in the
/// reference; a test enforces it, so a renamed heading fails the build
/// rather than silently merging two sections.
const QUERY_IR_SECTIONS: &[QueryIrSection] = &[
    QueryIrSection {
        slug: "document",
        title: "The document",
        description: "The endpoint, the top-level fields (irVersion, from, range, result, \
                      pipeline) and the table of pipeline stages with the version each needs.",
        starts_at: "## The endpoint",
    },
    QueryIrSection {
        slug: "where",
        title: "Predicates",
        description: "The `where` grammar: operators, and/or/not, attribute scopes, exception \
                      attributes, span events and links.",
        starts_at: "### Predicates",
    },
    QueryIrSection {
        slug: "aggregate",
        title: "Aggregates",
        description: "`aggregate` functions (count, count_distinct, sum, quantiles, ...), \
                      `step`, rates, range functions, value types and coercion.",
        starts_at: "### Aggregate functions",
    },
    QueryIrSection {
        slug: "results",
        title: "Result envelopes",
        description: "The `result` envelopes (rows, table, series, trace, ...) and warnings.",
        starts_at: "## Result envelopes",
    },
    QueryIrSection {
        slug: "paging",
        title: "Pagination and live tail",
        description: "Paging a large result with `page_size`/`cursor`, and following new rows \
                      with `tail`.",
        starts_at: "## Pagination",
    },
    QueryIrSection {
        slug: "graph",
        title: "Service graph",
        description: "The `graph` envelope over traces: service-to-service edges.",
        starts_at: "## Graph envelope",
    },
    QueryIrSection {
        slug: "profiles",
        title: "Profiles",
        description: "Profile summaries and the flamegraph envelope.",
        starts_at: "## Profile summaries",
    },
    QueryIrSection {
        slug: "metrics",
        title: "Metrics and series",
        description: "The `metrics` source, the metric Series algebra (`sample`, `reduce`, \
                      `binop`, ...), labels and scalars.",
        starts_at: "## Metrics",
    },
    QueryIrSection {
        slug: "histograms",
        title: "Histograms and exemplars",
        description: "`histogram_quantile`, `histogram_fraction`, heatmaps and exemplars.",
        starts_at: "## Histograms",
    },
    QueryIrSection {
        slug: "correlate",
        title: "Correlate",
        description: "The `correlate` stage: spans to their parents, and one signal to another.",
        starts_at: "## Correlate",
    },
    QueryIrSection {
        slug: "match",
        title: "Structural matching",
        description: "The `match` stage: finding traces by span structure.",
        starts_at: "## Structural matching",
    },
    QueryIrSection {
        slug: "formulas",
        title: "Formulas",
        description: "Arithmetic across several queries' results.",
        starts_at: "## Formulas",
    },
    QueryIrSection {
        slug: "discovery",
        title: "Discovery",
        description: "The `describe` stage: which fields and values exist, and what the answer \
                      cost.",
        starts_at: "## Discovery",
    },
    QueryIrSection {
        slug: "examples",
        title: "Worked example",
        description: "A worked example, submitting a query, and the roadmap.",
        starts_at: "## Worked example",
    },
];

/// The registry: every skill this server serves, split out of the
/// reference once.
static SKILLS: LazyLock<Vec<Skill>> = LazyLock::new(build_skills);

fn skills() -> &'static [Skill] {
    &SKILLS
}

fn build_skills() -> Vec<Skill> {
    let reference = strip_frontmatter(QUERY_IR_REFERENCE);
    let sections = query_ir_sections(reference);
    let mut skills = vec![Skill {
        name: "query-ir".to_string(),
        title: "Query IR: when and how".to_string(),
        description: "When the native query_ir tool covers more than search_traces/search_logs/\
                      query_metrics, a minimal document, and the index of the reference's \
                      sections (query-ir/<section>)."
            .to_string(),
        text: query_ir_index(reference, &sections),
    }];
    skills.extend(sections.into_iter().map(|(section, text)| Skill {
        name: format!("query-ir/{}", section.slug),
        title: format!("Query IR: {}", section.title),
        description: section.description.to_string(),
        text: text.to_string(),
    }));
    skills
}

/// Each section with its text, split at the section headings.
fn query_ir_sections(reference: &str) -> Vec<(&'static QueryIrSection, &str)> {
    let starts: Vec<usize> = QUERY_IR_SECTIONS
        .iter()
        .map(|section| heading_offset(reference, section.starts_at).unwrap_or(reference.len()))
        .collect();
    QUERY_IR_SECTIONS
        .iter()
        .zip(&starts)
        .enumerate()
        .map(|(i, (section, &start))| {
            let end = starts.get(i + 1).copied().unwrap_or(reference.len());
            (section, reference[start..end.max(start)].trim_end())
        })
        .collect()
}

/// The byte offset of the line starting with `heading`, if any.
fn heading_offset(doc: &str, heading: &str) -> Option<usize> {
    if doc.starts_with(heading) {
        return Some(0);
    }
    doc.find(&format!("\n{heading}")).map(|i| i + 1)
}

/// A complete, minimal `query_ir` document — the one a first guess gets
/// wrong. Also quoted in the `query_ir` tool description.
pub const QUERY_IR_MINIMAL_EXAMPLE: &str = r#"{"irVersion": 9, "from": "traces", "range": {"from": "now-1h", "to": "now"}, "result": "table", "pipeline": [{"aggregate": {"by": ["service.name"], "aggs": [{"fn": "count", "as": "spans"}, {"fn": "count_distinct", "of": "trace.id", "as": "traces"}]}}]}"#;

/// The `query-ir` skill: when to use the tool, a minimal document, the
/// reference's introduction, and the section index.
fn query_ir_index(reference: &str, sections: &[(&QueryIrSection, &str)]) -> String {
    let intro_end = sections
        .first()
        .and_then(|(section, _)| heading_offset(reference, section.starts_at))
        .unwrap_or(reference.len());
    let index: String = sections
        .iter()
        .map(|(section, _)| {
            format!(
                "- `query-ir/{}` — **{}**: {}\n",
                section.slug, section.title, section.description
            )
        })
        .collect();
    format!(
        "# When to reach for `query_ir`\n\n\
         `search_traces`, `search_logs`, and `query_metrics` cover the common case: one query, \
         in that signal's own dialect, against one signal. Reach for the `query_ir` tool instead \
         when you need a pipeline stage those dialects can't express (`topk`/`bottomk`, `extract` \
         on logs, a multi-stage `aggregate` with `step`), or you're already working from \
         `discover_sources` / `discover_fields` / `discover_field_values` and have the document \
         half-built. `get_profile` already returns query_ir's `flamegraph` envelope directly, so \
         profiles never need this tool.\n\n\
         ## A minimal document\n\n\
         Spans and distinct traces per service over the last hour:\n\n\
         ```json\n{QUERY_IR_MINIMAL_EXAMPLE}\n```\n\n\
         `irVersion`, `from`, `range` and `result` are required; `pipeline` is a list of stages.\n\n\
         ## The reference, by section\n\n\
         Read only the section you need with `get_skill(\"query-ir/<section>\")` (or the \
         `skill://query-ir/<section>/SKILL.md` resource):\n\n\
         {index}\n---\n\n{}",
        reference[..intro_end].trim_end()
    )
}

/// The hint appended to a Query IR error: which `query-ir` section to read.
pub fn query_ir_section_hint(message: &str) -> String {
    format!(
        " (see get_skill(\"query-ir/{}\"))",
        query_ir_section_for(message)
    )
}

/// The `query-ir` section to read for a Query IR error message: the one about
/// the stage or concept the message names (as a whole word), else the
/// document shape.
fn query_ir_section_for(message: &str) -> &'static str {
    const KEYWORDS: &[(&str, &str)] = &[
        ("correlate", "correlate"),
        ("match", "match"),
        ("formula", "formulas"),
        ("formulas", "formulas"),
        ("describe", "discovery"),
        ("cursor", "paging"),
        ("page_size", "paging"),
        ("tail", "paging"),
        ("histogram", "histograms"),
        ("exemplars", "histograms"),
        ("aggregate", "aggregate"),
        ("aggs", "aggregate"),
        ("fn", "aggregate"),
        ("step", "aggregate"),
        ("where", "where"),
        ("predicate", "where"),
        ("op", "where"),
        ("result", "results"),
    ];
    let message = message.to_ascii_lowercase();
    let words: Vec<&str> = message
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .collect();
    KEYWORDS
        .iter()
        .find(|(keyword, _)| words.contains(keyword))
        .map(|(_, section)| *section)
        .unwrap_or("document")
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

/// One registered skill, as returned by `list_skills` and the
/// `skill://index.json` resource.
#[derive(serde::Serialize)]
pub struct SkillSummary {
    pub name: String,
    pub title: String,
    pub description: String,
    pub uri: String,
}

/// The registered skills, summarized for discovery (`list_skills`,
/// `skill://index.json`).
pub fn skill_summaries() -> Vec<SkillSummary> {
    skills()
        .iter()
        .map(|skill| SkillSummary {
            uri: skill.uri(),
            name: skill.name.clone(),
            title: skill.title.clone(),
            description: skill.description.clone(),
        })
        .collect()
}

/// A registered skill's Markdown body by name (the `get_skill` tool's
/// argument), or `None` when `name` isn't registered.
pub fn skill_text(name: &str) -> Option<String> {
    skills()
        .iter()
        .find(|skill| skill.name == name)
        .map(|skill| skill.text.clone())
}

/// The names of every registered skill, for the `get_skill` error message.
pub fn skill_names() -> Vec<String> {
    skills().iter().map(|skill| skill.name.clone()).collect()
}

/// The skill resources this server exposes, for `resources/list`: each
/// registered skill's `SKILL.md` plus the discovery index.
pub fn skill_resources() -> Vec<Resource> {
    let mut resources: Vec<Resource> = skills()
        .iter()
        .map(|skill| {
            Resource::new(skill.uri(), skill.name.clone())
                .with_title(skill.title.clone())
                .with_description(skill.description.clone())
                .with_mime_type(SKILL_MIME_TYPE)
                .with_size(skill.text.len() as u64)
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
        assert!(text.contains(QUERY_IR_MINIMAL_EXAMPLE));
        for section in QUERY_IR_SECTIONS {
            assert!(
                text.contains(&format!("`query-ir/{}`", section.slug)),
                "index must list {}",
                section.slug
            );
        }
        assert!(
            !text.contains("Pipeline stages"),
            "the sections are read separately, not inlined"
        );
        assert!(!text.starts_with("---\n"), "frontmatter must be stripped");
    }

    #[test]
    fn every_section_heading_is_in_the_reference() {
        let reference = strip_frontmatter(QUERY_IR_REFERENCE);
        let mut previous = 0;
        for section in QUERY_IR_SECTIONS {
            let at = heading_offset(reference, section.starts_at)
                .unwrap_or_else(|| panic!("heading {:?} is missing", section.starts_at));
            assert!(at > previous, "{} is out of document order", section.slug);
            previous = at;
        }
    }

    #[test]
    fn sections_cover_the_reference_and_fit_a_tool_result() {
        let reference = strip_frontmatter(QUERY_IR_REFERENCE);
        let sections = query_ir_sections(reference);
        let intro_end = heading_offset(reference, QUERY_IR_SECTIONS[0].starts_at).unwrap_or(0);
        let non_space = |text: &str| text.chars().filter(|c| !c.is_whitespace()).count();
        let covered: usize = sections.iter().map(|(_, text)| non_space(text)).sum();
        assert_eq!(
            non_space(&reference[..intro_end]) + covered,
            non_space(reference),
            "sections must not drop any of the reference"
        );
        for (section, text) in &sections {
            assert!(text.starts_with(section.starts_at), "{}", section.slug);
            assert!(
                text.len() < 25_000,
                "query-ir/{} is {} bytes; split it",
                section.slug,
                text.len()
            );
        }
    }

    #[test]
    fn a_section_is_readable_by_name_and_uri() {
        let text = skill_text("query-ir/aggregate").expect("aggregate section is registered");
        assert!(
            text.starts_with("### Aggregate functions"),
            "got {text:.40}"
        );
        assert!(read_skill_resource("skill://query-ir/aggregate/SKILL.md").is_some());
    }

    #[test]
    fn errors_map_to_the_section_that_covers_them() {
        assert_eq!(query_ir_section_for("aggregate: unknown fn"), "aggregate");
        assert_eq!(query_ir_section_for("correlate needs traces"), "correlate");
        assert_eq!(query_ir_section_for("missing field `from`"), "document");
        assert_eq!(query_ir_section_for("steps must be positive"), "document");
        assert_eq!(
            query_ir_section_hint("aggs: unknown fn"),
            " (see get_skill(\"query-ir/aggregate\"))"
        );
    }

    #[test]
    fn index_json_lists_registered_skills() {
        let contents = read_skill_resource(INDEX_URI).expect("index is readable");
        let ResourceContents::TextResourceContents { text, .. } = contents else {
            panic!("index is served as text, not a blob");
        };
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(&text).expect("index is valid JSON");
        assert_eq!(parsed.len(), 1 + QUERY_IR_SECTIONS.len());
        assert_eq!(parsed[0]["name"], "query-ir");
        assert_eq!(parsed[0]["uri"], QUERY_IR_SKILL_URI);
        assert_eq!(parsed[1]["name"], "query-ir/document");
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
        let names = skill_names();
        assert_eq!(names[0], "query-ir");
        assert!(names.iter().any(|name| name == "query-ir/discovery"));
    }

    #[test]
    fn skill_summaries_match_the_registry() {
        let summaries = skill_summaries();
        assert_eq!(summaries.len(), 1 + QUERY_IR_SECTIONS.len());
        assert_eq!(summaries[0].name, "query-ir");
        assert_eq!(summaries[0].uri, QUERY_IR_SKILL_URI);
    }
}
