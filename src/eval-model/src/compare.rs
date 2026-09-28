//! # Run statistics and comparing two runs
//!
//! The run and comparison semantics of the Evaluate pages (design D4, D5),
//! mirroring the UI's `features/evals/evalModel.ts` so the CLI, the MCP
//! tools and the pages agree on every figure: per-evaluator stats folded
//! from aggregated result groups, run status, case classification between a
//! baseline and a candidate run, and the tool-trajectory diff. The shared
//! fixture `testdata/eval_compare.json` is checked by both test suites.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::{PASS_THRESHOLD, Verdict, verdict_of_label};

/// Results folded into counts. `pass + fail` can be less than
/// `results - errors`: results with neither a recognised label nor a score
/// are scored without a verdict.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EvalStats {
    pub results: u64,
    /// Results with `error.type` set: left out of pass rates and means.
    pub errors: u64,
    pub pass: u64,
    pub fail: u64,
    pub score_sum: f64,
    /// Non-error results carrying a numeric score.
    pub scored: u64,
}

/// One aggregated group of results sharing a label and error type: `high`
/// and `low` count scores at/above and below [`PASS_THRESHOLD`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResultGroup<'a> {
    pub label: Option<&'a str>,
    pub error: Option<&'a str>,
    pub n: u64,
    pub high: u64,
    pub low: u64,
    pub score_sum: f64,
    pub scored: u64,
}

impl EvalStats {
    /// Adds one result group under the pass rule (design D3): an errored
    /// group only counts as errors; a recognised label decides the whole
    /// group; otherwise each score decides against the threshold.
    pub fn fold(&mut self, g: &ResultGroup<'_>) {
        self.results += g.n;
        if g.error.is_some_and(|e| !e.is_empty()) {
            self.errors += g.n;
            return;
        }
        self.score_sum += g.score_sum;
        self.scored += g.scored;
        match g.label.and_then(verdict_of_label) {
            Some(Verdict::Pass) => self.pass += g.n,
            Some(Verdict::Fail) => self.fail += g.n,
            None => {
                self.pass += g.high;
                self.fail += g.low;
            }
        }
    }

    pub fn merge(&mut self, other: &EvalStats) {
        self.results += other.results;
        self.errors += other.errors;
        self.pass += other.pass;
        self.fail += other.fail;
        self.score_sum += other.score_sum;
        self.scored += other.scored;
    }

    /// Passes over results with a verdict, or `None` without any.
    pub fn pass_rate(&self) -> Option<f64> {
        let judged = self.pass + self.fail;
        (judged > 0).then(|| self.pass as f64 / judged as f64)
    }

    /// Mean score of the scored, non-error results.
    pub fn mean(&self) -> Option<f64> {
        (self.scored > 0).then(|| self.score_sum / self.scored as f64)
    }

    /// The verdict of one case/evaluator cell: its pass rate decides (several
    /// trials average), else no verdict.
    pub fn verdict(&self) -> Option<Verdict> {
        self.pass_rate().map(|rate| {
            if rate >= PASS_THRESHOLD {
                Verdict::Pass
            } else {
                Verdict::Fail
            }
        })
    }
}

/// What a [`StatsDelta`] measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaUnit {
    /// Change of the mean score.
    Score,
    /// Change of the pass rate (a fraction; ×100 for percentage points).
    PassRate,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct StatsDelta {
    pub d: f64,
    pub unit: DeltaUnit,
}

/// The change from `baseline` to `candidate`: the mean score's when both
/// have one, else the pass rate's when both have one.
pub fn stats_delta(baseline: &EvalStats, candidate: &EvalStats) -> Option<StatsDelta> {
    if let (Some(b), Some(c)) = (baseline.mean(), candidate.mean()) {
        return Some(StatsDelta {
            d: c - b,
            unit: DeltaUnit::Score,
        });
    }
    match (baseline.pass_rate(), candidate.pass_rate()) {
        (Some(b), Some(c)) => Some(StatsDelta {
            d: c - b,
            unit: DeltaUnit::PassRate,
        }),
        _ => None,
    }
}

// ---- runs ------------------------------------------------------------------

/// A run still receiving results is in progress until it has been quiet
/// this long (design D4).
pub const RUN_QUIET_MS: i64 = 10 * 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatusKind {
    Running,
    Complete,
    Partial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStatus {
    pub kind: RunStatusKind,
    /// Why a run is partial.
    pub reasons: Vec<String>,
}

/// Running while the newest result is under [`RUN_QUIET_MS`] old, then
/// complete, or partial with evaluator errors or results without trace
/// context.
pub fn run_status(last_ms: i64, errors: u64, unlinked: u64, now_ms: i64) -> RunStatus {
    if now_ms - last_ms < RUN_QUIET_MS {
        return RunStatus {
            kind: RunStatusKind::Running,
            reasons: Vec::new(),
        };
    }
    let mut reasons = Vec::new();
    if errors > 0 {
        reasons.push(format!("{errors} evaluator errors"));
    }
    if unlinked > 0 {
        reasons.push(format!("{unlinked} results without trace context"));
    }
    RunStatus {
        kind: if reasons.is_empty() {
            RunStatusKind::Complete
        } else {
            RunStatusKind::Partial
        },
        reasons,
    }
}

// ---- comparing two runs ----------------------------------------------------

/// A mean score has to move at least this much to count as a change.
pub const SCORE_EPSILON: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Worse,
    Better,
    Same,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseKind {
    Regression,
    Improvement,
    Unchanged,
}

/// Per evaluator stats for one case in one run.
pub type CaseScores = BTreeMap<String, EvalStats>;

/// `Math.round`, as the UI rounds: halves go towards positive infinity.
fn js_round(x: f64) -> f64 {
    (x + 0.5).floor()
}

fn direction(b: &EvalStats, c: &EvalStats) -> Direction {
    match (b.verdict(), c.verdict()) {
        (Some(Verdict::Pass), Some(Verdict::Fail)) => return Direction::Worse,
        (Some(Verdict::Fail), Some(Verdict::Pass)) => return Direction::Better,
        _ => {}
    }
    let (Some(mb), Some(mc)) = (b.mean(), c.mean()) else {
        return Direction::Same;
    };
    // Rounded so float noise (0.95 -> 0.9 is 0.04999...) doesn't decide.
    let d = js_round((mc - mb) * 1e6) / 1e6;
    if d <= -SCORE_EPSILON {
        Direction::Worse
    } else if d >= SCORE_EPSILON {
        Direction::Better
    } else {
        Direction::Same
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseComparison {
    pub kind: CaseKind,
    /// The case is in the candidate run only.
    pub no_baseline: bool,
    /// Evaluators scored in both runs, with how each moved.
    pub evaluators: BTreeMap<String, Direction>,
    /// Sum of mean-score changes across evaluators (±1 per verdict flip
    /// without means); sorts drops and gains.
    pub delta: f64,
}

/// A case is a regression when any evaluator got worse (even if another
/// improved), else an improvement when any got better, else unchanged. A
/// candidate-only case is a regression ("no baseline") when any evaluator
/// fails it, else unchanged.
pub fn classify_case(baseline: Option<&CaseScores>, candidate: &CaseScores) -> CaseComparison {
    let Some(baseline) = baseline else {
        let failing = candidate
            .values()
            .any(|s| s.verdict() == Some(Verdict::Fail));
        return CaseComparison {
            kind: if failing {
                CaseKind::Regression
            } else {
                CaseKind::Unchanged
            },
            no_baseline: true,
            evaluators: BTreeMap::new(),
            delta: 0.0,
        };
    };
    let mut evaluators = BTreeMap::new();
    let mut delta = 0.0;
    for (name, c) in candidate {
        let Some(b) = baseline.get(name) else {
            continue;
        };
        evaluators.insert(name.clone(), direction(b, c));
        match (b.mean(), c.mean()) {
            (Some(mb), Some(mc)) => delta += mc - mb,
            _ => match (b.verdict(), c.verdict()) {
                (Some(vb), Some(vc)) if vb != vc => {
                    delta += if vc == Verdict::Pass { 1.0 } else { -1.0 }
                }
                _ => {}
            },
        }
    }
    let kind = if evaluators.values().any(|d| *d == Direction::Worse) {
        CaseKind::Regression
    } else if evaluators.values().any(|d| *d == Direction::Better) {
        CaseKind::Improvement
    } else {
        CaseKind::Unchanged
    };
    CaseComparison {
        kind,
        no_baseline: false,
        evaluators,
        delta,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseRow {
    pub case_id: String,
    pub comparison: CaseComparison,
}

/// Every candidate case classified against the baseline: regressions by
/// largest drop, improvements by largest gain, unchanged by id; ties by id.
pub fn compare_cases(
    baseline: &BTreeMap<String, CaseScores>,
    candidate: &BTreeMap<String, CaseScores>,
) -> Vec<CaseRow> {
    let mut rows: Vec<CaseRow> = candidate
        .iter()
        .map(|(case_id, scores)| CaseRow {
            case_id: case_id.clone(),
            comparison: classify_case(baseline.get(case_id), scores),
        })
        .collect();
    rows.sort_by(|a, b| {
        let (ca, cb) = (&a.comparison, &b.comparison);
        ca.kind
            .cmp(&cb.kind)
            .then_with(|| {
                if ca.kind == CaseKind::Regression {
                    ca.delta.total_cmp(&cb.delta)
                } else {
                    cb.delta.total_cmp(&ca.delta)
                }
            })
            .then_with(|| a.case_id.cmp(&b.case_id))
    });
    rows
}

/// Per evaluator totals over both runs plus how many cases moved.
#[derive(Debug, Clone, PartialEq)]
pub struct EvaluatorSummary {
    pub name: String,
    pub baseline: EvalStats,
    pub candidate: EvalStats,
    pub worse: u64,
    pub better: u64,
}

/// One [`EvaluatorSummary`] per evaluator seen in either run, by name.
pub fn summarize_evaluators(
    baseline: &BTreeMap<String, CaseScores>,
    candidate: &BTreeMap<String, CaseScores>,
    rows: &[CaseRow],
) -> Vec<EvaluatorSummary> {
    let mut by_name: BTreeMap<&str, EvaluatorSummary> = BTreeMap::new();
    fn entry<'a, 'b>(
        by_name: &'b mut BTreeMap<&'a str, EvaluatorSummary>,
        name: &'a str,
    ) -> &'b mut EvaluatorSummary {
        by_name.entry(name).or_insert_with(|| EvaluatorSummary {
            name: name.to_string(),
            baseline: EvalStats::default(),
            candidate: EvalStats::default(),
            worse: 0,
            better: 0,
        })
    }
    for scores in baseline.values() {
        for (name, s) in scores {
            entry(&mut by_name, name).baseline.merge(s);
        }
    }
    for scores in candidate.values() {
        for (name, s) in scores {
            entry(&mut by_name, name).candidate.merge(s);
        }
    }
    for row in rows {
        for (name, dir) in &row.comparison.evaluators {
            let e = entry(&mut by_name, name);
            match dir {
                Direction::Worse => e.worse += 1,
                Direction::Better => e.better += 1,
                Direction::Same => {}
            }
        }
    }
    by_name.into_values().collect()
}

// ---- tool trajectories -----------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolMark {
    Same,
    Skipped,
    Reordered,
    Repeated,
    New,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolStep {
    pub name: String,
    pub kind: ToolMark,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Same,
    Removed,
    Added,
}

/// Past this many LCS cells the diff degrades to "all removed, then all
/// added", as the UI's `diffSequence` does.
const MAX_LCS_CELLS: usize = 1_000_000;

/// Longest-common-subsequence diff of two sequences; removed items precede
/// added ones at each divergence point.
fn diff_sequence<'a>(a: &'a [String], b: &'a [String]) -> Vec<(Op, &'a str)> {
    let (n, m) = (a.len(), b.len());
    if n.saturating_mul(m) > MAX_LCS_CELLS {
        return a
            .iter()
            .map(|x| (Op::Removed, x.as_str()))
            .chain(b.iter().map(|x| (Op::Added, x.as_str())))
            .collect();
    }
    // dp[i * (m + 1) + j] = LCS length of a[i..] and b[j..]
    let w = m + 1;
    let mut dp = vec![0usize; (n + 1) * w];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i * w + j] = if a[i] == b[j] {
                dp[(i + 1) * w + j + 1] + 1
            } else {
                dp[(i + 1) * w + j].max(dp[i * w + j + 1])
            };
        }
    }
    let mut out = Vec::with_capacity(n + m);
    let (mut i, mut j) = (0, 0);
    while i < n && j < m {
        if a[i] == b[j] {
            out.push((Op::Same, a[i].as_str()));
            i += 1;
            j += 1;
        } else if dp[(i + 1) * w + j] >= dp[i * w + j + 1] {
            out.push((Op::Removed, a[i].as_str()));
            i += 1;
        } else {
            out.push((Op::Added, b[j].as_str()));
            j += 1;
        }
    }
    out.extend(a[i..].iter().map(|x| (Op::Removed, x.as_str())));
    out.extend(b[j..].iter().map(|x| (Op::Added, x.as_str())));
    out
}

/// The candidate's tool calls aligned against the baseline's (LCS), in call
/// order, with baseline calls the candidate never made inserted where they
/// would have been. An extra call is "reordered" when the baseline made it
/// elsewhere, "repeated" when the baseline made it fewer times, else "new".
pub fn tool_diff(baseline: &[String], candidate: &[String]) -> Vec<ToolStep> {
    let ops = diff_sequence(baseline, candidate);
    let count = |kind: Op| {
        let mut out: HashMap<&str, usize> = HashMap::new();
        for (op, name) in &ops {
            if *op == kind {
                *out.entry(name).or_default() += 1;
            }
        }
        out
    };
    let removed = count(Op::Removed);
    let added = count(Op::Added);
    // A name both dropped and added somewhere is a move: its first `moves`
    // additions are reorders and as many removals aren't skips.
    let moves = |name: &str| {
        removed
            .get(name)
            .copied()
            .unwrap_or(0)
            .min(added.get(name).copied().unwrap_or(0))
    };
    let take = |seen: &mut HashMap<String, usize>, name: &str| {
        let used = seen.entry(name.to_string()).or_default();
        if *used >= moves(name) {
            return false;
        }
        *used += 1;
        true
    };
    let mut moved_in = HashMap::new();
    let mut moved_out = HashMap::new();
    let in_baseline: HashSet<&str> = baseline.iter().map(String::as_str).collect();
    let mut steps = Vec::with_capacity(ops.len());
    for (op, name) in &ops {
        let kind = match op {
            Op::Same => ToolMark::Same,
            Op::Added if take(&mut moved_in, name) => ToolMark::Reordered,
            Op::Added if in_baseline.contains(name) => ToolMark::Repeated,
            Op::Added => ToolMark::New,
            Op::Removed if take(&mut moved_out, name) => continue,
            Op::Removed => ToolMark::Skipped,
        };
        steps.push(ToolStep {
            name: (*name).to_string(),
            kind,
        });
    }
    steps
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(pass: u64, fail: u64, score_sum: f64, scored: u64) -> EvalStats {
        EvalStats {
            results: pass + fail,
            pass,
            fail,
            score_sum,
            scored,
            ..EvalStats::default()
        }
    }

    fn scores(entries: &[(&str, EvalStats)]) -> CaseScores {
        entries
            .iter()
            .map(|(name, s)| ((*name).to_string(), *s))
            .collect()
    }

    #[test]
    fn errors_are_counted_apart_and_never_as_failures() {
        let mut s = EvalStats::default();
        s.fold(&ResultGroup {
            error: Some("timeout"),
            n: 412,
            ..ResultGroup::default()
        });
        assert_eq!((s.results, s.errors, s.pass, s.fail), (412, 412, 0, 0));
        assert_eq!(s.pass_rate(), None);
    }

    #[test]
    fn a_recognised_label_decides_the_group_else_the_threshold_counts() {
        let mut s = EvalStats::default();
        s.fold(&ResultGroup {
            label: Some("pass"),
            n: 3,
            low: 3,
            score_sum: 0.99,
            scored: 3,
            ..ResultGroup::default()
        });
        assert_eq!((s.pass, s.fail), (3, 0));
        s.fold(&ResultGroup {
            label: Some("partial"),
            n: 4,
            high: 3,
            low: 1,
            score_sum: 2.8,
            scored: 4,
            ..ResultGroup::default()
        });
        assert_eq!((s.pass, s.fail), (6, 1));
        assert!((s.mean().unwrap_or_default() - 3.79 / 7.0).abs() < 1e-9);
    }

    #[test]
    fn run_status_follows_the_quiet_period_and_its_reasons() {
        let now = 1_000_000_000;
        let quiet = now - 12 * 60_000;
        assert_eq!(
            run_status(now - 60_000, 5, 5, now).kind,
            RunStatusKind::Running
        );
        assert_eq!(run_status(quiet, 0, 0, now).kind, RunStatusKind::Complete);
        let partial = run_status(quiet, 412, 4, now);
        assert_eq!(partial.kind, RunStatusKind::Partial);
        assert_eq!(partial.reasons.len(), 2);
    }

    #[test]
    fn stats_delta_prefers_means_then_pass_rates() {
        let d = stats_delta(&stats(0, 0, 0.9, 1), &stats(0, 0, 0.6, 1));
        assert!(
            matches!(d, Some(StatsDelta { unit: DeltaUnit::Score, d }) if (d + 0.3).abs() < 1e-9)
        );
        assert_eq!(
            stats_delta(&stats(3, 1, 0.0, 0), &stats(1, 1, 0.0, 0)),
            Some(StatsDelta {
                d: -0.25,
                unit: DeltaUnit::PassRate
            })
        );
        assert_eq!(
            stats_delta(&EvalStats::default(), &stats(1, 0, 0.0, 0)),
            None
        );
    }

    #[test]
    fn regressions_sort_by_largest_drop_first() {
        let b = BTreeMap::from([
            ("small".to_string(), scores(&[("G", stats(1, 0, 0.9, 1))])),
            ("big".to_string(), scores(&[("G", stats(1, 0, 1.0, 1))])),
        ]);
        let c = BTreeMap::from([
            ("small".to_string(), scores(&[("G", stats(1, 0, 0.8, 1))])),
            ("big".to_string(), scores(&[("G", stats(0, 1, 0.2, 1))])),
        ]);
        let rows = compare_cases(&b, &c);
        let order: Vec<_> = rows
            .iter()
            .map(|r| (r.case_id.as_str(), r.comparison.kind))
            .collect();
        assert_eq!(
            order,
            [
                ("big", CaseKind::Regression),
                ("small", CaseKind::Regression)
            ]
        );
        let summary = summarize_evaluators(&b, &c, &rows);
        assert_eq!(summary.len(), 1);
        assert_eq!((summary[0].worse, summary[0].better), (2, 0));
        assert_eq!(summary[0].baseline.pass, 2);
    }

    #[test]
    fn lcs_falls_back_to_remove_then_add_past_the_cell_cap() {
        let a: Vec<String> = (0..1001).map(|i| format!("t{i}")).collect();
        let b = a.clone();
        let steps = tool_diff(&a, &b);
        // Every name is removed then added: all moves, so all "reordered".
        assert_eq!(steps.len(), 1001);
        assert!(steps.iter().all(|s| s.kind == ToolMark::Reordered));
    }

    /// The shared comparison fixture (`testdata/eval_compare.json`), also
    /// loaded by the UI's `evalModel.test.ts`, so the Rust and TypeScript
    /// comparison rules are checked against the same cases.
    #[derive(Deserialize)]
    struct Fixture {
        cases: Vec<FixtureCase>,
        compare: Vec<FixtureCompare>,
        tool_diffs: Vec<FixtureToolDiff>,
    }

    #[derive(Deserialize)]
    struct FixtureCase {
        name: String,
        baseline: Option<CaseScores>,
        candidate: CaseScores,
        kind: CaseKind,
        no_baseline: bool,
        evaluators: BTreeMap<String, Direction>,
        delta: f64,
    }

    #[derive(Deserialize)]
    struct FixtureCompare {
        name: String,
        baseline: BTreeMap<String, CaseScores>,
        candidate: BTreeMap<String, CaseScores>,
        order: Vec<(String, CaseKind)>,
    }

    #[derive(Deserialize)]
    struct FixtureToolDiff {
        baseline: Vec<String>,
        candidate: Vec<String>,
        steps: Vec<ToolStep>,
    }

    fn fixture() -> Fixture {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/eval_compare.json"
        ));
        serde_json::from_str(raw).expect("fixture parses")
    }

    #[test]
    fn matches_the_shared_case_classification_fixture() {
        let fixture = fixture();
        assert!(!fixture.cases.is_empty());
        for case in &fixture.cases {
            let got = classify_case(case.baseline.as_ref(), &case.candidate);
            assert_eq!(got.kind, case.kind, "{}", case.name);
            assert_eq!(got.no_baseline, case.no_baseline, "{}", case.name);
            assert_eq!(got.evaluators, case.evaluators, "{}", case.name);
            assert!(
                (got.delta - case.delta).abs() < 1e-9,
                "{}: delta {} != {}",
                case.name,
                got.delta,
                case.delta
            );
        }
    }

    #[test]
    fn matches_the_shared_case_ordering_fixture() {
        for c in fixture().compare {
            let got: Vec<(String, CaseKind)> = compare_cases(&c.baseline, &c.candidate)
                .into_iter()
                .map(|r| (r.case_id, r.comparison.kind))
                .collect();
            assert_eq!(got, c.order, "{}", c.name);
        }
    }

    #[test]
    fn matches_the_shared_tool_diff_fixture() {
        for t in fixture().tool_diffs {
            assert_eq!(
                tool_diff(&t.baseline, &t.candidate),
                t.steps,
                "{:?} -> {:?}",
                t.baseline,
                t.candidate
            );
        }
    }
}
