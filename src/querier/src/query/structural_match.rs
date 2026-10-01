//! # The `match` stage's per-trace evaluator (`irVersion` 12)
//!
//! [`lower`] prunes the scan to candidate traces (each span-set matches some
//! span), tags every span with one boolean flag per span-set, and hands the
//! rows to [`StructuralMatchExec`]. The exec requires its input
//! hash-partitioned on `trace_id` and sorted by `(trace_id, start time)`, so
//! it buffers one trace at a time, evaluates the relations over that trace's
//! parent links in O(n), and emits the witnessing spans with a `spansets`
//! column, keeping the input order. A trace over `match_max_trace_spans` or
//! `match_max_trace_bytes` fails the query; it is never truncated or skipped.
//! A trace the range visibly cut (a span whose parent is not buffered, or a
//! span ending after the window) is counted in [`IncompleteTraces`] when the
//! stage has a relation; the result does not change. With a relation,
//! [`lower`] also passes on a trace that lacks a span-set but was visibly cut
//! (no in-range root, or a span ending after the window) as one null-padded
//! row marked not whole, which the exec counts as unmatched.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use common::flight::MatchIncompleteReport;
use common::query_ir::{Match, MatchOp};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, OffsetSizeTrait, RecordBatch, StringArray, make_comparator,
};
use datafusion::arrow::compute::{SortOptions, cast, concat, interleave, partition};
use datafusion::arrow::datatypes::{DataType, SchemaRef, UInt64Type};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{
    DFSchema, DFSchemaRef, DataFusionError, JoinType, Result as DFResult, ScalarValue, internal_err,
};
use datafusion::dataframe::DataFrame;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions::string::expr_fn::btrim;
use datafusion::functions_aggregate::expr_fn::bool_or;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
    cast as logical_cast, col, lit,
};
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexRequirement, OrderingRequirements, PhysicalExpr,
    PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputDistributionRequirements, Partitioning,
    PlanProperties,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;

use super::error::QuerierError;
use super::planner::with_querier_planner;

/// Reserved prefix of the internal per-span-set flag columns.
pub(crate) const FLAG_PREFIX: &str = "__match_";
const CANDIDATE_TRACE: &str = "__match_trace";
/// Whether a trace has a span for every span-set (relational stages only).
const WHOLE_TRACE: &str = "__match_whole";
const ROOTED: &str = "__match_rooted";
const OPEN_PAST_END: &str = "__match_open";
const TRACE_ID: &str = "trace_id";
const SPAN_ID: &str = "span_id";
const PARENT_SPAN_ID: &str = "parent_span_id";
const END_TIME: &str = "end_time_unix_nano";
/// How many example trace ids [`IncompleteTraces`] keeps.
const SAMPLE_TRACES: usize = 3;

/// One bit per span-set a span witnesses.
type Mask = u16;
const _: () = assert!(Match::MAX_SPANSETS <= Mask::BITS as usize);

/// `[querier].match_max_trace_spans` / `match_max_trace_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub(crate) struct MatchLimits {
    pub max_spans: usize,
    pub max_bytes: usize,
}

impl Default for MatchLimits {
    fn default() -> Self {
        let config = common::config::QuerierConfig::default();
        Self {
            max_spans: config.match_max_trace_spans,
            max_bytes: config.match_max_trace_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
struct Spec {
    names: Vec<String>,
    relations: Vec<(usize, MatchOp, usize)>,
    limits: MatchLimits,
    time_col: String,
    window_end_ns: i64,
}

/// Traces a relational `match` evaluated whose hierarchy the range visibly
/// cut, shared by every partition's evaluator and read once the plan has
/// run. A pipeline holds at most one `match` stage, so one handle covers
/// it. Only traces the evaluator finished count: a `limit` that stops the
/// stream early leaves a lower bound. Handles compare by identity.
#[derive(Debug, Clone, Default)]
pub(crate) struct IncompleteTraces(Arc<Mutex<MatchIncompleteReport>>);

impl IncompleteTraces {
    fn record(&self, matched: bool, trace: impl FnOnce() -> DFResult<String>) -> DFResult<()> {
        let mut report = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        // Matched samples sit before unmatched ones.
        let at = if matched {
            report.matched
        } else {
            report.sample_trace_ids.len() as u64
        };
        let sample = (at < SAMPLE_TRACES as u64).then(trace).transpose()?;
        *(if matched {
            &mut report.matched
        } else {
            &mut report.unmatched
        }) += 1;
        if let Some(sample) = sample {
            report.sample_trace_ids.insert(at as usize, sample);
            report.sample_trace_ids.truncate(SAMPLE_TRACES);
        }
        Ok(())
    }

    /// The counts so far, or `None` when no trace was counted.
    pub(crate) fn report(&self) -> Option<MatchIncompleteReport> {
        let report = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        (report.matched + report.unmatched > 0).then(|| report.clone())
    }
}

impl PartialEq for IncompleteTraces {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for IncompleteTraces {}

impl std::hash::Hash for IncompleteTraces {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

/// Lower a validated `match` stage over `df` (the traces scan up to
/// `window_end_ns`). `flags` holds one boolean expression per span-set, in
/// declaration order. The returned handle counts the traces the window cut.
pub(crate) fn lower(
    df: DataFrame,
    stage: &Match,
    flags: Vec<Expr>,
    time_col: &str,
    window_end_ns: i64,
    limits: MatchLimits,
) -> Result<(DataFrame, IncompleteTraces), QuerierError> {
    let internal = |msg: &str| QuerierError::QueryFailed(DataFusionError::Internal(msg.into()));
    let names: Vec<String> = stage.spansets.0.iter().map(|(n, _)| n.clone()).collect();
    let index = |name: &str| names.iter().position(|n| n == name);
    let relations = stage
        .relations
        .iter()
        .map(|r| Some((index(&r.left)?, r.op, index(&r.right)?)))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| internal("match relation names an undeclared span-set"))?;
    let flag_cols: Vec<String> = (0..flags.len())
        .map(|i| format!("{FLAG_PREFIX}{i}"))
        .collect();

    let mut flagged = df;
    for (name, flag) in flag_cols.iter().zip(flags) {
        flagged = flagged.with_column(name, flag)?;
    }
    let (Some(any), Some(all)) = (
        flag_cols.iter().map(col).reduce(Expr::or),
        flag_cols.iter().map(col).reduce(Expr::and),
    ) else {
        return Err(internal("match has no span-set"));
    };
    let set_flags = || flag_cols.iter().map(|c| bool_or(col(c)).alias(c));
    let candidates = |traces: DataFrame, keep: Expr| {
        traces
            .filter(keep)?
            .select(vec![col(TRACE_ID).alias(CANDIDATE_TRACE)])?
            .join(
                flagged.clone(),
                JoinType::RightSemi,
                &[CANDIDATE_TRACE],
                &[TRACE_ID],
                None,
            )
    };
    let kept = if relations.is_empty() {
        let traces = flagged
            .clone()
            .filter(any)?
            .aggregate(vec![col(TRACE_ID)], set_flags().collect())?;
        candidates(traces, all)?
    } else {
        // A trace missing a span-set cannot match. When one of its spans is
        // in a span-set and the range visibly cut it (no in-range span is a
        // root, or one ends after the window), it reaches the exec as a
        // single row marked not whole, which the exec counts as unmatched.
        // As in `evaluate`, an absent, empty or all-zero parent is a root.
        let parent = logical_cast(col(PARENT_SPAN_ID), DataType::Utf8);
        let root = coalesce(vec![btrim(vec![parent, lit("0")]), lit("")]).eq(lit(""));
        let mut aggs: Vec<Expr> = set_flags().collect();
        aggs.push(bool_or(root.clone()).alias(ROOTED));
        let (mut cut, mut relevant) = (!col(ROOTED), any.clone().or(root));
        if flagged.schema().has_column_with_unqualified_name(END_TIME) {
            let open = col(END_TIME).gt(lit(window_end_ns));
            aggs.push(bool_or(open.clone()).alias(OPEN_PAST_END));
            cut = cut.or(col(OPEN_PAST_END));
            relevant = relevant.or(open);
        }
        // Every per-trace flag is existential, so spans that set none of
        // them can be dropped before aggregating.
        let traces = flagged
            .clone()
            .filter(relevant)?
            .aggregate(vec![col(TRACE_ID)], aggs)?;
        let whole = coalesce(vec![all, lit(false)]);
        let whole_rows =
            candidates(traces.clone(), whole.clone())?.with_column(WHOLE_TRACE, lit(true))?;
        let schema = whole_rows.schema().clone();
        let mut padded = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let name = field.name();
            let value = if name == TRACE_ID {
                col(TRACE_ID)
            } else if name.starts_with(FLAG_PREFIX) {
                lit(false)
            } else {
                lit(ScalarValue::try_new_null(field.data_type())?)
            };
            padded.push(value.alias(name));
        }
        let cut_rows = traces.filter((!whole).and(any).and(cut))?.select(padded)?;
        whole_rows.union(cut_rows)?
    };

    let (state, input) = kept.into_parts();
    let incomplete = IncompleteTraces::default();
    let node = StructuralMatchNode {
        schema: output_schema(input.schema())?,
        input,
        spec: Spec {
            names,
            relations,
            limits,
            time_col: time_col.to_string(),
            window_end_ns,
        },
        incomplete: incomplete.clone(),
    };
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    });
    let df = DataFrame::new(with_querier_planner(state), plan).sort(vec![
        col(TRACE_ID).sort(true, true),
        col(time_col).sort(true, true),
    ])?;
    Ok((df, incomplete))
}

fn output_schema(input: &DFSchema) -> DFResult<DFSchemaRef> {
    let mut fields: Vec<_> = input
        .iter()
        .filter(|(_, f)| !f.name().starts_with(FLAG_PREFIX))
        .map(|(q, f)| (q.cloned(), Arc::clone(f)))
        .collect();
    fields.push((
        None,
        Arc::new(datafusion::arrow::datatypes::Field::new(
            Match::SPANSETS,
            DataType::Utf8,
            false,
        )),
    ));
    Ok(Arc::new(DFSchema::new_with_metadata(
        fields,
        input.metadata().clone(),
    )?))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StructuralMatchNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    spec: Spec,
    incomplete: IncompleteTraces,
}

impl PartialOrd for StructuralMatchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.spec.partial_cmp(&other.spec) {
            Some(Ordering::Equal) => self.input.partial_cmp(&other.input),
            other => other,
        }
    }
}

impl UserDefinedLogicalNodeCore for StructuralMatchNode {
    fn name(&self) -> &str {
        "StructuralMatch"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StructuralMatch: {}", self.spec.names.join(","))
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        let Ok([input]) = <[LogicalPlan; 1]>::try_from(inputs) else {
            return internal_err!("StructuralMatch takes exactly one input");
        };
        Ok(Self {
            schema: output_schema(input.schema())?,
            input,
            spec: self.spec.clone(),
            incomplete: self.incomplete.clone(),
        })
    }
}

#[derive(Debug)]
pub(super) struct StructuralMatchPlanner;

#[async_trait]
impl ExtensionPlanner for StructuralMatchPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<StructuralMatchNode>() else {
            return Ok(None);
        };
        let [input] = physical_inputs else {
            return internal_err!("StructuralMatch takes exactly one input");
        };
        let schema = Arc::new(node.schema.as_arrow().clone());
        Ok(Some(Arc::new(StructuralMatchExec::try_new(
            Arc::clone(input),
            node.spec.clone(),
            node.incomplete.clone(),
            schema,
        )?)))
    }
}

#[derive(Debug)]
struct StructuralMatchExec {
    input: Arc<dyn ExecutionPlan>,
    spec: Spec,
    incomplete: IncompleteTraces,
    schema: SchemaRef,
    /// `(trace_id, start time)` over the input.
    order: [Arc<dyn PhysicalExpr>; 2],
    properties: Arc<PlanProperties>,
}

impl StructuralMatchExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        spec: Spec,
        incomplete: IncompleteTraces,
        schema: SchemaRef,
    ) -> DFResult<Self> {
        let order_by = |schema: &SchemaRef| -> DFResult<[Arc<dyn PhysicalExpr>; 2]> {
            Ok([
                physical_col(TRACE_ID, schema)?,
                physical_col(&spec.time_col, schema)?,
            ])
        };
        // Witness rows keep the input's order, so the output is sorted the
        // same way and the final sort above only merges partitions.
        let ordering = order_by(&schema)?.map(PhysicalSortExpr::new_default);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new_with_orderings(Arc::clone(&schema), [ordering]),
            Partitioning::UnknownPartitioning(
                input.properties().output_partitioning().partition_count(),
            ),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            order: order_by(&input.schema())?,
            input,
            spec,
            incomplete,
            schema,
            properties,
        })
    }
}

impl DisplayAs for StructuralMatchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StructuralMatchExec: spansets=[{}] max_spans={} max_bytes={}",
            self.spec.names.join(","),
            self.spec.limits.max_spans,
            self.spec.limits.max_bytes
        )
    }
}

impl ExecutionPlan for StructuralMatchExec {
    fn name(&self) -> &str {
        "StructuralMatchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        let trace = Arc::clone(&self.order[0]);
        InputDistributionRequirements::new(vec![Distribution::KeyPartitioned(vec![trace])])
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let sort = self.order.iter().map(|expr| {
            PhysicalSortRequirement::new(Arc::clone(expr), Some(SortOptions::default()))
        });
        vec![LexRequirement::new(sort).map(OrderingRequirements::new)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        for expr in &self.order {
            if f(expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let Ok([input]) = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children) else {
            return internal_err!("StructuralMatchExec takes exactly one input");
        };
        let (spec, incomplete) = (self.spec.clone(), self.incomplete.clone());
        let schema = Arc::clone(&self.schema);
        Ok(Arc::new(Self::try_new(input, spec, incomplete, schema)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let reservation = MemoryConsumer::new(format!("StructuralMatchExec[{partition}]"))
            .register(context.memory_pool());
        let evaluator = Evaluator::try_new(
            &self.input.schema(),
            &self.schema,
            &self.spec,
            self.incomplete.clone(),
            reservation,
        )?;
        let stream = futures::stream::try_unfold(Some((input, evaluator)), |state| async move {
            let Some((mut input, mut evaluator)) = state else {
                return Ok(None);
            };
            while let Some(batch) = input.next().await {
                if let Some(out) = evaluator.push(&batch?)? {
                    return Ok(Some((out, Some((input, evaluator)))));
                }
            }
            Ok(evaluator.finish()?.map(|out| (out, None)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

/// Buffers the current trace's row slices; a finished trace's slices move
/// to `done` and its witnesses to `picks`, gathered once per input batch.
struct Evaluator {
    spec: Spec,
    schema: SchemaRef,
    trace: usize,
    span: usize,
    parent: usize,
    /// `None` without an end-time column: only dangling parents count then.
    end: Option<usize>,
    /// [`WHOLE_TRACE`], present when the stage has a relation.
    whole: Option<usize>,
    /// The id of the not-whole trace being skipped, already counted.
    skipped: Option<ArrayRef>,
    incomplete: IncompleteTraces,
    flags: Vec<usize>,
    kept: Vec<usize>,
    current: Vec<RecordBatch>,
    spans: usize,
    bytes: usize,
    done: Vec<RecordBatch>,
    /// The pool share of `current` and `done`; `done_bytes` of it is `done`'s.
    reservation: MemoryReservation,
    done_bytes: usize,
    /// `(index into done, row, mask)` per witness, in input order.
    picks: Vec<(usize, usize, Mask)>,
}

impl Evaluator {
    fn try_new(
        input: &SchemaRef,
        schema: &SchemaRef,
        spec: &Spec,
        incomplete: IncompleteTraces,
        reservation: MemoryReservation,
    ) -> DFResult<Self> {
        let flags = (0..spec.names.len())
            .map(|i| input.index_of(&format!("{FLAG_PREFIX}{i}")))
            .collect::<Result<Vec<_>, _>>()?;
        let kept = (0..input.fields().len())
            .filter(|&i| !input.field(i).name().starts_with(FLAG_PREFIX))
            .collect();
        Ok(Self {
            spec: spec.clone(),
            schema: Arc::clone(schema),
            trace: input.index_of(TRACE_ID)?,
            span: input.index_of(SPAN_ID)?,
            parent: input.index_of(PARENT_SPAN_ID)?,
            end: input.index_of(END_TIME).ok(),
            whole: input.index_of(WHOLE_TRACE).ok(),
            skipped: None,
            incomplete,
            flags,
            kept,
            current: Vec::new(),
            spans: 0,
            bytes: 0,
            done: Vec::new(),
            reservation,
            done_bytes: 0,
            picks: Vec::new(),
        })
    }

    /// Buffer `batch`, returning the witness rows of every trace it completed.
    fn push(&mut self, batch: &RecordBatch) -> DFResult<Option<RecordBatch>> {
        let traces = batch.column(self.trace);
        for (i, range) in partition(std::slice::from_ref(traces))?
            .ranges()
            .into_iter()
            .enumerate()
        {
            let continues = i == 0 && self.continues(traces.as_ref())?;
            if !continues {
                self.finish_trace()?;
                self.skipped = None;
            }
            let slice = batch.slice(range.start, range.len());
            if self.is_whole(&slice) {
                self.admit(&slice)?;
                self.current.push(slice);
            } else if !continues {
                // It lacks a span-set, so it cannot match; `lower` only
                // lets it through when the range visibly cut it.
                self.incomplete.record(false, || self.trace_id(&slice))?;
                self.skipped = Some(slice.column(self.trace).slice(0, 1));
            }
        }
        self.gather()
    }

    fn finish(&mut self) -> DFResult<Option<RecordBatch>> {
        self.finish_trace()?;
        self.gather()
    }

    /// Whether `slice`'s trace has a span for every span-set.
    fn is_whole(&self, slice: &RecordBatch) -> bool {
        self.whole.is_none_or(|i| {
            let whole = slice.column(i).as_boolean_opt();
            whole.is_none_or(|w| w.is_valid(0) && w.value(0))
        })
    }

    /// Whether `traces[0]` is the trace being buffered or skipped.
    fn continues(&self, traces: &dyn Array) -> DFResult<bool> {
        let prev = match (self.current.last(), &self.skipped) {
            (Some(last), _) => last.column(self.trace),
            (None, Some(skipped)) => skipped,
            (None, None) => return Ok(false),
        };
        let cmp = make_comparator(prev.as_ref(), traces, SortOptions::default())?;
        Ok(cmp(prev.len() - 1, 0) == Ordering::Equal)
    }

    /// Admit one more same-trace slice before buffering it, or fail naming
    /// the trace and the bound. Flag columns do not count toward the bytes.
    fn admit(&mut self, slice: &RecordBatch) -> DFResult<()> {
        let bytes = self
            .kept
            .iter()
            .map(|&i| value_bytes(slice.column(i).as_ref()))
            .sum::<DFResult<usize>>()?;
        let (spans, total) = (self.spans + slice.num_rows(), self.bytes + bytes);
        let limits = self.spec.limits;
        let over = if spans > limits.max_spans {
            format!(
                "exceeds the span bound of {} ([querier].match_max_trace_spans): it has at \
                 least {spans} spans",
                limits.max_spans
            )
        } else if total > limits.max_bytes {
            format!(
                "exceeds the byte bound of {} ([querier].match_max_trace_bytes): it has at \
                 least {total} bytes",
                limits.max_bytes
            )
        } else {
            match self.reservation.try_grow(bytes) {
                Ok(()) => {
                    (self.spans, self.bytes) = (spans, total);
                    return Ok(());
                }
                Err(e) => format!("does not fit the query memory pool at {total} bytes: {e}"),
            }
        };
        let trace = self.trace_id(slice)?;
        Err(DataFusionError::External(Box::new(
            QuerierError::ResourceExhausted(format!(
                "match: trace {trace} {over}; a trace is never evaluated partially, so narrow \
                 the range or raise the bound"
            )),
        )))
    }

    fn trace_id(&self, slice: &RecordBatch) -> DFResult<String> {
        let ids = slice.column(self.trace).as_ref();
        Ok(ArrayFormatter::try_new(ids, &FormatOptions::default())?
            .value(0)
            .to_string())
    }

    fn finish_trace(&mut self) -> DFResult<()> {
        self.done_bytes += self.bytes;
        (self.spans, self.bytes) = (0, 0);
        let slices = std::mem::take(&mut self.current);
        if slices.is_empty() {
            return Ok(());
        }
        let column = |i: usize| -> DFResult<ArrayRef> {
            let parts: Vec<&dyn Array> = slices.iter().map(|s| s.column(i).as_ref()).collect();
            Ok(concat(&parts)?)
        };
        let flags = (self.flags.iter().map(|&i| column(i))).collect::<DFResult<Vec<_>>>()?;
        let (spans, parents) = (column(self.span)?, column(self.parent)?);
        let (masks, dangling) = trace_masks(&self.spec.relations, &spans, &parents, &flags)?;
        if !self.spec.relations.is_empty() {
            let end_limit = u64::try_from(self.spec.window_end_ns).unwrap_or(0);
            let open = || -> DFResult<bool> {
                let Some(end) = self.end else {
                    return Ok(false);
                };
                for slice in &slices {
                    let ends = cast(slice.column(end), &DataType::UInt64)?;
                    let mut ends = ends.as_primitive::<UInt64Type>().iter();
                    if ends.any(|end| end.is_some_and(|end| end > end_limit)) {
                        return Ok(true);
                    }
                }
                Ok(false)
            };
            if dangling || open()? {
                self.incomplete
                    .record(masks.is_some(), || self.trace_id(&slices[0]))?;
            }
        }
        if let Some(masks) = masks {
            let base = self.done.len();
            let rows = slices
                .iter()
                .enumerate()
                .flat_map(|(s, slice)| (0..slice.num_rows()).map(move |row| (base + s, row)));
            let witnesses = rows.zip(masks).filter(|(_, mask)| *mask != 0);
            self.picks
                .extend(witnesses.map(|((batch, row), mask)| (batch, row, mask)));
        }
        self.done.extend(slices);
        Ok(())
    }

    fn gather(&mut self) -> DFResult<Option<RecordBatch>> {
        self.reservation
            .shrink(std::mem::take(&mut self.done_bytes));
        let done = std::mem::take(&mut self.done);
        let picks = std::mem::take(&mut self.picks);
        if picks.is_empty() {
            return Ok(None);
        }
        let rows: Vec<(usize, usize)> = picks.iter().map(|&(b, r, _)| (b, r)).collect();
        let mut columns = self
            .kept
            .iter()
            .map(|&i| {
                let arrays: Vec<&dyn Array> = done.iter().map(|b| b.column(i).as_ref()).collect();
                interleave(&arrays, &rows)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut labels: HashMap<Mask, String> = HashMap::new();
        let spansets: StringArray = picks
            .iter()
            .map(|&(.., mask)| {
                Some(
                    labels
                        .entry(mask)
                        .or_insert_with(|| self.label(mask))
                        .clone(),
                )
            })
            .collect();
        columns.push(Arc::new(spansets));
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            columns,
        )?))
    }

    fn label(&self, mask: Mask) -> String {
        let names = self.spec.names.iter().enumerate();
        names
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, n)| n.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// The value bytes of `array`: fixed widths plus variable-length value
/// lengths (a view counts its 16-byte slot too), recursing into nested
/// types. Additive over slices, so a trace costs the same however it is split.
fn value_bytes(array: &dyn Array) -> DFResult<usize> {
    fn child<O: OffsetSizeTrait>(offsets: &[O], values: &dyn Array) -> DFResult<usize> {
        let (Some(start), Some(end)) = (offsets.first(), offsets.last()) else {
            return Ok(0);
        };
        let (start, end) = (start.as_usize(), end.as_usize());
        value_bytes(values.slice(start, end - start).as_ref())
    }
    fn span<O: OffsetSizeTrait>(offsets: &[O]) -> usize {
        match (offsets.first(), offsets.last()) {
            (Some(start), Some(end)) => (*end - *start).as_usize(),
            _ => 0,
        }
    }
    let len = array.len();
    if let Some(width) = array.data_type().primitive_width() {
        return Ok(width * len);
    }
    Ok(match array.data_type() {
        DataType::Null | DataType::Boolean => len,
        DataType::FixedSizeBinary(width) => *width as usize * len,
        DataType::Utf8 => span(array.as_string::<i32>().value_offsets()),
        DataType::LargeUtf8 => span(array.as_string::<i64>().value_offsets()),
        DataType::Binary => span(array.as_binary::<i32>().value_offsets()),
        DataType::LargeBinary => span(array.as_binary::<i64>().value_offsets()),
        DataType::Utf8View => {
            let values = array.as_string_view().iter().flatten().map(str::len);
            16 * len + values.sum::<usize>()
        }
        DataType::BinaryView => {
            let values = array.as_binary_view().iter().flatten().map(<[u8]>::len);
            16 * len + values.sum::<usize>()
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            child(list.value_offsets(), list.values().as_ref())?
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            child(list.value_offsets(), list.values().as_ref())?
        }
        DataType::Map(..) => child(array.as_map().value_offsets(), array.as_map().entries())?,
        DataType::Struct(_) => (array.as_struct().columns().iter())
            .map(|c| value_bytes(c.as_ref()))
            .sum::<DFResult<usize>>()?,
        _ => array.to_data().get_slice_memory_size()?,
    })
}

/// [`evaluate`] over one trace's `span_id`, `parent_span_id` and per-span-set
/// flag columns.
fn trace_masks(
    relations: &[(usize, MatchOp, usize)],
    spans: &ArrayRef,
    parents: &ArrayRef,
    flags: &[ArrayRef],
) -> DFResult<(Option<Vec<Mask>>, bool)> {
    let [spans, parents] = [spans, parents].map(|ids| cast(ids, &DataType::Binary));
    let (spans, parents) = (spans?, parents?);
    fn ids(a: &ArrayRef) -> Vec<Option<&[u8]>> {
        let ids = a.as_binary::<i32>().iter();
        ids.map(|v| v.filter(|v| !v.is_empty())).collect()
    }
    let flags = flags
        .iter()
        .enumerate()
        .map(|(i, flag)| {
            let Some(flag) = flag.as_boolean_opt() else {
                return internal_err!("match flag of span-set {i} is not Boolean");
            };
            Ok(flag.iter().map(|v| v == Some(true)).collect())
        })
        .collect::<DFResult<Vec<Vec<bool>>>>()?;
    Ok(evaluate(relations, &ids(&spans), &ids(&parents), &flags))
}

/// The per-trace work of `anc descendant desc`, for the
/// `structural_ancestry` benchmark; not an API.
#[cfg(feature = "benchmarks")]
pub fn bench_descendant_masks(
    spans: &ArrayRef,
    parents: &ArrayRef,
    anc: &ArrayRef,
    desc: &ArrayRef,
) -> DFResult<Option<Vec<u16>>> {
    let flags = [Arc::clone(anc), Arc::clone(desc)];
    trace_masks(&[(0, MatchOp::Descendant, 1)], spans, parents, &flags).map(|(masks, _)| masks)
}

/// Per row, a bitmask of the span-sets it witnesses (`None` when the trace
/// does not match), and whether a span names a parent the trace lacks. Rows
/// sharing a `span_id` (a redelivered span) are one node: their flags are
/// OR'd and they witness together.
fn evaluate(
    relations: &[(usize, MatchOp, usize)],
    span_ids: &[Option<&[u8]>],
    parent_ids: &[Option<&[u8]>],
    row_flags: &[Vec<bool>],
) -> (Option<Vec<Mask>>, bool) {
    if row_flags.iter().any(|f| !f.contains(&true)) {
        return (None, false);
    }
    let mut index: HashMap<&[u8], usize> = HashMap::new();
    let (mut node_of, mut node_ids) = (Vec::with_capacity(span_ids.len()), Vec::new());
    let mut n = 0;
    for id in span_ids {
        let node = id.map_or(n, |id| *index.entry(id).or_insert(n));
        if node == n {
            node_ids.push(*id);
            n += 1;
        }
        node_of.push(node);
    }
    let mut parent_ids_of: Vec<Option<&[u8]>> = vec![None; n];
    for (&node, parent) in node_of.iter().zip(parent_ids) {
        parent_ids_of[node] = parent_ids_of[node].or(*parent);
    }
    let flags: Vec<Vec<bool>> = row_flags
        .iter()
        .map(|rows| {
            let mut nodes = vec![false; n];
            for (&node, &flag) in node_of.iter().zip(rows) {
                nodes[node] |= flag;
            }
            nodes
        })
        .collect();
    let mut dangling = false;
    let mut parent: Vec<Option<usize>> = (0..n)
        .map(|i| {
            let p = parent_ids_of[i]?;
            let found = index.get(p).copied();
            // Storage writes a root's absent parent as an all-zero id.
            dangling |= found.is_none() && p.iter().any(|&b| b != b'0' && b != 0);
            found.filter(|&p| p != i)
        })
        .collect();
    let order = break_cycles(&mut parent, &node_ids);

    let mut masks = vec![0; n];
    let mut related = vec![false; flags.len()];
    for &(l, op, r) in relations {
        let (fl, fr) = (&flags[l], &flags[r]);
        let (left, right) = match op {
            MatchOp::Child => {
                let (mut left, mut right) = (vec![false; n], vec![false; n]);
                for c in 0..n {
                    if let Some(p) = parent[c]
                        && fr[c]
                        && fl[p]
                    {
                        right[c] = true;
                        left[p] = true;
                    }
                }
                (left, right)
            }
            MatchOp::Descendant => descendant(fl, fr, &parent, &order),
            MatchOp::Ancestor => {
                let (ancestors, descendants) = descendant(fr, fl, &parent, &order);
                (descendants, ancestors)
            }
            MatchOp::Sibling => sibling(fl, fr, &parent_ids_of),
        };
        if !left.contains(&true) {
            return (None, dangling);
        }
        for i in 0..n {
            masks[i] |= (Mask::from(left[i]) << l) | (Mask::from(right[i]) << r);
        }
        related[l] = true;
        related[r] = true;
    }
    for (s, flag) in flags.iter().enumerate() {
        if !related[s] {
            for i in 0..n {
                masks[i] |= Mask::from(flag[i]) << s;
            }
        }
    }
    (
        Some(node_of.iter().map(|&node| masks[node]).collect()),
        dangling,
    )
}

/// Cut each parent cycle at its smallest span id, which becomes a root, and
/// return every span ordered parents before children.
fn break_cycles(parent: &mut [Option<usize>], ids: &[Option<&[u8]>]) -> Vec<usize> {
    let (new, on_path, done) = (0u8, 1u8, 2u8);
    let mut state = vec![new; parent.len()];
    let mut order = Vec::with_capacity(parent.len());
    for start in 0..parent.len() {
        let mut path = Vec::new();
        let mut at = Some(start);
        while let Some(i) = at {
            if state[i] == done {
                break;
            }
            if state[i] == on_path {
                let cycle = path.iter().skip_while(|&&p| p != i);
                if let Some(&root) = cycle.min_by_key(|&&p| ids[p]) {
                    parent[root] = None;
                }
                path.iter().for_each(|&p| state[p] = new);
                (path, at) = (Vec::new(), Some(start));
                continue;
            }
            state[i] = on_path;
            path.push(i);
            at = parent[i];
        }
        for &i in path.iter().rev() {
            state[i] = done;
            order.push(i);
        }
    }
    order
}

/// Witnesses of "a `desc` span has a proper ancestor that is an `anc` span":
/// (the `anc` spans with such a descendant, the `desc` spans with such an
/// ancestor). One top-down and one bottom-up pass over the forest.
fn descendant(
    anc: &[bool],
    desc: &[bool],
    parent: &[Option<usize>],
    order: &[usize],
) -> (Vec<bool>, Vec<bool>) {
    let n = anc.len();
    let mut has_anc = vec![false; n];
    for &i in order {
        if let Some(p) = parent[i] {
            has_anc[i] = has_anc[p] || anc[p];
        }
    }
    let mut has_desc = vec![false; n];
    for &i in order.iter().rev() {
        if let Some(p) = parent[i]
            && (desc[i] || has_desc[i])
        {
            has_desc[p] = true;
        }
    }
    (
        (0..n).map(|i| anc[i] && has_desc[i]).collect(),
        (0..n).map(|i| desc[i] && has_anc[i]).collect(),
    )
}

/// Spans sharing a non-empty `parent_span_id` (present in the trace or not).
fn sibling(left: &[bool], right: &[bool], parent_ids: &[Option<&[u8]>]) -> (Vec<bool>, Vec<bool>) {
    let mut counts: HashMap<&[u8], (usize, usize)> = HashMap::new();
    for (i, p) in parent_ids.iter().enumerate() {
        if let Some(p) = p {
            let c = counts.entry(p).or_default();
            c.0 += usize::from(left[i]);
            c.1 += usize::from(right[i]);
        }
    }
    let witness = |own: &[bool], other: &[bool], pick: fn(&(usize, usize)) -> usize| {
        (0..own.len())
            .map(|i| {
                own[i]
                    && parent_ids[i]
                        .and_then(|p| counts.get(p))
                        .is_some_and(|c| pick(c) > usize::from(other[i]))
            })
            .collect()
    };
    (witness(left, right, |c| c.1), witness(right, left, |c| c.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::IrQueryParams;
    use crate::query::ir_planner::IrService;
    use common::flight::{MatchIncompleteReport, QueryReport};
    use datafusion::arrow::array::{BooleanArray, Int64Array, StringViewArray, UInt64Array};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::catalog::{CatalogProvider, MemTable, SchemaProvider};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use serde_json::{Value, json};

    /// `[trace_id, span_id, parent_span_id ("" = none), span_name]`; start
    /// time is the row's position within its trace, and so is the end time,
    /// except that a span named [`OPEN`] ends 1ns after the test range and
    /// one named [`EDGE`] ends exactly at it.
    type Span = [String; 4];

    fn span(trace: &str, id: &str, parent: &str, name: &str) -> Span {
        [trace, id, parent, name].map(str::to_string)
    }

    /// A root named `root`, `depth - 1` `hop`s, then a `write` at `depth`.
    fn chain(trace: &str, depth: usize) -> Vec<Span> {
        (0..=depth)
            .map(|i| {
                let name = [(0, "root"), (depth, "write")]
                    .into_iter()
                    .find_map(|(at, name)| (at == i).then_some(name));
                let parent = if i == 0 {
                    String::new()
                } else {
                    format!("{trace}-{}", i - 1)
                };
                span(
                    trace,
                    &format!("{trace}-{i}"),
                    &parent,
                    name.unwrap_or("hop"),
                )
            })
            .collect()
    }

    const OPEN: &str = "open";
    const EDGE: &str = "edge";

    fn batch(spans: &[Span]) -> RecordBatch {
        let names = ["trace_id", "span_id", "parent_span_id", "span_name"];
        let mut fields: Vec<_> = names.map(|n| Field::new(n, DataType::Utf8, true)).into();
        fields.push(Field::new("start_time_unix_nano", DataType::Int64, false));
        let mut columns: Vec<ArrayRef> = (0..4)
            .map(|c| {
                let values = spans
                    .iter()
                    .map(|s| Some(s[c].as_str()).filter(|v| !v.is_empty()));
                Arc::new(values.collect::<StringArray>()) as ArrayRef
            })
            .collect();
        let starts =
            (0..spans.len()).map(|i| spans[..i].iter().filter(|s| s[0] == spans[i][0]).count());
        let starts: Vec<i64> = starts.map(|n| n as i64).collect();
        let ends = spans.iter().zip(&starts);
        let ends = ends.map(|(s, &start)| match s[3].as_str() {
            OPEN => 1001,
            EDGE => 1000,
            _ => start as u64,
        });
        let ends: UInt64Array = ends.collect();
        fields.push(Field::new("end_time_unix_nano", DataType::UInt64, false));
        columns.push(Arc::new(Int64Array::from(starts)));
        columns.push(Arc::new(ends));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn ctx(spans: &[Span]) -> SessionContext {
        ctx_of(batch(spans))
    }

    fn ctx_of(batch: RecordBatch) -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let sp = Arc::new(MemorySchemaProvider::new());
        sp.register_table("traces".to_string(), Arc::new(table))
            .unwrap();
        let cat = Arc::new(MemoryCatalogProvider::new());
        cat.register_schema("d", sp).unwrap();
        ctx.register_catalog("t", cat);
        ctx
    }

    fn name_is(name: &str) -> Value {
        json!({ "field": "name", "op": "eq", "value": name })
    }

    fn params(stage: Value, rest: &[Value]) -> IrQueryParams {
        let pipeline: Vec<Value> = [json!({ "match": stage })]
            .into_iter()
            .chain(rest.to_vec())
            .collect();
        let document = json!({
            "irVersion": 12, "from": "traces", "range": { "from": 0, "to": 1000 },
            "result": "rows", "pipeline": pipeline
        });
        IrQueryParams {
            document,
            now_ns: 0,
            page: None,
        }
    }

    fn relation(left: &str, op: &str, right: &str) -> Value {
        let sets = serde_json::Map::from_iter([left, right].map(|n| (n.to_string(), name_is(n))));
        json!({ "spansets": sets, "relations": [{ "left": left, "op": op, "right": right }] })
    }

    /// `"<first column>:<second column>"` per returned row, in result order.
    async fn run(ctx: SessionContext, params: IrQueryParams, columns: [&str; 2]) -> Vec<String> {
        run_reporting(ctx, params, columns).await.0
    }

    async fn run_reporting(
        ctx: SessionContext,
        params: IrQueryParams,
        columns: [&str; 2],
    ) -> (Vec<String>, QueryReport) {
        let (batches, _, report) = IrService::new(ctx).query(&params, "t", "d").await.unwrap();
        let text = |b: &RecordBatch, c: &str, i: usize| {
            let column = b.column_by_name(c).unwrap();
            let format = ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default());
            format.unwrap().value(i).to_string()
        };
        let rows = batches.iter().flat_map(|b| {
            (0..b.num_rows())
                .map(move |i| format!("{}:{}", text(b, columns[0], i), text(b, columns[1], i)))
        });
        (rows.collect(), report)
    }

    const WITNESS: [&str; 2] = ["span_id", "spansets"];

    /// `d1`/`d5`/`d200` hold a write at that depth below the root; `miss` a
    /// root and a write in separate subtrees; `c` a parent cycle; `o`
    /// orphaned writes (`c` and `c2` hold the same cycle in opposite row
    /// orders); `s` siblings; `n` a nested pair; `k` a chain below a
    /// parent cycle; `u` a duplicated child; `v` a duplicated parent.
    fn fixture() -> Vec<Span> {
        let mut spans = [chain("d1", 1), chain("d5", 5), chain("d200", 200)].concat();
        spans.extend(
            [
                ["miss", "m0", "", "root"],
                ["miss", "m1", "", "other"],
                ["miss", "m2", "m1", "write"],
                ["c", "cx", "cy", "root"],
                ["c", "cy", "cx", "write"],
                ["o", "or", "", "root"],
                ["o", "ow", "gone", "write"],
                ["o", "oz", "oz", "write"],
                ["s", "sp", "", "p"],
                ["s", "sa", "sp", "a"],
                ["s", "sb", "sp", "b"],
                ["n", "np", "", "p"],
                ["n", "na", "np", "a"],
                ["n", "nb", "na", "b"],
                ["c2", "c2y", "c2x", "write"],
                ["c2", "c2x", "c2y", "root"],
                ["k", "ka", "kb", "x"],
                ["k", "kb", "ka", "x"],
                ["k", "kc", "ka", "mid"],
                ["k", "kd", "kc", "leaf"],
                ["u", "up", "", "p"],
                ["u", "ua", "up", "a"],
                ["u", "ua", "up", "a"],
                ["v", "vr", "", "dparent"],
                ["v", "vr", "", "dparent"],
                ["v", "vc", "vr", "dchild"],
            ]
            .map(|[t, id, parent, name]| span(t, id, parent, name)),
        );
        spans
    }

    /// `a -> b -> c -> a` is cut at its smallest id, `a`, which becomes the
    /// root; `e`'s parent is not in the trace, so `e` is a root too.
    #[test]
    fn trace_masks_cuts_cycles_and_roots_dangling_parents() {
        let ids = |v: &[&str]| -> ArrayRef { Arc::new(StringArray::from(v.to_vec())) };
        let flags = |v: &[bool]| -> ArrayRef {
            Arc::new(datafusion::arrow::array::BooleanArray::from(v.to_vec()))
        };
        let spans = ids(&["a", "b", "c", "e"]);
        let parents = ids(&["c", "a", "b", "zz"]);
        let descendant = [(0, MatchOp::Descendant, 1)];
        let masks = |anc: &[bool], desc: &[bool]| {
            trace_masks(&descendant, &spans, &parents, &[flags(anc), flags(desc)])
                .expect("trace_masks")
        };

        let a_over_c_and_e = masks(&[true, false, false, false], &[false, false, true, true]);
        assert_eq!(a_over_c_and_e, (Some(vec![0b01, 0, 0b10, 0]), true));
        let (c_over_a, _) = masks(&[false, false, true, false], &[true, false, false, false]);
        assert_eq!(c_over_a, None);
        let (e_over_a, _) = masks(&[false, false, false, true], &[true, false, false, false]);
        assert_eq!(e_over_a, None);
    }

    #[tokio::test]
    async fn relations_return_their_witnesses_per_trace() {
        let deep = [
            "cx:root",
            "cy:write",
            "c2y:write",
            "c2x:root",
            "d1-0:root",
            "d1-1:write",
            "d200-0:root",
            "d200-200:write",
            "d5-0:root",
            "d5-5:write",
        ];
        let same_set = json!({
            "spansets": { "x": name_is("a"), "y": name_is("a") },
            "relations": [{ "left": "x", "op": "sibling", "right": "y" }]
        });
        let cases = [
            (relation("root", "descendant", "write"), deep.to_vec()),
            (
                relation("root", "child", "write"),
                vec![
                    "cx:root",
                    "cy:write",
                    "c2y:write",
                    "c2x:root",
                    "d1-0:root",
                    "d1-1:write",
                ],
            ),
            (relation("write", "ancestor", "root"), deep.to_vec()),
            (relation("a", "sibling", "b"), vec!["sa:a", "sb:b"]),
            (same_set, vec![]),
            (relation("mid", "child", "leaf"), vec!["kc:mid", "kd:leaf"]),
            (
                relation("mid", "descendant", "leaf"),
                vec!["kc:mid", "kd:leaf"],
            ),
            (
                relation("dparent", "child", "dchild"),
                vec!["vr:dparent", "vr:dparent", "vc:dchild"],
            ),
        ];
        for (stage, expected) in cases {
            let got = run(ctx(&fixture()), params(stage.clone(), &[]), WITNESS).await;
            assert_eq!(got, expected, "{stage}");
        }
    }

    #[tokio::test]
    async fn spansets_names_every_set_a_span_witnesses_in_declaration_order() {
        let stage = json!({
            "spansets": {
                "root": name_is("root"),
                "write": name_is("write"),
                "hop": name_is("hop"),
                "any": { "field": "name", "op": "in", "value": ["root", "write"] }
            },
            "relations": [{ "left": "root", "op": "descendant", "right": "write" }]
        });
        let got = run(ctx(&chain("d3", 3)), params(stage, &[]), WITNESS).await;
        assert_eq!(
            got,
            ["d3-0:root,any", "d3-1:hop", "d3-2:hop", "d3-3:write,any"]
        );
    }

    #[tokio::test]
    async fn later_stages_read_the_spansets_column() {
        let stage = relation("root", "descendant", "write");
        let only_writes = json!({ "where": { "field": "spansets", "op": "eq", "value": "write" } });
        let by_set = json!({ "order": [{ "of": "spansets", "dir": "desc" }] });
        let filtered = params(stage.clone(), &[only_writes, by_set]);
        assert_eq!(
            run(ctx(&chain("d2", 2)), filtered, WITNESS).await,
            ["d2-2:write"]
        );

        let count =
            json!({ "aggregate": { "by": ["spansets"], "aggs": [{ "fn": "count", "as": "n" }] } });
        let mut grouped = params(stage, &[count]);
        grouped.document["result"] = json!("table");
        let mut counts = run(ctx(&fixture()), grouped, ["spansets", "n"]).await;
        counts.sort();
        assert_eq!(counts, ["root:5", "write:5"]);
    }

    #[tokio::test]
    async fn a_trace_over_a_bound_fails_naming_it_and_one_at_it_passes() {
        let spans = [chain("d200", 200), chain("d1", 1)].concat();
        let d200 = batch(&chain("d200", 200));
        let bytes: usize = d200
            .columns()
            .iter()
            .map(|c| value_bytes(c.as_ref()).unwrap())
            .sum();
        let (span_key, byte_key) = ("match_max_trace_spans", "match_max_trace_bytes");
        let cases = [
            (201, usize::MAX, None),
            (200, usize::MAX, Some(span_key)),
            (usize::MAX, bytes, None),
            (usize::MAX, bytes - 1, Some(byte_key)),
        ];
        for (max_spans, max_bytes, key) in cases {
            let result = IrService::new(ctx(&spans))
                .with_match_limits(max_spans, max_bytes)
                .query(
                    &params(relation("root", "descendant", "write"), &[]),
                    "t",
                    "d",
                )
                .await;
            match (result, key) {
                (Ok(_), None) => {}
                (Err(QuerierError::ResourceExhausted(msg)), Some(key)) => {
                    let named =
                        msg.contains("trace d200") && msg.contains(&format!("[querier].{key}"));
                    assert!(named, "{msg}");
                }
                (other, _) => panic!("{max_spans}/{max_bytes}: unexpected {:?}", other.err()),
            }
        }
    }

    #[tokio::test]
    async fn the_exec_reads_one_trace_at_a_time_from_a_trace_partitioned_sorted_input() {
        let params = params(relation("root", "descendant", "write"), &[]);
        let d = serde_json::from_value(params.document).unwrap();
        let svc = IrService::new(ctx(&fixture()));
        let (df, _) = svc.plan(&d, "t", "d", 0).await.unwrap().unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        let lines: Vec<&str> = text.lines().map(str::trim).collect();
        let at = lines
            .iter()
            .position(|l| l.starts_with("StructuralMatchExec"))
            .unwrap();
        let expected = [
            "SortExec: expr=[trace_id@0 ASC, start_time_unix_nano@4 ASC]",
            "RepartitionExec: partitioning=Hash([trace_id@0], 4)",
        ];
        let below_is_expected = expected
            .iter()
            .zip(&lines[at + 1..])
            .all(|(e, l)| l.starts_with(e));
        assert!(below_is_expected, "{text}");
        assert!(
            !lines[..at].iter().any(|l| l.starts_with("SortExec")),
            "{text}"
        );
    }

    fn long_payloads(spans: &[Span]) -> RecordBatch {
        let batch = batch(spans);
        let payload: StringViewArray = spans.iter().map(|_| Some("x".repeat(1000))).collect();
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("payload", DataType::Utf8View, false)));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(payload));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[tokio::test]
    async fn view_typed_payloads_count_toward_the_byte_bound() {
        let result = IrService::new(ctx_of(long_payloads(&chain("v1", 1))))
            .with_match_limits(usize::MAX, 1500)
            .query(
                &params(relation("root", "descendant", "write"), &[]),
                "t",
                "d",
            )
            .await;
        match result {
            Err(QuerierError::ResourceExhausted(msg)) => assert!(msg.contains("trace v1"), "{msg}"),
            other => panic!("expected the byte bound, got {:?}", other.map(|_| ())),
        }
    }

    /// An `Evaluator` over `spans` with long payloads and one all-true flag.
    fn evaluator(spans: &[Span], pool_bytes: usize) -> (Evaluator, RecordBatch) {
        let batch = long_payloads(spans);
        let flag = datafusion::arrow::array::BooleanArray::from(vec![true; spans.len()]);
        let mut fields = batch.schema().fields().to_vec();
        let mut output = fields.clone();
        fields.push(Arc::new(Field::new("__match_0", DataType::Boolean, false)));
        output.push(Arc::new(Field::new(Match::SPANSETS, DataType::Utf8, false)));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(flag));
        let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let spec = Spec {
            names: vec!["root".into()],
            relations: Vec::new(),
            limits: MatchLimits::default(),
            time_col: "start_time_unix_nano".into(),
            window_end_ns: 1000,
        };
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let reservation = MemoryConsumer::new("test").register(&pool);
        let output = Arc::new(Schema::new(output));
        let evaluator = Evaluator::try_new(
            &input.schema(),
            &output,
            &spec,
            IncompleteTraces::default(),
            reservation,
        )
        .unwrap();
        (evaluator, input)
    }

    #[test]
    fn the_trace_buffer_is_reserved_from_the_memory_pool() {
        let (mut small, input) = evaluator(&chain("m1", 1), 1000);
        match QuerierError::from(small.push(&input).unwrap_err()) {
            QuerierError::ResourceExhausted(m) => {
                assert!(m.contains("trace m1") && m.contains("memory pool"), "{m}");
            }
            other => panic!("expected a resource error, got {other:?}"),
        }

        let (mut large, input) = evaluator(&[chain("m1", 1), chain("m2", 1)].concat(), 1 << 20);
        let emitted = large.push(&input).unwrap().unwrap();
        assert_eq!(emitted.num_rows(), 2, "m1 is complete");
        let m2 = large.current.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!((m2, large.reservation.size()), (2, large.bytes));
        assert!(large.bytes > 2000);
        large.finish().unwrap();
        assert_eq!(large.reservation.size(), 0);
    }

    #[tokio::test]
    async fn span_sets_filter_on_events_and_links() {
        let spans = [
            ["e1", "e1r", "", "root"],
            ["e1", "e1m", "e1r", "hop"],
            ["e1", "e1l", "e1m", "leaf"],
            ["e2", "e2r", "", "root"],
            ["e2", "e2l", "e2r", "leaf"],
            ["e3", "e3r", "", "root"],
            ["e3", "e3l", "e3r", "leaf"],
        ]
        .map(|[t, id, parent, name]| span(t, id, parent, name));
        let exception = r#"[{"name":"exception"}]"#;
        let link = |trace: &str| format!(r#"[{{"trace_id":"{trace}","span_id":"bbbb"}}]"#);
        let events = [
            Some(exception),
            None,
            None,
            Some(exception),
            None,
            None,
            None,
        ];
        let links = [
            None,
            None,
            Some(link("aaaa")),
            None,
            Some(link("cccc")),
            None,
            Some(link("aaaa")),
        ];
        let batch = batch(&spans);
        let mut fields = batch.schema().fields().to_vec();
        fields.extend(["events", "links"].map(|n| Arc::new(Field::new(n, DataType::Utf8, true))));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(StringArray::from(events.to_vec())));
        columns.push(Arc::new(links.into_iter().collect::<StringArray>()));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let stage = json!({
            "spansets": {
                "failed": { "field": "events.name", "op": "eq", "value": "exception" },
                "linked": { "field": "links.trace_id", "op": "eq", "value": "aaaa" }
            },
            "relations": [{ "left": "failed", "op": "descendant", "right": "linked" }]
        });
        let got = run(ctx_of(batch), params(stage, &[]), WITNESS).await;
        assert_eq!(got, ["e1r:failed", "e1l:linked"]);
    }

    /// `x` matches below a parent outside the range, `y` misses because its
    /// spans' shared parent is outside it, `w` matches with a span open past
    /// the range end, and `z` is whole.
    fn straddling() -> Vec<Span> {
        [
            ["w", "w0", "", "a"],
            ["w", "w1", "w0", "b"],
            ["w", "w2", "w0", OPEN],
            ["x", "x1", "x0", "a"],
            ["x", "x2", "x1", "b"],
            ["y", "y1", "y0", "a"],
            ["y", "y2", "y0", "b"],
            ["z", "z0", "", "a"],
            ["z", "z1", "z0", "b"],
        ]
        .map(|[t, id, parent, name]| span(t, id, parent, name))
        .to_vec()
    }

    #[tokio::test]
    async fn traces_cut_by_the_range_are_counted_by_outcome_without_changing_rows() {
        let stage = relation("a", "descendant", "b");
        let (rows, report) = run_reporting(ctx(&straddling()), params(stage, &[]), WITNESS).await;
        assert_eq!(rows, ["w0:a", "w1:b", "x1:a", "x2:b", "z0:a", "z1:b"]);
        let Some(MatchIncompleteReport {
            matched: 2,
            unmatched: 1,
            sample_trace_ids: mut samples,
        }) = report.match_incomplete
        else {
            panic!("{report:?}");
        };
        samples[..2].sort();
        assert_eq!(samples, ["w", "x", "y"]);
    }

    /// `r`'s root, the only `a`, starts before the range; `e` has no `a`
    /// and a span open past the range end. None of the rest counts: `h`
    /// has no `a` but is whole, `n` is cut but in no span-set, `p` keeps
    /// its root beside an orphan, and `q`'s root has an all-zero parent.
    #[tokio::test]
    async fn cut_traces_missing_a_span_set_count_as_unmatched() {
        let mut spans: Vec<Span> = straddling().into_iter().filter(|s| s[0] == "z").collect();
        spans.extend(
            [
                ["r", "r1", "r0", "b"],
                ["e", "e0", "", "b"],
                ["e", "e1", "e0", OPEN],
                ["h", "h0", "", "b"],
                ["n", "n1", "n0", "c"],
                ["p", "p0", "", "b"],
                ["p", "p1", "gone", "c"],
                ["q", "q0", &"0".repeat(32), "b"],
            ]
            .map(|[t, id, parent, name]| span(t, id, parent, name)),
        );
        let stage = relation("a", "child", "b");
        let (rows, report) = run_reporting(ctx(&spans), params(stage, &[]), WITNESS).await;
        assert_eq!(rows, ["z0:a", "z1:b"]);
        let Some(MatchIncompleteReport {
            matched: 0,
            unmatched: 2,
            sample_trace_ids: mut samples,
        }) = report.match_incomplete
        else {
            panic!("{report:?}");
        };
        samples.sort();
        assert_eq!(samples, ["e", "r"]);
    }

    /// The per-trace aggregate is the join's build side, and a cut trace
    /// that lacks a span-set reaches the exec's sort as one row.
    #[tokio::test]
    async fn a_cut_trace_missing_a_span_set_reaches_the_exec_as_one_row() {
        let mut spans = chain("d1", 1);
        spans.extend(
            [
                ["big", "b1", "b0", "write"],
                ["big", "b2", "b1", "write"],
                ["big", "b3", "b1", "write"],
            ]
            .map(|[t, id, parent, name]| span(t, id, parent, name)),
        );
        let params = params(relation("root", "descendant", "write"), &[]);
        let d = serde_json::from_value(params.document).unwrap();
        let ctx = ctx(&spans);
        let (df, _) = IrService::new(ctx.clone())
            .plan(&d, "t", "d", 0)
            .await
            .unwrap()
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        let lines: Vec<&str> = text.lines().map(str::trim).collect();
        let join = lines
            .iter()
            .position(|l| l.starts_with("HashJoinExec"))
            .unwrap();
        let scan = join
            + lines[join..]
                .iter()
                .position(|l| l.starts_with("DataSourceExec"))
                .unwrap();
        assert!(
            lines[join..scan]
                .iter()
                .any(|l| l.starts_with("AggregateExec")),
            "the build side is the per-trace aggregate: {text}"
        );

        datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .unwrap();
        fn sort_rows_below_match(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
            if plan.name() == "StructuralMatchExec" {
                return plan.children()[0].metrics()?.output_rows();
            }
            plan.children().into_iter().find_map(sort_rows_below_match)
        }
        assert_eq!(sort_rows_below_match(&plan), Some(3), "{text}");
    }

    /// Input slices as the exec sees them: whole traces and a skipped one
    /// in one batch, and a skipped trace split across two batches.
    #[test]
    fn the_exec_skips_a_not_whole_trace_once_across_slices_and_batches() {
        let input = |rows: &[(&str, &str, &str, &str, bool)]| -> RecordBatch {
            let text = |i: usize| -> ArrayRef {
                let values = rows.iter().map(|r| Some([r.0, r.1, r.2, r.3][i]));
                Arc::new(
                    values
                        .map(|v| v.filter(|v| !v.is_empty()))
                        .collect::<StringArray>(),
                )
            };
            let flag = |name: &'static str| -> ArrayRef {
                Arc::new(
                    rows.iter()
                        .map(|r| Some(r.3 == name))
                        .collect::<BooleanArray>(),
                )
            };
            let fields = [
                ("trace_id", DataType::Utf8),
                ("span_id", DataType::Utf8),
                ("parent_span_id", DataType::Utf8),
                ("span_name", DataType::Utf8),
                ("start_time_unix_nano", DataType::Int64),
                ("__match_0", DataType::Boolean),
                ("__match_1", DataType::Boolean),
                (WHOLE_TRACE, DataType::Boolean),
            ]
            .map(|(n, t)| Field::new(n, t, true));
            let columns: Vec<ArrayRef> = vec![
                text(0),
                text(1),
                text(2),
                text(3),
                Arc::new(Int64Array::from(vec![0; rows.len()])),
                flag("a"),
                flag("b"),
                Arc::new(rows.iter().map(|r| Some(r.4)).collect::<BooleanArray>()),
            ];
            RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), columns).unwrap()
        };
        let batches = [
            input(&[
                ("t1", "t1a", "", "a", true),
                ("t1", "t1b", "t1a", "b", true),
                ("t2", "t2b", "t2a", "b", false),
                ("t3", "t3a", "", "a", true),
                ("t3", "t3b", "t3a", "b", true),
                ("t4", "t4b", "t4a", "b", false),
            ]),
            input(&[
                ("t4", "t4c", "t4a", "b", false),
                ("t5", "t5a", "", "a", true),
                ("t5", "t5b", "t5a", "b", true),
            ]),
        ];
        let output = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, true),
            Field::new("start_time_unix_nano", DataType::Int64, true),
            Field::new(Match::SPANSETS, DataType::Utf8, false),
        ]));
        let spec = Spec {
            names: vec!["a".into(), "b".into()],
            relations: vec![(0, MatchOp::Child, 1)],
            limits: MatchLimits::default(),
            time_col: "start_time_unix_nano".into(),
            window_end_ns: 1000,
        };
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(usize::MAX));
        let incomplete = IncompleteTraces::default();
        let mut evaluator = Evaluator::try_new(
            &batches[0].schema(),
            &output,
            &spec,
            incomplete.clone(),
            MemoryConsumer::new("test").register(&pool),
        )
        .unwrap();
        let mut outputs: Vec<RecordBatch> = Vec::new();
        for batch in &batches {
            outputs.extend(evaluator.push(batch).unwrap());
        }
        outputs.extend(evaluator.finish().unwrap());
        let spans: Vec<&str> = outputs
            .iter()
            .flat_map(|out| out.column(1).as_string::<i32>().iter().flatten())
            .collect();
        assert_eq!(spans, ["t1a", "t1b", "t3a", "t3b", "t5a", "t5b"]);
        let report = incomplete.report().unwrap();
        assert_eq!((report.matched, report.unmatched), (0, 2));
        assert_eq!(report.sample_trace_ids, ["t2", "t4"]);
    }

    /// A cut trace that lacks a span-set cannot match, so it is counted
    /// without being buffered and never trips the trace bounds.
    #[tokio::test]
    async fn a_cut_trace_missing_a_span_set_is_counted_under_the_span_bound() {
        let spans = [
            ["big", "b1", "b0", "b"],
            ["big", "b2", "b1", "b"],
            ["big", "b3", "b1", "b"],
        ]
        .map(|[t, id, parent, name]| span(t, id, parent, name));
        let (_, _, report) = IrService::new(ctx(&spans))
            .with_match_limits(2, usize::MAX)
            .query(&params(relation("a", "child", "b"), &[]), "t", "d")
            .await
            .unwrap();
        let incomplete = report.match_incomplete.unwrap();
        assert_eq!((incomplete.matched, incomplete.unmatched), (0, 1));
        assert_eq!(incomplete.sample_trace_ids, ["big"]);
    }

    #[tokio::test]
    async fn at_most_three_sample_traces_are_named_matched_first() {
        let mut spans: Vec<Span> = ["p", "q", "r", "s"]
            .iter()
            .flat_map(|t| {
                [(1, 0, "a"), (2, 1, "b")].map(|(id, parent, name)| {
                    span(t, &format!("{t}{id}"), &format!("{t}{parent}"), name)
                })
            })
            .collect();
        spans.extend(straddling().into_iter().filter(|s| s[0] == "y"));
        let stage = relation("a", "descendant", "b");
        let (_, report) = run_reporting(ctx(&spans), params(stage, &[]), WITNESS).await;
        let incomplete = report.match_incomplete.unwrap();
        assert_eq!((incomplete.matched, incomplete.unmatched), (4, 1));
        let samples = incomplete.sample_trace_ids;
        assert!(
            samples.len() == 3 && !samples.contains(&"y".into()),
            "{samples:?}"
        );
    }

    #[tokio::test]
    async fn a_redelivered_span_with_a_dangling_parent_counts_its_trace_once() {
        let spans = [
            ["dup", "d1", "d0", "a"],
            ["dup", "d1", "d0", "a"],
            ["dup", "d2", "d1", "b"],
        ]
        .map(|[t, id, parent, name]| span(t, id, parent, name));
        let stage = relation("a", "descendant", "b");
        let (_, report) = run_reporting(ctx(&spans), params(stage, &[]), WITNESS).await;
        let incomplete = report.match_incomplete.unwrap();
        assert_eq!((incomplete.matched, incomplete.unmatched), (1, 0));
        assert_eq!(incomplete.sample_trace_ids, ["dup"]);
    }

    #[tokio::test]
    async fn without_an_end_time_column_only_dangling_parents_count() {
        let batch = batch(&straddling());
        let end = batch.schema().index_of(END_TIME).unwrap();
        let mut batch = batch;
        batch.remove_column(end);
        let stage = relation("a", "descendant", "b");
        let (rows, report) = run_reporting(ctx_of(batch), params(stage, &[]), WITNESS).await;
        assert_eq!(rows, ["w0:a", "w1:b", "x1:a", "x2:b", "z0:a", "z1:b"]);
        let incomplete = report.match_incomplete.unwrap();
        assert_eq!((incomplete.matched, incomplete.unmatched), (1, 1));
    }

    #[tokio::test]
    async fn whole_traces_and_relation_free_matches_report_nothing() {
        let mut whole = [chain("d1", 1), chain("d5", 5)].concat();
        whole.extend([
            span("zero", "z0", "0000000000000000", "root"),
            span("zero", "z1", "z0", "write"),
            span("zero", "z2", "z0", EDGE),
            span("zero32", "y0", &"0".repeat(32), "root"),
            span("zero32", "y1", "y0", "write"),
        ]);
        let stage = relation("root", "descendant", "write");
        let (rows, report) = run_reporting(ctx(&whole), params(stage, &[]), WITNESS).await;
        assert_eq!(rows.len(), 8);
        assert_eq!(report.match_incomplete, None);

        let stage = json!({ "spansets": { "a": name_is("a"), "b": name_is("b") } });
        let (rows, report) = run_reporting(ctx(&straddling()), params(stage, &[]), WITNESS).await;
        assert_eq!(rows.len(), 8);
        assert_eq!(report.match_incomplete, None);
    }

    #[tokio::test]
    async fn match_on_logs_is_rejected_even_without_a_logs_table() {
        let mut params = params(relation("root", "descendant", "write"), &[]);
        params.document["from"] = json!("logs");
        let err = IrService::new(ctx(&chain("d1", 1)))
            .query(&params, "t", "d")
            .await
            .unwrap_err();
        assert!(
            matches!(&err, QuerierError::InvalidInput(m) if m.contains("match") && m.contains("traces only")),
            "{err:?}"
        );
    }
}
