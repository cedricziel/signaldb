//! # Streaming row cap for the `correlate` stage
//!
//! [`Lowering::lower_correlate`](super::ir_planner) must bound a `correlate`
//! join's output at `correlate_max_rows` without materializing the join
//! (previously: `.limit(cap + 1).collect()` then `read_batches` — up to
//! 5,000,000 full trace rows held in memory before a following `aggregate`
//! ever ran). This module is a small DataFusion extension node
//! ([`CorrelateCapNode`]) lowered to a streaming [`ExecutionPlan`]
//! ([`CorrelateCapExec`]) instead: it passes batches through unchanged up to
//! the cap, and on the batch that would cross it, slices off the excess,
//! flips a shared `Arc<AtomicBool>`, and ends its stream — never holding more
//! than the one batch currently in flight, the same as any other streaming
//! operator (`FilterExec`, `LimitExec`, ...).
//!
//! The cap must be enforced on a single logical stream of rows (a per-
//! partition cap would let `correlate_max_rows * partition_count` rows
//! through): [`CorrelateCapExec::required_input_distribution`] declares
//! `SinglePartition`, so DataFusion's `EnforceDistribution` physical
//! optimizer pass inserts a `CoalescePartitionsExec` — itself a standard
//! streaming merge, not a buffering one — below us whenever the child ends
//! up with more than one partition, including partitioning decisions later
//! optimizer passes make after this module's own planning.
//!
//! [`CorrelateCapQueryPlanner`] is a [`QueryPlanner`] that recognizes
//! [`LogicalPlan::Extension`] nodes wrapping a [`CorrelateCapNode`]; it is
//! installed on a per-query [`SessionState`] built from the ambient session's
//! own state (see [`wrap_with_cap`]), so it never needs registering globally
//! and every other query on the shared, long-lived `SessionContext` is
//! unaffected.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchemaRef, Result as DFResult};
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use futures::{Stream, StreamExt};

use super::error::QuerierError;

/// Logical node: "the input relation, capped at `cap` rows; report overflow
/// on `truncated`." Carries the `Arc<AtomicBool>` by value through ordinary
/// Rust ownership — from [`wrap_with_cap`], through the logical plan, into
/// [`CorrelateCapExec`] once [`CorrelateCapPlanner`] lowers it — so no
/// global registry or query ID is needed to find it again after execution.
#[derive(Debug, Clone)]
struct CorrelateCapNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    cap: usize,
    truncated: Arc<AtomicBool>,
}

impl PartialEq for CorrelateCapNode {
    fn eq(&self, other: &Self) -> bool {
        self.cap == other.cap
            && self.input == other.input
            && Arc::ptr_eq(&self.truncated, &other.truncated)
    }
}
impl Eq for CorrelateCapNode {}

impl PartialOrd for CorrelateCapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.cap.partial_cmp(&other.cap) {
            Some(Ordering::Equal) => self.input.partial_cmp(&other.input),
            other => other,
        }
    }
}

impl Hash for CorrelateCapNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cap.hash(state);
        self.input.hash(state);
        (Arc::as_ptr(&self.truncated) as usize).hash(state);
    }
}

impl UserDefinedLogicalNodeCore for CorrelateCapNode {
    fn name(&self) -> &str {
        "CorrelateCap"
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
        write!(f, "CorrelateCap: cap={}", self.cap)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        let input = inputs.remove(0);
        let schema = Arc::clone(input.schema());
        Ok(Self {
            input,
            schema,
            cap: self.cap,
            truncated: Arc::clone(&self.truncated),
        })
    }
}

/// Wrap `df`'s current logical plan in a [`CorrelateCapNode`] and return a
/// new `DataFrame` over a per-query `SessionState` that knows how to plan
/// it — see the module doc comment for why this is per-query rather than
/// global. Every later stage (`aggregate`/`where`/`limit`, built through the
/// ordinary `DataFrame` builder methods `Lowering::lower_stage` already
/// uses) composes on top of the returned frame exactly as it would over any
/// other `DataFrame`.
pub(super) fn wrap_with_cap(
    df: DataFrame,
    cap: usize,
    truncated: Arc<AtomicBool>,
) -> Result<DataFrame, QuerierError> {
    let (state, plan) = df.into_parts();
    let schema = Arc::clone(plan.schema());
    let node = CorrelateCapNode {
        input: plan,
        schema,
        cap,
        truncated,
    };
    let capped_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    });
    let state = SessionStateBuilder::new_from_existing(state)
        .with_query_planner(Arc::new(CorrelateCapQueryPlanner))
        .build();
    Ok(DataFrame::new(state, capped_plan))
}

/// [`QueryPlanner`] that recognizes [`CorrelateCapNode`] via
/// [`CorrelateCapPlanner`], delegating everything else to DataFusion's own
/// default planning — the same shape as DataFusion's internal
/// `DefaultQueryPlanner`, just with our extension planner attached.
#[derive(Debug)]
struct CorrelateCapQueryPlanner;

#[async_trait]
impl QueryPlanner for CorrelateCapQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(CorrelateCapPlanner)]);
        planner.create_physical_plan(logical_plan, session).await
    }
}

/// Lowers [`CorrelateCapNode`] to [`CorrelateCapExec`]. The cap must be
/// enforced on a single logical stream of rows, not once per partition —
/// [`CorrelateCapExec::required_input_distribution`] declares that
/// requirement so DataFusion's `EnforceDistribution` physical-optimizer pass
/// inserts a `CoalescePartitionsExec` below us whenever the child ends up
/// with more than one partition, including partitioning decisions made by
/// *later* optimizer passes (e.g. a join reordering into a partitioned
/// hash join) that run after this planner has already produced its initial
/// tree — a fixed coalesce inserted here, before those passes run, would
/// miss that case and silently execute only one of several partitions.
#[derive(Debug)]
struct CorrelateCapPlanner;

#[async_trait]
impl ExtensionPlanner for CorrelateCapPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn datafusion::logical_expr::UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(cap_node) = node.as_any().downcast_ref::<CorrelateCapNode>() else {
            return Ok(None);
        };
        Ok(Some(Arc::new(CorrelateCapExec::new(
            Arc::clone(&physical_inputs[0]),
            cap_node.cap,
            Arc::clone(&cap_node.truncated),
        ))))
    }
}

/// Physical operator: passes rows through unchanged up to `cap`, then slices
/// the batch that would cross it, sets `truncated`, and ends its stream.
/// Never buffers more than the one batch currently being processed.
#[derive(Debug)]
struct CorrelateCapExec {
    input: Arc<dyn ExecutionPlan>,
    cap: usize,
    truncated: Arc<AtomicBool>,
    properties: Arc<PlanProperties>,
}

impl CorrelateCapExec {
    fn new(input: Arc<dyn ExecutionPlan>, cap: usize, truncated: Arc<AtomicBool>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            cap,
            truncated,
            properties,
        }
    }
}

impl DisplayAs for CorrelateCapExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CorrelateCapExec: cap={}", self.cap)
    }
}

impl ExecutionPlan for CorrelateCapExec {
    fn name(&self) -> &str {
        "CorrelateCapExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// The cap must be enforced on one logical stream of rows, not once per
    /// partition — see the module doc comment and [`CorrelateCapPlanner`].
    #[allow(deprecated)]
    fn required_input_distribution(&self) -> Vec<datafusion::physical_expr::Distribution> {
        vec![datafusion::physical_expr::Distribution::SinglePartition]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    #[allow(deprecated)]
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CorrelateCapExec::new(
            children.remove(0),
            self.cap,
            Arc::clone(&self.truncated),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner = self.input.execute(partition, context)?;
        Ok(Box::pin(CorrelateCapStream {
            inner,
            schema: self.input.schema(),
            cap: self.cap,
            seen: 0,
            truncated: Arc::clone(&self.truncated),
            done: false,
        }))
    }
}

struct CorrelateCapStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    cap: usize,
    seen: usize,
    truncated: Arc<AtomicBool>,
    done: bool,
}

impl Stream for CorrelateCapStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let remaining = self.cap.saturating_sub(self.seen);
                if remaining == 0 {
                    // Every prior batch already reached the cap exactly;
                    // this batch's mere existence is the overflow.
                    self.truncated.store(true, AtomicOrdering::Relaxed);
                    self.done = true;
                    return Poll::Ready(None);
                }
                if batch.num_rows() <= remaining {
                    self.seen += batch.num_rows();
                    Poll::Ready(Some(Ok(batch)))
                } else {
                    let sliced = batch.slice(0, remaining);
                    self.seen += remaining;
                    self.truncated.store(true, AtomicOrdering::Relaxed);
                    self.done = true;
                    Poll::Ready(Some(Ok(sliced)))
                }
            }
            other => other,
        }
    }
}

impl RecordBatchStream for CorrelateCapStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
