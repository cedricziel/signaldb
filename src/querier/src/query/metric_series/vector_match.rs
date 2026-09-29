//! # Vector matching for the `binop` stage
//!
//! Combines two operands step by step (`bucket`), following Prometheus'
//! vector-matching rules. A Series operand is a frame of `bucket`,
//! `__labels` (canonical JSON) and `value`; a Scalar operand is `bucket` and
//! `value`, or a number. [`vector_match`] wraps both plans in a
//! [`VectorMatchNode`], planned by a per-query [`QueryPlanner`] (the
//! `correlate_cap` pattern, through the querier's shared planner) into
//! [`VectorMatchExec`], which folds both sides into matching state (Series
//! are bounded, each side's series count is capped, and the state is
//! reserved against the memory pool) and emits one sorted batch.

#![expect(dead_code, reason = "not wired into a planner yet")]

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use common::query_ir::Binop;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{
    DFSchema, DFSchemaRef, DataFusionError, Result as DFResult, internal_err,
};
use datafusion::dataframe::DataFrame;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;

use self::eval::{Input, Kind, MatchSpec, evaluate};
use self::fold::{SideBuilder, fold_scalar};
use crate::query::error::QuerierError;
use crate::query::planner::with_querier_planner;

mod eval;
mod fold;

const BUCKET: &str = "bucket";
const LABELS: &str = "__labels";
const VALUE: &str = "value";

/// One `binop` operand, resolved by the planner.
pub(crate) enum Operand {
    Series(DataFrame),
    Scalar(DataFrame),
    Number(f64),
}

/// Plan `left op right` per `spec` (its `right` field is ignored: the
/// planner resolves it into `right`). `reverse` evaluates `right op left`,
/// and `group.side` then names a side of that swapped expression, as the IR
/// validator reads it. The result is a Series frame when either operand is
/// a Series, else a Scalar frame.
pub(crate) fn vector_match(
    left: Operand,
    right: Operand,
    spec: &Binop,
) -> Result<DataFrame, QuerierError> {
    if spec.on.is_some() && spec.ignoring.is_some() {
        return Err(QuerierError::QueryFailed(DataFusionError::Internal(
            "binop `on` and `ignoring` are both set".to_string(),
        )));
    }
    let (left, right) = if spec.reverse {
        (right, left)
    } else {
        (left, right)
    };
    let mut state = None;
    let mut inputs = Vec::new();
    let mut take = |operand: Operand| -> Result<Kind, QuerierError> {
        let (kind, df, columns) = match operand {
            Operand::Number(n) => return Ok(Kind::Number(n)),
            Operand::Series(df) => (Kind::Series, df, &[BUCKET, LABELS, VALUE][..]),
            Operand::Scalar(df) => (Kind::Scalar, df, &[BUCKET, VALUE][..]),
        };
        if let Some(missing) = columns
            .iter()
            .find(|c| !df.schema().has_column_with_unqualified_name(c))
        {
            return Err(QuerierError::QueryFailed(DataFusionError::Internal(
                format!("binop operand frame lacks the `{missing}` column"),
            )));
        }
        let (df_state, plan) = df.into_parts();
        state.get_or_insert(df_state);
        inputs.push(plan);
        Ok(kind)
    };
    let left = take(left)?;
    let right = take(right)?;
    let Some(state) = state else {
        return Err(QuerierError::InvalidInput(
            "binop needs a series or scalar operand, not two numbers".to_string(),
        ));
    };
    let both_series = left == Kind::Series && right == Kind::Series;
    if spec.op.is_set() && !both_series {
        return Err(QuerierError::InvalidInput(
            "binop `and`/`or`/`unless` need a series on both sides".to_string(),
        ));
    }
    let any_series = left == Kind::Series || right == Kind::Series;
    if spec.op.is_comparison() && !any_series && !spec.bool {
        return Err(QuerierError::InvalidInput(
            "binop comparisons between scalars need `bool`".to_string(),
        ));
    }
    let schema = output_schema(any_series);
    let node = VectorMatchNode {
        inputs,
        schema: Arc::new(DFSchema::try_from(schema.as_ref().clone())?),
        spec: Arc::new(MatchSpec {
            op: spec.op,
            on: spec.on.clone(),
            ignoring: spec.ignoring.clone(),
            group: spec.group.clone(),
            bool: spec.bool,
            left,
            right,
        }),
    };
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    });
    Ok(DataFrame::new(with_querier_planner(state), plan))
}

fn output_schema(series: bool) -> SchemaRef {
    let bucket = Field::new(
        BUCKET,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    );
    let value = Field::new(VALUE, DataType::Float64, false);
    let fields = if series {
        vec![bucket, Field::new(LABELS, DataType::Utf8, false), value]
    } else {
        vec![bucket, value]
    };
    Arc::new(Schema::new(fields))
}

/// Logical node: `inputs` holds one plan per frame operand, left first.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VectorMatchNode {
    inputs: Vec<LogicalPlan>,
    schema: DFSchemaRef,
    spec: Arc<MatchSpec>,
}

impl PartialOrd for VectorMatchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.spec != other.spec {
            return None;
        }
        self.inputs.partial_cmp(&other.inputs)
    }
}

impl UserDefinedLogicalNodeCore for VectorMatchNode {
    fn name(&self) -> &str {
        "VectorMatch"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VectorMatch: op={}", self.spec.op_name())
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        Ok(Self {
            inputs,
            schema: Arc::clone(&self.schema),
            spec: Arc::clone(&self.spec),
        })
    }
}

/// Lowers [`VectorMatchNode`] to [`VectorMatchExec`].
#[derive(Debug)]
pub(crate) struct VectorMatchPlanner;

#[async_trait]
impl ExtensionPlanner for VectorMatchPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<VectorMatchNode>() else {
            return Ok(None);
        };
        Ok(Some(Arc::new(VectorMatchExec::new(
            physical_inputs.to_vec(),
            Arc::clone(&node.spec),
            Arc::clone(node.schema.inner()),
        ))))
    }
}

/// Physical operator: folds every input into matching state batch by batch
/// (reserved against the task's memory pool), matches per `bucket`, and
/// emits one batch sorted by (`bucket`, `__labels`).
#[derive(Debug)]
struct VectorMatchExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    spec: Arc<MatchSpec>,
    properties: Arc<PlanProperties>,
}

impl VectorMatchExec {
    fn new(inputs: Vec<Arc<dyn ExecutionPlan>>, spec: Arc<MatchSpec>, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            inputs,
            spec,
            properties,
        }
    }
}

impl DisplayAs for VectorMatchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VectorMatchExec: op={}", self.spec.op_name())
    }
}

impl ExecutionPlan for VectorMatchExec {
    fn name(&self) -> &str {
        "VectorMatchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    #[allow(deprecated)]
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition; self.inputs.len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.inputs.len()]
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != self.inputs.len() {
            return internal_err!(
                "VectorMatchExec takes {} children, got {}",
                self.inputs.len(),
                children.len()
            );
        }
        Ok(Arc::new(VectorMatchExec::new(
            children,
            Arc::clone(&self.spec),
            self.schema(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("VectorMatchExec has one partition, got partition {partition}");
        }
        if let Some(input) = self
            .inputs
            .iter()
            .find(|input| input.output_partitioning().partition_count() != 1)
        {
            return internal_err!(
                "VectorMatchExec needs single-partition inputs; {} has {}",
                input.name(),
                input.output_partitioning().partition_count()
            );
        }
        let streams = self
            .inputs
            .iter()
            .map(|input| input.execute(0, Arc::clone(&context)))
            .collect::<DFResult<Vec<_>>>()?;
        let reservation = MemoryConsumer::new("VectorMatchExec").register(context.memory_pool());
        let fut = fold_and_match(Arc::clone(&self.spec), streams, reservation, self.schema());
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(fut),
        )))
    }
}

async fn fold_and_match(
    spec: Arc<MatchSpec>,
    streams: Vec<SendableRecordBatchStream>,
    mut reservation: MemoryReservation,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let mut streams = streams.into_iter();
    let left = fold_input(&spec, spec.left, "left", &mut streams, &mut reservation).await?;
    let right = fold_input(&spec, spec.right, "right", &mut streams, &mut reservation).await?;
    evaluate(&spec, &left, &right, schema)
}

async fn fold_input(
    spec: &MatchSpec,
    kind: Kind,
    side: &str,
    streams: &mut impl Iterator<Item = SendableRecordBatchStream>,
    reservation: &mut MemoryReservation,
) -> DFResult<Input> {
    let mut stream = match kind {
        Kind::Number(n) => return Ok(Input::Number(n)),
        Kind::Series | Kind::Scalar => streams
            .next()
            .ok_or_else(|| DataFusionError::Internal(format!("binop {side} input missing")))?,
    };
    if kind == Kind::Scalar {
        let mut values = BTreeMap::new();
        while let Some(batch) = stream.next().await {
            reservation.try_grow(fold_scalar(&mut values, &batch?)?)?;
        }
        return Ok(Input::Scalar(values));
    }
    let mut builder = SideBuilder::default();
    while let Some(batch) = stream.next().await {
        reservation.try_grow(builder.fold(spec, &batch?, side)?)?;
    }
    Ok(Input::Series(builder.finish(side)?))
}
