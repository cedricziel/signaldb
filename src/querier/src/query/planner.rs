//! The querier's per-query [`QueryPlanner`]: DataFusion's default physical
//! planner with every querier extension node attached, so a plan mixing
//! them (a `correlate` cap under a `binop`) plans in one pass. Installed on
//! a per-query `SessionState` by [`with_querier_planner`], never globally.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

use super::correlate_cap::CorrelateCapPlanner;
use super::metric_series::vector_match::VectorMatchPlanner;
use super::structural_match::StructuralMatchPlanner;

#[derive(Debug)]
struct QuerierQueryPlanner;

#[async_trait]
impl QueryPlanner for QuerierQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(CorrelateCapPlanner),
            Arc::new(VectorMatchPlanner),
            Arc::new(StructuralMatchPlanner),
        ])
        .create_physical_plan(logical_plan, session)
        .await
    }
}

/// `state` with the querier's extension planners installed.
pub(super) fn with_querier_planner(state: SessionState) -> SessionState {
    SessionStateBuilder::new_from_existing(state)
        .with_query_planner(Arc::new(QuerierQueryPlanner))
        .build()
}
