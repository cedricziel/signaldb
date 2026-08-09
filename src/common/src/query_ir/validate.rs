//! # The IR validator
//!
//! Validation is a **function of [`RelationType`]**, not prose. The validator:
//!
//! 1. checks the `irVersion` is in the supported range,
//! 2. resolves `from` against the source registry,
//! 3. seeds the initial relation and infers it stage-by-stage — rejecting a
//!    stage whose input constraint or column references are unmet, naming the
//!    stage,
//! 4. validates the declared envelope against the inferred terminal relation,
//!    and the `fields` projection against it.
//!
//! Every literal is coerced against its field's canonical type here (coercibility
//! only for relative timestamps); an un-coercible literal is rejected, never
//! silently cast at runtime.

use super::document::{Document, Range, ResultEnvelope};
use super::predicate::{ComparisonOp, Leaf, Predicate};
use super::relation::{Column, Grain, Heatmap as HeatmapRelation, RelationType, RowSet, Series};
use super::resolver::FieldResolver;
use super::source::{SourceDef, SourceRegistry};
use super::stage::{
    Agg, AggFn, Aggregate, Extract, Heatmap, Order, Rank, Stage, is_expression_string,
};
use super::value::{ValueType, coerce, parse_duration_ns};

/// Errors raised while validating an IR document.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IrError {
    #[error("unsupported irVersion {found}; supported range is {min}..={max}")]
    UnsupportedVersion { found: i64, min: i64, max: i64 },

    #[error("unknown source '{name}'; registered sources: {available}")]
    UnknownSource { name: String, available: String },

    #[error("value {value} cannot be coerced to {target} for field '{field}'")]
    Coercion {
        field: String,
        value: String,
        target: String,
    },

    #[error("stage '{stage}' is not legal here: {reason}")]
    IllegalStage { stage: String, reason: String },

    #[error("result envelope '{declared}' does not match terminal relation ({terminal})")]
    EnvelopeMismatch {
        declared: &'static str,
        terminal: String,
    },

    #[error("field '{field}' names a physical column or storage detail; use a logical name")]
    PhysicalAddressing { field: String },

    #[error("operand '{operand}' must be a structured value, not an expression string")]
    ExpressionString { operand: String },

    #[error("duplicate output name '{name}'")]
    DuplicateName { name: String },

    #[error("derived field '{name}' collides with an existing field; extract may not shadow it")]
    NameCollision { name: String },

    #[error("reference to unknown name '{name}'")]
    UnknownReference { name: String },

    #[error("field '{field}' has no canonical type in the registry")]
    UnknownFieldType { field: String },

    #[error("field '{field}' is retrievable but cannot be used in a predicate")]
    UnfilterableField { field: String },

    #[error("topk/bottomk `n` must be an integer > 0, got {n}")]
    InvalidRankSize { n: i64 },

    #[error("`fields` projection is only valid for rows/table results, not series or heatmap")]
    FieldsOnSeries,

    #[error("`fields` entry '{field}' is not present in the terminal relation")]
    FieldNotInTerminal { field: String },

    #[error("invalid query: {0}")]
    Invalid(String),
}

/// The result of validating a document: the inferred terminal relation.
#[derive(Debug, Clone, PartialEq)]
pub struct Validated {
    pub terminal: RelationType,
}

/// Parse an IR document from JSON, mapping serde errors into [`IrError`].
pub fn parse_document(json: &str) -> Result<Document, IrError> {
    serde_json::from_str(json).map_err(|e| IrError::Invalid(e.to_string()))
}

/// Validate a document against a source registry and a field resolver.
pub fn validate(
    doc: &Document,
    sources: &SourceRegistry,
    resolver: &dyn FieldResolver,
) -> Result<Validated, IrError> {
    // 1. Version range.
    if !super::version::is_supported(doc.ir_version) {
        return Err(IrError::UnsupportedVersion {
            found: doc.ir_version,
            min: super::version::MIN_IR_VERSION,
            max: super::version::MAX_IR_VERSION,
        });
    }
    if doc.ir_version < 2
        && (doc.result == ResultEnvelope::Heatmap
            || doc
                .pipeline
                .iter()
                .any(|stage| matches!(stage, Stage::Heatmap(_))))
    {
        return Err(IrError::Invalid(
            "heatmap stage and result envelope require irVersion 2".to_string(),
        ));
    }

    // 2. Source resolution — unknown source is a clear error, not a parse fail.
    let source_def = sources
        .resolve(&doc.from)
        .ok_or_else(|| IrError::UnknownSource {
            name: doc.from.clone(),
            available: sources.names().join(", "),
        })?;

    // Range literals must be coercible to timestamps (relative anchors stay
    // symbolic; only well-formedness is checked here).
    check_range(&doc.range)?;

    // 3. Seed the initial relation and infer stage-by-stage.
    let mut ctx = InferCtx {
        source: &doc.from,
        source_def,
        resolver,
        relation: RelationType::RowSet(RowSet {
            source: doc.from.clone(),
            columns: Vec::new(),
            grain: source_def.grain,
            aggregated: false,
            open: true,
        }),
        names: Vec::new(),
    };
    for stage in &doc.pipeline {
        ctx.apply_stage(stage)?;
    }

    // 4. Envelope + fields validation against the terminal relation.
    validate_envelope(doc.result, &ctx.relation)?;
    validate_fields(doc, &ctx)?;

    Ok(Validated {
        terminal: ctx.relation,
    })
}

struct InferCtx<'a> {
    source: &'a str,
    source_def: &'a SourceDef,
    resolver: &'a dyn FieldResolver,
    relation: RelationType,
    /// Names introduced by extract/aggregate — unique across the pipeline.
    names: Vec<String>,
}

impl InferCtx<'_> {
    fn apply_stage(&mut self, stage: &Stage) -> Result<(), IrError> {
        match stage {
            Stage::Where(pred) => self.apply_where(pred),
            Stage::Extract(extract) => self.apply_extract(extract),
            Stage::Aggregate(agg) => self.apply_aggregate(agg),
            Stage::Topk(rank) => self.apply_rank("topk", rank),
            Stage::Bottomk(rank) => self.apply_rank("bottomk", rank),
            Stage::Order(keys) => self.apply_order(keys),
            Stage::Limit(_) => Ok(()),
            Stage::Heatmap(heatmap) => self.apply_heatmap(heatmap),
        }
    }

    fn require_rowset(&self, stage: &str) -> Result<&RowSet, IrError> {
        match &self.relation {
            RelationType::RowSet(rs) => Ok(rs),
            RelationType::Series(_) | RelationType::Heatmap(_) => Err(IrError::IllegalStage {
                stage: stage.to_string(),
                reason: "expects a row-set input but the pipeline is a series".to_string(),
            }),
        }
    }

    /// Resolve a referenced name to its type in the current relation, applying
    /// the logical-namespace and expression-string guards.
    fn ref_type(&self, name: &str) -> Result<ValueType, IrError> {
        if is_expression_string(name) {
            return Err(IrError::ExpressionString {
                operand: name.to_string(),
            });
        }
        self.guard_logical_name(name)?;
        match &self.relation {
            RelationType::Series(_) | RelationType::Heatmap(_) => Err(IrError::UnknownReference {
                name: name.to_string(),
            }),
            RelationType::RowSet(rs) => {
                if let Some(col) = rs.columns.iter().find(|c| c.name == name) {
                    return Ok(col.value_type.clone());
                }
                if rs.aggregated {
                    // Closed schema: only the aggregate/group outputs exist.
                    return Err(IrError::UnknownReference {
                        name: name.to_string(),
                    });
                }
                match self.resolver.resolve(self.source, name) {
                    Some(r) => Ok(r.value_type().clone()),
                    // Defined rejection: a field with no canonical type (12.1a).
                    None => Err(IrError::UnknownFieldType {
                        field: name.to_string(),
                    }),
                }
            }
        }
    }

    fn apply_where(&mut self, pred: &Predicate) -> Result<(), IrError> {
        self.require_rowset("where")?;
        let mut result = Ok(());
        pred.walk_leaves(&mut |leaf| {
            if result.is_ok() {
                result = self.check_leaf(leaf);
            }
        });
        result
        // `where` preserves the relation type.
    }

    fn check_leaf(&self, leaf: &Leaf) -> Result<(), IrError> {
        self.require_filterable(&leaf.field)?;
        let ty = self.ref_type(&leaf.field)?;
        match (leaf.op.takes_value(), &leaf.value) {
            (true, None) => {
                return Err(IrError::Invalid(format!(
                    "operator '{}' requires a value",
                    leaf.op.as_str()
                )));
            }
            (false, Some(_)) => {
                return Err(IrError::Invalid(format!(
                    "operator '{}' takes no value",
                    leaf.op.as_str()
                )));
            }
            _ => {}
        }
        // Coerce the operand(s) against the field's canonical type.
        match leaf.op {
            ComparisonOp::Exists => {}
            ComparisonOp::In => {
                let items = leaf
                    .value
                    .as_ref()
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| IrError::Invalid("`in` requires an array value".to_string()))?;
                for item in items {
                    coerce_for(&leaf.field, item, &ty)?;
                }
            }
            ComparisonOp::Between => {
                let bounds = leaf
                    .value
                    .as_ref()
                    .and_then(|v| v.as_array())
                    .filter(|a| a.len() == 2)
                    .ok_or_else(|| {
                        IrError::Invalid("`between` requires a two-element array value".to_string())
                    })?;
                for b in bounds {
                    coerce_for(&leaf.field, b, &ty)?;
                }
            }
            ComparisonOp::Contains | ComparisonOp::Regex => {
                // Substring/regex operate on the string form.
                let v = leaf.value.as_ref().unwrap();
                coerce_for(&leaf.field, v, &ValueType::String)?;
            }
            _ => {
                let v = leaf.value.as_ref().unwrap();
                coerce_for(&leaf.field, v, &ty)?;
            }
        }
        Ok(())
    }

    fn require_filterable(&self, field: &str) -> Result<(), IrError> {
        if self.resolver.filterability(self.source, field)
            == crate::schema::logical::Filterability::RetrievalOnly
        {
            return Err(IrError::UnfilterableField {
                field: field.to_string(),
            });
        }
        Ok(())
    }

    fn apply_extract(&mut self, extract: &Extract) -> Result<(), IrError> {
        if !self.source_def.allows_extract {
            return Err(IrError::IllegalStage {
                stage: "extract".to_string(),
                reason: format!(
                    "source '{}' does not support extract (log-only)",
                    self.source
                ),
            });
        }
        let rs = self.require_rowset("extract")?;
        if rs.aggregated {
            return Err(IrError::IllegalStage {
                stage: "extract".to_string(),
                reason: "cannot extract from an aggregated relation".to_string(),
            });
        }
        let mut new_cols = Vec::new();
        for f in &extract.as_fields {
            self.guard_logical_name(&f.name)?;
            // No silent shadowing: collide with a registry field, an earlier
            // derived/agg name, or an existing column → rejected.
            let collides = self.names.iter().any(|n| n == &f.name)
                || self.resolver.is_known(self.source, &f.name)
                || self.relation.column(&f.name).is_some();
            if collides {
                return Err(IrError::NameCollision {
                    name: f.name.clone(),
                });
            }
            self.names.push(f.name.clone());
            new_cols.push(Column::new(f.name.clone(), f.value_type.clone()));
        }
        if let RelationType::RowSet(rs) = &mut self.relation {
            rs.columns.extend(new_cols);
        }
        Ok(())
    }

    fn apply_aggregate(&mut self, agg: &Aggregate) -> Result<(), IrError> {
        let rs = self.require_rowset("aggregate")?;
        if rs.aggregated {
            return Err(IrError::IllegalStage {
                stage: "aggregate".to_string(),
                reason: "cannot aggregate an already-aggregated relation".to_string(),
            });
        }
        if agg.aggs.is_empty() {
            return Err(IrError::Invalid(
                "aggregate requires at least one aggregate output".to_string(),
            ));
        }

        // Group columns keep their canonical types.
        let mut group_cols = Vec::new();
        for by in &agg.by {
            self.require_filterable(by)?;
            let ty = self.ref_type(by)?;
            group_cols.push(Column::new(by.clone(), ty));
        }

        // Aggregate outputs — each `as` name unique across the pipeline and not
        // colliding with a source column or a group field.
        let mut out_cols = Vec::new();
        for a in &agg.aggs {
            let out = self.check_agg(a, &group_cols)?;
            self.names.push(a.as_name.clone());
            out_cols.push(out);
        }

        match &agg.step {
            Some(step) => {
                let step_ns = parse_duration_ns(step).ok_or_else(|| IrError::Coercion {
                    field: "step".to_string(),
                    value: step.clone(),
                    target: ValueType::DurationNs.to_string(),
                })?;
                if step_ns <= 0 {
                    return Err(IrError::Invalid("aggregate `step` must be > 0".to_string()));
                }
                if agg.aggs.len() != 1 {
                    return Err(IrError::Invalid(
                        "a `step` aggregate requires exactly one aggregate output".to_string(),
                    ));
                }
                self.relation = RelationType::Series(Series {
                    labels: agg.by.clone(),
                    value: out_cols[0].value_type.clone(),
                    step_ns,
                });
            }
            None => {
                let mut columns = group_cols;
                columns.extend(out_cols);
                self.relation = RelationType::RowSet(RowSet {
                    source: self.source.to_string(),
                    columns,
                    grain: Grain::Group,
                    aggregated: true,
                    open: false,
                });
            }
        }
        Ok(())
    }

    fn apply_heatmap(&mut self, heatmap: &Heatmap) -> Result<(), IrError> {
        if self.source != "traces" {
            return Err(IrError::IllegalStage {
                stage: "heatmap".into(),
                reason: "is currently supported for traces only".into(),
            });
        }
        if self.require_rowset("heatmap")?.aggregated {
            return Err(IrError::IllegalStage {
                stage: "heatmap".into(),
                reason: "cannot aggregate an already-aggregated relation".into(),
            });
        }
        if heatmap.x.align != "epoch" {
            return Err(IrError::Invalid("heatmap x.align must be 'epoch'".into()));
        }
        let step_ns = parse_duration_ns(&heatmap.x.step).ok_or_else(|| IrError::Coercion {
            field: "heatmap.x.step".into(),
            value: heatmap.x.step.clone(),
            target: ValueType::DurationNs.to_string(),
        })?;
        if step_ns <= 0 {
            return Err(IrError::Invalid("heatmap x.step must be > 0".into()));
        }
        let ty = self.ref_type(&heatmap.y.of)?;
        if ty != ValueType::DurationNs {
            return Err(IrError::Invalid(format!(
                "heatmap y.of requires a duration field, got {ty}"
            )));
        }
        if heatmap.value.func != AggFn::Count || heatmap.value.as_name != "count" {
            return Err(IrError::Invalid(
                "heatmap value must be { fn: 'count', as: 'count' }".into(),
            ));
        }
        if !heatmap.y.overflow {
            return Err(IrError::Invalid("heatmap y.overflow must be true".into()));
        }
        if heatmap.y.bounds.is_empty() || heatmap.y.bounds.len() > 32 {
            return Err(IrError::Invalid("heatmap requires 1..=32 y bounds".into()));
        }
        let bounds = heatmap
            .y
            .bounds
            .iter()
            .map(|v| {
                coerce(v, &ty).map_err(|_| IrError::Coercion {
                    field: heatmap.y.of.clone(),
                    value: v.to_string(),
                    target: ty.to_string(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let bounds = bounds
            .iter()
            .map(|v| match v {
                super::value::Literal::Duration(n) => Ok(*n),
                _ => Err(IrError::Invalid("heatmap bounds must be durations".into())),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if bounds.windows(2).any(|p| p[0] >= p[1]) {
            return Err(IrError::Invalid(
                "heatmap y.bounds must be strictly increasing".into(),
            ));
        }
        self.relation = RelationType::Heatmap(HeatmapRelation {
            x_step_ns: step_ns,
            y_of: heatmap.y.of.clone(),
            y_type: ty,
            y_bounds: bounds,
            value: "count".into(),
        });
        Ok(())
    }

    fn check_agg(&self, a: &Agg, group_cols: &[Column]) -> Result<Column, IrError> {
        self.guard_logical_name(&a.as_name)?;
        // Uniqueness: against earlier introduced names, group fields, and any
        // source column.
        let collides = self.names.iter().any(|n| n == &a.as_name)
            || group_cols.iter().any(|c| c.name == a.as_name)
            || self.resolver.is_known(self.source, &a.as_name);
        if collides {
            return Err(IrError::DuplicateName {
                name: a.as_name.clone(),
            });
        }

        // A scoping predicate is checked exactly as a `where` stage's is — same
        // resolver, same literal coercion, same logical-namespace guard — so a
        // scope can never reach fields or spellings `where` would have refused.
        // It narrows this aggregate only; the grouping is unaffected, so no
        // relation-type change follows from it.
        if let Some(scope) = &a.scope {
            let mut result = Ok(());
            scope.walk_leaves(&mut |leaf| {
                if result.is_ok() {
                    result = self.check_leaf(leaf);
                }
            });
            result?;
        }

        // Field operand requirements.
        let of_type = match (&a.of, a.func.needs_field()) {
            (Some(of), _) => {
                self.require_filterable(of)?;
                Some(self.ref_type(of)?)
            }
            (None, true) => {
                return Err(IrError::Invalid(format!(
                    "aggregate '{}' requires an `of` field",
                    a.func.as_str()
                )));
            }
            (None, false) => None,
        };
        if a.of.is_some() && !a.func.needs_field() {
            return Err(IrError::Invalid(format!(
                "aggregate '{}' takes no `of` field",
                a.func.as_str()
            )));
        }
        if a.func.needs_arg() {
            let arg = a.arg.ok_or_else(|| {
                IrError::Invalid(format!(
                    "aggregate '{}' requires a numeric `arg`",
                    a.func.as_str()
                ))
            })?;
            if !(0.0..=1.0).contains(&arg) {
                return Err(IrError::Invalid(
                    "quantile `arg` must be within [0, 1]".to_string(),
                ));
            }
        }

        // Numeric requirement for sum/avg/quantile.
        if matches!(a.func, AggFn::Sum | AggFn::Avg | AggFn::Quantile)
            && let Some(t) = &of_type
            && !is_numeric(t)
        {
            return Err(IrError::Invalid(format!(
                "aggregate '{}' requires a numeric field, got {t}",
                a.func.as_str()
            )));
        }

        let out_ty = match a.func {
            AggFn::Count => ValueType::Int64,
            AggFn::Avg | AggFn::Quantile => ValueType::Float64,
            AggFn::Sum | AggFn::Min | AggFn::Max => of_type.unwrap_or(ValueType::Float64),
        };
        Ok(Column::new(a.as_name.clone(), out_ty))
    }

    fn apply_rank(&mut self, stage: &str, rank: &Rank) -> Result<(), IrError> {
        self.require_rowset(stage)?;
        if rank.n <= 0 {
            return Err(IrError::InvalidRankSize { n: rank.n });
        }
        self.require_filterable(&rank.of)?;
        let ty = self.ref_type(&rank.of)?;
        if !is_numeric(&ty) {
            return Err(IrError::Invalid(format!(
                "{stage} requires a numeric column, but '{}' is {ty}",
                rank.of
            )));
        }
        // Rank preserves the relation type.
        Ok(())
    }

    fn apply_order(&mut self, keys: &[Order]) -> Result<(), IrError> {
        self.require_rowset("order")?;
        for key in keys {
            self.require_filterable(&key.of)?;
            let _ = self.ref_type(&key.of)?;
        }
        Ok(())
    }

    fn guard_logical_name(&self, name: &str) -> Result<(), IrError> {
        if self.resolver.is_physical_name(self.source, name)
            && !self.resolver.is_known(self.source, name)
        {
            return Err(IrError::PhysicalAddressing {
                field: name.to_string(),
            });
        }
        Ok(())
    }
}

fn coerce_for(field: &str, value: &serde_json::Value, target: &ValueType) -> Result<(), IrError> {
    coerce(value, target)
        .map(|_| ())
        .map_err(|_| IrError::Coercion {
            field: field.to_string(),
            value: value.to_string(),
            target: target.to_string(),
        })
}

fn check_range(range: &Range) -> Result<(), IrError> {
    coerce_for("range.from", &range.from, &ValueType::TimestampNs)?;
    coerce_for("range.to", &range.to, &ValueType::TimestampNs)?;
    Ok(())
}

fn is_numeric(t: &ValueType) -> bool {
    matches!(
        t,
        ValueType::Int64 | ValueType::Float64 | ValueType::DurationNs | ValueType::TimestampNs
    )
}

fn validate_envelope(declared: ResultEnvelope, terminal: &RelationType) -> Result<(), IrError> {
    let ok = match (declared, terminal) {
        (ResultEnvelope::Rows, RelationType::RowSet(rs)) => !rs.aggregated,
        (ResultEnvelope::Table, RelationType::RowSet(rs)) => rs.aggregated,
        (ResultEnvelope::Series, RelationType::Series(_)) => true,
        (ResultEnvelope::Heatmap, RelationType::Heatmap(_)) => true,
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(IrError::EnvelopeMismatch {
            declared: declared.as_str(),
            terminal: terminal.describe(),
        })
    }
}

fn validate_fields(doc: &Document, ctx: &InferCtx<'_>) -> Result<(), IrError> {
    let Some(fields) = &doc.fields else {
        return Ok(());
    };
    if matches!(doc.result, ResultEnvelope::Series | ResultEnvelope::Heatmap) {
        return Err(IrError::FieldsOnSeries);
    }
    for field in fields {
        // A field is valid iff it is present in the terminal relation — a
        // closed column when aggregated, or a resolvable logical field when not.
        guard_logical_name(field)?;
        if ctx.ref_type(field).is_err() {
            return Err(IrError::FieldNotInTerminal {
                field: field.clone(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_ir::resolver::InMemoryResolver;
    use serde_json::json;

    fn logs_resolver() -> InMemoryResolver {
        InMemoryResolver::new()
            .with_column(
                "logs",
                "severity_number",
                "severity_number",
                ValueType::Int64,
            )
            .with_column("logs", "service.name", "service_name", ValueType::String)
            .with_physical_name("logs", "attributes_json")
            .with_physical_name("logs", "attr_tokens")
            .with_column("logs", "body", "body", ValueType::String)
            .with_attribute(
                "logs",
                "structured.payload",
                "log_attributes",
                ValueType::String,
                false,
            )
            .with_retrieval_only("logs", "structured.payload")
            .with_attribute(
                "logs",
                "deployment.environment",
                "log_attributes",
                ValueType::String,
                false,
            )
            .with_column(
                "traces",
                "duration_nano",
                "duration_nano",
                ValueType::DurationNs,
            )
            .with_column("traces", "service.name", "service_name", ValueType::String)
            .with_column("traces", "name", "name", ValueType::String)
    }

    fn profiles_resolver() -> InMemoryResolver {
        InMemoryResolver::new()
            .with_column("profiles", "profile.id", "profile_id", ValueType::String)
            .with_column("profiles", "timestamp", "timestamp", ValueType::TimestampNs)
            .with_column(
                "profiles",
                "duration",
                "duration_nano",
                ValueType::DurationNs,
            )
            .with_column("profiles", "sample.type", "sample_type", ValueType::String)
            .with_column(
                "profiles",
                "service.name",
                "service_name",
                ValueType::String,
            )
            .with_attribute(
                "profiles",
                "resource.deployment.environment",
                "resource_attributes",
                ValueType::String,
                false,
            )
    }

    fn doc(v: serde_json::Value) -> Document {
        serde_json::from_value(v).expect("document parses")
    }

    fn validate_json(v: serde_json::Value) -> Result<Validated, IrError> {
        validate(&doc(v), &SourceRegistry::core(), &logs_resolver())
    }

    // Task 1.3 — relation-type inference.
    #[test]
    fn aggregate_with_step_infers_series() {
        let v = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "series",
            "pipeline": [
                { "where": { "field": "severity_number", "op": "gte", "value": 17 } },
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" } }
            ]
        }))
        .unwrap();
        match v.terminal {
            RelationType::Series(s) => {
                assert_eq!(s.labels, vec!["service.name"]);
                assert_eq!(s.value, ValueType::Int64);
                assert_eq!(s.step_ns, 60_000_000_000);
            }
            other => panic!("expected series, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_without_step_infers_aggregated_table() {
        let v = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }] } }
            ]
        }))
        .unwrap();
        match v.terminal {
            RelationType::RowSet(rs) => {
                assert!(rs.aggregated);
                assert_eq!(rs.grain, Grain::Group);
                assert!(rs.columns.iter().any(|c| c.name == "n"));
                assert!(rs.columns.iter().any(|c| c.name == "service.name"));
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn extract_on_traces_is_rejected_naming_the_stage() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "traces", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [
                { "extract": { "parser": "json", "as": [{ "name": "x", "type": "string" }] } }
            ]
        }))
        .unwrap_err();
        match err {
            IrError::IllegalStage { stage, .. } => assert_eq!(stage, "extract"),
            other => panic!("expected IllegalStage, got {other:?}"),
        }
    }

    #[test]
    fn topk_after_aggregate_dropped_column_fails() {
        // `severity_number` is not carried past the aggregate; ranking by it fails.
        let err = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }] } },
                { "topk": { "n": 5, "of": "severity_number" } }
            ]
        }))
        .unwrap_err();
        assert!(
            matches!(err, IrError::UnknownReference { .. }),
            "got {err:?}"
        );
    }

    // Task 1.4 — versioning.
    #[test]
    fn unsupported_version_reports_range() {
        let err = validate_json(json!({
            "irVersion": 99, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows", "pipeline": []
        }))
        .unwrap_err();
        assert_eq!(
            err,
            IrError::UnsupportedVersion {
                found: 99,
                min: 1,
                max: 2
            }
        );
    }

    fn heatmap_doc(version: i64, bounds: serde_json::Value) -> serde_json::Value {
        json!({
            "irVersion": version, "from": "traces", "range": { "from": "now-1h", "to": "now" },
            "result": "heatmap", "pipeline": [{ "heatmap": {
                "x": { "step": "1m", "align": "epoch" },
                "y": { "of": "duration_nano", "bounds": bounds, "overflow": true },
                "value": { "fn": "count", "as": "count" }
            }}]
        })
    }

    #[test]
    fn v2_duration_heatmap_parses_coerces_bounds_and_infers_relation() {
        let validated = validate_json(heatmap_doc(2, json!(["1ms", "5ms", "1s"]))).unwrap();
        assert!(
            matches!(validated.terminal, RelationType::Heatmap(ref heatmap)
            if heatmap.y_bounds == vec![1_000_000, 5_000_000, 1_000_000_000])
        );
    }

    #[test]
    fn v1_rejects_heatmap_stage_and_envelope() {
        let err = validate_json(heatmap_doc(1, json!(["1ms"]))).unwrap_err();
        assert!(matches!(err, IrError::Invalid(message) if message.contains("irVersion 2")));
    }

    #[test]
    fn heatmap_rejects_non_increasing_duration_bounds() {
        let err = validate_json(heatmap_doc(2, json!(["5ms", "5ms"]))).unwrap_err();
        assert!(
            matches!(err, IrError::Invalid(message) if message.contains("strictly increasing"))
        );
    }

    #[test]
    fn heatmap_rejects_non_duration_y_axes() {
        let mut document = heatmap_doc(2, json!([1, 5]));
        document["pipeline"][0]["heatmap"]["y"]["of"] = json!("numeric");

        let resolver =
            logs_resolver().with_column("traces", "numeric", "numeric", ValueType::Int64);
        let err = validate(&doc(document), &SourceRegistry::core(), &resolver).unwrap_err();

        assert!(
            matches!(err, IrError::Invalid(ref message) if message.contains("duration field")),
            "got {err:?}"
        );
    }

    #[test]
    fn heatmap_envelope_requires_terminal_heatmap_relation() {
        let err = validate_json(json!({
            "irVersion": 2, "from": "traces", "range": { "from": "now-1h", "to": "now" },
            "result": "heatmap", "pipeline": []
        }))
        .unwrap_err();
        assert!(matches!(err, IrError::EnvelopeMismatch { .. }));
    }

    // Task 1.5 — declared-envelope validation.
    #[test]
    fn series_over_rowset_terminal_is_rejected() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "series",
            "pipeline": [ { "where": { "field": "body", "op": "contains", "value": "x" } } ]
        }))
        .unwrap_err();
        assert!(
            matches!(err, IrError::EnvelopeMismatch { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn rows_over_grouped_aggregate_is_rejected() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }] } } ]
        }))
        .unwrap_err();
        assert!(
            matches!(err, IrError::EnvelopeMismatch { .. }),
            "got {err:?}"
        );
    }

    // Task 1.6 — extensible source forward-compat.
    #[test]
    fn unregistered_source_is_a_clear_error_not_a_parse_failure() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "metrics", "range": { "from": "now-1h", "to": "now" },
            "result": "rows", "pipeline": []
        }))
        .unwrap_err();
        assert!(matches!(err, IrError::UnknownSource { .. }), "got {err:?}");
    }

    #[test]
    fn a_previously_valid_document_still_validates_after_registering_a_source() {
        let d = doc(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows", "pipeline": []
        }));
        let mut sources = SourceRegistry::core();
        sources.register(SourceDef {
            name: "metrics".to_string(),
            grain: Grain::Event,
            allows_extract: false,
        });
        // Same document, unchanged shape, still validates.
        assert!(validate(&d, &sources, &logs_resolver()).is_ok());
    }

    #[test]
    fn profiles_is_registered_as_a_summary_row_source() {
        let sources = SourceRegistry::core();
        let source = sources
            .resolve("profiles")
            .expect("profiles source is registered");
        assert_eq!(source.grain, Grain::Event);
        assert!(!source.allows_extract);
    }

    #[test]
    fn profiles_accept_registered_summary_fields_and_resource_attributes() {
        let document = doc(json!({
            "irVersion": 1, "from": "profiles", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "fields": ["profile.id", "timestamp", "duration", "sample.type", "service.name"],
            "pipeline": [{ "where": { "field": "resource.deployment.environment", "op": "eq", "value": "prod" } }]
        }));
        assert!(validate(&document, &SourceRegistry::core(), &profiles_resolver()).is_ok());
    }

    #[test]
    fn profiles_reject_payload_json_and_log_extraction() {
        let payload = doc(json!({
            "irVersion": 1, "from": "profiles", "range": { "from": "now-1h", "to": "now" },
            "result": "rows", "fields": ["samples_json"], "pipeline": []
        }));
        assert!(matches!(
            validate(&payload, &SourceRegistry::core(), &profiles_resolver()),
            Err(IrError::PhysicalAddressing { .. })
        ));

        let extract = doc(json!({
            "irVersion": 1, "from": "profiles", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [{ "extract": { "parser": "json", "as": [{ "name": "x", "type": "string" }] } }]
        }));
        assert!(matches!(
            validate(&extract, &SourceRegistry::core(), &profiles_resolver()),
            Err(IrError::IllegalStage { stage, .. }) if stage == "extract"
        ));
    }

    // Task 2.1 — logical-namespace guard.
    #[test]
    fn physical_column_reference_is_rejected() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [ { "where": { "field": "attributes_json", "op": "contains", "value": "x" } } ]
        }))
        .unwrap_err();
        assert!(
            matches!(err, IrError::PhysicalAddressing { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn physical_alias_is_rejected_but_its_logical_field_is_allowed() {
        let physical = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [ { "where": { "field": "service_name", "op": "eq", "value": "api" } } ]
        }))
        .unwrap_err();
        assert!(matches!(physical, IrError::PhysicalAddressing { .. }));

        validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [ { "where": { "field": "service.name", "op": "eq", "value": "api" } } ]
        }))
        .unwrap();
    }

    #[test]
    fn retrieval_only_field_is_rejected_in_a_predicate() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [ { "where": { "field": "structured.payload", "op": "exists" } } ]
        }))
        .unwrap_err();
        assert!(matches!(err, IrError::UnfilterableField { .. }));
    }

    #[test]
    fn retrieval_only_field_is_rejected_in_grouping_and_ordering() {
        for pipeline in [
            json!([{ "aggregate": { "by": ["structured.payload"], "aggs": [{ "fn": "count", "as": "n" }] } }]),
            json!([{ "order": [{ "of": "structured.payload", "dir": "asc" }] }]),
        ] {
            let err = validate_json(json!({
                "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
                "result": "rows", "pipeline": pipeline
            }))
            .unwrap_err();
            assert!(
                matches!(err, IrError::UnfilterableField { .. }),
                "got {err:?}"
            );
        }
    }

    // Task 2.2 — structured operands.
    #[test]
    fn expression_string_operand_is_rejected() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "traces", "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "max", "of": "duration_nano", "as": "max_dur" }] } },
                { "topk": { "n": 10, "of": "max(duration_nano)" } }
            ]
        }))
        .unwrap_err();
        assert!(
            matches!(err, IrError::ExpressionString { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn duplicate_aggregate_name_is_rejected() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "traces", "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["service.name"], "aggs": [
                    { "fn": "max", "of": "duration_nano", "as": "d" },
                    { "fn": "min", "of": "duration_nano", "as": "d" }
                ] } }
            ]
        }))
        .unwrap_err();
        assert!(matches!(err, IrError::DuplicateName { .. }), "got {err:?}");
    }

    #[test]
    fn order_reference_to_unknown_name_is_rejected() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "traces", "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }] } },
                { "order": [{ "of": "nonexistent", "dir": "desc" }] }
            ]
        }))
        .unwrap_err();
        assert!(
            matches!(err, IrError::UnknownReference { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn non_positive_rank_size_is_rejected() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "traces", "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [
                { "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "max", "of": "duration_nano", "as": "d" }] } },
                { "topk": { "n": 0, "of": "d" } }
            ]
        }))
        .unwrap_err();
        assert!(
            matches!(err, IrError::InvalidRankSize { n: 0 }),
            "got {err:?}"
        );
    }

    // Task 2.3 — extract field resolution + non-shadowing.
    #[test]
    fn extract_derives_usable_typed_field() {
        let v = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "fields": ["level"],
            "pipeline": [
                { "extract": { "parser": "logfmt", "as": [{ "name": "level", "type": "string" }] } },
                { "where": { "field": "level", "op": "eq", "value": "error" } }
            ]
        }))
        .unwrap();
        assert!(matches!(v.terminal, RelationType::RowSet(_)));
    }

    #[test]
    fn extract_colliding_with_registry_field_is_rejected() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [
                { "extract": { "parser": "json", "as": [{ "name": "service.name", "type": "string" }] } }
            ]
        }))
        .unwrap_err();
        assert!(matches!(err, IrError::NameCollision { .. }), "got {err:?}");
    }

    // Coercion — un-coercible literal rejected at validation (task 1.1 at the
    // document level).
    #[test]
    fn uncoercible_literal_rejected_at_validation() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [ { "where": { "field": "severity_number", "op": "eq", "value": "banana" } } ]
        }))
        .unwrap_err();
        assert!(matches!(err, IrError::Coercion { .. }), "got {err:?}");
    }

    // 12.1a — a field with no canonical registry type is a defined rejection.
    #[test]
    fn unknown_field_type_is_a_defined_rejection() {
        let err = validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "pipeline": [ { "where": { "field": "not.in.registry", "op": "eq", "value": "x" } } ]
        }))
        .unwrap_err();
        assert!(
            matches!(err, IrError::UnknownFieldType { .. }),
            "got {err:?}"
        );
    }

    /// A grouped document whose second aggregate carries `scope` as its scoping
    /// predicate — the RED shape (total beside errors) the traces group table
    /// submits.
    fn scoped_agg_doc(scope: serde_json::Value) -> serde_json::Value {
        json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                { "fn": "count", "as": "n" },
                { "fn": "count", "as": "errors", "where": scope }
            ] } } ]
        })
    }

    #[test]
    fn a_scoped_aggregate_validates_alongside_an_unscoped_one() {
        let v = validate_json(scoped_agg_doc(
            json!({ "field": "severity_number", "op": "gte", "value": 17 }),
        ))
        .expect("a scoped aggregate validates");
        let cols = match &v.terminal {
            RelationType::RowSet(rs) => &rs.columns,
            other => panic!("expected a row-set relation, got {other:?}"),
        };
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert!(
            names.contains(&"n") && names.contains(&"errors"),
            "both outputs are declared: {names:?}"
        );
    }

    #[test]
    fn a_scope_naming_an_unresolvable_field_is_rejected() {
        let err = validate_json(scoped_agg_doc(
            json!({ "field": "not.in.registry", "op": "eq", "value": "x" }),
        ))
        .unwrap_err();
        assert!(
            matches!(err, IrError::UnknownFieldType { .. }),
            "the scope resolves through the same registry as `where`: got {err:?}"
        );
    }

    #[test]
    fn a_scope_with_an_uncoercible_literal_is_rejected() {
        let err = validate_json(scoped_agg_doc(
            json!({ "field": "severity_number", "op": "eq", "value": "banana" }),
        ))
        .unwrap_err();
        assert!(
            matches!(err, IrError::Coercion { .. }),
            "the scope coerces literals like `where`: got {err:?}"
        );
    }

    #[test]
    fn a_scope_misusing_an_operator_is_rejected() {
        let err = validate_json(scoped_agg_doc(
            json!({ "field": "service.name", "op": "exists", "value": "x" }),
        ))
        .unwrap_err();
        assert!(
            matches!(err, IrError::Invalid(ref m) if m.contains("exists")),
            "got {err:?}"
        );
    }

    #[test]
    fn a_scope_may_not_address_physical_storage() {
        let err = validate_json(scoped_agg_doc(
            json!({ "field": "log_attributes", "op": "exists" }),
        ))
        .unwrap_err();
        assert!(
            !matches!(err, IrError::Invalid(ref m) if m.is_empty()),
            "the logical-namespace guard applies to a scope too: got {err:?}"
        );
    }

    #[test]
    fn a_scope_on_a_non_count_aggregate_validates() {
        validate_json(json!({
            "irVersion": 1, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "table",
            "pipeline": [ { "aggregate": { "by": ["service.name"], "aggs": [
                { "fn": "max", "of": "severity_number", "as": "worst",
                  "where": { "field": "body", "op": "contains", "value": "timeout" } }
            ] } } ]
        }))
        .expect("a scoped non-count aggregate validates");
    }
}
