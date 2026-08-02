# query-ir-core

Versioned, structured, signal-agnostic query IR (with a type system defined independently of the plan) lowered to DataFusion LogicalPlan, over a native POST /api/v1/query surface — single-signal (logs, traces) core. Cross-signal correlation and structural trace matching are follow-up sibling changes, not part of this capability.
