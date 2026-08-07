//! Error rendering helpers shared across services.

/// Render an error together with its full cause chain.
///
/// `anyhow`'s plain `Display` renders only the outermost `.context(...)`, so a
/// log written with `error = %e` shows the wrapper and silently discards the
/// cause that actually explains the failure. Every distinct underlying fault
/// then reaches the operator as the same indistinguishable string. The
/// alternate `{:#}` form joins the whole chain with `: `.
pub fn format_error_chain(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_error_chain_preserves_causes_beneath_the_outermost_context() {
        // The acceptor wraps every forward failure as "Flight do_put failed".
        // Without the chain, a schema rejection, a size limit and a dead
        // writer are indistinguishable in the log — which is exactly what
        // blocked diagnosis of the stuck metrics entry on hive (issue #1058).
        let error = anyhow::anyhow!("status: InvalidArgument, message: schema mismatch")
            .context("Flight do_put failed");

        let rendered = format_error_chain(&error);

        assert!(
            rendered.contains("Flight do_put failed"),
            "outermost context must survive: {rendered}"
        );
        assert!(
            rendered.contains("schema mismatch"),
            "underlying cause must survive: {rendered}"
        );
    }

    #[test]
    fn plain_display_drops_the_cause_chain() {
        // Guards the reason this helper exists: if `Display` ever started
        // including causes, the helper would be redundant.
        let error = anyhow::anyhow!("status: InvalidArgument, message: schema mismatch")
            .context("Flight do_put failed");

        assert!(
            !format!("{error}").contains("schema mismatch"),
            "plain Display is expected to drop causes"
        );
    }
}
