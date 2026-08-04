use std::time::Duration;
use writer::RetryConfig;

#[test]
fn retry_config_default_matches_documented_policy() {
    // The default retry policy is what every writer uses unless explicitly
    // overridden, so pin its literal values directly against RetryConfig's
    // Default impl without paying for catalog/writer setup.
    let retry_config = RetryConfig::default();

    assert_eq!(retry_config.max_attempts, 3);
    assert_eq!(retry_config.initial_delay, Duration::from_millis(100));
    assert_eq!(retry_config.max_delay, Duration::from_secs(5));
    assert_eq!(retry_config.backoff_multiplier, 2.0);
}
