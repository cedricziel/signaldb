use ratatui::Terminal;
use ratatui::backend::TestBackend;

/// Create a test terminal with standard dimensions (120x40)
#[allow(dead_code)]
pub fn create_test_terminal() -> Terminal<TestBackend> {
    Terminal::new(TestBackend::new(120, 40)).unwrap()
}

/// Assert that the terminal buffer contains the given text
#[allow(dead_code)]
pub fn assert_buffer_contains(terminal: &Terminal<TestBackend>, text: &str) {
    let buffer = terminal.backend().buffer().clone();
    let content: String = buffer.content().iter().map(|c| c.symbol()).collect();
    assert!(
        content.contains(text),
        "Buffer does not contain '{text}'.\nBuffer content: {content}"
    );
}
