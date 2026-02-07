//! Application state management

use std::time::Duration;
use std::time::SystemTime;

/// Permission level detected from authentication
#[derive(Clone, Debug, PartialEq)]
pub enum Permission {
    /// Admin access with admin API key
    Admin { admin_key: String },
    /// Tenant access with API key
    Tenant {
        api_key: String,
        tenant_id: String,
        dataset_id: Option<String>,
    },
    /// Permission not yet detected
    Unknown,
}

/// Available tabs in the TUI
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Tab {
    Dashboard,
    Traces,
    Logs,
    Metrics,
    Admin,
}

impl Tab {
    /// All available tabs (including Admin)
    pub fn all() -> Vec<Tab> {
        vec![
            Tab::Dashboard,
            Tab::Traces,
            Tab::Logs,
            Tab::Metrics,
            Tab::Admin,
        ]
    }

    /// Non-admin tabs only
    pub fn non_admin() -> Vec<Tab> {
        vec![Tab::Dashboard, Tab::Traces, Tab::Logs, Tab::Metrics]
    }

    /// Display label for the tab
    pub fn label(&self) -> &str {
        match self {
            Tab::Dashboard => "Dashboard",
            Tab::Traces => "Traces",
            Tab::Logs => "Logs",
            Tab::Metrics => "Metrics",
            Tab::Admin => "Admin",
        }
    }

    /// Keyboard shortcut for the tab
    pub fn shortcut(&self) -> char {
        match self {
            Tab::Dashboard => '1',
            Tab::Traces => '2',
            Tab::Logs => '3',
            Tab::Metrics => '4',
            Tab::Admin => '5',
        }
    }
}

/// Connection status to SignalDB
#[derive(Clone, Debug, PartialEq)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Connecting,
}

/// Main application state
#[derive(Clone, Debug)]
pub struct AppState {
    /// Current permission level
    pub permission: Permission,
    /// Currently active tab
    pub active_tab: Tab,
    /// Tabs available based on permission
    pub available_tabs: Vec<Tab>,
    /// Connection status to SignalDB
    pub connection_status: ConnectionStatus,
    /// Last error message (if any)
    pub last_error: Option<String>,
    /// Timestamp of last error
    pub last_error_at: Option<SystemTime>,
    /// Refresh rate for data updates
    pub refresh_rate: Duration,
    /// SignalDB HTTP URL
    pub url: String,
    /// SignalDB Flight URL
    pub flight_url: String,
}

impl AppState {
    /// Create a new AppState with default values
    pub fn new(url: String, flight_url: String, refresh_rate: Duration) -> Self {
        Self {
            permission: Permission::Unknown,
            active_tab: Tab::Dashboard,
            available_tabs: Tab::non_admin(),
            connection_status: ConnectionStatus::Disconnected,
            last_error: None,
            last_error_at: None,
            refresh_rate,
            url,
            flight_url,
        }
    }

    /// Set the latest user-visible error with timestamp.
    pub fn set_error(&mut self, message: impl Into<String>) {
        self.last_error = Some(message.into());
        self.last_error_at = Some(SystemTime::now());
    }

    /// Clear the last recorded error.
    pub fn clear_error(&mut self) {
        self.last_error = None;
        self.last_error_at = None;
    }

    /// Update permission and adjust available tabs accordingly
    pub fn set_permission(&mut self, permission: Permission) {
        self.permission = permission.clone();
        self.available_tabs = match permission {
            Permission::Admin { .. } => Tab::all(),
            Permission::Tenant { .. } => Tab::non_admin(),
            Permission::Unknown => Tab::non_admin(),
        };

        // If current tab is not available, switch to Dashboard
        if !self.available_tabs.contains(&self.active_tab) {
            self.active_tab = Tab::Dashboard;
        }
    }

    /// Switch to the next tab (wraps around)
    pub fn next_tab(&mut self) {
        if self.available_tabs.is_empty() {
            return;
        }

        let current_index = self
            .available_tabs
            .iter()
            .position(|t| t == &self.active_tab)
            .unwrap_or(0);

        let next_index = (current_index + 1) % self.available_tabs.len();
        self.active_tab = self.available_tabs[next_index].clone();
    }

    /// Switch to the previous tab (wraps around)
    pub fn prev_tab(&mut self) {
        if self.available_tabs.is_empty() {
            return;
        }

        let current_index = self
            .available_tabs
            .iter()
            .position(|t| t == &self.active_tab)
            .unwrap_or(0);

        let prev_index = if current_index == 0 {
            self.available_tabs.len() - 1
        } else {
            current_index - 1
        };
        self.active_tab = self.available_tabs[prev_index].clone();
    }

    /// Switch to a specific tab by index
    pub fn switch_tab(&mut self, index: usize) {
        if index < self.available_tabs.len() {
            self.active_tab = self.available_tabs[index].clone();
        }
    }
}

/// Detect permission level by attempting authentication
pub async fn detect_permission(
    url: &str,
    admin_key: Option<&str>,
    api_key: Option<&str>,
    tenant_id: Option<&str>,
    dataset_id: Option<&str>,
) -> Permission {
    if let Some(admin_key) = admin_key
        && try_admin_auth(url, admin_key).await
    {
        return Permission::Admin {
            admin_key: admin_key.to_string(),
        };
    }

    if let Some(api_key) = api_key
        && let Some(tenant_id) = tenant_id
    {
        return Permission::Tenant {
            api_key: api_key.to_string(),
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.map(|s| s.to_string()),
        };
    }

    Permission::Unknown
}

/// Try admin authentication by calling the admin API
async fn try_admin_auth(url: &str, admin_key: &str) -> bool {
    let client = reqwest::Client::new();
    let admin_url = format!("{}/api/v1/admin/tenants", url.trim_end_matches('/'));

    let response = client
        .get(&admin_url)
        .header("Authorization", format!("Bearer {}", admin_key))
        .timeout(Duration::from_secs(5))
        .send()
        .await;

    match response {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_state_new_defaults() {
        let state = AppState::new(
            "http://localhost:3000".to_string(),
            "http://localhost:50053".to_string(),
            Duration::from_secs(5),
        );

        assert_eq!(state.permission, Permission::Unknown);
        assert_eq!(state.active_tab, Tab::Dashboard);
        assert_eq!(state.available_tabs, Tab::non_admin());
        assert_eq!(state.connection_status, ConnectionStatus::Disconnected);
        assert_eq!(state.last_error, None);
        assert_eq!(state.last_error_at, None);
        assert_eq!(state.refresh_rate, Duration::from_secs(5));
        assert_eq!(state.url, "http://localhost:3000");
        assert_eq!(state.flight_url, "http://localhost:50053");
    }

    #[test]
    fn test_set_permission_admin() {
        let mut state = AppState::new(
            "http://localhost:3000".to_string(),
            "http://localhost:50053".to_string(),
            Duration::from_secs(5),
        );

        state.set_permission(Permission::Admin {
            admin_key: "test-key".to_string(),
        });

        assert!(matches!(state.permission, Permission::Admin { .. }));
        assert_eq!(state.available_tabs, Tab::all());
        assert_eq!(state.available_tabs.len(), 5);
        assert!(state.available_tabs.contains(&Tab::Admin));
    }

    #[test]
    fn test_set_permission_tenant() {
        let mut state = AppState::new(
            "http://localhost:3000".to_string(),
            "http://localhost:50053".to_string(),
            Duration::from_secs(5),
        );

        state.set_permission(Permission::Tenant {
            api_key: "test-key".to_string(),
            tenant_id: "acme".to_string(),
            dataset_id: Some("production".to_string()),
        });

        assert!(matches!(state.permission, Permission::Tenant { .. }));
        assert_eq!(state.available_tabs, Tab::non_admin());
        assert_eq!(state.available_tabs.len(), 4);
        assert!(!state.available_tabs.contains(&Tab::Admin));
    }

    #[test]
    fn test_set_permission_switches_tab_if_unavailable() {
        let mut state = AppState::new(
            "http://localhost:3000".to_string(),
            "http://localhost:50053".to_string(),
            Duration::from_secs(5),
        );

        // Set admin permission and switch to Admin tab
        state.set_permission(Permission::Admin {
            admin_key: "test-key".to_string(),
        });
        state.active_tab = Tab::Admin;

        // Switch to tenant permission - should move to Dashboard
        state.set_permission(Permission::Tenant {
            api_key: "test-key".to_string(),
            tenant_id: "acme".to_string(),
            dataset_id: None,
        });

        assert_eq!(state.active_tab, Tab::Dashboard);
    }

    #[test]
    fn test_tab_cycling() {
        let mut state = AppState::new(
            "http://localhost:3000".to_string(),
            "http://localhost:50053".to_string(),
            Duration::from_secs(5),
        );

        // Start at Dashboard
        assert_eq!(state.active_tab, Tab::Dashboard);

        // Cycle forward
        state.next_tab();
        assert_eq!(state.active_tab, Tab::Traces);

        state.next_tab();
        assert_eq!(state.active_tab, Tab::Logs);

        state.next_tab();
        assert_eq!(state.active_tab, Tab::Metrics);

        // Wrap around to Dashboard
        state.next_tab();
        assert_eq!(state.active_tab, Tab::Dashboard);

        // Cycle backward
        state.prev_tab();
        assert_eq!(state.active_tab, Tab::Metrics);

        state.prev_tab();
        assert_eq!(state.active_tab, Tab::Logs);
    }

    #[test]
    fn test_switch_tab() {
        let mut state = AppState::new(
            "http://localhost:3000".to_string(),
            "http://localhost:50053".to_string(),
            Duration::from_secs(5),
        );

        // Switch to index 2 (Logs)
        state.switch_tab(2);
        assert_eq!(state.active_tab, Tab::Logs);

        // Switch to index 0 (Dashboard)
        state.switch_tab(0);
        assert_eq!(state.active_tab, Tab::Dashboard);

        // Out of bounds index should be ignored
        state.switch_tab(10);
        assert_eq!(state.active_tab, Tab::Dashboard);
    }

    #[test]
    fn test_tab_labels_and_shortcuts() {
        assert_eq!(Tab::Dashboard.label(), "Dashboard");
        assert_eq!(Tab::Traces.label(), "Traces");
        assert_eq!(Tab::Logs.label(), "Logs");
        assert_eq!(Tab::Metrics.label(), "Metrics");
        assert_eq!(Tab::Admin.label(), "Admin");

        assert_eq!(Tab::Dashboard.shortcut(), '1');
        assert_eq!(Tab::Traces.shortcut(), '2');
        assert_eq!(Tab::Logs.shortcut(), '3');
        assert_eq!(Tab::Metrics.shortcut(), '4');
        assert_eq!(Tab::Admin.shortcut(), '5');
    }

    #[test]
    fn test_set_and_clear_error() {
        let mut state = AppState::new(
            "http://localhost:3000".to_string(),
            "http://localhost:50053".to_string(),
            Duration::from_secs(5),
        );

        state.set_error("network timeout");
        assert_eq!(state.last_error.as_deref(), Some("network timeout"));
        assert!(state.last_error_at.is_some());

        state.clear_error();
        assert!(state.last_error.is_none());
        assert!(state.last_error_at.is_none());
    }

    #[tokio::test]
    async fn test_detect_permission_unknown() {
        let permission = detect_permission("http://localhost:3000", None, None, None, None).await;

        assert_eq!(permission, Permission::Unknown);
    }

    #[tokio::test]
    async fn test_detect_permission_tenant() {
        let permission = detect_permission(
            "http://localhost:3000",
            None,
            Some("test-api-key"),
            Some("acme"),
            Some("production"),
        )
        .await;

        assert!(matches!(permission, Permission::Tenant { .. }));
        if let Permission::Tenant {
            api_key,
            tenant_id,
            dataset_id,
        } = permission
        {
            assert_eq!(api_key, "test-api-key");
            assert_eq!(tenant_id, "acme");
            assert_eq!(dataset_id, Some("production".to_string()));
        }
    }

    #[tokio::test]
    async fn test_detect_permission_tenant_without_dataset() {
        let permission = detect_permission(
            "http://localhost:3000",
            None,
            Some("test-api-key"),
            Some("acme"),
            None,
        )
        .await;

        assert!(matches!(permission, Permission::Tenant { .. }));
        if let Permission::Tenant {
            api_key,
            tenant_id,
            dataset_id,
        } = permission
        {
            assert_eq!(api_key, "test-api-key");
            assert_eq!(tenant_id, "acme");
            assert_eq!(dataset_id, None);
        }
    }
}
