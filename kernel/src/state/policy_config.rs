// Policy Configuration
//
// Defines configurable mappings from drift severity
// to intended actions.

use serde::{Deserialize, Serialize};

use crate::state::drift::DriftSeverity;
use crate::state::policy::IntendedAction;

/// Policy configuration loaded from JSON/YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    pub rules: Vec<PolicyRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRule {
    pub severity: DriftSeverity,
    pub action: IntendedAction,
    pub reason: String,
}

impl PolicyConfig {
    /// Default built-in policy (used if no config is provided).
    pub fn default_policy() -> Self {
        Self {
            rules: vec![
                PolicyRule {
                    severity: DriftSeverity::Info,
                    action: IntendedAction::Observe,
                    reason: "informational drift".into(),
                },
                PolicyRule {
                    severity: DriftSeverity::Warning,
                    action: IntendedAction::Alert,
                    reason: "warning-level drift".into(),
                },
                PolicyRule {
                    severity: DriftSeverity::Critical,
                    action: IntendedAction::Enforce,
                    reason: "critical drift".into(),
                },
            ],
        }
    }
}
