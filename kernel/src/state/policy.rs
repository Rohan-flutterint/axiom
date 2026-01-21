// Policy-Driven Drift Handling (Dry-Run)
//
// Converts drift signals into intended actions without enforcement.
// This module is pure, deterministic, and auditable.

use crate::state::drift::{DriftReport, DriftSeverity};
use serde::Serialize;

/// Intended action for a detected drift.
///
/// NOTE:
/// These actions are *not executed* in dry-run mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum IntendedAction {
    /// Log only, no escalation.
    Observe,

    /// Notify operators or governance systems.
    Alert,

    /// Would block or rollback in enforcement mode.
    Enforce,
}

/// A single policy decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PolicyDecision {
    pub severity: DriftSeverity,
    pub action: IntendedAction,
    pub reason: String,
}

/// Output of policy evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecisionPlan {
    pub decisions: Vec<PolicyDecision>,
}

impl DecisionPlan {
    pub fn is_empty(&self) -> bool {
        self.decisions.is_empty()
    }
}

/// Policy engine (dry-run).
///
/// In the future this will be configurable.
/// For now it is deterministic and rule-based.
pub fn evaluate_drift_policy(report: &DriftReport) -> DecisionPlan {
    let mut decisions = Vec::new();

    for finding in &report.findings {
        let (action, reason) = match finding.severity {
            DriftSeverity::Info => (
                IntendedAction::Observe,
                "informational drift, no action required",
            ),
            DriftSeverity::Warning => (
                IntendedAction::Alert,
                "warning-level drift, operator attention recommended",
            ),
            DriftSeverity::Critical => (
                IntendedAction::Enforce,
                "critical drift detected, enforcement would be required",
            ),
        };

        decisions.push(PolicyDecision {
            severity: finding.severity.clone(),
            action,
            reason: reason.into(),
        });
    }

    DecisionPlan { decisions }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::drift::{DriftFinding, DriftReport, DriftType};

    #[test]
    fn policy_maps_severity_to_action() {
        let report = DriftReport {
            findings: vec![
                DriftFinding {
                    drift_type: DriftType::UnexpectedMutation,
                    severity: DriftSeverity::Warning,
                    message: "mutation".into(),
                },
                DriftFinding {
                    drift_type: DriftType::SchemaMismatch,
                    severity: DriftSeverity::Critical,
                    message: "schema".into(),
                },
            ],
        };

        let plan = evaluate_drift_policy(&report);

        assert_eq!(plan.decisions.len(), 2);
        assert_eq!(plan.decisions[0].action, IntendedAction::Alert);
        assert_eq!(plan.decisions[1].action, IntendedAction::Enforce);
    }

    #[test]
    fn empty_report_produces_empty_plan() {
        let report = DriftReport { findings: vec![] };
        let plan = evaluate_drift_policy(&report);
        assert!(plan.is_empty());
    }
}
