// Drift Detection & Classification
//
// Compares expected table state (from Axiom replay)
// with actual Iceberg table state and classifies drift
// by severity and intent.

use crate::adapters::iceberg::IcebergTableState;
use crate::state::TableState;

/// Severity of detected drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftSeverity {
    /// Informational drift (no immediate risk).
    Info,

    /// Warning-level drift (potential risk).
    Warning,

    /// Critical drift (data correctness at risk).
    Critical,
}

/// Types of drift that can occur.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftType {
    UnexpectedMutation,
    SchemaMismatch,
    SnapshotMismatch,
}

/// A single drift finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriftFinding {
    pub drift_type: DriftType,
    pub severity: DriftSeverity,
    pub message: String,
}

/// Full drift report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriftReport {
    pub findings: Vec<DriftFinding>,
}

impl DriftReport {
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }

    pub fn highest_severity(&self) -> Option<&DriftSeverity> {
        self.findings
            .iter()
            .map(|f| &f.severity)
            .max_by_key(|s| match s {
                DriftSeverity::Info => 0,
                DriftSeverity::Warning => 1,
                DriftSeverity::Critical => 2,
            })
    }
}

/// Detect and classify drift between expected and actual state.
pub fn detect_drift(expected: &TableState, actual: &IcebergTableState) -> DriftReport {
    let mut findings = Vec::new();

    // Rule 1: Unexpected mutation while ACTIVE
    if expected == &TableState::Active && actual.current_snapshot_id.is_some() {
        findings.push(DriftFinding {
            drift_type: DriftType::UnexpectedMutation,
            severity: DriftSeverity::Warning,
            message: "table snapshot changed while expected state is ACTIVE".into(),
        });
    }

    // Rule 2: Schema mismatch (future: compare with expected schema id)
    if actual.current_schema_id < 0 {
        findings.push(DriftFinding {
            drift_type: DriftType::SchemaMismatch,
            severity: DriftSeverity::Critical,
            message: "invalid schema identifier detected".into(),
        });
    }

    DriftReport { findings }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn warning_drift_detected() {
        let expected = TableState::Active;

        let actual = IcebergTableState {
            table_uuid: Uuid::new_v4(),
            current_snapshot_id: Some(99),
            current_schema_id: 1,
        };

        let report = detect_drift(&expected, &actual);
        assert_eq!(report.findings.len(), 1);
        assert_eq!(report.findings[0].severity, DriftSeverity::Warning);
    }

    #[test]
    fn highest_severity_computed_correctly() {
        let report = DriftReport {
            findings: vec![
                DriftFinding {
                    drift_type: DriftType::UnexpectedMutation,
                    severity: DriftSeverity::Warning,
                    message: "warning".into(),
                },
                DriftFinding {
                    drift_type: DriftType::SchemaMismatch,
                    severity: DriftSeverity::Critical,
                    message: "critical".into(),
                },
            ],
        };

        assert_eq!(report.highest_severity(), Some(&DriftSeverity::Critical));
    }

    #[test]
    fn clean_state_has_no_severity() {
        let report = DriftReport { findings: vec![] };
        assert!(report.is_clean());
        assert!(report.highest_severity().is_none());
    }
}
