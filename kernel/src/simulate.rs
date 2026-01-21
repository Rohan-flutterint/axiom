// End-to-End Control Plane Simulation
//
// Runs the full Axiom pipeline in dry-run mode:
// log → replay → drift → policy → decision plan

use crate::adapters::iceberg::IcebergTableState;
use crate::invariants::InvariantEngine;
use crate::log::{MetadataLog, MetadataLogStore};
use crate::replay::{replay_table_state, ReplayError};
use crate::state::drift::{detect_drift, DriftReport};
use crate::state::policy::{evaluate_drift_policy_with_config, DecisionPlan};
use crate::state::policy_config::PolicyConfig;
use crate::state::TableState;

/// Result of a full simulation run.
#[derive(Debug)]
pub struct SimulationResult {
    pub expected_state: TableState,
    pub drift_report: DriftReport,
    pub decision_plan: DecisionPlan,
}

/// Errors that can occur during simulation.
#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    #[error("replay failed: {0}")]
    Replay(#[from] ReplayError),
}

/// Run a full end-to-end simulation.
///
/// This function is:
/// - deterministic
/// - side-effect free
/// - safe to run repeatedly
pub fn simulate_table<S: MetadataLogStore>(
    log: &MetadataLog<S>,
    invariants: &InvariantEngine,
    actual_state: &IcebergTableState,
    policy: &PolicyConfig,
) -> Result<SimulationResult, SimulationError> {
    // 1. Derive expected state
    let expected_state = replay_table_state(log, invariants)?;

    // 2. Detect drift
    let drift_report = detect_drift(&expected_state, actual_state);

    // 3. Evaluate policy (dry-run)
    let decision_plan = evaluate_drift_policy_with_config(&drift_report, policy);


    Ok(SimulationResult {
        expected_state,
        drift_report,
        decision_plan,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariants::{Invariant, InvariantResult};
    use crate::log::{EventType, InMemoryLogStore, MetadataLog, TableEvent, TableId};
    use uuid::Uuid;

    struct NoMutateFromCreated;

    impl Invariant for NoMutateFromCreated {
        fn name(&self) -> &'static str {
            "no-mutate-from-created"
        }

        fn validate(
            &self,
            previous: &TableState,
            _event: &TableEvent,
            next: &TableState,
        ) -> InvariantResult {
            if previous == &TableState::Created && next == &TableState::Mutating {
                InvariantResult::Fail("mutation not allowed".into())
            } else {
                InvariantResult::Pass
            }
        }
    }

    fn event(version: u64, event_type: EventType) -> TableEvent {
        TableEvent {
            table_id: TableId(Uuid::new_v4()),
            version,
            event_type,
            payload: vec![],
        }
    }

    #[test]
    fn full_simulation_pipeline_runs() {
        // Metadata log
        let store = InMemoryLogStore::default();
        let mut log = MetadataLog::new(store);

        log.append(event(1, EventType::TableCreated)).unwrap();
        log.append(event(2, EventType::SchemaUpdated)).unwrap();

        // Invariants
        let mut invariants = InvariantEngine::new();
        invariants.register(NoMutateFromCreated);

        // Actual Iceberg state (simulated)
        let actual = IcebergTableState {
            table_uuid: Uuid::new_v4(),
            current_snapshot_id: Some(42),
            current_schema_id: 1,
        };

        use crate::state::policy_config::PolicyConfig;

        let policy = PolicyConfig::default_policy();

        let result = simulate_table(&log, &invariants, &actual, &policy).unwrap();


        assert_eq!(result.expected_state, TableState::Mutating);

        // During a valid mutation, there should be no drift
        assert!(result.drift_report.is_clean());

        // No drift => no policy decisions
        assert!(result.decision_plan.is_empty());
    }
}
