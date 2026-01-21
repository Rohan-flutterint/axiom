// Deterministic Replay Engine
//
// Replays metadata events while enforcing invariants and
// producing a final derived table state.

use crate::invariants::{InvariantEngine, InvariantViolation};
use crate::log::MetadataLog;
use crate::state::{StateError, TableState, TableStateMachine};

/// Errors that can occur during replay.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("state machine error: {0}")]
    State(#[from] StateError),

    #[error("invariant violation: {0}")]
    Invariant(#[from] InvariantViolation),
}

/// Replay the metadata log and derive the final table state.
///
/// This is the *only* supported way to derive table state.
pub fn replay_table_state(
    log: &MetadataLog,
    invariants: &InvariantEngine,
) -> Result<TableState, ReplayError> {
    let mut state_machine = TableStateMachine::new();
    let mut current_state = state_machine.current_state().clone();

    for event in log.replay() {
        // Apply event to state machine
        state_machine.apply(event)?;
        let next_state = state_machine.current_state().clone();

        // Enforce invariants
        invariants.evaluate(&current_state, event, &next_state)?;

        // Commit transition
        current_state = next_state;
    }

    Ok(current_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariants::{Invariant, InvariantResult};
    use crate::log::{EventType, MetadataLog, TableEvent, TableId};
    use crate::state::TableState;
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
                InvariantResult::Fail("cannot mutate from CREATED".into())
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
    fn replay_succeeds_with_valid_invariants() {
        let mut log = MetadataLog::new();
        log.append(event(1, EventType::TableCreated)).unwrap();
        log.append(event(2, EventType::SchemaUpdated)).unwrap();
        log.append(event(3, EventType::SnapshotAdded)).unwrap();

        let mut invariants = InvariantEngine::new();
        invariants.register(NoMutateFromCreated);

        let state = replay_table_state(&log, &invariants).unwrap();
        assert_eq!(state, TableState::Active);
    }

    #[test]
    fn replay_fails_on_invalid_transition() {
        let mut log = MetadataLog::new();
        log.append(event(1, EventType::SchemaUpdated)).unwrap();

        let mut invariants = InvariantEngine::new();
        invariants.register(NoMutateFromCreated);

        let err = replay_table_state(&log, &invariants).unwrap_err();

        // Could be state or invariant failure — both are acceptable
        let msg = err.to_string();
        assert!(
            msg.contains("state machine") || msg.contains("illegal"),
            "unexpected error: {msg}"
        );
    }
}
