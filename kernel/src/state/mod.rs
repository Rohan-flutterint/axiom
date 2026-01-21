// Table State Machine
//
// Derives the current table state from a sequence of metadata events.
// This module is pure, deterministic, and side-effect free.

use crate::log::{EventType, TableEvent};
pub mod drift;
pub mod policy;
pub mod policy_config;

/// High-level lifecycle state of a table.
///
/// NOTE:
/// States are intentionally coarse-grained in early versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableState {
    /// Table exists but has no committed data yet.
    Created,

    /// Table is readable and stable.
    Active,

    /// Table is undergoing a mutation (schema change, rewrite, etc.).
    Mutating,
}

/// Errors produced during state transitions.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StateError {
    #[error("illegal state transition: {0}")]
    IllegalTransition(String),
}

/// Stateful reducer for table events.
#[derive(Debug)]
pub struct TableStateMachine {
    state: TableState,
}

impl TableStateMachine {
    /// Create a new state machine for a freshly created table.
    pub fn new() -> Self {
        Self {
            state: TableState::Created,
        }
    }

    /// Apply a single metadata event to the state machine.
    pub fn apply(&mut self, event: &TableEvent) -> Result<(), StateError> {
        use EventType::*;
        use TableState::*;

        self.state = match (&self.state, &event.event_type) {
            // Table creation
            (Created, TableCreated) => Active,

            // Schema changes or snapshots cause mutations
            (Active, SchemaUpdated | SnapshotAdded | SnapshotRemoved) => Mutating,

            // Completing mutation returns to Active
            (Mutating, SchemaUpdated | SnapshotAdded | SnapshotRemoved) => Active,

            // Anything else is illegal
            (state, evt) => {
                return Err(StateError::IllegalTransition(format!(
                    "cannot apply {:?} while in {:?}",
                    evt, state
                )))
            }
        };

        Ok(())
    }

    /// Get the current derived state.
    pub fn current_state(&self) -> &TableState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{EventType, TableEvent, TableId};
    use uuid::Uuid;

    fn event(event_type: EventType) -> TableEvent {
        TableEvent {
            table_id: TableId(Uuid::new_v4()),
            version: 1,
            event_type,
            payload: vec![],
        }
    }

    #[test]
    fn valid_lifecycle() {
        let mut sm = TableStateMachine::new();

        sm.apply(&event(EventType::TableCreated)).unwrap();
        assert_eq!(sm.current_state(), &TableState::Active);

        sm.apply(&event(EventType::SchemaUpdated)).unwrap();
        assert_eq!(sm.current_state(), &TableState::Mutating);

        sm.apply(&event(EventType::SnapshotAdded)).unwrap();
        assert_eq!(sm.current_state(), &TableState::Active);
    }

    #[test]
    fn illegal_transition_is_rejected() {
        let mut sm = TableStateMachine::new();

        let err = sm.apply(&event(EventType::SchemaUpdated)).unwrap_err();

        assert!(matches!(err, StateError::IllegalTransition(_)));
    }
}
