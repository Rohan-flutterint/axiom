// Invariant Framework
//
// Invariants are pure rules that must always hold true during
// table state transitions. Violations are detected *before*
// data corruption occurs.

use crate::log::TableEvent;
use crate::state::TableState;

/// Result of invariant evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvariantResult {
    Pass,
    Fail(String),
}

/// Trait implemented by all invariants.
///
/// Invariants must be:
/// - Pure
/// - Deterministic
/// - Side-effect free
pub trait Invariant: Send + Sync {
    fn name(&self) -> &'static str;

    fn validate(
        &self,
        previous_state: &TableState,
        event: &TableEvent,
        next_state: &TableState,
    ) -> InvariantResult;
}

/// Invariant engine that evaluates a set of invariants.
#[derive(Default)]
pub struct InvariantEngine {
    invariants: Vec<Box<dyn Invariant>>,
}

impl InvariantEngine {
    /// Create a new invariant engine.
    pub fn new() -> Self {
        Self {
            invariants: Vec::new(),
        }
    }

    /// Register an invariant.
    pub fn register<I: Invariant + 'static>(&mut self, invariant: I) {
        self.invariants.push(Box::new(invariant));
    }

    /// Evaluate all invariants.
    ///
    /// Stops at the first failure.
    pub fn evaluate(
        &self,
        previous_state: &TableState,
        event: &TableEvent,
        next_state: &TableState,
    ) -> Result<(), InvariantViolation> {
        for invariant in &self.invariants {
            match invariant.validate(previous_state, event, next_state) {
                InvariantResult::Pass => continue,
                InvariantResult::Fail(reason) => {
                    return Err(InvariantViolation {
                        invariant: invariant.name(),
                        reason,
                    })
                }
            }
        }
        Ok(())
    }
}

/// Returned when an invariant is violated.
#[derive(Debug, thiserror::Error)]
#[error("invariant `{invariant}` violated: {reason}")]
pub struct InvariantViolation {
    pub invariant: &'static str,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{EventType, TableEvent, TableId};
    use crate::state::TableState;
    use uuid::Uuid;

    struct NoMutationFromCreated;

    impl Invariant for NoMutationFromCreated {
        fn name(&self) -> &'static str {
            "no-mutation-from-created"
        }

        fn validate(
            &self,
            previous: &TableState,
            _event: &TableEvent,
            next: &TableState,
        ) -> InvariantResult {
            if previous == &TableState::Created && next == &TableState::Mutating {
                InvariantResult::Fail("cannot mutate table before activation".into())
            } else {
                InvariantResult::Pass
            }
        }
    }

    fn event(event_type: EventType) -> TableEvent {
        TableEvent {
            table_id: TableId(Uuid::new_v4()),
            version: 1,
            event_type,
            payload: vec![],
        }
    }

    #[test]
    fn invariant_blocks_invalid_transition() {
        let mut engine = InvariantEngine::new();
        engine.register(NoMutationFromCreated);

        let previous = TableState::Created;
        let next = TableState::Mutating;

        let err = engine
            .evaluate(&previous, &event(EventType::SchemaUpdated), &next)
            .unwrap_err();

        assert!(err.to_string().contains("no-mutation-from-created"));
    }
}
