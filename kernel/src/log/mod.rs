use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use uuid::Uuid;

mod store;
pub use store::MetadataLogStore;

/// Logical version of a table.
pub type Version = u64;

/// Stable identifier for a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    TableCreated,
    SchemaUpdated,
    SnapshotAdded,
    SnapshotRemoved,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableEvent {
    pub table_id: TableId,
    pub version: Version,
    pub event_type: EventType,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LogError {
    #[error("version conflict: expected {expected}, got {actual}")]
    VersionConflict { expected: Version, actual: Version },

    #[error("storage error: {0}")]
    Storage(String),
}

/// In-memory store (reference implementation).
#[derive(Default)]
pub struct InMemoryLogStore {
    events: VecDeque<TableEvent>,
}

impl MetadataLogStore for InMemoryLogStore {
    fn append(&mut self, event: &TableEvent) -> Result<(), LogError> {
        let expected = match self.events.back() {
            Some(last) => last.version + 1,
            None => 1,
        };

        if event.version != expected {
            return Err(LogError::VersionConflict {
                expected,
                actual: event.version,
            });
        }

        self.events.push_back(event.clone());
        Ok(())
    }

    fn load(&self) -> Result<Vec<TableEvent>, LogError> {
        Ok(self.events.iter().cloned().collect())
    }

    fn current_version(&self) -> Result<Version, LogError> {
        Ok(self.events.back().map(|e| e.version).unwrap_or(0))
    }
}

/// Semantic metadata log backed by a store.
pub struct MetadataLog<S: MetadataLogStore> {
    store: S,
}

impl<S: MetadataLogStore> MetadataLog<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn append(&mut self, event: TableEvent) -> Result<(), LogError> {
        self.store.append(&event)
    }

    pub fn replay(&self) -> Result<Vec<TableEvent>, LogError> {
        self.store.load()
    }

    pub fn current_version(&self) -> Result<Version, LogError> {
        self.store.current_version()
    }
}
