use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use uuid::Uuid;

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
}

#[derive(Debug, Default)]
pub struct MetadataLog {
    events: VecDeque<TableEvent>,
}

impl MetadataLog {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
        }
    }

    pub fn append(&mut self, event: TableEvent) -> Result<(), LogError> {
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

        self.events.push_back(event);
        Ok(())
    }

    pub fn replay(&self) -> impl Iterator<Item = &TableEvent> {
        self.events.iter()
    }

    pub fn current_version(&self) -> Version {
        self.events.back().map(|e| e.version).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_replay() {
        let table_id = TableId(Uuid::new_v4());
        let mut log = MetadataLog::new();

        log.append(TableEvent {
            table_id: table_id.clone(),
            version: 1,
            event_type: EventType::TableCreated,
            payload: vec![],
        })
        .unwrap();

        log.append(TableEvent {
            table_id,
            version: 2,
            event_type: EventType::SchemaUpdated,
            payload: vec![1, 2, 3],
        })
        .unwrap();

        let events: Vec<_> = log.replay().collect();
        assert_eq!(events.len(), 2);
    }
}
