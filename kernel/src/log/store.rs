// Metadata Log Storage Abstraction
//
// Defines the durability contract for the authoritative metadata log.
// Implementations may persist to disk, object storage, databases, etc.
//
// This module defines *interfaces only*.

use super::{LogError, TableEvent, Version};

/// Storage backend for the metadata log.
///
/// Properties required from implementations:
/// - Append-only
/// - Ordered
/// - Durable
/// - CAS semantics on version
///
/// Implementations MUST NOT:
/// - Reorder events
/// - Mutate existing events
/// - Allow version gaps
pub trait MetadataLogStore: Send + Sync {
    /// Append an event to storage.
    ///
    /// Implementations must enforce:
    /// - event.version == last_version + 1
    fn append(&mut self, event: &TableEvent) -> Result<(), LogError>;

    /// Load all events in order.
    ///
    /// Used for deterministic replay.
    fn load(&self) -> Result<Vec<TableEvent>, LogError>;

    /// Return the current persisted version.
    fn current_version(&self) -> Result<Version, LogError>;
}
