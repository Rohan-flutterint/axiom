// Iceberg Read-Only Adapter
//
// Parses Iceberg table metadata and exposes a normalized,
// read-only view suitable for drift detection and validation.

use serde::Deserialize;
use uuid::Uuid;

/// Subset of Iceberg table metadata we care about.
///
/// This intentionally ignores:
/// - manifests
/// - file-level details
/// - partition specs
///
/// We only care about *table identity and evolution*.
#[derive(Debug, Deserialize)]
pub struct IcebergMetadata {
    #[serde(rename = "table-uuid")]
    pub table_uuid: Uuid,

    #[serde(rename = "current-snapshot-id")]
    pub current_snapshot_id: Option<i64>,

    #[serde(rename = "schemas")]
    pub schemas: Vec<IcebergSchema>,

    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
}

#[derive(Debug, Deserialize)]
pub struct IcebergSchema {
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
}

/// Normalized view of Iceberg state used by Axiom.
#[derive(Debug, PartialEq, Eq)]
pub struct IcebergTableState {
    pub table_uuid: Uuid,
    pub current_snapshot_id: Option<i64>,
    pub current_schema_id: i32,
}

impl IcebergMetadata {
    /// Convert raw Iceberg metadata into normalized state.
    pub fn into_table_state(self) -> IcebergTableState {
        IcebergTableState {
            table_uuid: self.table_uuid,
            current_snapshot_id: self.current_snapshot_id,
            current_schema_id: self.current_schema_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_iceberg_metadata_json() {
        let json = r#"
        {
          "format-version": 2,
          "table-uuid": "9f7c8b31-3f9d-4b0a-9c3c-6b8df92f7e11",
          "current-snapshot-id": 123456789,
          "current-schema-id": 1,
          "schemas": [
            { "schema-id": 0 },
            { "schema-id": 1 }
          ]
        }
        "#;

        let metadata: IcebergMetadata = serde_json::from_str(json).unwrap();
        let state = metadata.into_table_state();

        assert_eq!(state.current_snapshot_id, Some(123456789));
        assert_eq!(state.current_schema_id, 1);
    }
}
