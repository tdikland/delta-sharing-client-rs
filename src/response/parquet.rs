use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ParquetAction {
    Protocol(ParquetProtocolAction),
    #[serde(rename = "metaData")]
    Metadata(ParquetMetadataAction),
    File(ParquetFileAction),
}

impl ParquetAction {
    pub fn is_protocol(&self) -> bool {
        matches!(self, ParquetAction::Protocol(_))
    }

    pub fn is_metadata(&self) -> bool {
        matches!(self, ParquetAction::Metadata(_))
    }

    pub fn is_file(&self) -> bool {
        matches!(self, ParquetAction::File(_))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParquetProtocolAction {
    min_reader_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParquetMetadataAction {
    id: String,
    name: Option<String>,
    description: Option<String>,
    // format: ParquetResponseFormat,
    schema_string: String,
    partition_columns: Vec<String>,
    #[serde(default)]
    configuration: HashMap<String, Option<String>>,
    version: Option<u64>,
    size: Option<u64>,
    num_files: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParquetFileAction {
    url: String,
    id: String,
    partition_values: HashMap<String, Option<String>>,
    size: u64,
    stats: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<i64>,
}
