use delta_kernel::actions::{Add, Metadata, Protocol};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeltaAction {
    Protocol(DeltaProtocolAction),
    #[serde(rename = "metaData")]
    Metadata(DeltaMetadataAction),
    File(DeltaFileAction),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaProtocolAction {
    delta_protocol: Protocol,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaMetadataAction {
    delta_metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeltaSingleAction {
    Add(Add),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaFileAction {
    id: String,
    deletion_vector_field_id: Option<String>,
    version: Option<u64>,
    timestamp: Option<u64>,
    expiration_timestamp: Option<u64>,
    delta_single_action: DeltaSingleAction,
}
