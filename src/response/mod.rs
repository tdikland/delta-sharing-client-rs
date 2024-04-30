//! Delta Sharing server response types.

use std::fmt;

use delta_kernel::actions::{Add, Metadata, Protocol};
use serde::{Deserialize, Serialize};

use self::{delta::DeltaAction, parquet::ParquetAction};

mod delta;
mod parquet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Share {
    id: Option<String>,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    name: String,
    share: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    name: String,
    schema: String,
    share: String,
    share_id: Option<String>,
    id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ListResponse<T> {
    items: Vec<T>,
    next_page_token: Option<String>,
}

impl<T> ListResponse<T> {
    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}

impl<T> IntoIterator for ListResponse<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a ListResponse<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

pub type ListSharesResponse = ListResponse<Share>;
pub type ListSchemasResponse = ListResponse<Schema>;
pub type ListTablesResponse = ListResponse<Table>;

/// Delta Sharing server response for successful `get_share` requests.
#[derive(Debug, Deserialize)]
pub struct GetShareResponse {
    pub share: Share,
}

impl GetShareResponse {
    /// Retrieve the share of the response
    pub fn share(&self) -> &Share {
        &self.share
    }
}

pub struct TableVersionResponse {
    version: u64,
}

pub struct TableMetadataResponse {
    protocol: TableAction,
    metadata: TableAction,
}

pub struct TableDataResponse {
    protocol: TableAction,
    metadata: TableAction,
    files: Vec<TableAction>,
}

pub struct TableChangesResponse {
    protocol: TableAction,
    metadata: TableAction,
    files: Vec<TableAction>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TableAction {
    Parquet(ParquetAction),
    Delta(DeltaAction),
}

impl TableAction {
    pub fn is_parquet(&self) -> bool {
        matches!(self, TableAction::Parquet(_))
    }

    pub fn is_delta(&self) -> bool {
        matches!(self, TableAction::Delta(_))
    }

    pub fn as_parquet(&self) -> Option<&ParquetAction> {
        match self {
            TableAction::Parquet(p) => Some(p),
            _ => None,
        }
    }

    pub fn as_delta(&self) -> Option<&DeltaAction> {
        match self {
            TableAction::Delta(d) => Some(d),
            _ => None,
        }
    }

    pub fn to_parquet(self) -> Option<ParquetAction> {
        match self {
            TableAction::Parquet(p) => Some(p),
            _ => None,
        }
    }

    pub fn to_delta(self) -> Option<DeltaAction> {
        match self {
            TableAction::Delta(d) => Some(d),
            _ => None,
        }
    }
}

/// Delta Sharing server response for failed requests.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    error_code: String,
    message: String,
}

impl ErrorResponse {
    /// Retrieve the error code of the response
    pub fn error_code(&self) -> &str {
        &self.error_code
    }

    /// Retrieve the message of the response
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.error_code, self.message)
    }
}
