use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::{Method, StatusCode};
use reqwest::{RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::profile::TokenProvider;
use crate::request::pagination::{Pagination, PaginationExt};
use crate::response::{
    ErrorResponse, GetShareResponse, ListSchemasResponse, ListSharesResponse, ListTablesResponse,
    QueryTableChangesResponse, QueryTableDataResponse, QueryTableMetadataResponse,
    QueryTableVersionResponse,
};
use crate::DeltaSharingError;
use crate::{
    response::{Schema, Share, Table},
    Profile, Result,
};

const QUERY_PARAM_VERSION_TIMESTAMP: &str = "startingTimestamp";

#[derive(Debug)]
pub struct DeltaSharingClient {
    client: reqwest::Client,
    profile: Profile,
}

impl DeltaSharingClient {
    #[tracing::instrument]
    pub async fn list_shares(&self) -> Result<Vec<Share>> {
        let mut shares = vec![];
        let mut pagination = Pagination::default();
        while !pagination.is_finished() {
            let response = self.list_shares_raw(&pagination).await?;
            pagination.set_page_token(response.next_page_token());
            shares.extend(response);
        }
        Ok(shares)
    }

    #[tracing::instrument]
    pub async fn get_share(&self, share_name: &str) -> Result<Option<Share>> {
        let res = self.get_share_raw(share_name).await;
        match res {
            Ok(r) => Ok(Some(r.share)),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(e),
        }
    }

    #[tracing::instrument]
    pub async fn list_schemas(&self, share_name: &str) -> Result<Vec<Schema>> {
        let mut schemas = vec![];
        let mut pagination = Pagination::default();
        while !pagination.is_finished() {
            let response = self.list_schemas_raw(share_name, &pagination).await?;
            pagination.set_page_token(response.next_page_token());
            schemas.extend(response);
        }
        Ok(schemas)
    }

    #[tracing::instrument]
    pub async fn list_tables_in_share(&self, share: &str) -> Result<Vec<Table>> {
        let mut tables = vec![];
        let mut pagination = Pagination::default();
        while !pagination.is_finished() {
            let response = self.list_tables_in_share_raw(share, &pagination).await?;
            pagination.set_page_token(response.next_page_token());
            tables.extend(response);
        }
        Ok(tables)
    }

    #[tracing::instrument]
    pub async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
    ) -> Result<Vec<Table>> {
        let mut tables = vec![];
        let mut pagination = Pagination::default();
        while !pagination.is_finished() {
            let response = self
                .list_tables_in_schema_raw(share_name, schema_name, &pagination)
                .await?;
            pagination.set_page_token(response.next_page_token());
            tables.extend(response);
        }
        Ok(tables)
    }

    #[tracing::instrument]
    pub async fn get_table_version(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        version: &TableVersionQuery,
    ) -> Result<QueryTableVersionResponse> {
        self.get_table_version_raw(
            share_name,
            schema_name,
            table_name,
            version.to_timestamp().as_deref(),
        )
        .await
    }

    #[tracing::instrument]
    pub async fn get_table_metadata(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<QueryTableMetadataResponse> {
        todo!()
    }

    #[tracing::instrument]
    pub async fn get_table_data(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<QueryTableDataResponse> {
        todo!()
    }

    #[tracing::instrument]
    pub async fn get_table_changes(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<QueryTableChangesResponse> {
        todo!()
    }
}

impl DeltaSharingClient {
    #[tracing::instrument]
    pub async fn list_shares_raw(&self, pagination: &Pagination) -> Result<ListSharesResponse> {
        let url = self
            .profile
            .endpoint()
            .join("/shares")
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingError::internal("failed to construct URL")
            })?
            .with_pagination(pagination);
        tracing::debug!(url = %url, "endpoint URL constructed");

        let request = self
            .client
            .request(Method::GET, url)
            .authorize(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingError::profile("failed to authorize request")
            })?;
        tracing::debug!("prepared request");

        let response = request.send().await?;
        tracing::debug!("received response");

        handle_response(response).await
    }

    #[tracing::instrument]
    pub async fn get_share_raw(&self, share_name: &str) -> Result<GetShareResponse> {
        let url = self
            .profile
            .endpoint()
            .join(&format!("/shares/{share_name}"))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingError::internal("failed to construct URL")
            })?;
        tracing::debug!(url = %url, "endpoint URL constructed");

        let request = self
            .client
            .request(Method::GET, url)
            .authorize(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingError::profile("failed to authorize request")
            })?;
        tracing::debug!("prepared request");

        let response = request.send().await?;
        tracing::debug!("received response");

        handle_response(response).await
    }

    #[tracing::instrument]
    pub async fn list_schemas_raw(
        &self,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<ListSchemasResponse> {
        let url = self
            .profile
            .endpoint()
            .join(&format!("/shares/{share_name}/schemas"))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingError::internal("failed to construct URL")
            })?
            .with_pagination(pagination);

        let request = self
            .client
            .request(Method::GET, url)
            .authorize(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingError::profile("failed to authorize request")
            })?;

        let response = request.send().await?;
        handle_response(response).await
    }

    #[tracing::instrument]
    pub async fn list_tables_in_share_raw(
        &self,
        share_name: &str,
        pagination: &Pagination,
    ) -> Result<ListTablesResponse> {
        let url = self
            .profile
            .endpoint()
            .join(&format!("/shares/{share_name}/schemas/all-tables"))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingError::internal("failed to construct URL")
            })?
            .with_pagination(pagination);

        let request = self
            .client
            .request(Method::GET, url)
            .authorize(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingError::profile("failed to authorize request")
            })?;

        let response = request.send().await?;
        handle_response(response).await
    }

    #[tracing::instrument]
    pub async fn list_tables_in_schema_raw(
        &self,
        share_name: &str,
        schema_name: &str,
        pagination: &Pagination,
    ) -> Result<ListTablesResponse> {
        let url = self
            .profile
            .endpoint()
            .join(&format!(
                "/shares/{share_name}/schemas/{schema_name}/tables"
            ))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingError::internal("failed to construct URL")
            })?
            .with_pagination(pagination);

        let request = self
            .client
            .request(Method::GET, url)
            .authorize(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingError::profile("failed to authorize request")
            })?;

        let response = request.send().await?;
        handle_response(response).await
    }

    #[tracing::instrument]
    pub async fn get_table_version_raw(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
        starting_timestamp: Option<&str>,
    ) -> Result<QueryTableVersionResponse> {
        let mut endpoint = self
            .profile
            .prefix()
            .join(&format!(
                "/shares/{share_name}/schemas/{schema_name}/tables/{table_name}/version"
            ))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingError::internal("failed to construct endpoint URL")
            })?;
        if let Some(ts) = starting_timestamp {
            endpoint
                .query_pairs_mut()
                .append_pair(QUERY_PARAM_VERSION_TIMESTAMP, ts);
        }
        tracing::debug!(endpoint = %endpoint, "URL constructed");

        let request = self
            .client
            .get(endpoint)
            .authorize(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingError::profile(format!("failed to authorize request. Reason: {e}"))
            })?;
        tracing::debug!("prepared request");

        let response = request.send().await?;
        let status = response.status();
        tracing::debug!(status_code = %status, "server responded");

        match status {
            StatusCode::OK => response
                .headers()
                .get("Delta-Table-Version")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or(DeltaSharingError::parse_response(
                    "parsing delta-table-version header failed",
                )),
            StatusCode::BAD_REQUEST
            | StatusCode::UNAUTHORIZED
            | StatusCode::FORBIDDEN
            | StatusCode::NOT_FOUND => Err(DeltaSharingError::internal("ugh")),
            StatusCode::INTERNAL_SERVER_ERROR => Err(DeltaSharingError::internal("ugh")),
            _ => {
                tracing::error!(status_code = %status, "unexpected server status code");
                Err(DeltaSharingError::internal("unknown server response"))
            }
        };

        todo!()
    }

    #[tracing::instrument]
    pub async fn get_table_metadata_raw(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<QueryTableMetadataResponse> {
        let endpoint = self
            .profile
            .prefix()
            .join(&format!(
                "/shares/{share_name}/schemas/{schema_name}/tables/{table_name}/metadata"
            ))
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to construct URL");
                DeltaSharingError::internal("failed to construct endpoint URL")
            })?;

        let request = self
            .client
            .get(endpoint)
            .authorize(&self.profile)
            .await
            .map_err(|e| {
                tracing::error!(err = ?e, "failed to authorize request");
                DeltaSharingError::profile(format!("failed to authorize request. Reason: {e}"))
            })?;

        let response = request.send().await?;
        let status = response.status();
        tracing::debug!(status_code = %status, "server responded");

        todo!()
    }

    #[tracing::instrument]
    pub async fn get_table_data_raw(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<QueryTableDataResponse> {
        todo!()
    }

    #[tracing::instrument]
    pub async fn get_table_changes_raw(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<QueryTableChangesResponse> {
        todo!()
    }
}

#[derive(Debug, Serialize)]
pub struct TableDataQuery {
    predicate_hints: Option<String>,
    json_predicate_hints: Option<String>,
    limit_hint: Option<u32>,
    version: Option<u32>,
    timestamp: Option<String>,
    starting_version: Option<u32>,
    ending_version: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct TableChangesQuery {
    starting_version: Option<u32>,
    ending_version: Option<u32>,
    starting_timestamp: Option<String>,
    ending_timestamp: Option<String>,
    include_historical_metadata: Option<bool>,
}

#[derive(Debug)]
pub enum TableVersionQuery {
    Latest,
    Timestamp(DateTime<Utc>),
}

impl TableVersionQuery {
    fn to_timestamp(&self) -> Option<String> {
        match self {
            TableVersionQuery::Latest => None,
            TableVersionQuery::Timestamp(ts) => Some(ts.to_rfc3339()),
        }
    }
}

impl Default for TableVersionQuery {
    fn default() -> Self {
        TableVersionQuery::Latest
    }
}

impl FromStr for TableVersionQuery {
    type Err = DeltaSharingError;

    fn from_str(s: &str) -> Result<Self> {
        if s == "latest" {
            Ok(TableVersionQuery::Latest)
        } else {
            let ts = DateTime::parse_from_rfc3339(s).map_err(|e| {
                DeltaSharingError::internal(format!("failed to parse timestamp: {}", e))
            })?;
            Ok(TableVersionQuery::Timestamp(ts.into()))
        }
    }
}

async fn handle_response<T: DeserializeOwned>(response: Response) -> Result<T> {
    let status_code = response.status();
    tracing::debug!(status_code = %status_code, "server responded");

    match status_code {
        StatusCode::OK => {
            let res = response.json::<T>().await.map_err(|e| {
                tracing::error!(err = ?e, "failed to parse server response");
                DeltaSharingError::parse_response("failed to parse server response")
            })?;
            tracing::debug!("response parsed");
            Ok(res)
        }

        StatusCode::BAD_REQUEST
        | StatusCode::UNAUTHORIZED
        | StatusCode::FORBIDDEN
        | StatusCode::NOT_FOUND => {
            let err = response.json::<ErrorResponse>().await.map_err(|e| {
                tracing::error!(err = ?e, "failed to parse server response");
                DeltaSharingError::parse_response("failed to parse server response")
            })?;
            tracing::debug!("response parsed");
            Err(DeltaSharingError::client(status_code, err.error_code(), err.message()).into())
        }
        StatusCode::INTERNAL_SERVER_ERROR => {
            let err = response.json::<ErrorResponse>().await.map_err(|e| {
                tracing::error!(err = ?e, "failed to parse server response");
                DeltaSharingError::parse_response("failed to parse server response")
            })?;
            tracing::debug!("response parsed");
            Err(DeltaSharingError::server(status_code, err.error_code(), err.message()).into())
        }
        _ => {
            tracing::warn!(status_code = %status_code, "unexpected HTTP status");
            Err(DeltaSharingError::internal("unknown server response"))
        }
    }
}

#[async_trait]
trait AuthorizationExt
where
    Self: Sized,
{
    async fn authorize(self, profile: &Profile) -> Result<Self>;
}

#[async_trait]
impl AuthorizationExt for RequestBuilder {
    async fn authorize(self, profile: &Profile) -> Result<Self> {
        let token = profile.provide_token().await?;
        let auth_req = self.bearer_auth(token);
        Ok(auth_req)
    }
}

// TODO
impl From<reqwest::Error> for DeltaSharingError {
    fn from(e: reqwest::Error) -> Self {
        DeltaSharingError::client(StatusCode::INTERNAL_SERVER_ERROR, "", e.to_string())
    }
}
