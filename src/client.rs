use async_trait::async_trait;
use http::{Method, StatusCode};
use reqwest::RequestBuilder;

use crate::profile::TokenProvider;
use crate::request::pagination::{Pagination, PaginationExt};
use crate::response::{ErrorResponse, GetShareResponse, ListSharesResponse};
use crate::DeltaSharingError;
use crate::{
    response::{Schema, Share, Table},
    Profile, Result,
};

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
            let response = self.list_shares_paginated(&pagination).await?;
            pagination.set_page_token(response.next_page_token());
            shares.extend(response);
        }
        Ok(shares)
    }

    #[tracing::instrument]
    pub async fn get_share(&self, share_name: &str) -> Result<Option<Share>> {
        let res = self.get_share_raw(share_name).await;
        match res {
            Ok(response) => Ok(Some(response.share)),
            Err(error) => {
                if e.kind() == &DeltaSharingError::client(StatusCode::NOT_FOUND, "", "") {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub async fn list_schemas(&self, share_name: &str) -> Result<Vec<Schema>> {
        todo!()
    }

    pub async fn list_tables_in_schema(
        &self,
        share_name: &str,
        schema_name: &str,
    ) -> Result<Vec<Table>> {
        todo!()
    }

    pub async fn list_tables_in_share(&self, share: &str) -> Result<Vec<Table>> {
        todo!()
    }

    pub async fn get_table_version(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<u64> {
        todo!()
    }

    pub async fn get_table_metadata(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<String> {
        todo!()
    }

    pub async fn get_table_data(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<String> {
        todo!()
    }

    pub async fn get_table_changes(
        &self,
        share_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<String> {
        todo!()
    }
}

impl DeltaSharingClient {
    #[tracing::instrument]
    pub async fn list_shares_paginated(
        &self,
        pagination: &Pagination,
    ) -> Result<ListSharesResponse> {
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
        let status_code = response.status();
        tracing::debug!(status_code = %status_code, "server responded");

        match status_code {
            StatusCode::OK => {
                let res = response.json::<ListSharesResponse>().await.map_err(|e| {
                    tracing::error!(err = ?e, "failed to parse server response");
                    DeltaSharingError::parse_response("failed to parse server response")
                })?;
                tracing::debug!("response parsed");
                Ok(res)
            }
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
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
                Err("unknown server response".into())
            }
        }
    }

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
        let status_code = response.status();
        tracing::debug!(status_code = %status_code, "server responded");

        match status_code {
            StatusCode::OK => {
                let res = response.json::<GetShareResponse>().await?;
                tracing::debug!("response parsed");
                Ok(res)
            }
            StatusCode::BAD_REQUEST
            | StatusCode::UNAUTHORIZED
            | StatusCode::FORBIDDEN
            | StatusCode::NOT_FOUND => {
                let err = response.json::<ErrorResponse>().await?;
                tracing::debug!("response parsed");
                Err(DeltaSharingError::client(status_code, err.error_code(), err.message()).into())
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                let err = response.json::<ErrorResponse>().await?;
                tracing::debug!("response parsed");
                Err(DeltaSharingError::server(status_code, err.error_code(), err.message()).into())
            }
            _ => {
                tracing::warn!(status_code = %status_code, "unexpected HTTP status");
                Err("unknown server response".into())
            }
        }
    }

    pub async fn list_schema_page() {}
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
