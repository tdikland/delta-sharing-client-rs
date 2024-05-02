use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::DeltaSharingError;
use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableVersionQuery {
    Latest,
    Timestamp(DateTime<Utc>),
}

impl TableVersionQuery {
    pub fn to_timestamp(&self) -> Option<String> {
        match self {
            TableVersionQuery::Latest => None,
            TableVersionQuery::Timestamp(ts) => {
                Some(ts.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            }
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
        if s.to_lowercase() == "latest" {
            Ok(TableVersionQuery::Latest)
        } else {
            let ts = DateTime::parse_from_rfc3339(s).map_err(|e| {
                tracing::error!(err = ?e, "Cannot parse TableVersionQuery");
                DeltaSharingError::request("Cannot parse TableVersionQuery. The string must be either `latest` or a timestamp in ISO8601 format like `2021-08-01T00:00:00Z`.")
            })?;
            Ok(TableVersionQuery::Timestamp(ts.into()))
        }
    }
}

impl From<DateTime<Utc>> for TableVersionQuery {
    fn from(ts: DateTime<Utc>) -> Self {
        TableVersionQuery::Timestamp(ts)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum VersionRange {
    Number {
        start: u32,
        end: Option<u32>,
    },
    Timestamp {
        start: DateTime<Utc>,
        end: Option<DateTime<Utc>>,
    },
}

impl VersionRange {
    pub fn new_number(start_version: u32, end_version: Option<u32>) -> Self {
        VersionRange::Number {
            start: start_version,
            end: end_version,
        }
    }

    pub fn new_timestamp(start_ts: DateTime<Utc>, end_ts: Option<DateTime<Utc>>) -> Self {
        VersionRange::Timestamp {
            start: start_ts,
            end: end_ts,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TableChangesQuery {
    range: VersionRange,
    include_historical_metadata: Option<bool>,
}

impl TableChangesQuery {
    pub fn new(version_range: VersionRange) -> Self {
        Self {
            range: version_range,
            include_historical_metadata: None,
        }
    }

    pub fn with_historical_metadata(mut self, include: bool) -> Self {
        self.include_historical_metadata = Some(include);
        self
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn default_table_version_query() {
        let default_table_version = TableVersionQuery::default();
        assert_eq!(default_table_version, TableVersionQuery::Latest);
    }

    #[test]
    fn table_version_query_from_datetime() {
        let ts = DateTime::<Utc>::from_timestamp(1627776000, 0).unwrap();
        let table_version = TableVersionQuery::from(ts);
        assert_eq!(table_version, TableVersionQuery::Timestamp(ts));
    }

    #[test]
    fn parse_table_version_query() {
        let parsed_table_version = "latest".parse::<TableVersionQuery>().unwrap();
        assert_eq!(parsed_table_version, TableVersionQuery::Latest);

        let parsed_table_version = "2021-08-01T00:00:00Z".parse::<TableVersionQuery>().unwrap();
        let expected_ts = DateTime::<Utc>::from_timestamp(1627776000, 0).unwrap();
        assert_eq!(
            parsed_table_version,
            TableVersionQuery::Timestamp(expected_ts)
        );

        let parse_err = "nonsense".parse::<TableVersionQuery>().unwrap_err();
        assert_eq!(
            parse_err.message(),
            "Cannot parse TableVersionQuery. The string must be either `latest` or a timestamp in ISO8601 format like `2021-08-01T00:00:00Z`."
        );
    }

    #[test]
    fn table_version_query_to_timestamp() {
        let ts = DateTime::<Utc>::from_timestamp(1627776000, 0).unwrap();
        let table_version = TableVersionQuery::Timestamp(ts);
        assert_eq!(
            table_version.to_timestamp(),
            Some("2021-08-01T00:00:00Z".to_string())
        );

        let table_version = TableVersionQuery::Latest;
        assert_eq!(table_version.to_timestamp(), None);
    }
}
