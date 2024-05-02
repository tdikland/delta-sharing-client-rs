use serde::Serialize;

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
