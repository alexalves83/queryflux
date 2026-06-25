//! Minimal Snowflake SQL API v2 client for e2e tests.
//!
//! Uses the stateless SQL API v2 endpoint:
//!   POST /api/v2/statements  → executes SQL with optional bindings, returns jsonv2
//!
//! No session management needed — each request is self-contained with Bearer auth
//! (or no auth when `NoneAuthProvider` is configured in the test harness).

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde_json::{json, Value};

pub struct SnowflakeClient {
    client: Client,
    base_url: String,
}

/// Decoded query result from a Snowflake SQL API v2 response.
pub struct SfQueryResult {
    /// `true` when the query succeeded.
    pub success: bool,
    /// Error message when `success` is false.
    pub error: Option<String>,
    /// Total number of rows returned.
    pub total_rows: u64,
    /// All values as strings, row-major: `rows[row][col]`.
    /// `None` represents a SQL NULL.
    pub rows: Vec<Vec<Option<String>>>,
}

impl SnowflakeClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("build reqwest client"),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Execute SQL with optional Snowflake-style parameter bindings via the SQL API v2.
    ///
    /// `bindings` format mirrors the Snowflake wire protocol:
    /// ```json
    /// { "1": {"type": "FIXED", "value": "42"} }
    /// ```
    pub async fn query(&self, sql: &str, bindings: Option<Value>) -> Result<SfQueryResult> {
        let mut body = json!({"statement": sql});
        if let Some(b) = bindings {
            body["bindings"] = b;
        }

        let resp = self
            .client
            .post(format!("{}/api/v2/statements", self.base_url))
            .json(&body)
            .send()
            .await?
            .json::<Value>()
            .await?;

        // SQL API v2 success responses have no top-level `code` field; errors always do.
        if resp.get("code").is_some() {
            return Ok(SfQueryResult {
                success: false,
                error: resp["message"].as_str().map(|s| s.to_string()),
                total_rows: 0,
                rows: vec![],
            });
        }

        let num_rows = resp["resultSetMetaData"]["numRows"].as_u64().unwrap_or(0);
        let rows = decode_jsonv2_rows(&resp)?;

        Ok(SfQueryResult {
            success: true,
            error: None,
            total_rows: num_rows,
            rows,
        })
    }
}

/// Decode the `data` field from a SQL API v2 jsonv2 response into row-major strings.
fn decode_jsonv2_rows(resp: &Value) -> Result<Vec<Vec<Option<String>>>> {
    let data = match resp.get("data").and_then(|d| d.as_array()) {
        Some(arr) => arr,
        None => return Ok(vec![]),
    };

    let rows = data
        .iter()
        .map(|row| {
            row.as_array()
                .ok_or_else(|| anyhow!("SQL API v2 row is not an array"))?
                .iter()
                .map(|cell| {
                    Ok(match cell {
                        Value::Null => None,
                        Value::String(s) => Some(s.clone()),
                        other => Some(other.to_string()),
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(rows)
}
