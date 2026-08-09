//! Asking Trino where a table actually lives.
//!
//! A table name is not its location. `hive.silver.orders` is a catalog entry that points
//! at a path, and that pointer can change without the data moving or the name changing —
//! a `CREATE OR REPLACE` with a different `location`, a migration between storage
//! accounts, a table dropped and rebuilt elsewhere. Guessing the path from a naming
//! convention works right up until it doesn't, and the failure is silent: `ddi` keeps
//! polling a table nobody writes to any more.
//!
//! Delta itself cannot help here. The transaction log has no forwarding pointer, and a
//! relocation is not a Delta operation at all — it happens in the catalog, and storage
//! never hears about it. So the only honest answer is to ask the catalog, and to keep
//! asking.
//!
//! This speaks Trino's HTTP protocol directly rather than taking a client dependency:
//! `POST /v1/statement`, then follow `nextUri` until the server stops handing one back.
//! For one query shape that is a small amount of code to own, and `reqwest` is already in
//! the tree underneath the object store.

use std::time::Duration;

use serde::Deserialize;
use tracing::debug;

use crate::error::{Error, Result};

/// How to reach a Trino/Starburst cluster. Normally read from dbt's `profiles.yml`.
#[derive(Clone, Debug)]
pub struct TrinoConnection {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub http_scheme: String,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    /// Accept an untrusted certificate. Some Starburst deployments terminate TLS with an
    /// internal CA; dbt-trino spells this `cert: false`.
    pub verify_tls: bool,
}

impl TrinoConnection {
    fn statement_url(&self) -> String {
        format!(
            "{}://{}:{}/v1/statement",
            self.http_scheme, self.host, self.port
        )
    }

    /// A short, safe description for logs and errors. Never includes the password.
    pub fn describe(&self) -> String {
        format!("{}@{}:{}", self.user, self.host, self.port)
    }
}

/// One page of a Trino response.
#[derive(Debug, Deserialize)]
struct Page {
    #[serde(default)]
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    #[serde(default)]
    columns: Option<Vec<Column>>,
    #[serde(default)]
    data: Option<Vec<Vec<serde_json::Value>>>,
    #[serde(default)]
    error: Option<TrinoError>,
}

#[derive(Debug, Deserialize)]
struct Column {
    name: String,
}

#[derive(Debug, Deserialize)]
struct TrinoError {
    message: String,
    #[serde(default)]
    #[serde(rename = "errorName")]
    error_name: Option<String>,
}

/// A completed query: column names plus every row, in order.
#[derive(Debug, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

impl QueryResult {
    /// The first column of the first row, as text.
    pub fn scalar(&self) -> Option<String> {
        match self.rows.first().and_then(|r| r.first()) {
            Some(serde_json::Value::String(s)) => Some(s.clone()),
            Some(serde_json::Value::Null) | None => None,
            Some(other) => Some(other.to_string()),
        }
    }

    /// Value of a named column in the first row.
    pub fn column(&self, name: &str) -> Option<String> {
        let idx = self.columns.iter().position(|c| c == name)?;
        match self.rows.first().and_then(|r| r.get(idx)) {
            Some(serde_json::Value::String(s)) => Some(s.clone()),
            Some(serde_json::Value::Null) | None => None,
            Some(other) => Some(other.to_string()),
        }
    }
}

/// A Trino client that runs one statement at a time.
#[derive(Clone, Debug)]
pub struct TrinoClient {
    conn: TrinoConnection,
    http: reqwest::Client,
}

impl TrinoClient {
    pub fn new(conn: TrinoConnection) -> Result<Self> {
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(!conn.verify_tls)
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| Error::Config(format!("cannot build an HTTP client for Trino: {e}")))?;
        Ok(Self { conn, http })
    }

    pub fn connection(&self) -> &TrinoConnection {
        &self.conn
    }

    /// Run a statement and collect every page of the result.
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        debug!(trino = %self.conn.describe(), sql, "running statement");

        let mut request = self
            .http
            .post(self.conn.statement_url())
            .header("X-Trino-User", &self.conn.user)
            .header("X-Trino-Source", "ddi")
            .body(sql.to_string());
        if let Some(c) = &self.conn.catalog {
            request = request.header("X-Trino-Catalog", c);
        }
        if let Some(s) = &self.conn.schema {
            request = request.header("X-Trino-Schema", s);
        }
        if let Some(p) = &self.conn.password {
            request = request.basic_auth(&self.conn.user, Some(p));
        }

        let mut page: Page = self.send(request).await?;
        let mut out = QueryResult::default();

        loop {
            if let Some(e) = page.error {
                return Err(Error::Config(format!(
                    "Trino ({}) rejected the statement: {}{}",
                    self.conn.describe(),
                    e.message,
                    e.error_name.map(|n| format!(" [{n}]")).unwrap_or_default()
                )));
            }
            if out.columns.is_empty() {
                if let Some(cols) = page.columns {
                    out.columns = cols.into_iter().map(|c| c.name).collect();
                }
            }
            if let Some(rows) = page.data {
                out.rows.extend(rows);
            }

            // Trino streams results across pages and only stops offering a nextUri when
            // the query is finished. Following it to the end is what marks the query
            // complete server-side, so it is not optional even once we have our row.
            let Some(next) = page.next_uri else { break };
            page = self.send(self.http.get(next)).await?;
        }
        Ok(out)
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<Page> {
        let response = request.send().await.map_err(|e| {
            Error::Config(format!(
                "cannot reach Trino at {}: {e}",
                self.conn.describe()
            ))
        })?;
        let status = response.status();
        let body = response.text().await.map_err(|e| {
            Error::Config(format!(
                "Trino ({}) sent an unreadable reply: {e}",
                self.conn.describe()
            ))
        })?;
        if !status.is_success() {
            return Err(Error::Config(format!(
                "Trino ({}) returned HTTP {status}: {}",
                self.conn.describe(),
                body.chars().take(300).collect::<String>()
            )));
        }
        serde_json::from_str(&body).map_err(|e| {
            Error::Config(format!(
                "Trino ({}) sent a reply this client did not understand: {e}",
                self.conn.describe()
            ))
        })
    }

    /// The physical location of `catalog.schema.table`, as the catalog currently records it.
    pub async fn table_location(&self, relation: &str) -> Result<String> {
        let sql = format!("SHOW CREATE TABLE {relation}");
        let result = self.query(&sql).await?;
        let ddl = result.scalar().ok_or_else(|| {
            Error::Config(format!(
                "SHOW CREATE TABLE {relation} returned nothing; does the table exist and is \
                 it visible to {}?",
                self.conn.describe()
            ))
        })?;
        location_from_ddl(&ddl).ok_or_else(|| {
            Error::Config(format!(
                "SHOW CREATE TABLE {relation} did not include a location property, so this \
                 table's data cannot be located. That is expected for a view or a managed \
                 table on a connector that hides its path.\n{ddl}"
            ))
        })
    }
}

impl TrinoClient {
    /// Catalogs whose connector stores Delta tables.
    ///
    /// A catalog called `delta` need not be one, and one called `lake` may well be: the
    /// name is a label, the connector is the fact. `system.metadata.catalogs` reports
    /// both, which is the only way to tell a Delta table from an Iceberg or Hive one
    /// without opening it — and in a mixed warehouse most tables are not Delta.
    pub async fn delta_catalogs(&self) -> Result<std::collections::BTreeSet<String>> {
        let result = self
            .query("SELECT catalog_name, connector_name FROM system.metadata.catalogs")
            .await?;
        let name = result
            .columns
            .iter()
            .position(|c| c == "catalog_name")
            .unwrap_or(0);
        let connector = result
            .columns
            .iter()
            .position(|c| c == "connector_name")
            .unwrap_or(1);

        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                let c = row.get(connector)?.as_str()?;
                // Trino spells it `delta_lake`; Starburst has shipped `delta` too.
                is_delta_connector(c)
                    .then(|| row.get(name)?.as_str().map(str::to_string))
                    .flatten()
            })
            .collect())
    }
}

/// True for connectors that store Delta tables.
pub fn is_delta_connector(connector: &str) -> bool {
    let c = connector.to_ascii_lowercase();
    c == "delta" || c == "delta_lake" || c == "delta-lake" || c.starts_with("delta_lake")
}

/// Pull `location = '...'` out of a `SHOW CREATE TABLE` result.
///
/// Trino renders table properties as `WITH ( key = value, ... )`, and quotes string
/// values with single quotes, doubling any that appear inside.
pub fn location_from_ddl(ddl: &str) -> Option<String> {
    let lower = ddl.to_ascii_lowercase();
    let mut from = 0usize;
    // `location` can appear inside a column comment, so keep looking until one is
    // followed by an `=` and a quoted value.
    while let Some(rel) = lower[from..].find("location") {
        let at = from + rel + "location".len();
        let rest = &ddl[at..];
        let trimmed = rest.trim_start();
        if let Some(after_eq) = trimmed.strip_prefix('=') {
            let value = after_eq.trim_start();
            if let Some(body) = value.strip_prefix('\'') {
                let mut out = String::new();
                let mut chars = body.chars().peekable();
                while let Some(c) = chars.next() {
                    if c == '\'' {
                        if chars.peek() == Some(&'\'') {
                            chars.next();
                            out.push('\'');
                            continue;
                        }
                        return Some(out);
                    }
                    out.push(c);
                }
            }
        }
        from = at;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const DDL: &str = "CREATE TABLE hive.silver.orders_stg (\n\
                        order_id bigint,\n\
                        status varchar\n\
                     )\n\
                     WITH (\n\
                        format = 'PARQUET',\n\
                        location = 'abfss://lake@acct.dfs.core.windows.net/silver/orders_stg'\n\
                     )";

    #[test]
    fn only_delta_connectors_count_as_delta() {
        assert!(is_delta_connector("delta_lake"));
        assert!(is_delta_connector("DELTA"));
        assert!(!is_delta_connector("iceberg"));
        assert!(!is_delta_connector("hive"));
        assert!(
            !is_delta_connector("postgresql"),
            "a catalog named `delta` on a postgres connector is not a Delta catalog"
        );
    }

    #[test]
    fn location_is_read_out_of_the_ddl() {
        assert_eq!(
            location_from_ddl(DDL).as_deref(),
            Some("abfss://lake@acct.dfs.core.windows.net/silver/orders_stg")
        );
    }

    #[test]
    fn a_table_with_no_location_yields_none() {
        assert_eq!(
            location_from_ddl("CREATE VIEW v AS SELECT 1"),
            None,
            "a view has no location, and pretending otherwise would point at nothing"
        );
    }

    #[test]
    fn the_word_location_in_a_comment_is_not_mistaken_for_the_property() {
        let ddl = "CREATE TABLE t (\n   city varchar COMMENT 'the location of the store'\n)\n\
                   WITH (\n   location = 's3://bucket/t'\n)";
        assert_eq!(location_from_ddl(ddl).as_deref(), Some("s3://bucket/t"));
    }

    #[test]
    fn a_quote_inside_the_path_is_unescaped() {
        let ddl = "WITH ( location = 's3://bucket/it''s/here' )";
        assert_eq!(
            location_from_ddl(ddl).as_deref(),
            Some("s3://bucket/it's/here")
        );
    }

    #[test]
    fn scalar_reads_the_first_cell() {
        let r = QueryResult {
            columns: vec!["Create Table".into()],
            rows: vec![vec![serde_json::Value::String("CREATE TABLE x".into())]],
        };
        assert_eq!(r.scalar().as_deref(), Some("CREATE TABLE x"));
        assert_eq!(r.column("Create Table").as_deref(), Some("CREATE TABLE x"));
        assert_eq!(r.column("nope"), None);
    }

    #[test]
    fn a_password_never_appears_in_a_description() {
        let c = TrinoConnection {
            host: "trino.internal".into(),
            port: 443,
            user: "svc_ddi".into(),
            password: Some("hunter2".into()),
            http_scheme: "https".into(),
            catalog: Some("hive".into()),
            schema: None,
            verify_tls: true,
        };
        let d = c.describe();
        assert!(!d.contains("hunter2"), "got: {d}");
        assert_eq!(d, "svc_ddi@trino.internal:443");
        assert_eq!(c.statement_url(), "https://trino.internal:443/v1/statement");
    }
}
