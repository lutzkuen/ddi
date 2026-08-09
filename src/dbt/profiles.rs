//! Reading connection details out of dbt's `profiles.yml`.
//!
//! The credentials for the warehouse are already written down, in the file dbt uses to
//! connect. Asking for them a second time in `ddi`'s own config would be one more thing
//! to keep in step, and one more place for a password to live.
//!
//! `ddi run -s orders_stg -t prod` therefore reads the same file dbt does, picks the
//! named output, and connects with it — the same way `dbt run -t prod` would.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::trino::TrinoConnection;

#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub outputs: HashMap<String, Output>,
}

/// One output block. Only the fields needed to connect are modelled; dbt adapters add
/// plenty more, and an unknown one must not stop us reading the file.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Output {
    #[serde(default, rename = "type")]
    pub adapter: String,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub http_scheme: Option<String>,
    /// dbt-trino calls the catalog `database`.
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    /// dbt-trino's `cert`: `false` disables verification.
    #[serde(default)]
    pub cert: Option<serde_norway::Value>,
}

/// Where dbt looks for `profiles.yml`, in the order dbt looks.
pub fn default_locations() -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(dir) = std::env::var("DBT_PROFILES_DIR") {
        v.push(PathBuf::from(dir).join("profiles.yml"));
    }
    v.push(PathBuf::from("profiles.yml"));
    if let Ok(home) = std::env::var("HOME") {
        v.push(PathBuf::from(home).join(".dbt").join("profiles.yml"));
    }
    v
}

/// Parse a `profiles.yml`.
pub fn parse(text: &str) -> Result<HashMap<String, Profile>> {
    serde_norway::from_str(text)
        .map_err(|e| Error::Config(format!("could not parse profiles.yml: {e}")))
}

/// Find the output to connect with.
///
/// `profile` selects the top-level block; when omitted and the file holds exactly one,
/// that one is used, because requiring the name would be pedantry. `target` overrides the
/// profile's own default, exactly as `dbt --target` does.
pub fn resolve_output(
    profiles: &HashMap<String, Profile>,
    profile: Option<&str>,
    target: Option<&str>,
) -> Result<Output> {
    // dbt keeps a `config:` block alongside the profiles; it is not one.
    let named: Vec<&String> = profiles.keys().filter(|k| *k != "config").collect();

    let chosen = match profile {
        Some(p) => profiles.get(p).ok_or_else(|| {
            let mut known: Vec<&str> = named.iter().map(|s| s.as_str()).collect();
            known.sort();
            Error::Config(format!(
                "profiles.yml has no profile {p:?}. Known: [{}]",
                known.join(", ")
            ))
        })?,
        None if named.len() == 1 => &profiles[named[0]],
        None => {
            let mut known: Vec<&str> = named.iter().map(|s| s.as_str()).collect();
            known.sort();
            return Err(Error::Config(format!(
                "profiles.yml holds {} profiles, so one must be named. Known: [{}]",
                named.len(),
                known.join(", ")
            )));
        }
    };

    let want = target
        .map(str::to_string)
        .or_else(|| chosen.target.clone())
        .ok_or_else(|| {
            Error::Config(
                "no target given and the profile declares no default; pass --target".into(),
            )
        })?;

    chosen.outputs.get(&want).cloned().ok_or_else(|| {
        let mut known: Vec<&str> = chosen.outputs.keys().map(|s| s.as_str()).collect();
        known.sort();
        Error::Config(format!(
            "the profile has no output {want:?}. Known: [{}]",
            known.join(", ")
        ))
    })
}

/// Load a `profiles.yml` and turn the chosen output into a Trino connection.
pub fn trino_connection(
    path: Option<&Path>,
    profile: Option<&str>,
    target: Option<&str>,
) -> Result<TrinoConnection> {
    let candidates: Vec<PathBuf> = match path {
        Some(p) => vec![p.to_path_buf()],
        None => default_locations(),
    };
    let found = candidates.iter().find(|p| p.exists()).ok_or_else(|| {
        Error::Config(format!(
            "no profiles.yml found. Looked in: [{}]. Set DBT_PROFILES_DIR or pass \
             --profiles-dir.",
            candidates
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })?;

    let text = std::fs::read_to_string(found)
        .map_err(|e| Error::Config(format!("cannot read {}: {e}", found.display())))?;
    let output = resolve_output(&parse(&text)?, profile, target)?;
    output.into_trino().map_err(|e| {
        Error::Config(format!(
            "{} ({}): {e}",
            found.display(),
            target.unwrap_or("default")
        ))
    })
}

impl Output {
    /// Interpret this output as a Trino connection.
    pub fn into_trino(self) -> Result<TrinoConnection> {
        if !self.adapter.is_empty() && self.adapter != "trino" && self.adapter != "starburst" {
            return Err(Error::Config(format!(
                "this output is a {:?} profile, and locating tables through the catalog is \
                 implemented for Trino and Starburst. Resolve locations from dbt instead \
                 (location_root, delta_table_path, or meta.ddi_location).",
                self.adapter
            )));
        }
        let host = self
            .host
            .ok_or_else(|| Error::Config("the output declares no host".into()))?;
        let http_scheme = self.http_scheme.unwrap_or_else(|| "https".into());
        let port = self
            .port
            .unwrap_or(if http_scheme == "https" { 443 } else { 8080 });

        // dbt-trino's `cert` is either a bool or a path to a CA bundle. Only `false`
        // means "do not verify"; a path means "verify, using this", which reqwest cannot
        // be told here, so it stays verifying.
        let verify_tls = !matches!(self.cert, Some(serde_norway::Value::Bool(false)));

        Ok(TrinoConnection {
            host,
            port,
            user: self.user.unwrap_or_else(|| "ddi".into()),
            password: self.password,
            http_scheme,
            catalog: self.database,
            schema: self.schema,
            verify_tls,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const YML: &str = r#"
jaffle:
  target: dev
  outputs:
    dev:
      type: trino
      host: localhost
      port: 8080
      user: admin
      http_scheme: http
      database: hive
      schema: silver
    prod:
      type: trino
      host: starburst.internal
      user: svc_ddi
      password: secret
      database: lake
      schema: silver
config:
  send_anonymous_usage_stats: false
"#;

    #[test]
    fn the_profiles_default_target_is_used_when_none_is_given() {
        let p = parse(YML).unwrap();
        let o = resolve_output(&p, None, None).unwrap();
        assert_eq!(o.host.as_deref(), Some("localhost"));
    }

    #[test]
    fn target_overrides_the_default_just_like_dbt() {
        let p = parse(YML).unwrap();
        let o = resolve_output(&p, None, Some("prod")).unwrap();
        assert_eq!(o.host.as_deref(), Some("starburst.internal"));
    }

    #[test]
    fn the_config_block_is_not_mistaken_for_a_profile() {
        // profiles.yml carries a `config:` block next to the profiles; counting it would
        // make a single-profile file look ambiguous.
        let p = parse(YML).unwrap();
        assert!(resolve_output(&p, None, None).is_ok());
    }

    #[test]
    fn an_unknown_target_lists_the_ones_there_are() {
        let p = parse(YML).unwrap();
        let e = resolve_output(&p, None, Some("staging"))
            .unwrap_err()
            .to_string();
        assert!(e.contains("staging"), "got: {e}");
        assert!(e.contains("dev") && e.contains("prod"), "got: {e}");
    }

    #[test]
    fn defaults_fill_in_scheme_and_port() {
        let p = parse(YML).unwrap();
        let c = resolve_output(&p, None, Some("prod"))
            .unwrap()
            .into_trino()
            .unwrap();
        assert_eq!(c.http_scheme, "https");
        assert_eq!(c.port, 443, "https implies 443 unless told otherwise");
        assert_eq!(c.catalog.as_deref(), Some("lake"));
        assert!(c.verify_tls);
    }

    #[test]
    fn an_explicit_http_output_keeps_its_port() {
        let p = parse(YML).unwrap();
        let c = resolve_output(&p, None, Some("dev"))
            .unwrap()
            .into_trino()
            .unwrap();
        assert_eq!((c.http_scheme.as_str(), c.port), ("http", 8080));
    }

    #[test]
    fn cert_false_disables_verification_but_a_path_does_not() {
        let mut o = Output {
            adapter: "trino".into(),
            host: Some("h".into()),
            cert: Some(serde_norway::Value::Bool(false)),
            ..Default::default()
        };
        assert!(!o.clone().into_trino().unwrap().verify_tls);
        o.cert = Some(serde_norway::Value::String("/etc/ssl/ca.pem".into()));
        assert!(
            o.into_trino().unwrap().verify_tls,
            "a CA bundle means verify with it, not skip verification"
        );
    }

    #[test]
    fn a_non_trino_adapter_says_what_to_do_instead() {
        let o = Output {
            adapter: "duckdb".into(),
            host: Some("h".into()),
            ..Default::default()
        };
        let e = o.into_trino().unwrap_err().to_string();
        assert!(e.contains("duckdb"), "got: {e}");
        assert!(e.contains("location_root"), "got: {e}");
    }

    #[test]
    fn unknown_adapter_fields_do_not_break_parsing() {
        let p = parse(
            "p:\n  target: t\n  outputs:\n    t:\n      type: trino\n      host: h\n      \
             some_future_field: 42\n",
        )
        .unwrap();
        assert!(resolve_output(&p, None, None).is_ok());
    }
}
