//! Opening tables, wherever they live.
//!
//! Every table this tool touches is named by URI, and the scheme decides the backend:
//! a bare path or `file://` for local disk, `abfss://` or `az://` for Azure. The handlers
//! register themselves when the corresponding feature is compiled in, so adding a cloud
//! is a feature flag and a set of credentials — no code above this module changes.
//!
//! Credentials are the one thing a dbt project cannot tell us: it knows *which* table,
//! never *how to authenticate*. So they live in `[storage].options` and are threaded to
//! every table open from here.
//!
//! # Azure
//!
//! ```toml
//! [storage.options]
//! azure_storage_account_name = "mylake"
//! azure_storage_account_key  = "..."       # or one of the alternatives below
//! ```
//!
//! Recognised alternatives, in the object-store spelling: `azure_storage_sas_key`,
//! `azure_storage_token`, `azure_client_id` + `azure_client_secret` + `azure_tenant_id`
//! for a service principal, or `azure_use_azure_cli = "true"`. Setting
//! `azure_msi_endpoint`, or nothing at all on a machine with a managed identity, uses
//! that. The same keys are read from the environment in upper case
//! (`AZURE_STORAGE_ACCOUNT_NAME`), which is usually how a container gets them.
//!
//! URIs take either shape:
//!
//! ```text
//! abfss://container@account.dfs.core.windows.net/path/to/table
//! az://container/path/to/table          # account from the options
//! ```

use std::collections::HashMap;

use deltalake::{ensure_table_uri, open_table_with_storage_options, DeltaTable};

use crate::error::{Error, Result};

/// How to reach object storage. Cheap to clone; it is a credential bag, not a connection.
#[derive(Clone, Debug, Default)]
pub struct Storage {
    options: HashMap<String, String>,
}

impl Storage {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self { options }
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    /// Check that a URI's backend is compiled in and its credentials assemble, without
    /// touching storage.
    ///
    /// Resolving the log store builds the object store and stops there — no request is
    /// made. That makes it something `ddi validate` can do for every pipeline before a
    /// daemon starts: a missing feature or an unparseable account is a startup problem,
    /// and finding it at startup beats finding it on the first batch.
    pub fn check(&self, uri: &str) -> Result<()> {
        use deltalake::logstore::{logstore_for, StorageConfig};

        let url = ensure_table_uri(uri)
            .map_err(|e| Error::Config(format!("{uri:?} is not a usable table URI: {e}")))?;
        let scheme = url.scheme().to_string();
        let config = StorageConfig::parse_options(self.options.clone()).map_err(|e| {
            Error::Config(format!(
                "storage options are not usable for {uri:?}: {e}{}",
                hint(&scheme, self.options.is_empty())
            ))
        })?;
        logstore_for(&url, config).map_err(|e| {
            Error::Config(format!(
                "no storage backend for {uri:?}: {e}{}",
                hint(&scheme, self.options.is_empty())
            ))
        })?;
        Ok(())
    }

    /// Open a Delta table by URI.
    ///
    /// The error names the scheme, because "not found" for `abfss://…` almost always
    /// means a credential problem or a build without the feature, not a missing table.
    pub async fn open(&self, uri: &str) -> Result<DeltaTable> {
        let url = ensure_table_uri(uri)
            .map_err(|e| Error::Config(format!("{uri:?} is not a usable table URI: {e}")))?;
        let scheme = url.scheme().to_string();
        open_table_with_storage_options(url, self.options.clone())
            .await
            .map_err(|e| {
                Error::Config(format!(
                    "cannot open {uri:?}: {e}{}",
                    hint(&scheme, self.options.is_empty())
                ))
            })
    }
}

/// Turn the usual failure into the usual next step.
fn hint(scheme: &str, no_options: bool) -> String {
    match scheme {
        "abfss" | "abfs" | "az" | "adl" if no_options => {
            ". No [storage.options] are set, so this depends entirely on the environment \
             (AZURE_STORAGE_ACCOUNT_NAME and friends) or on a managed identity being \
             available."
                .into()
        }
        "abfss" | "abfs" | "az" | "adl" => {
            ". Check that the account, container and credentials in [storage.options] are \
             right, and that the identity may read the container."
                .into()
        }
        // A scheme with no backend never reaches here: `ensure_table_uri` rejects it
        // first, and its message already lists the schemes that are registered.
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_missing_local_table_says_so_plainly() {
        let s = Storage::default();
        let e = s.open("/definitely/not/a/table").await.unwrap_err();
        assert!(!e.to_string().contains("storage.options"), "got: {e}");
    }

    #[test]
    fn the_azure_backend_is_compiled_in() {
        // `check` resolves the backend without a request, so this stays a unit test
        // rather than something that quietly depends on reaching Azure. If the feature
        // were dropped from Cargo.toml, this is what would catch it.
        let s = Storage::new(HashMap::from([
            ("azure_storage_account_name".to_string(), "acct".to_string()),
            ("azure_storage_account_key".to_string(), "a2V5".to_string()),
        ]));
        s.check("abfss://container@acct.dfs.core.windows.net/some/table")
            .expect("abfss:// must resolve");
        s.check("az://container/some/table")
            .expect("az:// must resolve");
    }

    #[test]
    fn a_local_path_needs_no_credentials() {
        Storage::default().check("/tmp/some/table").unwrap();
    }

    #[test]
    fn an_unusable_credential_is_caught_before_anything_starts() {
        let s = Storage::new(HashMap::from([(
            "azure_storage_use_emulator".to_string(),
            "not-a-bool".to_string(),
        )]));
        let e = s
            .check("abfss://c@acct.dfs.core.windows.net/t")
            .unwrap_err();
        assert!(e.to_string().contains("storage.options"), "got: {e}");
    }

    #[test]
    fn only_object_store_schemes_get_a_credential_hint() {
        assert!(hint("abfss", true).contains("AZURE_STORAGE_ACCOUNT_NAME"));
        assert!(hint("abfss", false).contains("[storage.options]"));
        assert!(hint("file", false).is_empty(), "a local path needs no hint");
    }

    #[test]
    fn a_scheme_with_no_backend_is_named_along_with_the_ones_there_are() {
        // The message comes from delta-rs, which lists what is registered — better than
        // anything guessed here, and it is what proves the azure feature took effect.
        let e = Storage::default().check("s3://bucket/table").unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("Unknown scheme: s3"), "got: {msg}");
        assert!(msg.contains("abfss"), "azure must be registered: {msg}");
    }
}
