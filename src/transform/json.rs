//! JSON field extraction, for bronze tables that carry a payload as text.
//!
//! Row-local and stateless, exactly like the `array_*` UDFs: a value is derived from one
//! row's own column and nothing else, so batch boundaries cannot change the answer.
//!
//! # Why these names
//!
//! A dbt model has to run in two places — in the warehouse when the batch rebuilds it,
//! and here when `ddi` streams it — so the SQL must mean the same thing in both. These
//! are the spellings the warehouses already use:
//!
//! | Engine | Function |
//! |---|---|
//! | Trino / Starburst | `json_extract_scalar(json, '$.field')` |
//! | DuckDB | `json_extract_string(json, '$.field')` |
//! | Spark | `get_json_object(json, '$.field')` |
//!
//! All three are registered, with identical behaviour, so a model written for any of them
//! streams unchanged.
//!
//! The result is always text. Casting it is the model's job — `CAST(... AS BIGINT)` — and
//! that cast goes through the same hard-failing coercion as everything else, so a payload
//! whose field is not a number is an error rather than a silent NULL.

use std::any::Any;
use std::sync::Arc;

use deltalake::arrow::array::{Array, ArrayRef, StringArray};
use deltalake::arrow::datatypes::DataType;
use deltalake::datafusion::common::Result as DFResult;
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use deltalake::datafusion::prelude::SessionContext;

/// Every spelling of "pull this field out of that JSON".
pub const NAMES: &[&str] = &[
    "json_extract_scalar",
    "json_extract_string",
    "get_json_object",
];

pub fn register(ctx: &SessionContext) {
    for name in NAMES {
        ctx.register_udf(ScalarUDF::from(JsonExtract::new(name)));
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonExtract {
    name: &'static str,
    signature: Signature,
}

impl JsonExtract {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

/// Resolve a `$.a.b` path against one document.
///
/// Only the dotted subset is supported: it is what these functions are used for in
/// practice, and an unsupported path is an error rather than a silently empty result.
fn lookup<'a>(
    doc: &'a serde_json::Value,
    path: &str,
) -> Result<Option<&'a serde_json::Value>, String> {
    let rest = path
        .strip_prefix("$.")
        .or_else(|| path.strip_prefix('$'))
        .unwrap_or(path);
    if rest.contains('[') || rest.contains('*') {
        return Err(format!(
            "JSON path {path:?} is not supported: only dotted field access like \
             '$.customer.id' is. Array indexing and wildcards reach across structure in \
             ways this tool does not model."
        ));
    }

    let mut cur = doc;
    for part in rest.split('.').filter(|p| !p.is_empty()) {
        match cur.get(part) {
            Some(v) => cur = v,
            None => return Ok(None),
        }
    }
    Ok(Some(cur))
}

/// Render a JSON value as the text these functions return: scalars bare, containers as
/// their JSON encoding, null as SQL NULL.
fn render(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

impl ScalarUDFImpl for JsonExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: deltalake::datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let docs = to_text(&args.args[0], rows, self.name)?;
        let paths = to_text(&args.args[1], rows, self.name)?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(rows);
        for i in 0..rows {
            if docs.is_null(i) || paths.is_null(i) {
                out.push(None);
                continue;
            }
            let doc: serde_json::Value = serde_json::from_str(docs.value(i)).map_err(|e| {
                // Loud, like every other malformed-input path here: there is no
                // dead-letter queue, so bad JSON stops the pipeline.
                DataFusionError::Execution(format!(
                    "{}: row {i} is not valid JSON: {e}. Input is a typed column, not \
                     arbitrary text, so this is a data-quality failure rather than a row \
                     to skip.",
                    self.name
                ))
            })?;
            let found = lookup(&doc, paths.value(i)).map_err(DataFusionError::Execution)?;
            out.push(found.and_then(render));
        }
        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(out)) as ArrayRef
        ))
    }
}

fn to_text(v: &ColumnarValue, rows: usize, who: &str) -> DFResult<StringArray> {
    let arr = match v {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(rows)?,
    };
    let cast = deltalake::arrow::compute::cast(&arr, &DataType::Utf8).map_err(|e| {
        DataFusionError::Execution(format!(
            "{who}: argument is not text and cannot become it: {e}"
        ))
    })?;
    Ok(cast
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8")
        .clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::{SqlTransform, Transform};
    use deltalake::arrow::array::RecordBatch;
    use deltalake::arrow::datatypes::{Field, Schema};

    fn batch(payloads: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(payloads.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    async fn run(sql: &str, payloads: &[&str]) -> Vec<Option<String>> {
        let out = SqlTransform::new(sql)
            .apply(vec![batch(payloads)])
            .await
            .expect("transform should succeed");
        let mut v = Vec::new();
        for b in out {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 out");
            for i in 0..a.len() {
                v.push((!a.is_null(i)).then(|| a.value(i).to_string()));
            }
        }
        v
    }

    #[tokio::test]
    async fn extracts_a_top_level_field() {
        let got = run(
            "SELECT json_extract_scalar(data, '$.status') AS s FROM source",
            &[r#"{"status":"paid"}"#, r#"{"status":"shipped"}"#],
        )
        .await;
        assert_eq!(got, vec![Some("paid".into()), Some("shipped".into())]);
    }

    #[tokio::test]
    async fn extracts_a_nested_field() {
        let got = run(
            "SELECT json_extract_scalar(data, '$.customer.id') AS s FROM source",
            &[r#"{"customer":{"id":42}}"#],
        )
        .await;
        assert_eq!(got, vec![Some("42".into())], "numbers come back as text");
    }

    #[tokio::test]
    async fn every_engines_spelling_behaves_the_same() {
        // The property that lets one dbt model run in the warehouse and here.
        for f in NAMES {
            let got = run(
                &format!("SELECT {f}(data, '$.a') AS s FROM source"),
                &[r#"{"a":"x"}"#],
            )
            .await;
            assert_eq!(got, vec![Some("x".into())], "{f} disagreed");
        }
    }

    #[tokio::test]
    async fn a_missing_field_is_null_not_an_error() {
        let got = run(
            "SELECT json_extract_scalar(data, '$.nope') AS s FROM source",
            &[r#"{"a":1}"#],
        )
        .await;
        assert_eq!(got, vec![None]);
    }

    #[tokio::test]
    async fn a_json_null_is_sql_null() {
        let got = run(
            "SELECT json_extract_scalar(data, '$.a') AS s FROM source",
            &[r#"{"a":null}"#],
        )
        .await;
        assert_eq!(got, vec![None]);
    }

    #[tokio::test]
    async fn malformed_json_stops_the_pipeline() {
        // No dead-letter queue by design: bad input is a failure, not a skipped row.
        let e = SqlTransform::new("SELECT json_extract_scalar(data, '$.a') AS s FROM source")
            .apply(vec![batch(&["{not json"])])
            .await
            .unwrap_err();
        assert!(e.to_string().contains("not valid JSON"), "got: {e}");
    }

    #[tokio::test]
    async fn array_paths_are_rejected_rather_than_silently_empty() {
        let e = SqlTransform::new("SELECT json_extract_scalar(data, '$.a[0]') AS s FROM source")
            .apply(vec![batch(&[r#"{"a":[1]}"#])])
            .await
            .unwrap_err();
        assert!(e.to_string().contains("not supported"), "got: {e}");
    }

    #[tokio::test]
    async fn a_casted_field_becomes_a_real_number() {
        let out = SqlTransform::new(
            "SELECT CAST(json_extract_scalar(data, '$.amount') AS BIGINT) AS amount FROM source",
        )
        .apply(vec![batch(&[r#"{"amount":"1250"}"#])])
        .await
        .unwrap();
        let a = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::Int64Array>()
            .expect("int64 after cast");
        assert_eq!(a.value(0), 1250);
    }
}
