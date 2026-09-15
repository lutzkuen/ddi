//! Trino/Starburst JSON functions, for bronze tables that carry a payload as text.
//!
//! Row-local and stateless, exactly like the `array_*` UDFs: a value is derived from one
//! row's own column and nothing else, so batch boundaries cannot change the answer.
//!
//! # Why these, and why these names
//!
//! A dbt model has to run in two places — in the warehouse when the batch rebuilds it,
//! and here when `ddi` streams it — so the SQL must mean the same thing in both. These
//! follow [Trino's JSON
//! functions](https://trino.io/docs/current/functions/json.html), which is what Starburst
//! runs, including the parts people get wrong:
//!
//! - `json_extract_scalar` returns **NULL for an object or array**. Only `json_extract`
//!   returns those, as JSON text. Rendering a container from the scalar form would put
//!   `{"a":1}` in a column somebody casts to a number.
//! - A missing path is NULL, but malformed JSON is an error. Input is a typed column, not
//!   arbitrary text, so bad JSON is a data-quality failure rather than a row to skip.
//! - `json_size` counts members of an object or elements of an array, and is 0 for a
//!   scalar.
//!
//! Since `ddi` has no distinct JSON type, `json` and `varchar` are both text here. What
//! Trino's type system knows — that `json_extract` returns JSON and `json_extract_scalar`
//! returns text — rides on a field marker instead, see [`JSON_MARKER`]; it is what lets
//! `json_object` embed one and quote the other, as Starburst does. `json_format` is
//! identity, and `json_parse` stores what Trino stores: the value re-serialised with its
//! keys sorted and its floats spelt by `BigDecimal` — see [`crate::transform::jsonval`],
//! which reads and writes JSON the way Jackson does so the two engines agree to the byte.
//!
//! DuckDB's `json_extract_string` and Spark's `get_json_object` are registered as aliases
//! of `json_extract_scalar`, so a model written against either streams unchanged.

use std::any::Any;
use std::sync::Arc;

use deltalake::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray,
};
use deltalake::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use deltalake::datafusion::common::Result as DFResult;
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use deltalake::datafusion::prelude::SessionContext;

use crate::transform::jsonval::{self, Json, Members, Numbers};

/// One step of a JSON path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    Field(String),
    Index(usize),
}

/// Parse the JSONPath subset Trino accepts: `$`, `.name`, `["name"]`, `[0]`.
///
/// Wildcards and filters are rejected rather than silently ignored — they select sets,
/// and a function declared to return one value cannot honestly do that.
fn parse_path(path: &str) -> Result<Vec<Step>, String> {
    let bytes: Vec<char> = path.chars().collect();
    let mut i = 0;
    let mut steps = Vec::new();

    if bytes.first() == Some(&'$') {
        i = 1;
    }
    while i < bytes.len() {
        match bytes[i] {
            '.' => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != '.' && bytes[i] != '[' {
                    i += 1;
                }
                if start == i {
                    return Err(format!("JSON path {path:?}: empty field name after '.'"));
                }
                let name: String = bytes[start..i].iter().collect();
                if name.contains('*') {
                    return Err(wildcard(path));
                }
                steps.push(Step::Field(name));
            }
            '[' => {
                i += 1;
                if i < bytes.len() && (bytes[i] == '"' || bytes[i] == '\'') {
                    let quote = bytes[i];
                    i += 1;
                    let start = i;
                    while i < bytes.len() && bytes[i] != quote {
                        i += 1;
                    }
                    let name: String = bytes[start..i].iter().collect();
                    i += 1; // closing quote
                    if i >= bytes.len() || bytes[i] != ']' {
                        return Err(format!("JSON path {path:?}: unterminated ["));
                    }
                    i += 1;
                    steps.push(Step::Field(name));
                } else {
                    let start = i;
                    while i < bytes.len() && bytes[i] != ']' {
                        i += 1;
                    }
                    let raw: String = bytes[start..i].iter().collect();
                    if i >= bytes.len() {
                        return Err(format!("JSON path {path:?}: unterminated ["));
                    }
                    i += 1; // ']'
                    if raw.contains('*') {
                        return Err(wildcard(path));
                    }
                    let n: usize = raw.trim().parse().map_err(|_| {
                        format!("JSON path {path:?}: {raw:?} is not an array index")
                    })?;
                    steps.push(Step::Index(n));
                }
            }
            // A bare leading field, as in 'a.b' — tolerated so a path that forgot its $
            // behaves the way the author obviously meant.
            c if steps.is_empty() && (c.is_alphanumeric() || c == '_') => {
                let start = i;
                while i < bytes.len() && bytes[i] != '.' && bytes[i] != '[' {
                    i += 1;
                }
                steps.push(Step::Field(bytes[start..i].iter().collect()));
            }
            c => return Err(format!("JSON path {path:?}: unexpected {c:?}")),
        }
    }
    Ok(steps)
}

fn wildcard(path: &str) -> String {
    format!(
        "JSON path {path:?} is not supported: wildcards select a set of values, and these \
         functions return a single one. Unnest the array instead."
    )
}

fn resolve<'a>(doc: &'a Json, steps: &[Step]) -> Option<&'a Json> {
    let mut cur = doc;
    for s in steps {
        cur = match s {
            Step::Field(f) => cur.get(f)?,
            Step::Index(n) => cur.index(*n)?,
        };
    }
    Some(cur)
}

/// A value copied out as JSON text: as it was written, order and digits included, which
/// is what Trino's streaming extractor does.
fn extract_text(v: &Json) -> String {
    jsonval::to_string(v, Members::Verbatim, Numbers::Verbatim)
}

/// Every function this module provides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    /// `json_extract(json, path) -> json`
    Extract,
    /// `json_extract_scalar(json, path) -> varchar`
    ExtractScalar,
    /// `json_query(json, path) -> varchar`
    ///
    /// The same lookup as `json_extract`, but the SQL/JSON standard function returns
    /// *text* in Trino unless told `RETURNING json` — which is why it may be nested in a
    /// `json_object` without `FORMAT JSON` while `json_extract` may not.
    Query,
    /// `json_size(json, path) -> bigint`
    Size,
    /// `json_array_length(json) -> bigint`
    ArrayLength,
    /// `json_array_contains(json, value) -> boolean`
    ArrayContains,
    /// `json_array_get(json, index) -> json`
    ArrayGet,
    /// `json_format(json) -> varchar`
    Format,
    /// `json_parse(varchar) -> json`
    Parse,
    /// `is_json_scalar(json) -> boolean`
    IsScalar,
    /// `json_exists(json, path) -> boolean`
    Exists,
    /// `json_array_elements(json) -> array<json>`
    ///
    /// The one function here that returns a *list*, and the reason it exists: `unnest`
    /// needs a real Arrow list, and every other function in this module hands back text.
    /// It is what `CAST(<json> AS ARRAY(JSON))` is rewritten into — see
    /// [`crate::transform::unnest`].
    ArrayElements,
}

impl Kind {
    fn arity(&self) -> usize {
        match self {
            Kind::ArrayLength
            | Kind::Format
            | Kind::Parse
            | Kind::IsScalar
            | Kind::ArrayElements => 1,
            _ => 2,
        }
    }

    fn return_type(&self) -> DataType {
        match self {
            Kind::Size | Kind::ArrayLength => DataType::Int64,
            Kind::ArrayContains | Kind::IsScalar | Kind::Exists => DataType::Boolean,
            Kind::ArrayElements => DataType::List(element_field()),
            _ => DataType::Utf8,
        }
    }
}

/// The element field of `json_array_elements`' result.
///
/// Named and shaped in one place because the declared return type and the array actually
/// built have to agree exactly, or DataFusion rejects the result rather than the data.
pub(crate) fn element_field() -> FieldRef {
    Arc::new(json_field("item", true))
}

/// The field metadata key that says what a text column is, in Trino's type system.
///
/// `ddi` has no JSON *type*: `json` and `varchar` are both `Utf8`. What Trino keeps in
/// its type system — that `json_extract` returns JSON while `json_extract_scalar` returns
/// text — is kept here as a marker on the Arrow field. DataFusion carries field metadata
/// through column references, aliases, CTEs and function results, so a value extracted
/// two CTEs up is still known to be JSON where `json_object` decides how to embed it, and
/// decides it the way Starburst does. The marker never leaves a transform — see
/// [`strip_json_marker`] — so nothing that writes, compares or publishes a batch sees it.
pub(crate) const JSON_MARKER: &str = "ddi:json";

/// What the marker says a text field is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Marker {
    /// Trino's JSON type: the result of `json_extract`, `json_array_get`, `json_parse`,
    /// `CAST(.. AS JSON)`, an element of `CAST(.. AS ARRAY(JSON))`, or a constructor told
    /// `RETURNING JSON`. Embedded in a constructor only after `json_format(..) FORMAT
    /// JSON`, exactly as Starburst requires.
    Typed,
    /// Text that `FORMAT JSON` has read: embedded as JSON, and nowhere else meaningful.
    Formatted,
}

impl Marker {
    fn value(self) -> &'static str {
        match self {
            Marker::Typed => "typed",
            Marker::Formatted => "formatted",
        }
    }
}

/// Mark a field.
pub(crate) fn mark(field: Field, marker: Marker) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(JSON_MARKER.to_string(), marker.value().to_string());
    field.with_metadata(metadata)
}

/// A `Utf8` field of Trino's JSON type.
pub(crate) fn json_field(name: &str, nullable: bool) -> Field {
    mark(Field::new(name, DataType::Utf8, nullable), Marker::Typed)
}

/// What the field's marker says, if it has one.
pub(crate) fn marker_of(field: &Field) -> Option<Marker> {
    match field.metadata().get(JSON_MARKER).map(String::as_str) {
        Some("typed") => Some(Marker::Typed),
        Some("formatted") => Some(Marker::Formatted),
        _ => None,
    }
}

/// Does this field carry a marker at all?
pub(crate) fn is_json_field(field: &Field) -> bool {
    marker_of(field).is_some()
}

/// Remove the JSON marker from the top-level fields of a transform's output.
///
/// The marker is an implementation detail of expression evaluation. Once the rows are
/// out of the transform they are text like any other, and the sink, the grain check, the
/// staged-upsert merge and the publisher all compare schemas — a stray metadata entry
/// would read as a mismatch to code that has never heard of it.
pub(crate) fn strip_json_marker(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    batches
        .into_iter()
        .map(|batch| {
            let schema = batch.schema();
            if !schema.fields().iter().any(|f| is_json_field(f)) {
                return batch;
            }
            let fields: Vec<FieldRef> = schema
                .fields()
                .iter()
                .map(|f| {
                    let mut metadata = f.metadata().clone();
                    metadata.remove(JSON_MARKER);
                    Arc::new(f.as_ref().clone().with_metadata(metadata))
                })
                .collect();
            let stripped = Schema::new_with_metadata(fields, schema.metadata().clone());
            let columns = batch.columns().to_vec();
            RecordBatch::try_new(Arc::new(stripped), columns).unwrap_or(batch)
        })
        .collect()
}

/// Name → behaviour. Several names share one implementation, which is the point: the same
/// model streams whichever engine wrote it.
const FUNCTIONS: &[(&str, Kind)] = &[
    // Trino / Starburst
    ("json_extract", Kind::Extract),
    ("json_extract_scalar", Kind::ExtractScalar),
    ("json_size", Kind::Size),
    ("json_array_length", Kind::ArrayLength),
    ("json_array_contains", Kind::ArrayContains),
    ("json_array_get", Kind::ArrayGet),
    ("json_format", Kind::Format),
    ("json_parse", Kind::Parse),
    ("is_json_scalar", Kind::IsScalar),
    // SQL/JSON standard spellings, which Trino also accepts
    ("json_value", Kind::ExtractScalar),
    ("json_query", Kind::Query),
    ("json_exists", Kind::Exists),
    // Postgres/DuckDB lineage. Written directly it is not portable to Trino, which is why
    // the documented spelling is `CAST(<json> AS ARRAY(JSON))` — rewritten to this.
    ("json_array_elements", Kind::ArrayElements),
    // Other engines, same behaviour
    ("json_extract_string", Kind::ExtractScalar), // DuckDB
    ("get_json_object", Kind::ExtractScalar),     // Spark
];

pub fn register(ctx: &SessionContext) {
    for (name, kind) in FUNCTIONS {
        ctx.register_udf(ScalarUDF::from(JsonFn::new(name, *kind)));
    }
}

/// The names registered, for diagnostics and tests.
pub fn names() -> Vec<&'static str> {
    FUNCTIONS.iter().map(|(n, _)| *n).collect()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonFn {
    name: &'static str,
    kind: Kind,
    signature: Signature,
}

impl JsonFn {
    fn new(name: &'static str, kind: Kind) -> Self {
        Self {
            name,
            kind,
            signature: Signature::any(kind.arity(), Volatility::Immutable),
        }
    }

    fn bad_json(&self, row: usize, e: impl std::fmt::Display) -> DataFusionError {
        DataFusionError::Execution(format!(
            "{}: row {row} is not valid JSON: {e}. Input is a typed column, not arbitrary \
             text, so this is a data-quality failure rather than a row to skip.",
            self.name
        ))
    }
}

impl ScalarUDFImpl for JsonFn {
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
        Ok(self.kind.return_type())
    }

    /// The same types as [`Self::return_type`], plus the JSON marker on the functions that
    /// return JSON in Trino — `json_extract`, `json_array_get`, `json_parse` and the
    /// elements of `json_array_elements`. `json_extract_scalar`, `json_value`, `json_query`
    /// and `json_format` return text there, so they return plain text here.
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let field = match self.kind {
            Kind::Extract | Kind::ArrayGet | Kind::Parse => json_field(self.name, true),
            Kind::ArrayElements => Field::new(self.name, DataType::List(element_field()), true),
            other => Field::new(self.name, other.return_type(), true),
        };
        Ok(Arc::new(field))
    }

    fn invoke_with_args(
        &self,
        args: deltalake::datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let docs = to_text(&args.args[0], rows, self.name)?;
        let second = if self.kind.arity() == 2 {
            Some(to_text(&args.args[1], rows, self.name)?)
        } else {
            None
        };

        let mut text: Vec<Option<String>> = Vec::new();
        let mut nums: Vec<Option<i64>> = Vec::new();
        let mut bools: Vec<Option<bool>> = Vec::new();
        // Only ArrayElements fills this. A row is `None` when the document was null or the
        // path did not lead to an array; an element is `None` when it was JSON null.
        let mut lists: Vec<Option<Vec<Option<String>>>> = Vec::new();

        for i in 0..rows {
            let arg2 = second.as_ref().map(|a| (a.is_null(i), a.value(i)));
            if docs.is_null(i) || matches!(arg2, Some((true, _))) {
                match self.kind.return_type() {
                    DataType::Int64 => nums.push(None),
                    DataType::Boolean => bools.push(None),
                    DataType::List(_) => lists.push(None),
                    _ => text.push(None),
                }
                continue;
            }
            let raw = docs.value(i);

            // json_parse converts; json_format passes through. Both need the parse.
            let doc: Json = jsonval::parse(raw).map_err(|e| self.bad_json(i, e))?;

            match self.kind {
                // What Trino stores for its JSON type: keys sorted, whitespace gone, and
                // floats re-spelt by `BigDecimal` — `json_format(json_parse(x))` is that.
                Kind::Parse => text.push(Some(jsonval::to_string(
                    &doc,
                    Members::Sorted,
                    Numbers::BigDecimal,
                ))),
                Kind::Format => text.push(Some(raw.to_string())),
                Kind::IsScalar => bools.push(Some(!doc.is_container())),
                Kind::ArrayLength => nums.push(doc.as_array().map(|a| a.len() as i64)),
                // Each element back as JSON text, which is what `ARRAY(JSON)` means: the
                // shape is not decided here, it is decided by whatever reads the elements.
                // A non-array is NULL rather than an error, matching every other path
                // lookup in this module; malformed JSON already errored above.
                Kind::ArrayElements => lists.push(doc.as_array().map(|a| {
                    a.iter()
                        .map(|v| (!v.is_null()).then(|| extract_text(v)))
                        .collect()
                })),
                Kind::ArrayContains => {
                    let needle = arg2.expect("arity 2").1;
                    // Trino compares against a typed value; from SQL text, the honest
                    // reading is "as JSON if it parses, else as a string".
                    let want: Json =
                        jsonval::parse(needle).unwrap_or_else(|_| Json::String(needle.to_string()));
                    bools.push(
                        doc.as_array()
                            .map(|a| a.iter().any(|v| v.same_value(&want))),
                    );
                }
                Kind::ArrayGet => {
                    let idx = arg2.expect("arity 2").1;
                    let n: Result<i64, _> = idx.trim().parse();
                    let got = match (doc.as_array(), n) {
                        (Some(a), Ok(n)) => {
                            // Trino allows negative indexing from the end.
                            let pos = if n < 0 { a.len() as i64 + n } else { n };
                            usize::try_from(pos).ok().and_then(|p| a.get(p))
                        }
                        _ => None,
                    };
                    text.push(got.filter(|v| !v.is_null()).map(extract_text));
                }
                Kind::Extract | Kind::Query | Kind::ExtractScalar | Kind::Size | Kind::Exists => {
                    let path = arg2.expect("arity 2").1;
                    let steps = parse_path(path).map_err(DataFusionError::Execution)?;
                    let found = resolve(&doc, &steps);
                    match self.kind {
                        // JSON in, JSON out: a string keeps its quotes, so the result
                        // composes with the other json_* functions. Unwrapping it here is
                        // what `json_extract_scalar` is for.
                        Kind::Extract | Kind::Query => {
                            text.push(found.filter(|v| !v.is_null()).map(extract_text))
                        }
                        Kind::ExtractScalar => text.push(found.and_then(Json::scalar_text)),
                        Kind::Size => nums.push(found.map(|v| v.size() as i64)),
                        Kind::Exists => bools.push(Some(found.is_some())),
                        _ => unreachable!(),
                    }
                }
            }
        }

        Ok(ColumnarValue::Array(match self.kind.return_type() {
            DataType::Int64 => Arc::new(Int64Array::from(nums)) as ArrayRef,
            DataType::Boolean => Arc::new(BooleanArray::from(bools)) as ArrayRef,
            DataType::List(_) => {
                use deltalake::arrow::array::{ListBuilder, StringBuilder};
                let mut b = ListBuilder::new(StringBuilder::new()).with_field(element_field());
                for row in lists {
                    match row {
                        Some(items) => {
                            for it in items {
                                b.values().append_option(it);
                            }
                            b.append(true);
                        }
                        None => b.append(false),
                    }
                }
                Arc::new(b.finish()) as ArrayRef
            }
            _ => Arc::new(StringArray::from(text)) as ArrayRef,
        }))
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

    const ORDER: &str = r#"{"id":7,"customer":{"id":42,"country":"DE"},
        "lines":[{"sku":"A","qty":2},{"sku":"B","qty":1}],
        "status":"paid","paid":true,"note":null}"#;

    fn batch(payloads: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(payloads.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    async fn one(expr: &str) -> Option<String> {
        let out = SqlTransform::new(format!("SELECT {expr} AS v FROM source"))
            .apply(vec![batch(&[ORDER])])
            .await
            .unwrap_or_else(|e| panic!("{expr} failed: {e}"));
        let b = &out[0];
        let c = deltalake::arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap();
        let c = c.as_any().downcast_ref::<StringArray>().unwrap();
        (!c.is_null(0)).then(|| c.value(0).to_string())
    }

    #[tokio::test]
    async fn extract_scalar_pulls_out_values() {
        assert_eq!(
            one("json_extract_scalar(data, '$.status')").await,
            Some("paid".into())
        );
        assert_eq!(
            one("json_extract_scalar(data, '$.id')").await,
            Some("7".into())
        );
        assert_eq!(
            one("json_extract_scalar(data, '$.paid')").await,
            Some("true".into())
        );
        assert_eq!(
            one("json_extract_scalar(data, '$.customer.country')").await,
            Some("DE".into())
        );
    }

    #[tokio::test]
    async fn extract_scalar_is_null_for_a_container() {
        // Trino's rule, and the one that matters: otherwise `{"id":42,...}` lands in a
        // column somebody casts to a number.
        assert_eq!(one("json_extract_scalar(data, '$.customer')").await, None);
        assert_eq!(one("json_extract_scalar(data, '$.lines')").await, None);
    }

    #[tokio::test]
    async fn extract_returns_containers_as_json_in_source_order() {
        // Trino's extractor copies the structure as it streams past, so the keys come out
        // in the order the payload wrote them, and a number keeps its digits.
        assert_eq!(
            one("json_extract(data, '$.customer')").await,
            Some(r#"{"id":42,"country":"DE"}"#.into())
        );
        assert_eq!(
            one("json_extract(data, '$.lines[0]')").await,
            Some(r#"{"sku":"A","qty":2}"#.into())
        );
    }

    #[tokio::test]
    async fn parse_stores_the_canonical_form_trino_stores() {
        // `json_parse` sorts keys and re-spells floats through BigDecimal; that is what
        // `json_format(json_parse(x))` returns in Starburst, byte for byte.
        let out = SqlTransform::new("SELECT json_format(json_parse(data)) AS v FROM source")
            .apply(vec![batch(&[
                r#"{"b": 1.10, "a": [1e2, {"y": 1, "x": null}], "b": 2}"#,
            ])])
            .await
            .unwrap();
        let c = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(c.value(0), r#"{"a":[1E+2,{"x":null,"y":1}],"b":2}"#);
    }

    #[tokio::test]
    async fn a_scalar_keeps_its_digits() {
        let out = SqlTransform::new("SELECT json_extract_scalar(data, '$.p') AS v FROM source")
            .apply(vec![batch(&[r#"{"p":1.10}"#])])
            .await
            .unwrap();
        let c = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(c.value(0), "1.10");
    }

    #[tokio::test]
    async fn array_indexing_works() {
        assert_eq!(
            one("json_extract_scalar(data, '$.lines[0].sku')").await,
            Some("A".into())
        );
        assert_eq!(
            one("json_extract_scalar(data, '$.lines[1].qty')").await,
            Some("1".into())
        );
    }

    #[tokio::test]
    async fn bracketed_field_names_work() {
        assert_eq!(
            one(r#"json_extract_scalar(data, '$["customer"]["country"]')"#).await,
            Some("DE".into())
        );
    }

    #[tokio::test]
    async fn sizes_and_lengths() {
        assert_eq!(one("json_size(data, '$.lines')").await, Some("2".into()));
        assert_eq!(one("json_size(data, '$.customer')").await, Some("2".into()));
        assert_eq!(one("json_size(data, '$.status')").await, Some("0".into()));
        assert_eq!(
            one("json_array_length(json_extract(data, '$.lines'))").await,
            Some("2".into())
        );
    }

    #[tokio::test]
    async fn array_contains_and_get() {
        assert_eq!(
            one("json_array_contains(json_extract(data, '$.lines'), '{\"sku\":\"A\",\"qty\":2}')")
                .await,
            Some("true".into())
        );
        assert_eq!(
            one("json_extract_scalar(json_array_get(json_extract(data, '$.lines'), '0'), '$.sku')")
                .await,
            Some("A".into())
        );
    }

    #[tokio::test]
    async fn exists_and_is_scalar() {
        assert_eq!(
            one("json_exists(data, '$.status')").await,
            Some("true".into())
        );
        assert_eq!(
            one("json_exists(data, '$.nope')").await,
            Some("false".into())
        );
        assert_eq!(one("is_json_scalar(data)").await, Some("false".into()));
        assert_eq!(
            one("is_json_scalar(json_extract(data, '$.status'))").await,
            Some("true".into())
        );
    }

    #[tokio::test]
    async fn parse_and_format_round_trip() {
        assert_eq!(
            one("json_extract_scalar(json_format(json_parse(data)), '$.status')").await,
            Some("paid".into())
        );
    }

    #[tokio::test]
    async fn a_json_null_and_a_missing_path_are_both_sql_null() {
        assert_eq!(one("json_extract_scalar(data, '$.note')").await, None);
        assert_eq!(one("json_extract_scalar(data, '$.nope')").await, None);
    }

    #[tokio::test]
    async fn json_value_and_json_query_are_the_standard_spellings() {
        assert_eq!(
            one("json_value(data, '$.status')").await,
            Some("paid".into())
        );
        assert_eq!(
            one("json_query(data, '$.customer')").await,
            Some(r#"{"id":42,"country":"DE"}"#.into())
        );
    }

    #[tokio::test]
    async fn every_engines_alias_agrees() {
        for f in [
            "json_extract_scalar",
            "json_extract_string",
            "get_json_object",
        ] {
            assert_eq!(
                one(&format!("{f}(data, '$.status')")).await,
                Some("paid".into()),
                "{f} disagreed"
            );
        }
    }

    #[tokio::test]
    async fn malformed_json_stops_the_pipeline() {
        let e = SqlTransform::new("SELECT json_extract_scalar(data, '$.a') AS v FROM source")
            .apply(vec![batch(&["{not json"])])
            .await
            .unwrap_err();
        assert!(e.to_string().contains("not valid JSON"), "got: {e}");
    }

    #[tokio::test]
    async fn wildcards_are_rejected_rather_than_silently_wrong() {
        let e = SqlTransform::new(
            "SELECT json_extract_scalar(data, '$.lines[*].sku') AS v FROM source",
        )
        .apply(vec![batch(&[ORDER])])
        .await
        .unwrap_err();
        assert!(e.to_string().contains("wildcards"), "got: {e}");
    }

    #[test]
    fn paths_parse() {
        assert_eq!(parse_path("$.a").unwrap(), vec![Step::Field("a".into())]);
        assert_eq!(
            parse_path("$.a.b[2]").unwrap(),
            vec![
                Step::Field("a".into()),
                Step::Field("b".into()),
                Step::Index(2)
            ]
        );
        assert_eq!(
            parse_path(r#"$["a b"]"#).unwrap(),
            vec![Step::Field("a b".into())]
        );
        assert_eq!(parse_path("$").unwrap(), vec![], "the document itself");
        assert!(parse_path("$.a[").is_err());
    }
}
