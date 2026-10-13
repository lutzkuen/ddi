//! A writer schema's records walked on the wire, value by value, without decoding them: the
//! second half of the strict per-message check (docs/kafka-design.md §5.4).
//!
//! arrow-avro's `Decoder` reads a stream, not messages, and lets through two kinds of message
//! that apache-avro 0.21.0, and so kafka-delta-ingest, refused:
//!
//! - **A record followed by a copy of its own Confluent header.** After a record, the decoder
//!   reads what follows as the next message's header: five bytes naming the same writer schema
//!   are taken as one, and the decoder then waits for a body (reader/mod.rs:706-735). It reports
//!   every byte read and one row, so the malformed message passes; the next message of that
//!   writer schema is then read as the body, header included, and fails or becomes a row of
//!   garbage. Walking the record says where it ends, and anything after that end is the
//!   message's own fault.
//! - **A two-branch union's index out of range.** For `["null", T]` and `[T, "null"]` arrow-avro
//!   reads the index as a raw varint and only compares it with 0 (`NullablePlan::ReadTag`,
//!   reader/record.rs:1350-1364): index 5, or -1, reads as the value's branch, or as null. A
//!   union of more branches is checked. The same holds for a boolean, which arrow-avro reads as
//!   true for any byte but 0, where apache-avro refuses one that is not 0 or 1
//!   (decode.rs:85-100).
//!
//! The walk reads only what the wire format says (lengths, counts, indexes), never a value, and
//! allocates nothing per message. It runs on a message arrow-avro accepted, so it agrees with
//! arrow-avro on every message that is well-formed.

use std::collections::HashMap;

use serde_json::Value;

/// How a writer schema's values lie on the wire.
pub(super) struct Wire {
    /// Every type, a node's children always before it.
    nodes: Vec<Node>,
    /// Whether a value of each node takes no bytes at all.
    empty: Vec<bool>,
    root: usize,
}

enum Node {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    /// `bytes` and `string`: a length, then that many bytes.
    Bytes,
    Fixed(usize),
    Enum,
    Array(usize),
    Map(usize),
    Union(Vec<usize>),
    Record(Vec<usize>),
}

impl Wire {
    /// The wire form of `schema`'s values, read as arrow-avro reads the schema: named types by
    /// `(namespace, name)`, a reference resolved in its enclosing namespace, a name registered
    /// only once its definition is read (codec.rs:1228-1246, 1497-1520). Logical types do not
    /// change the wire form and are not read. Fails where arrow-avro could not build a decoder
    /// either.
    pub(super) fn new(schema: &str) -> Result<Self, String> {
        let value: Value = serde_json::from_str(schema).map_err(|e| e.to_string())?;
        let mut parser = Parser {
            nodes: Vec::new(),
            named: HashMap::new(),
        };
        let root = parser.parse(&value, None)?;
        let mut empty = Vec::with_capacity(parser.nodes.len());
        for node in &parser.nodes {
            empty.push(match node {
                Node::Null | Node::Fixed(0) => true,
                Node::Record(fields) => fields.iter().all(|f| empty[*f]),
                _ => false,
            });
        }
        Ok(Self {
            nodes: parser.nodes,
            empty,
            root,
        })
    }

    /// The length of the record at the start of `body`, which is the rest of a message after
    /// its header; or why `body` holds no well-formed record there.
    pub(super) fn record_len(&self, body: &[u8]) -> Result<usize, String> {
        let mut rest = body;
        self.walk(self.root, &mut rest)?;
        Ok(body.len() - rest.len())
    }

    fn walk(&self, node: usize, buf: &mut &[u8]) -> Result<(), String> {
        match &self.nodes[node] {
            Node::Null => Ok(()),
            Node::Boolean => match take(buf, 1)?[0] {
                0 | 1 => Ok(()),
                b => Err(format!("a boolean of byte {b}, which is neither 0 nor 1")),
            },
            Node::Int => {
                if varint(buf)? > u64::from(u32::MAX) {
                    return Err("an int out of range".to_string());
                }
                Ok(())
            }
            Node::Long => varint(buf).map(drop),
            Node::Float => take(buf, 4).map(drop),
            Node::Double => take(buf, 8).map(drop),
            Node::Bytes => {
                let len = long(buf)?;
                let len = usize::try_from(len).map_err(|_| format!("a length of {len}"))?;
                take(buf, len).map(drop)
            }
            Node::Fixed(n) => take(buf, *n).map(drop),
            Node::Enum => {
                if varint(buf)? > u64::from(u32::MAX) {
                    return Err("an enum index out of range".to_string());
                }
                Ok(())
            }
            Node::Array(item) => self.blocks(buf, |buf| self.walk(*item, buf), self.empty[*item]),
            Node::Map(value) => self.blocks(
                buf,
                |buf| {
                    self.walk_bytes(buf)?;
                    self.walk(*value, buf)
                },
                false,
            ),
            Node::Union(branches) => {
                let index = long(buf)?;
                let branch = usize::try_from(index)
                    .ok()
                    .and_then(|i| branches.get(i))
                    .ok_or_else(|| {
                        format!(
                            "union branch {index} out of range ({} branches)",
                            branches.len()
                        )
                    })?;
                self.walk(*branch, buf)
            }
            Node::Record(fields) => fields.iter().try_for_each(|f| self.walk(*f, buf)),
        }
    }

    fn walk_bytes(&self, buf: &mut &[u8]) -> Result<(), String> {
        let len = long(buf)?;
        let len = usize::try_from(len).map_err(|_| format!("a length of {len}"))?;
        take(buf, len).map(drop)
    }

    /// An array's or a map's blocks: a count, then that many items, until a count of 0; a
    /// negative count is followed by the block's size in bytes. Items that take no bytes are
    /// not walked one by one, so a hostile count costs nothing.
    fn blocks(
        &self,
        buf: &mut &[u8],
        mut item: impl FnMut(&mut &[u8]) -> Result<(), String>,
        empty_items: bool,
    ) -> Result<(), String> {
        loop {
            let count = long(buf)?;
            if count == 0 {
                return Ok(());
            }
            let count = if count < 0 {
                long(buf)?;
                count
                    .checked_neg()
                    .ok_or_else(|| format!("a block of {count} items"))?
            } else {
                count
            };
            if empty_items {
                continue;
            }
            // Each item takes at least one byte, so the bytes there are bound the loop.
            for _ in 0..count {
                item(buf)?;
            }
        }
    }
}

/// The next `n` bytes.
fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8], String> {
    if buf.len() < n {
        return Err("the body ends inside a value".to_string());
    }
    let (head, rest) = buf.split_at(n);
    *buf = rest;
    Ok(head)
}

/// A varint, as arrow-avro reads one (reader/vlq.rs): at most ten bytes, the tenth below 2.
fn varint(buf: &mut &[u8]) -> Result<u64, String> {
    let mut value = 0u64;
    for (i, &byte) in buf.iter().take(10).enumerate() {
        value |= u64::from(byte & 0x7F) << (7 * i);
        if byte < 0x80 {
            if i == 9 && byte >= 2 {
                break;
            }
            *buf = &buf[i + 1..];
            return Ok(value);
        }
    }
    Err("bad varint".to_string())
}

/// A zigzag long.
fn long(buf: &mut &[u8]) -> Result<i64, String> {
    let v = varint(buf)?;
    Ok((v >> 1) as i64 ^ -((v & 1) as i64))
}

struct Parser {
    nodes: Vec<Node>,
    /// Named types by `(namespace, name)`, the namespace empty when there is none.
    named: HashMap<(String, String), usize>,
}

impl Parser {
    fn push(&mut self, node: Node) -> usize {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    fn parse(&mut self, v: &Value, namespace: Option<&str>) -> Result<usize, String> {
        match v {
            Value::String(name) => self.named_type(name, namespace),
            Value::Array(branches) => {
                let branches = branches
                    .iter()
                    .map(|b| self.parse(b, namespace))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(self.push(Node::Union(branches)))
            }
            Value::Object(o) => {
                let kind = o
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("a type that is not a name: {v}"))?;
                let str_of = |key: &str| {
                    o.get(key)
                        .and_then(Value::as_str)
                        .ok_or_else(|| format!("a {kind} without a {key}"))
                };
                let own_namespace = o.get("namespace").and_then(Value::as_str).or(namespace);
                match kind {
                    "record" => {
                        let name = str_of("name")?;
                        let fields = o
                            .get("fields")
                            .and_then(Value::as_array)
                            .ok_or_else(|| format!("the record {name} without fields"))?;
                        let fields = fields
                            .iter()
                            .map(|f| match f.get("type") {
                                Some(t) => self.parse(t, own_namespace),
                                None => Err(format!("a field of {name} without a type")),
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        let id = self.push(Node::Record(fields));
                        self.register(name, own_namespace, id);
                        Ok(id)
                    }
                    "enum" => {
                        let name = str_of("name")?;
                        o.get("symbols")
                            .and_then(Value::as_array)
                            .ok_or_else(|| format!("the enum {name} without symbols"))?;
                        let id = self.push(Node::Enum);
                        self.register(name, own_namespace, id);
                        Ok(id)
                    }
                    "fixed" => {
                        let name = str_of("name")?;
                        let size = o
                            .get("size")
                            .and_then(Value::as_u64)
                            .and_then(|s| usize::try_from(s).ok())
                            .ok_or_else(|| format!("the fixed {name} without a size"))?;
                        let id = self.push(Node::Fixed(size));
                        self.register(name, own_namespace, id);
                        Ok(id)
                    }
                    "array" => {
                        let items = o.get("items").ok_or("an array without items")?;
                        let items = self.parse(items, namespace)?;
                        Ok(self.push(Node::Array(items)))
                    }
                    "map" => {
                        let values = o.get("values").ok_or("a map without values")?;
                        let values = self.parse(values, namespace)?;
                        Ok(self.push(Node::Map(values)))
                    }
                    // A primitive or a named type, with attributes (a logical type among them).
                    other => self.named_type(other, namespace),
                }
            }
            other => Err(format!("not a schema: {other}")),
        }
    }

    fn named_type(&mut self, name: &str, namespace: Option<&str>) -> Result<usize, String> {
        let node = match name {
            "null" => Node::Null,
            "boolean" => Node::Boolean,
            "int" => Node::Int,
            "long" => Node::Long,
            "float" => Node::Float,
            "double" => Node::Double,
            "bytes" | "string" => Node::Bytes,
            _ => {
                let (namespace, name) = name
                    .rsplit_once('.')
                    .unwrap_or((namespace.unwrap_or(""), name));
                return self
                    .named
                    .get(&(namespace.to_string(), name.to_string()))
                    .copied()
                    .ok_or_else(|| format!("the type {namespace}.{name} is not defined before"));
            }
        };
        Ok(self.push(node))
    }

    fn register(&mut self, name: &str, namespace: Option<&str>, id: usize) {
        self.named
            .insert((namespace.unwrap_or("").to_string(), name.to_string()), id);
    }
}
