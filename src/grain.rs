//! Proving a target holds one row per key, without holding the target.
//!
//! # Why the aggregate could not be bounded
//!
//! `GROUP BY key HAVING count(*) > 1 LIMIT 3` reads as though it stops at the third duplicate.
//! It does not. `LimitedDistinctAggregation` is the only rule that pushes a limit into a
//! grouped aggregate, and it refuses any aggregate carrying an expression — which `count(*)`
//! is. So the whole input is consumed before the `HAVING` can discard one row, duplicates or
//! not. The limit truncates the *answer*, never the work.
//!
//! The memory budget made it worse rather than better. [`crate::budget::Budget::resolve`]
//! divides the process budget by the pipeline count and `FairSpillPool` is divided again by
//! `target_partitions`, so a *tighter* memory setting makes this spill sooner, into more and
//! smaller files, which the multi-level merge then reads back two at a time and rewrites as
//! intermediates. Memory and disk pull in opposite directions here, which is exactly why
//! raising memory did not bound the scan that prompted this module.
//!
//! # What replaces it
//!
//! The answer [`crate::dedup::Dedup::read`] reached, for the same reason: a hand-written
//! streaming pass instead of a DataFusion aggregate. Hash each key to eight bytes, keep only
//! the hashes belonging to one congruence class of the key space, sort them in place, and look
//! for two the same. Nothing registers with the memory pool, nothing asks the disk manager for
//! a file, and the whole pass is one `Vec<u64>` whose length limit was decided before the first
//! row arrived. **This check writes nothing to disk, at any target size, under any
//! configuration.** That is a property of its shape, not of its settings.
//!
//! One class is not the answer, so the scan is repeated once per class. How many classes there
//! are is arithmetic rather than a guess: eight bytes per row, divided by
//! `[runtime] max_grain_check_memory`, using the exact live row count that is already written
//! down in the target's own log. Reading the log costs nothing and opens no data file — the
//! same trick [`crate::upsert::Window`] uses to size a merge.
//!
//! # Why splitting is exact
//!
//! Equal keys hash equally, so a duplicate pair is congruent modulo *anything* and lands in the
//! same class however the space is divided. That is the whole correctness argument, and it is
//! what lets a class that turns out too big be replaced mid-run by its two halves: `(m, r)`
//! becomes `(2m, r)` and `(2m, r+m)`, whose union is exactly `(m, r)`. No pair is separated, and
//! no class already finished has to be looked at again. A target whose log records no row counts
//! therefore still ends up bounded — it starts small and splits its way down, paying only for
//! the passes it abandoned.
//!
//! # Why sixty-four bits is still exact
//!
//! It is not, on its own. At two billion keys the birthday bound puts about a tenth of a
//! collision in every run, which across a fleet and a year is several — and reporting a
//! collision as a duplicate would refuse a table that is perfectly correct, which is the worse
//! of the two errors this check can make. So a pass does not answer; it *nominates*. Hashes
//! that appear twice are candidates, and a second pass resolves them against the real key
//! values. The check is exact because that second pass exists, and it is affordable because on
//! a clean target there is almost never anything to resolve.
//!
//! # What it costs, honestly
//!
//! N reads of one column. For almost every target N is one, and then this is strictly cheaper
//! than the aggregate it replaces — no plan, no repartition, no spill. For a target with
//! billions of mostly-distinct keys N is tens, and that is the real price of an exact answer
//! under a fixed memory bound: the alternatives are an approximate answer that cannot prove
//! uniqueness, or an unbounded one that evicts a pod. Two things make the price payable. The
//! expensive outcome is "the target is fine", never "the target is broken" — a broken one is
//! answered by the first pass that meets a duplicate. And N is exactly linear in the ceiling,
//! is logged at startup, and is refused outright above [`MAX_PASSES`] rather than run silently
//! for hours.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use deltalake::arrow::array::{Array, ArrayRef};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::DataType;
use deltalake::arrow::row::{RowConverter, SortField};
use deltalake::DeltaTable;
use futures::TryStreamExt;

use crate::error::{Error, Result};

/// What one check may hold, and where that number came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ceiling {
    bytes: u64,
    source: CeilingSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CeilingSource {
    Configured,
    Default,
    /// The process budget was smaller than the default, so it won.
    ClampedToBudget,
}

impl Ceiling {
    /// Below this a pass holds so few keys that even a small target needs hundreds.
    ///
    /// Decimal, not binary, and for the same reason [`crate::spill::MIN_TEMP_DIRECTORY_SIZE`]
    /// is: `bytesize` reads `"16MB"` as sixteen million, and the refusal message tells the
    /// operator to write exactly that. A binary floor would reject its own advice.
    pub const MIN: u64 = 16_000_000;
    /// Not derived from `max_memory`, and that is deliberate — see the field doc on
    /// [`crate::config::Defaults::max_grain_check_memory`]. Decimal so that writing the
    /// default out explicitly resolves to the default.
    pub const DEFAULT: u64 = 512_000_000;
    /// An eighth is reserved for the scan's Arrow batches, the row encoder and the candidate
    /// list, so `bytes` is what the process grows by rather than what one `Vec` is.
    const USABLE_NUM: u64 = 7;
    const USABLE_DEN: u64 = 8;
    /// A guess, used only when the target's log records no row counts at all.
    const ASSUMED_BYTES_PER_ROW: u64 = 64;

    pub fn resolve(configured: Option<u64>) -> Result<Self> {
        if let Some(n) = configured {
            if n < Self::MIN {
                return Err(Error::Config(format!(
                    "runtime.max_grain_check_memory is {n} bytes, which leaves the startup \
                     uniqueness check nowhere to put a single key. Give it at least \"16MB\" — \
                     eight bytes per row of the target, divided by however many passes over it \
                     you are willing to pay for — or remove the key to take the 512MB default."
                )));
            }
            return Ok(Self {
                bytes: n,
                source: CeilingSource::Configured,
            });
        }
        // Not divided by the pipeline count, and not a fraction of one pipeline's share. The
        // check runs behind `max_concurrent_upsert_preflights`, once, before this pipeline has
        // a batch in flight — so it is not competing with the steady-state work `max_memory`
        // exists to divide. It is clamped down only when the whole process budget is smaller
        // than the default, where "512MB for one check" would be a lie.
        match crate::budget::current().per_pipeline() {
            Some(b) if b < Self::DEFAULT => Ok(Self {
                bytes: b.max(Self::MIN),
                source: CeilingSource::ClampedToBudget,
            }),
            _ => Ok(Self {
                bytes: Self::DEFAULT,
                source: CeilingSource::Default,
            }),
        }
    }

    /// A ceiling of exactly this many bytes, with no floor applied.
    ///
    /// The seam a test needs to exercise the multi-pass path without building a target large
    /// enough to overflow a real ceiling — [`Self::MIN`] is sixteen megabytes, which is close
    /// to two million keys. Not reachable from configuration: [`Self::resolve`] applies the
    /// floor and it is the only thing `crate::config` calls.
    #[doc(hidden)]
    pub fn exactly(bytes: u64) -> Self {
        Self {
            bytes,
            source: CeilingSource::Configured,
        }
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn source(&self) -> CeilingSource {
        self.source
    }

    /// How many eight-byte hashes one pass may hold. This is the whole memory bound.
    pub fn hashes_per_pass(&self) -> usize {
        ((self.bytes * Self::USABLE_NUM / Self::USABLE_DEN) / 8) as usize
    }
}

/// A congruence class of the key space: the keys whose hash is `≡ residue (mod modulus)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Class {
    pub modulus: u64,
    pub residue: u64,
}

impl Class {
    /// The whole key space, which is where a check with no row count to go on starts.
    pub fn everything() -> Self {
        Self {
            modulus: 1,
            residue: 0,
        }
    }

    pub fn holds(&self, h: u64) -> bool {
        h % self.modulus == self.residue
    }

    /// The two classes whose union is exactly this one, or `None` at the end of the u64.
    ///
    /// Fallible rather than `self.modulus * 2`, because an unchecked double is a panic sixty
    /// -three splits down and a panic escapes the supervisor's retry loop — which kills the
    /// pipeline for the life of the process instead of failing it. Nothing should reach this
    /// now that a pass compacts instead of splitting on repeats (see [`scan_class`]), so
    /// `None` means an assumption has broken, and the caller says so with the key in hand.
    pub fn split(self) -> Option<(Class, Class)> {
        let modulus = self.modulus.checked_mul(2)?;
        Some((
            Class {
                modulus,
                residue: self.residue,
            },
            Class {
                modulus,
                residue: self.residue + self.modulus,
            },
        ))
    }
}

/// A key the target holds more than once, and how many times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Duplicate {
    pub key: String,
    pub rows: u64,
}

/// What the check found, and what it cost.
#[derive(Debug)]
pub enum Grain {
    Unique {
        /// The live row count from the log, when the log recorded one.
        rows: Option<u64>,
        passes: u32,
    },
    Duplicated {
        examples: Vec<Duplicate>,
        passes: u32,
    },
}

/// The most classes worth starting with. Above this the check is a cost surprise, not a check.
pub const MAX_PASSES: u64 = 256;

/// How many candidate hashes are resolved against real keys in one verification pass.
///
/// A bound on the verification's memory in the one case that needs it: a grossly duplicated
/// target can nominate every hash it has, and resolving all of them at once would be the
/// unbounded map this module exists to avoid.
const VERIFY_CHUNK: usize = 4096;

/// Exact live row count from the target's own log, and the bytes its files occupy.
///
/// `None` for the count when any live file records no `numRecords`: a partial sum would size
/// the passes wrong in the unsafe direction, and this tool's convention is to say it does not
/// know rather than to guess — see [`crate::upsert::Window`].
pub fn live_rows(target: &DeltaTable) -> Result<(Option<u64>, u64)> {
    let snapshot = target.snapshot().map_err(Error::Delta)?;
    let mut rows: Option<u64> = Some(0);
    let mut bytes: u64 = 0;
    for file in snapshot.log_data().iter() {
        bytes = bytes.saturating_add(file.size().max(0) as u64);
        let Some(n) = file.num_records() else {
            rows = None;
            continue;
        };
        // `num_records` is the file's row count before deletion vectors, so a table that has
        // been deleted from would size its passes too large without this.
        let deleted = file
            .deletion_vector_descriptor()
            .map(|dv| dv.cardinality.max(0) as u64)
            .unwrap_or(0);
        if let Some(acc) = rows.as_mut() {
            *acc = acc.saturating_add((n as u64).saturating_sub(deleted));
        }
    }
    Ok((rows, bytes))
}

/// How many classes to start with, and why that many.
pub fn initial_modulus(rows: Option<u64>, bytes: u64, ceiling: Ceiling) -> u64 {
    let cap = ceiling.hashes_per_pass().max(1) as u64;
    let est = rows.unwrap_or(bytes / Ceiling::ASSUMED_BYTES_PER_ROW);
    // One per cent of slack. On a class of 56M that is about 75 standard deviations
    // (σ = √56e6 ≈ 7,483, i.e. 0.013%), so anything past it is not fluctuation — it is a
    // wrong row count or a skewed hash, and the split mechanism is what handles those.
    //
    // Not rounded up to a power of two: splitting works for any modulus, and rounding 35 to 64
    // would buy a cheap bitmask for twenty-nine extra full scans.
    let want = est.saturating_mul(101) / 100;
    want.div_ceil(cap).max(1)
}

/// What one pass over one class came back with.
enum PassOutcome {
    /// The class held more keys than the ceiling allows. Nothing was decided.
    Overflowed,
    /// The hashes that appeared at least twice in this class.
    Candidates(Vec<u64>),
}

/// Does `target` hold `key_column` more than once?
///
/// Exact, and bounded by `ceiling` in memory and by zero on disk. Stops as soon as it has
/// `examples` duplicates, so the expensive outcome is always "the target is fine".
pub async fn check(
    target: &DeltaTable,
    key_column: &str,
    ceiling: Ceiling,
    examples: usize,
) -> Result<Grain> {
    check_with(target, key_column, ceiling, examples, &hash64).await
}

/// As [`check`], with the hash injected.
///
/// The seam exists so a test can hand in a deliberately terrible hash and construct collisions
/// on demand — the one thing this module's exactness argument rests on and the one thing a
/// good hash makes impossible to observe.
pub async fn check_with<H>(
    target: &DeltaTable,
    key_column: &str,
    ceiling: Ceiling,
    examples: usize,
    hash: &H,
) -> Result<Grain>
where
    H: Fn(&[u8]) -> u64 + Sync,
{
    let (rows, bytes) = live_rows(target)?;
    let n0 = initial_modulus(rows, bytes, ceiling);

    if n0 > MAX_PASSES {
        return Err(Error::Config(format!(
            "the startup uniqueness check on {key_column:?} would need about {n0} passes over \
             the target's key column at the current [runtime] max_grain_check_memory of {}, and \
             the target holds {} rows. That is a cost, not a failure — the check writes nothing \
             to disk and cannot exhaust anything — but it would take hours on every start, so \
             it is refused rather than run silently. Raise runtime.max_grain_check_memory (each \
             doubling halves the passes; \"4GB\" would make this about {}), or set \
             upsert_grain_check = \"off\" on this pipeline if you already know the target holds \
             one row per key. Nothing has been read and no other pipeline was touched.",
            bytesize::ByteSize(ceiling.bytes()),
            rows.map(|r| r.to_string()).unwrap_or_else(|| format!(
                "an estimated {}",
                bytes / Ceiling::ASSUMED_BYTES_PER_ROW
            )),
            initial_modulus(
                rows,
                bytes,
                Ceiling {
                    bytes: 4 * 1024 * 1024 * 1024,
                    source: CeilingSource::Configured
                }
            ),
        )));
    }

    let mut queue: VecDeque<Class> = (0..n0)
        .map(|residue| Class {
            modulus: n0,
            residue,
        })
        .collect();

    let mut found: Vec<Duplicate> = Vec::new();
    let mut passes: u32 = 0;
    // A backstop, not a budget: splitting only happens when a class overflowed, so reaching
    // this means the hash is pathologically skewed against these keys rather than that the
    // target is large.
    let ceiling_on_passes = MAX_PASSES.saturating_mul(4);

    while let Some(class) = queue.pop_front() {
        if u64::from(passes) >= ceiling_on_passes {
            return Err(Error::Config(format!(
                "the startup uniqueness check on {key_column:?} has read the target {passes} \
                 times and is still splitting the key space, which means the hash is landing \
                 far more keys in one class than in the others rather than that the target is \
                 large. Raise runtime.max_grain_check_memory so a class fits, or set \
                 upsert_grain_check = \"off\" on this pipeline. Nothing was written and no \
                 other pipeline was stopped."
            )));
        }
        passes += 1;

        match scan_class(target, key_column, class, ceiling, rows, hash).await? {
            PassOutcome::Overflowed => {
                // Only the class that overflowed is repeated, at the granularity it needs.
                // Classes already finished are never redone: the keys of `(m, j)` are exactly
                // those of `(2m, j)` ∪ `(2m, j+m)`, so an answer for the coarse class answers
                // both fine ones.
                //
                // A class overflows only on *distinct* keys — repeats are compacted away
                // inside the pass — so splitting always halves the population that caused it
                // and this terminates. The `None` arm is the proof obligation for that
                // sentence: if it is ever reached, the invariant is broken and the operator
                // gets a message rather than an unwind.
                let Some((a, b)) = class.split() else {
                    return Err(Error::Config(format!(
                        "the startup uniqueness check on {key_column:?} divided the key space \
                         as far as it can and one class is still too large. That should not be \
                         reachable — a class is only split for distinct keys, and splitting \
                         halves those — so this is a bug in ddi rather than a fact about your \
                         target. Set upsert_grain_check = \"off\" on this pipeline to start it \
                         while it is investigated. Nothing was written and no other pipeline \
                         was stopped."
                    )));
                };
                queue.push_front(b);
                queue.push_front(a);
            }
            PassOutcome::Candidates(candidates) => {
                if candidates.is_empty() {
                    continue;
                }
                let wanted = examples.saturating_sub(found.len());
                found.extend(verify(target, key_column, &candidates, wanted, hash).await?);
                if found.len() >= examples {
                    break;
                }
            }
        }
    }

    if found.is_empty() {
        Ok(Grain::Unique { rows, passes })
    } else {
        found.truncate(examples);
        Ok(Grain::Duplicated {
            examples: found,
            passes,
        })
    }
}

/// The declared schema of the target, so every file reads at the table's types.
fn declared_schema(target: &DeltaTable) -> Result<deltalake::arrow::datatypes::SchemaRef> {
    use deltalake::delta_datafusion::DataFusionMixins;
    Ok(target
        .snapshot()
        .map_err(Error::Delta)?
        .snapshot()
        .read_schema())
}

/// A projected streaming scan of the key column alone.
///
/// One `CoalescePartitionsExec` over the provider's scan — no aggregate, no sort, no
/// repartition. Nothing in that plan registers a spillable `MemoryConsumer`, so
/// `DiskManager::create_tmp_file` is never reached. That is why this check's disk footprint is
/// zero by construction rather than by configuration.
///
/// Built by hand rather than through `LoadBuilder::with_columns`, and the reason is a real
/// trap: that builder resolves the column name to an index against the snapshot's *declared*
/// schema and then hands the index to a provider whose schema orders partition columns last.
/// On a partitioned target whose partition column is declared before the key, the two orders
/// disagree and the scan silently returns a different column — after which this check would
/// be hashing the wrong values, and the "column is not in the target table" error it raised
/// would name a table that does contain it. Resolving against the schema the scan actually
/// uses is the whole difference.
async fn key_stream(
    target: &DeltaTable,
    key_column: &str,
) -> Result<deltalake::datafusion::execution::SendableRecordBatchStream> {
    use deltalake::datafusion::catalog::TableProvider;
    use deltalake::datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use deltalake::datafusion::physical_plan::ExecutionPlan;

    let provider = target
        .table_provider()
        .await
        .map_err(|e| Error::Other(format!("upsert: cannot read the target's grain: {e}")))?;
    let schema = TableProvider::schema(provider.as_ref());
    let idx = schema.index_of(key_column).map_err(|_| {
        Error::Config(format!(
            "upsert column {key_column:?} is not in the target table. Columns: [{}]",
            schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })?;

    let state = crate::budget::session(target)?;
    let scan = provider
        .scan(&state, Some(&vec![idx]), &[], None)
        .await
        .map_err(|e| {
            Error::Other(format!(
                "upsert: cannot scan the target to check its grain: {e}"
            ))
        })?;
    // One partition, so the pass sees every row on one stream and the buffer it fills is the
    // only thing holding rows.
    let plan = Arc::new(CoalescePartitionsExec::new(scan));
    plan.execute(0, state.task_ctx()).map_err(|e| {
        Error::Other(format!(
            "upsert: cannot scan the target to check its grain: {e}"
        ))
    })
}

/// One pass: hash the key column, keep the hashes in `class`, and report the repeats.
async fn scan_class<H>(
    target: &DeltaTable,
    key_column: &str,
    class: Class,
    ceiling: Ceiling,
    rows: Option<u64>,
    hash: &H,
) -> Result<PassOutcome>
where
    H: Fn(&[u8]) -> u64 + Sync,
{
    let schema = declared_schema(target)?;
    let mut stream = key_stream(target, key_column).await?;

    let cap = ceiling.hashes_per_pass().max(1);
    // Reserve what this class is expected to hold, not the whole ceiling. The hard bound is
    // the `len() == cap` branch below, not the capacity, so under-reserving costs a realloc
    // and over-reserving costs address space a six-million-row target never needs.
    let expected = rows
        .map(|r| (r.saturating_mul(101) / 100).div_ceil(class.modulus) as usize)
        .unwrap_or(1 << 16);
    let mut hashes: Vec<u64> = Vec::with_capacity(expected.min(cap).max(1));
    // Hashes this class has already seen twice. A subset of what `hashes` held, kept apart
    // so a repeated key costs one slot rather than one per row.
    let mut nominated: Vec<u64> = Vec::new();

    let mut converter: Option<RowConverter> = None;

    while let Some(batch) = stream.try_next().await.map_err(|e| {
        crate::spill::classify(e, "upsert: cannot scan the target to check its grain")
    })? {
        if batch.num_rows() == 0 {
            continue;
        }
        // As the table declares its columns, not as whichever engine wrote each file happened
        // to type them — the same guard `Dedup::read` applies, and for the same reason: two
        // encodings of one logical key must hash alike or a duplicate hides between files.
        let batch = crate::schema::read_as_declared(batch, &schema)?;
        let col = key_column_of(&batch, key_column)?;

        let conv = match &converter {
            Some(c) => c,
            None => {
                converter = Some(
                    RowConverter::new(vec![SortField::new(col.data_type().clone())]).map_err(
                        |e| {
                            Error::Config(format!(
                                "upsert key {key_column:?} cannot be compared: {e}"
                            ))
                        },
                    )?,
                );
                converter.as_ref().expect("just set")
            }
        };
        // A byte-comparable, injective encoding for every Delta type — the same device
        // `Dedup::read` and `upsert::collapse` use. An Int64, a UUID string, a timestamp and a
        // struct all hash without a match arm per type, and a NULL key encodes distinctly and
        // consistently, preserving exactly what `GROUP BY` did with two NULLs.
        let encoded = conv
            .convert_columns(std::slice::from_ref(&col))
            .map_err(|e| Error::Other(format!("upsert: cannot encode the target's keys: {e}")))?;

        for i in 0..batch.num_rows() {
            let h = hash(encoded.row(i).as_ref());
            if !class.holds(h) {
                continue;
            }
            hashes.push(h);
            if hashes.len() + nominated.len() < cap {
                continue;
            }
            // Full. Before concluding the class is too wide, take out what is *repeated* —
            // and this is the whole reason the buffer is compacted rather than simply
            // overflowing. Splitting separates distinct keys; it can never separate rows that
            // share one key value, because equal keys are congruent modulo everything. A
            // class holding one key a million times would therefore overflow, split, and
            // overflow again forever, doubling the modulus until it left u64 and panicked —
            // on precisely the broken target this check exists to report.
            compact(&mut hashes, &mut nominated);
            // If compaction barely helped, the class really is holding too many *distinct*
            // keys, and splitting it halves them. The slack stops a class that sits exactly
            // at the boundary from re-sorting on every row.
            if hashes.len() + nominated.len() > cap - cap / 4 {
                return Ok(PassOutcome::Overflowed);
            }
        }
    }

    compact(&mut hashes, &mut nominated);
    Ok(PassOutcome::Candidates(nominated))
}

/// Move every hash seen at least twice out of `hashes` and into `nominated`.
///
/// Both come back sorted and distinct, and no hash is in both — a nomination is final, so
/// holding it in `hashes` as well would let one repeated key consume a slot per occurrence,
/// which is the unbounded growth this exists to prevent. Together they never exceed the
/// pass's ceiling, which is what makes the memory bound a fact about the loop rather than
/// about the data.
fn compact(hashes: &mut Vec<u64>, nominated: &mut Vec<u64>) {
    hashes.sort_unstable();
    for w in hashes.windows(2) {
        if w[0] == w[1] {
            nominated.push(w[0]);
        }
    }
    nominated.sort_unstable();
    nominated.dedup();
    hashes.dedup();
    // A hash already nominated needs no further evidence, so later copies of it are dropped
    // rather than accumulated. `nominated` is sorted, so this is a binary search per entry.
    hashes.retain(|h| nominated.binary_search(h).is_err());
}

/// Resolve nominated hashes against the real key values.
///
/// **This is what makes sixty-four bits exact.** A hash carrying two or more *distinct*
/// encodings is a collision and is discarded; a single encoding seen twice or more is a
/// duplicate.
async fn verify<H>(
    target: &DeltaTable,
    key_column: &str,
    candidates: &[u64],
    wanted: usize,
    hash: &H,
) -> Result<Vec<Duplicate>>
where
    H: Fn(&[u8]) -> u64 + Sync,
{
    let schema = declared_schema(target)?;
    let mut out: Vec<Duplicate> = Vec::new();

    for chunk in candidates.chunks(VERIFY_CHUNK) {
        let wanted_hashes: HashSet<u64> = chunk.iter().copied().collect();
        // The encoded key, how many rows carried it, and how it should be shown.
        let mut seen: HashMap<Box<[u8]>, (u64, String)> = HashMap::new();
        let mut stream = key_stream(target, key_column).await?;
        let mut converter: Option<RowConverter> = None;

        while let Some(batch) = stream.try_next().await.map_err(|e| {
            crate::spill::classify(e, "upsert: cannot scan the target to check its grain")
        })? {
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = crate::schema::read_as_declared(batch, &schema)?;
            let col = key_column_of(&batch, key_column)?;
            let conv = match &converter {
                Some(c) => c,
                None => {
                    converter = Some(
                        RowConverter::new(vec![SortField::new(col.data_type().clone())]).map_err(
                            |e| {
                                Error::Config(format!(
                                    "upsert key {key_column:?} cannot be compared: {e}"
                                ))
                            },
                        )?,
                    );
                    converter.as_ref().expect("just set")
                }
            };
            let encoded = conv
                .convert_columns(std::slice::from_ref(&col))
                .map_err(|e| {
                    Error::Other(format!("upsert: cannot encode the target's keys: {e}"))
                })?;
            // Rendered once per batch rather than per candidate row: the cast is vectorised
            // and the candidate rows are a handful out of millions, so the branch that skips
            // it would cost more in complexity than it saves.
            let shown = cast(&col, &DataType::Utf8).map_err(|e| {
                Error::Config(format!("upsert key {key_column:?} is not comparable: {e}"))
            })?;
            let shown = shown
                .as_any()
                .downcast_ref::<deltalake::arrow::array::StringArray>()
                .expect("cast to Utf8 yields a StringArray");

            for i in 0..batch.num_rows() {
                let row = encoded.row(i);
                if !wanted_hashes.contains(&hash(row.as_ref())) {
                    continue;
                }
                let entry = seen
                    .entry(row.as_ref().to_vec().into_boxed_slice())
                    .or_insert_with(|| {
                        (
                            0,
                            if shown.is_null(i) {
                                "NULL".to_string()
                            } else {
                                shown.value(i).to_string()
                            },
                        )
                    });
                entry.0 += 1;
            }
        }

        let mut duplicates: Vec<Duplicate> = seen
            .into_values()
            .filter(|(n, _)| *n > 1)
            .map(|(n, key)| Duplicate { key, rows: n })
            .collect();
        // Sorted so the examples an operator is shown do not depend on hash-map iteration
        // order — the same message twice is what makes a message quotable in a ticket.
        duplicates.sort();
        out.extend(duplicates);
        if out.len() >= wanted {
            break;
        }
    }
    Ok(out)
}

impl PartialOrd for Duplicate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Duplicate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key).then(self.rows.cmp(&other.rows))
    }
}

fn key_column_of(batch: &deltalake::arrow::array::RecordBatch, name: &str) -> Result<ArrayRef> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        Error::Config(format!(
            "upsert column {name:?} is not in the target table. Columns: [{}]",
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })?;
    Ok(batch.column(idx).clone())
}

/// SipHash-1-3 over the row encoding.
///
/// `std::hash::DefaultHasher` rather than a hash crate: `Cargo.toml` argues at length about
/// every dependency in the tree and one hash function does not survive the argument. It is
/// deterministic across runs, which matters only for reproducing a report.
pub fn hash64(bytes: &[u8]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

static CEILING: OnceLock<Ceiling> = OnceLock::new();

/// Install the process's grain-check ceiling. The first call wins; later ones are ignored.
pub fn install(c: Ceiling) -> bool {
    CEILING.set(c).is_ok()
}

/// The ceiling in force, or the default if none was installed.
pub fn ceiling() -> Ceiling {
    *CEILING.get_or_init(|| Ceiling {
        bytes: Ceiling::DEFAULT,
        source: CeilingSource::Default,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ceiling_of(bytes: u64) -> Ceiling {
        Ceiling::exactly(bytes)
    }

    #[test]
    fn splitting_a_class_in_two_keeps_every_equal_pair_together() {
        // The whole correctness argument: equal keys hash equally, so they are congruent
        // modulo anything and land in the same half however the space is divided.
        let c = Class {
            modulus: 6,
            residue: 4,
        };
        let (a, b) = c.split().expect("a small modulus splits");
        for h in 0u64..2_000 {
            if !c.holds(h) {
                assert!(!a.holds(h) && !b.holds(h), "{h} escaped the class");
                continue;
            }
            assert!(
                a.holds(h) ^ b.holds(h),
                "{h} must land in exactly one half of {c:?}"
            );
        }
    }

    #[test]
    fn a_class_and_its_two_halves_cover_exactly_the_same_keys() {
        for modulus in [1u64, 2, 3, 7, 35] {
            for residue in 0..modulus {
                let c = Class { modulus, residue };
                let (a, b) = c.split().expect("a small modulus splits");
                let whole: HashSet<u64> = (0..5_000).filter(|h| c.holds(*h)).collect();
                let halves: HashSet<u64> =
                    (0..5_000).filter(|h| a.holds(*h) || b.holds(*h)).collect();
                assert_eq!(whole, halves, "{c:?} split unevenly");
            }
        }
    }

    #[test]
    fn the_number_of_passes_falls_in_step_with_the_ceiling() {
        let rows = Some(2_000_000_000u64);
        let mut previous = u64::MAX;
        for mb in [256u64, 512, 1024, 4096] {
            let n = initial_modulus(rows, 0, ceiling_of(mb * 1_000_000));
            assert!(
                n < previous,
                "{mb}MB needed {n}, which is not fewer than {previous}"
            );
            previous = n;
        }
        // The published table: 512MB holds 56,000,000 hashes, so two billion rows plus one per
        // cent of slack is 37 classes.
        assert_eq!(initial_modulus(rows, 0, ceiling_of(Ceiling::DEFAULT)), 37);
    }

    #[test]
    fn a_target_that_fits_in_one_pass_is_read_once() {
        assert_eq!(
            initial_modulus(Some(6_000_000), 0, ceiling_of(Ceiling::DEFAULT)),
            1
        );
        assert_eq!(initial_modulus(Some(0), 0, ceiling_of(Ceiling::DEFAULT)), 1);
        assert_eq!(initial_modulus(None, 0, ceiling_of(Ceiling::DEFAULT)), 1);
    }

    #[test]
    fn a_target_whose_log_records_no_row_counts_sizes_itself_from_bytes_instead() {
        // 64 GB of files at an assumed 64 bytes per row is a billion rows, which does not fit
        // one 512 MiB pass — so an unknown row count must not silently mean "one class".
        let n = initial_modulus(None, 64 * 1024 * 1024 * 1024, ceiling_of(Ceiling::DEFAULT));
        assert!(
            n > 1,
            "an unknown row count still has to be bounded, got {n}"
        );
    }

    #[test]
    fn a_ceiling_smaller_than_the_floor_is_refused_rather_than_making_a_pass_of_zero() {
        let e = Ceiling::resolve(Some(1024)).unwrap_err().to_string();
        assert!(e.contains("runtime.max_grain_check_memory"), "{e}");
        assert!(e.contains("16MB"), "{e}");
        // The size the message prescribes must itself be accepted — a floor that rejects its
        // own advice sends the operator round a loop.
        assert!(Ceiling::resolve(Some(bytesize::ByteSize::mb(16).as_u64())).is_ok());
        assert!(Ceiling::resolve(Some(Ceiling::MIN)).is_ok());
    }

    #[test]
    fn an_unset_grain_ceiling_takes_the_default_rather_than_a_share_of_the_memory_budget() {
        // No budget is installed in this test binary, so `per_pipeline()` is None and the
        // default stands. The clamp is exercised by `tests/memory_budget.rs`, which installs
        // one.
        let c = Ceiling::resolve(None).unwrap();
        assert_eq!(c.bytes(), Ceiling::DEFAULT);
        assert_eq!(c.source(), CeilingSource::Default);
    }

    #[test]
    fn the_ceiling_is_a_count_of_hashes_and_leaves_room_for_the_scan() {
        let c = ceiling_of(Ceiling::DEFAULT);
        assert_eq!(c.hashes_per_pass(), 56_000_000);
        // Seven eighths, so the batch in flight and the candidate list are inside the number
        // an operator wrote down rather than beside it.
        assert!((c.hashes_per_pass() as u64) * 8 < c.bytes());
    }

    #[test]
    fn a_duplicate_is_nominated_however_the_key_space_is_divided() {
        // The property `check` relies on, exercised without a table: whatever the modulus,
        // two equal hashes are always in the same class, so exactly one pass nominates them.
        let hashes: Vec<u64> = vec![7, 11, 7, 19, 23];
        for modulus in 1u64..=8 {
            let nominated: usize = (0..modulus)
                .filter(|residue| {
                    let c = Class {
                        modulus,
                        residue: *residue,
                    };
                    let mut kept: Vec<u64> =
                        hashes.iter().copied().filter(|h| c.holds(*h)).collect();
                    kept.sort_unstable();
                    kept.windows(2).any(|w| w[0] == w[1])
                })
                .count();
            assert_eq!(
                nominated, 1,
                "modulus {modulus} nominated {nominated} classes"
            );
        }
    }

    #[test]
    fn the_hash_is_stable_within_a_run() {
        // Only so a report can be reproduced; nothing depends on it across versions.
        assert_eq!(hash64(b"order-1"), hash64(b"order-1"));
        assert_ne!(hash64(b"order-1"), hash64(b"order-2"));
    }
}
