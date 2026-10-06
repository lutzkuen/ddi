//! What a member knows about each partition it owns, and the batch it is filling
//! (docs/kafka-design.md §3.3).
//!
//! A partition is **Ready**, with the next offset it expects, or **Blocked** on something an
//! operator has to settle; one it does not own has no state at all. Every message librdkafka
//! hands over is first admitted here ([`Partitions::admit`]): a message of a partition that is
//! not Ready, or below the offset expected, is dropped and counted; one that skips offsets is
//! taken, and the gap counted (compaction, transaction markers). Only an admitted message
//! reaches the [`Buffer`], so the buffer's offsets per partition are a contiguous run of what
//! the topic holds from the partition's base on.
//!
//! # The re-read
//!
//! A partition resumed at its stored offset `b` is assigned at `b`, not `b + 1`: librdkafka
//! re-reads `b`, and ddi compares it with the mark its own commit recorded for `b` (timestamp
//! and value length). A different message there means the topic was recreated and refilled
//! past `b` (I19), and the partition blocks; the same one is dropped and reading goes on at
//! `b + 1`, as kafka-delta-ingest's own seek-and-drop did. Without a mark the partition is
//! Unverifiable: `b` is re-read and dropped unchecked.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::error::Error;
use crate::sink::MessageMark;

/// What a partition's resume checks on the message at its stored offset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reread {
    /// The message ddi recorded there: it must have this timestamp and length.
    Expect(MessageMark),
    /// No mark was recorded for this offset (kafka-delta-ingest's, or a baseline's): the
    /// message there is dropped unchecked.
    Unverifiable(i64),
}

impl Reread {
    fn offset(self) -> i64 {
        match self {
            Self::Expect(m) => m.offset,
            Self::Unverifiable(o) => o,
        }
    }
}

/// Why a partition is paused until an operator acts (§3.6, one partition).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Block {
    /// Retention removed offsets this member never read: `KafkaOffsetsUnavailable`.
    Unavailable { wanted: i64, low: i64 },
    /// The stored offset is past the end of the topic: `KafkaOffsetsRegressed`.
    Regressed { stored: i64, high: i64 },
    /// The message at the stored offset is another one: `KafkaTopicReplaced`.
    Replaced {
        recorded: MessageMark,
        found: MessageMark,
    },
    /// Assigned while the pipeline could not read the target's offsets (a blocked or failing
    /// target): its start is resolved by the recheck that can.
    Unresolved,
}

impl Block {
    /// The error this block is, for its log line and its gauge.
    pub(crate) fn error(&self, topic: &str, partition: i32) -> Error {
        let topic = topic.to_string();
        match *self {
            Self::Unavailable { wanted, low } => Error::KafkaOffsetsUnavailable {
                topic,
                partition,
                wanted,
                low,
            },
            Self::Regressed { stored, high } => Error::KafkaOffsetsRegressed {
                topic,
                partition,
                stored,
                high,
            },
            Self::Replaced { recorded, found } => Error::KafkaTopicReplaced {
                topic,
                partition,
                offset: recorded.offset,
                recorded: describe(&recorded),
                found: describe(&found),
            },
            Self::Unresolved => Error::Other(format!(
                "partition {partition} of topic {topic:?} was assigned while the target's \
                 offsets could not be read; it starts once they can be"
            )),
        }
    }
}

fn describe(m: &MessageMark) -> String {
    format!(
        "timestamp {} ms, {} value bytes",
        m.timestamp_ms, m.value_len
    )
}

/// One owned partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum State {
    Ready {
        /// The offset the next message should have.
        next_expected: i64,
        /// Where the partition was assigned or last sought: once-mode's start.
        start: i64,
        /// The stored offset, still to be re-read and checked.
        reread: Option<Reread>,
        /// When the last message of this partition arrived, or the member last went back to
        /// polling it: after a commit, and when it resumed after a pause. Once-mode's idle
        /// rule counts from here, so that time spent committing, holding a batch or waiting
        /// is never idle.
        last_message: Instant,
    },
    Blocked {
        block: Block,
        since: Instant,
    },
}

/// Why a message was dropped rather than taken (`ddi_kafka_messages_dropped_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Dropped {
    /// Its partition is not this member's: a message that was in flight across a revoke.
    NotOwned,
    /// Its partition is blocked, or paused while a batch is held.
    Blocked,
    /// Below the offset expected: read before.
    Duplicate,
    /// The stored offset, re-read to check it.
    Reread,
}

/// What [`Partitions::admit`] decided about one message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Admit {
    /// Into the buffer. `gap` offsets were skipped before it.
    Take {
        gap: u64,
    },
    Drop(Dropped),
    /// The partition blocks on it: the re-read found another message.
    Block(Block),
}

/// Every partition this member owns.
#[derive(Debug, Default)]
pub(crate) struct Partitions {
    map: BTreeMap<i32, State>,
}

impl Partitions {
    pub(crate) fn get(&self, p: i32) -> Option<&State> {
        self.map.get(&p)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&i32, &State)> {
        self.map.iter()
    }

    pub(crate) fn owned(&self) -> Vec<i32> {
        self.map.keys().copied().collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
    }

    pub(crate) fn blocked(&self) -> impl Iterator<Item = (i32, &Block)> {
        self.map.iter().filter_map(|(p, s)| match s {
            State::Blocked { block, .. } => Some((*p, block)),
            State::Ready { .. } => None,
        })
    }

    pub(crate) fn ready(&self) -> impl Iterator<Item = (i32, i64)> + '_ {
        self.map.iter().filter_map(|(p, s)| match s {
            State::Ready { next_expected, .. } => Some((*p, *next_expected)),
            State::Blocked { .. } => None,
        })
    }

    /// Partition `p` reads from `offset` on, re-reading it first when `reread` says so.
    pub(crate) fn ready_at(&mut self, p: i32, offset: i64, reread: Option<Reread>, now: Instant) {
        // A re-read is of the offset assigned, and the one expected after it is the next.
        self.map.insert(
            p,
            State::Ready {
                next_expected: offset,
                start: offset,
                reread,
                last_message: now,
            },
        );
    }

    /// Partition `p` waits for an operator, for `block`. One already blocked keeps the time it
    /// blocked at, whatever its reason is now: a recheck that finds it still blocked does not
    /// make it newly so (once-mode's idle rule counts from there).
    pub(crate) fn block(&mut self, p: i32, block: Block, now: Instant) {
        let since = match self.map.get(&p) {
            Some(State::Blocked { since, .. }) => *since,
            _ => now,
        };
        self.map.insert(p, State::Blocked { block, since });
    }

    /// The member goes back to polling every Ready partition, or partition `p` only: idle
    /// counts from `now` (§3.8).
    pub(crate) fn polled_again(&mut self, p: Option<i32>, now: Instant) {
        for (q, state) in &mut self.map {
            if let State::Ready { last_message, .. } = state {
                if p.is_none_or(|p| p == *q) {
                    *last_message = now;
                }
            }
        }
    }

    /// Decide what to do with the message at `mark.offset` of partition `p` (§3.3), and move
    /// the partition's expectations on when it is taken or re-read.
    pub(crate) fn admit(&mut self, p: i32, mark: MessageMark, now: Instant) -> Admit {
        let offset = mark.offset;
        let Some(state) = self.map.get_mut(&p) else {
            return Admit::Drop(Dropped::NotOwned);
        };
        let State::Ready {
            next_expected,
            reread,
            last_message,
            ..
        } = state
        else {
            return Admit::Drop(Dropped::Blocked);
        };
        *last_message = now;
        if let Some(expected) = *reread {
            let at = expected.offset();
            if offset == at {
                if let Reread::Expect(recorded) = expected {
                    if (recorded.timestamp_ms, recorded.value_len)
                        != (mark.timestamp_ms, mark.value_len)
                    {
                        return Admit::Block(Block::Replaced {
                            recorded,
                            found: mark,
                        });
                    }
                }
                *reread = None;
                *next_expected = offset + 1;
                return Admit::Drop(Dropped::Reread);
            }
            if offset < at {
                return Admit::Drop(Dropped::Duplicate);
            }
            // The stored offset is not there to re-read (compacted, or a transaction marker):
            // nothing to verify it against.
            *reread = None;
            *next_expected = at + 1;
        }
        if offset < *next_expected {
            return Admit::Drop(Dropped::Duplicate);
        }
        let gap = (offset - *next_expected) as u64;
        *next_expected = offset + 1;
        Admit::Take { gap }
    }

    /// Whether partition `p` has read everything up to `end`, or nothing has arrived for it for
    /// `idle` while it was polled (§3.8): once-mode's done rule.
    pub(crate) fn done(&self, p: i32, end: i64, idle: Duration, now: Instant) -> bool {
        match self.map.get(&p) {
            None => true,
            Some(State::Ready {
                next_expected,
                start,
                reread,
                last_message,
            }) => {
                let next = match reread {
                    // The stored offset is re-read before anything after it.
                    Some(r) => r.offset() + 1,
                    None => *next_expected,
                };
                next.max(*start) >= end || now.duration_since(*last_message) >= idle
            }
            Some(State::Blocked { since, .. }) => now.duration_since(*since) >= idle,
        }
    }
}

/// The batch being filled: what it read of each partition, and how big it has grown.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Buffer {
    /// When its first message arrived: the latency timer starts there, not at the last
    /// commit (kafka-delta-ingest #233).
    pub opened_at: Option<Instant>,
    /// Per partition, the first and the last offset taken.
    pub ranges: BTreeMap<i32, (i64, i64)>,
    /// Per partition, the message at the last offset taken.
    pub marks: BTreeMap<i32, MessageMark>,
    /// Messages taken, tombstones and undecodable ones included.
    pub messages: u64,
    /// Their key and value bytes.
    pub raw_bytes: u64,
}

/// When a batch closes (§3.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
    pub latency: Duration,
    pub max_messages: Option<u64>,
    pub max_bytes: u64,
    /// The batch's share of the memory budget, measured on decoded rows; `None` unbounded.
    pub max_decoded: Option<u64>,
}

impl Buffer {
    pub(crate) fn is_empty(&self) -> bool {
        self.messages == 0
    }

    /// Account for a message taken into the batch.
    pub(crate) fn take(&mut self, p: i32, mark: MessageMark, raw_bytes: u64, now: Instant) {
        self.opened_at.get_or_insert(now);
        self.ranges
            .entry(p)
            .and_modify(|(_, last)| *last = mark.offset)
            .or_insert((mark.offset, mark.offset));
        self.marks.insert(p, mark);
        self.messages += 1;
        self.raw_bytes += raw_bytes;
    }

    /// When the latency limit closes the batch, if it has a message.
    pub(crate) fn deadline(&self, latency: Duration) -> Option<Instant> {
        self.opened_at.map(|t| t + latency)
    }

    /// Whether the batch closes now: it holds a message, and one of the limits is reached.
    pub(crate) fn closes(&self, limits: &Limits, decoded_bytes: u64, now: Instant) -> bool {
        if self.is_empty() {
            return false;
        }
        self.deadline(limits.latency).is_some_and(|d| now >= d)
            || limits.max_messages.is_some_and(|m| self.messages >= m)
            || self.raw_bytes >= limits.max_bytes
            || limits.max_decoded.is_some_and(|m| decoded_bytes >= m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mark(offset: i64) -> MessageMark {
        MessageMark {
            offset,
            timestamp_ms: 1_700_000_000_000 + offset,
            value_len: 10,
        }
    }

    fn ready(p: i32, at: i64, reread: Option<Reread>) -> (Partitions, Instant) {
        let now = Instant::now();
        let mut parts = Partitions::default();
        parts.ready_at(p, at, reread, now);
        (parts, now)
    }

    #[test]
    fn not_owned_dropped() {
        let (mut parts, now) = ready(0, 5, None);
        assert_eq!(parts.admit(1, mark(5), now), Admit::Drop(Dropped::NotOwned));
    }

    #[test]
    fn blocked_dropped() {
        let (mut parts, now) = ready(0, 5, None);
        parts.block(0, Block::Unavailable { wanted: 5, low: 9 }, now);
        assert_eq!(parts.admit(0, mark(9), now), Admit::Drop(Dropped::Blocked));
    }

    #[test]
    fn duplicate_below_next_expected_dropped() {
        let (mut parts, now) = ready(0, 5, None);
        assert_eq!(parts.admit(0, mark(5), now), Admit::Take { gap: 0 });
        assert_eq!(parts.admit(0, mark(6), now), Admit::Take { gap: 0 });
        assert_eq!(
            parts.admit(0, mark(5), now),
            Admit::Drop(Dropped::Duplicate)
        );
        assert_eq!(
            parts.admit(0, mark(6), now),
            Admit::Drop(Dropped::Duplicate)
        );
        assert_eq!(parts.admit(0, mark(7), now), Admit::Take { gap: 0 });
    }

    /// The stored offset is re-read, checked against its mark and dropped; the next message
    /// is the first taken.
    #[test]
    fn reread_verified_and_dropped() {
        let (mut parts, now) = ready(0, 41, Some(Reread::Expect(mark(41))));
        assert_eq!(parts.admit(0, mark(41), now), Admit::Drop(Dropped::Reread));
        assert_eq!(parts.admit(0, mark(42), now), Admit::Take { gap: 0 });

        // Without a mark it is re-read and dropped all the same, unchecked.
        let (mut parts, now) = ready(0, 41, Some(Reread::Unverifiable(41)));
        let other = MessageMark {
            value_len: 999,
            ..mark(41)
        };
        assert_eq!(parts.admit(0, other, now), Admit::Drop(Dropped::Reread));
        assert_eq!(parts.admit(0, mark(42), now), Admit::Take { gap: 0 });
    }

    /// Another message at the stored offset (a recreated topic refilled past it) blocks the
    /// partition (I19), whether its timestamp or its length differs.
    #[test]
    fn reread_mismatch_blocks() {
        for found in [
            MessageMark {
                timestamp_ms: 1,
                ..mark(41)
            },
            MessageMark {
                value_len: 11,
                ..mark(41)
            },
        ] {
            let (mut parts, now) = ready(0, 41, Some(Reread::Expect(mark(41))));
            assert_eq!(
                parts.admit(0, found, now),
                Admit::Block(Block::Replaced {
                    recorded: mark(41),
                    found
                })
            );
        }
    }

    /// The stored offset is not there to re-read (compacted away, or a transaction marker):
    /// the first message after it is taken, without verification and without a gap.
    #[test]
    fn reread_compacted_skips_verification() {
        let (mut parts, now) = ready(0, 41, Some(Reread::Expect(mark(41))));
        assert_eq!(parts.admit(0, mark(42), now), Admit::Take { gap: 0 });
        let (mut parts, now) = ready(0, 41, Some(Reread::Expect(mark(41))));
        assert_eq!(parts.admit(0, mark(45), now), Admit::Take { gap: 3 });
    }

    #[test]
    fn gap_counted() {
        let (mut parts, now) = ready(0, 0, None);
        assert_eq!(parts.admit(0, mark(0), now), Admit::Take { gap: 0 });
        assert_eq!(parts.admit(0, mark(4), now), Admit::Take { gap: 3 });
        assert_eq!(parts.admit(0, mark(5), now), Admit::Take { gap: 0 });
    }

    fn limits() -> Limits {
        Limits {
            latency: Duration::from_secs(60),
            max_messages: Some(3),
            max_bytes: 1000,
            max_decoded: Some(5000),
        }
    }

    /// A batch closes on whichever limit it reaches first: the latency since its first
    /// message, its message count, its raw bytes, or its measured decoded bytes. Never empty.
    #[test]
    fn batch_close_by_latency_messages_bytes_decoded() {
        let now = Instant::now();
        let l = limits();
        let empty = Buffer::default();
        assert!(!empty.closes(&l, u64::MAX, now + Duration::from_secs(3600)));

        let mut b = Buffer::default();
        b.take(0, mark(1), 10, now);
        assert!(!b.closes(&l, 100, now));
        assert!(b.closes(&l, 100, now + Duration::from_secs(60)), "latency");
        assert!(b.closes(&l, 5000, now), "decoded bytes");

        let mut b = Buffer::default();
        for o in 0..3 {
            b.take(0, mark(o), 10, now);
        }
        assert!(b.closes(&l, 0, now), "message count");
        assert_eq!(b.ranges[&0], (0, 2));
        assert_eq!(b.marks[&0], mark(2));

        let mut b = Buffer::default();
        b.take(1, mark(7), 1000, now);
        assert!(b.closes(&l, 0, now), "raw bytes");

        let unbounded = Limits {
            max_messages: None,
            max_decoded: None,
            ..l
        };
        let mut b = Buffer::default();
        for o in 0..10 {
            b.take(0, mark(o), 1, now);
        }
        assert!(!b.closes(&unbounded, u64::MAX, now));
    }

    /// The latency timer starts at the first message after a commit, not at the commit: a
    /// buffer opened long after the last commit still waits a full latency (kdi #233).
    #[test]
    fn latency_timer_starts_after_commit() {
        let committed = Instant::now();
        let l = limits();
        let first = committed + Duration::from_secs(600);
        let mut b = Buffer::default();
        b.take(0, mark(1), 1, first);
        assert!(!b.closes(&l, 0, first + Duration::from_secs(59)));
        assert!(b.closes(&l, 0, first + Duration::from_secs(60)));
        b.take(0, mark(2), 1, first + Duration::from_secs(30));
        assert_eq!(b.deadline(l.latency), Some(first + Duration::from_secs(60)));
    }

    /// Once-mode: a partition is done at its end offset, or when nothing came for it for the
    /// idle time; a blocked one only by the idle rule.
    #[test]
    fn once_done_rule() {
        let idle = Duration::from_secs(10);
        let (mut parts, now) = ready(0, 5, None);
        assert!(!parts.done(0, 7, idle, now));
        assert!(parts.done(0, 5, idle, now), "started at its end");
        parts.admit(0, mark(5), now);
        parts.admit(0, mark(6), now);
        assert!(parts.done(0, 7, idle, now));
        assert!(parts.done(0, 9, idle, now + idle), "idle");

        let (parts, now) = ready(0, 3, Some(Reread::Unverifiable(3)));
        assert!(
            parts.done(0, 4, idle, now),
            "only the stored offset was left"
        );
        assert!(!parts.done(0, 5, idle, now));

        // A blocked partition is done once it has been blocked for the idle time, however
        // often a recheck finds it still blocked.
        // Idle counts only from when the member went back to polling: a commit or a pause as
        // long as the idle time is not idle.
        let (mut parts, now) = ready(0, 3, None);
        parts.polled_again(None, now + idle);
        assert!(!parts.done(0, 5, idle, now + idle + idle / 2));
        assert!(parts.done(0, 5, idle, now + idle * 2));
        parts.polled_again(Some(1), now + idle * 2);
        assert!(
            parts.done(0, 5, idle, now + idle * 2),
            "another partition's resume"
        );
        parts.polled_again(Some(0), now + idle * 2);
        assert!(!parts.done(0, 5, idle, now + idle * 2));

        let (mut parts, now) = ready(0, 3, None);
        let block = Block::Regressed { stored: 9, high: 5 };
        parts.block(0, block.clone(), now);
        parts.block(0, block, now + idle / 2);
        assert!(parts.done(0, 5, idle, now + idle));
    }
}
