//! Redis streams: the data structure and every `X*` command, ported from
//! `t_stream.c` (Redis 7.2).
//!
//! A stream is held whole in one value per key — serialised with [`Stream::
//! encode`] — and loaded, changed and stored back by each command through a
//! [`StreamStore`], which each storage half implements. The model keeps
//! Redis's own node layout (which entries share a listpack, when a new one
//! starts), because two visible behaviours depend on it: `XINFO`'s
//! `radix-tree-keys`, and approximate trimming, which only ever drops whole
//! nodes.
#![forbid(unsafe_code)]

use crate::commands::{Response, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    fn is_zero(&self) -> bool {
        *self == Self::ZERO
    }

    /// Redis's `streamIncrID`; `None` past the last possible ID.
    fn incr(self) -> Option<StreamId> {
        if self.seq == u64::MAX {
            if self.ms == u64::MAX {
                None
            } else {
                Some(StreamId {
                    ms: self.ms + 1,
                    seq: 0,
                })
            }
        } else {
            Some(StreamId {
                ms: self.ms,
                seq: self.seq + 1,
            })
        }
    }

    /// Redis's `streamDecrID`; `None` before 0-0.
    fn decr(self) -> Option<StreamId> {
        if self.seq == 0 {
            if self.ms == 0 {
                None
            } else {
                Some(StreamId {
                    ms: self.ms - 1,
                    seq: u64::MAX,
                })
            }
        } else {
            Some(StreamId {
                ms: self.ms,
                seq: self.seq - 1,
            })
        }
    }

    pub fn render(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    fn value(&self) -> Value {
        Value::Bulk(self.render().into_bytes())
    }
}

type Fields = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, Clone)]
struct Entry {
    id: StreamId,
    fields: Fields,
    deleted: bool,
}

/// One radix-tree node: a listpack of entries sharing a master entry.
#[derive(Debug, Clone)]
struct Node {
    master_fields: Vec<Vec<u8>>,
    entries: Vec<Entry>,
    /// The listpack's size in bytes, as Redis would have encoded it.
    bytes: usize,
}

impl Node {
    fn live(&self) -> usize {
        self.entries.iter().filter(|e| !e.deleted).count()
    }

    /// Account for the master entry's live and deleted counts having moved
    /// from `before`: Redis rewrites both in place (`lpReplaceInteger`), in
    /// whatever width the new values need.
    fn recount(&mut self, before: (usize, usize)) {
        let live = self.live();
        let deleted = self.entries.len() - live;
        self.bytes = self.bytes + lp_int(live as i64) + lp_int(deleted as i64)
            - lp_int(before.0 as i64)
            - lp_int(before.1 as i64);
    }

    fn counts(&self) -> (usize, usize) {
        let live = self.live();
        (live, self.entries.len() - live)
    }
}

#[derive(Debug, Clone)]
pub struct Nack {
    consumer: Vec<u8>,
    delivery_time: i64,
    delivery_count: u64,
}

#[derive(Debug, Clone)]
pub struct Consumer {
    seen_time: i64,
    active_time: i64,
    pel: std::collections::BTreeSet<StreamId>,
}

#[derive(Debug, Clone)]
pub struct Group {
    last_id: StreamId,
    /// -1 when the read counter is unknown, as Redis's SCG_INVALID_ENTRIES_READ.
    entries_read: i64,
    pel: BTreeMap<StreamId, Nack>,
    consumers: BTreeMap<Vec<u8>, Consumer>,
}

#[derive(Debug, Clone, Default)]
pub struct Stream {
    nodes: Vec<Node>,
    length: u64,
    last_id: StreamId,
    first_id: StreamId,
    max_deleted_id: StreamId,
    entries_added: u64,
    groups: BTreeMap<Vec<u8>, Group>,
}

fn now_ms() -> i64 {
    crate::server::now_ms()
}

// ───────────────────────── listpack size accounting ─────────────────────────

fn backlen(l: usize) -> usize {
    match l {
        0..=127 => 1,
        128..=16382 => 2,
        16383..=2097150 => 3,
        2097151..=268435454 => 4,
        _ => 5,
    }
}

/// The bytes one element takes in a listpack: integers in the narrowest
/// encoding that fits, strings with a length prefix, both with a backlen.
fn lp_elem(s: &[u8]) -> usize {
    let as_int = std::str::from_utf8(s)
        .ok()
        .filter(|t| {
            !t.is_empty()
                && t.len() <= 20
                && (t == &"0" || !t.starts_with('0'))
                && !t.starts_with("-0")
        })
        .and_then(|t| t.parse::<i64>().ok());
    let enc = match as_int {
        Some(v) if (0..=127).contains(&v) => 1,
        Some(v) if (-4096..=4095).contains(&v) => 2,
        Some(v) if (i16::MIN as i64..=i16::MAX as i64).contains(&v) => 3,
        Some(v) if (-(1 << 23)..(1 << 23)).contains(&v) => 4,
        Some(v) if (i32::MIN as i64..=i32::MAX as i64).contains(&v) => 5,
        Some(_) => 9,
        None => match s.len() {
            0..=63 => 1 + s.len(),
            64..=4095 => 2 + s.len(),
            _ => 5 + s.len(),
        },
    };
    enc + backlen(enc)
}

fn lp_int(v: i64) -> usize {
    lp_elem(v.to_string().as_bytes())
}

/// A listpack's size once `entry` is appended to it.
fn entry_bytes(node: &Node, master: StreamId, id: StreamId, fields: &Fields, same: bool) -> usize {
    let mut b = lp_int(if same { 2 } else { 0 }) // flags
        + lp_int((id.ms - master.ms) as i64)
        + lp_int(id.seq.wrapping_sub(master.seq) as i64);
    if same {
        b += fields.iter().map(|(_, v)| lp_elem(v)).sum::<usize>();
        b += lp_int(fields.len() as i64 + 3);
    } else {
        b += lp_int(fields.len() as i64);
        b += fields
            .iter()
            .map(|(f, v)| lp_elem(f) + lp_elem(v))
            .sum::<usize>();
        b += lp_int(fields.len() as i64 * 2 + 4);
    }
    let _ = node;
    b
}

// ────────────────────────────── the structure ───────────────────────────────

impl Stream {
    fn iter(&self) -> impl DoubleEndedIterator<Item = &Entry> {
        self.nodes
            .iter()
            .flat_map(|n| n.entries.iter())
            .filter(|e| !e.deleted)
    }

    fn range(&self, start: StreamId, end: StreamId, rev: bool, count: usize) -> Vec<&Entry> {
        let it = self.iter().filter(move |e| e.id >= start && e.id <= end);
        let v: Vec<&Entry> = if rev {
            it.rev().collect()
        } else {
            it.collect()
        };
        if count > 0 {
            v.into_iter().take(count).collect()
        } else {
            v
        }
    }

    fn find(&self, id: StreamId) -> Option<&Entry> {
        self.iter().find(|e| e.id == id)
    }

    fn exists(&self, id: StreamId) -> bool {
        self.find(id).is_some()
    }

    /// The last entry not deleted, or 0-0.
    fn last_valid_id(&self) -> StreamId {
        self.iter().next_back().map_or(StreamId::ZERO, |e| e.id)
    }

    fn first_valid_id(&self) -> StreamId {
        self.iter().next().map_or(StreamId::ZERO, |e| e.id)
    }

    /// Redis's `streamAppendItem`.
    fn append(
        &mut self,
        fields: Fields,
        use_id: Option<(StreamId, bool)>,
        cfg: &Cfg,
    ) -> Result<StreamId, &'static str> {
        const EQUAL_OR_SMALLER: &str =
            "ERR The ID specified in XADD is equal or smaller than the target stream top item";
        let id = match use_id {
            Some((id, true)) => id,
            Some((id, false)) => {
                if self.last_id.ms == id.ms {
                    if self.last_id.seq == u64::MAX {
                        return Err(EQUAL_OR_SMALLER);
                    }
                    StreamId {
                        ms: id.ms,
                        seq: self.last_id.seq + 1,
                    }
                } else {
                    id
                }
            }
            None => {
                let ms = now_ms() as u64;
                if ms > self.last_id.ms {
                    StreamId { ms, seq: 0 }
                } else {
                    self.last_id.incr().ok_or(EQUAL_OR_SMALLER)?
                }
            }
        };
        if id <= self.last_id {
            return Err(EQUAL_OR_SMALLER);
        }
        let totelelen: usize = fields.iter().map(|(f, v)| f.len() + v.len()).sum();
        if totelelen > u32::MAX as usize {
            return Err("ERR Elements are too large to be stored");
        }
        let max_bytes = if cfg.node_max_bytes == 0 {
            usize::MAX
        } else {
            cfg.node_max_bytes
        };
        let new_node = match self.nodes.last() {
            None => true,
            Some(n) => {
                n.bytes + totelelen >= max_bytes
                    || (cfg.node_max_entries > 0 && n.entries.len() >= cfg.node_max_entries)
            }
        };
        if new_node {
            let master_fields: Vec<Vec<u8>> = fields.iter().map(|(f, _)| f.clone()).collect();
            // header + terminator + master entry: count, deleted, num-fields,
            // the fields, and its zero terminator. The count is accounted at
            // zero here and moves to one with the entry below.
            let bytes = 7
                + lp_int(0)
                + lp_int(0)
                + lp_int(master_fields.len() as i64)
                + master_fields.iter().map(|f| lp_elem(f)).sum::<usize>()
                + lp_int(0);
            self.nodes.push(Node {
                master_fields,
                entries: vec![],
                bytes,
            });
        }
        let node = self.nodes.last_mut().expect("just ensured");
        let master = node.entries.first().map_or(id, |e| e.id);
        let same = node.master_fields.len() == fields.len()
            && node
                .master_fields
                .iter()
                .zip(&fields)
                .all(|(m, (f, _))| m == f);
        node.bytes += entry_bytes(node, master, id, &fields, same);
        let before = node.counts();
        node.entries.push(Entry {
            id,
            fields,
            deleted: false,
        });
        node.recount(before);
        self.length += 1;
        self.entries_added += 1;
        self.last_id = id;
        if self.length == 1 {
            self.first_id = id;
        }
        Ok(id)
    }

    /// Redis's `streamTrim`.
    fn trim(&mut self, t: &Trim) -> u64 {
        let mut deleted: u64 = 0;
        let i = 0;
        while i < self.nodes.len() {
            if let TrimBy::MaxLen(max) = t.by
                && self.length <= max
            {
                break;
            }
            let entries = self.nodes[i].live() as u64;
            if t.limit > 0 && deleted + entries > t.limit as u64 {
                break;
            }
            let remove_node = match t.by {
                TrimBy::MaxLen(max) => self.length - entries >= max,
                TrimBy::MinId(min) => {
                    let last = self.nodes[i]
                        .entries
                        .last()
                        .map_or(StreamId::ZERO, |e| e.id);
                    last < min
                }
            };
            if remove_node {
                self.nodes.remove(i);
                self.length -= entries;
                deleted += entries;
                continue;
            }
            if t.approx {
                break;
            }
            let mut from_lp = 0;
            let before = self.nodes[i].counts();
            for e in self.nodes[i].entries.iter_mut() {
                let stop = match t.by {
                    TrimBy::MaxLen(max) => self.length <= max,
                    TrimBy::MinId(min) => e.id >= min,
                };
                if stop {
                    break;
                }
                if !e.deleted {
                    e.deleted = true;
                    from_lp += 1;
                    self.length -= 1;
                }
            }
            self.nodes[i].recount(before);
            deleted += from_lp;
            break;
        }
        if self.length == 0 {
            self.first_id = StreamId::ZERO;
        } else if deleted > 0 {
            self.first_id = self.first_valid_id();
        }
        deleted
    }

    /// Redis's `streamDeleteItem`: mark the entry deleted, and drop its node
    /// once nothing in it is left.
    fn delete(&mut self, id: StreamId) -> bool {
        for i in 0..self.nodes.len() {
            if let Some(e) = self.nodes[i]
                .entries
                .iter_mut()
                .find(|e| e.id == id && !e.deleted)
            {
                e.deleted = true;
                self.length -= 1;
                let live = self.nodes[i].live();
                if live == 0 {
                    self.nodes.remove(i);
                } else {
                    let total = self.nodes[i].entries.len();
                    self.nodes[i].recount((live + 1, total - live - 1));
                }
                return true;
            }
        }
        false
    }

    fn range_has_tombstones(&self, start: Option<StreamId>, end: Option<StreamId>) -> bool {
        if self.length == 0 || self.max_deleted_id.is_zero() {
            return false;
        }
        if self.first_id > self.max_deleted_id {
            return false;
        }
        let s = start.unwrap_or(StreamId::ZERO);
        let e = end.unwrap_or(StreamId::MAX);
        s <= self.max_deleted_id && self.max_deleted_id <= e
    }

    /// Redis's `streamEstimateDistanceFromFirstEverEntry`; -1 when unknown.
    fn estimate_distance(&self, id: StreamId) -> i64 {
        if self.entries_added == 0 {
            return 0;
        }
        if self.length == 0 && id <= self.last_id {
            return self.entries_added as i64;
        }
        match id.cmp(&self.last_id) {
            std::cmp::Ordering::Equal => return self.entries_added as i64,
            std::cmp::Ordering::Greater => return -1,
            _ => {}
        }
        if self.max_deleted_id.is_zero() || self.max_deleted_id < self.first_id {
            match id.cmp(&self.first_id) {
                std::cmp::Ordering::Less => return (self.entries_added - self.length) as i64,
                std::cmp::Ordering::Equal => return (self.entries_added - self.length + 1) as i64,
                _ => {}
            }
        }
        -1
    }

    fn lag(&self, g: &Group) -> Value {
        if self.entries_added == 0 {
            return Value::Int(0);
        }
        if g.entries_read != -1 && !self.range_has_tombstones(Some(g.last_id), None) {
            return Value::Int(self.entries_added as i64 - g.entries_read);
        }
        let read = self.estimate_distance(g.last_id);
        if read != -1 {
            Value::Int(self.entries_added as i64 - read)
        } else {
            Value::Nil
        }
    }

    // ───────────────────────────── serialisation ────────────────────────────

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::default();
        w.bytes(b"PGRS1");
        for v in [
            self.length,
            self.last_id.ms,
            self.last_id.seq,
            self.first_id.ms,
            self.first_id.seq,
            self.max_deleted_id.ms,
            self.max_deleted_id.seq,
            self.entries_added,
        ] {
            w.u64(v);
        }
        w.u64(self.nodes.len() as u64);
        for n in &self.nodes {
            w.u64(n.bytes as u64);
            w.u64(n.master_fields.len() as u64);
            for f in &n.master_fields {
                w.blob(f);
            }
            w.u64(n.entries.len() as u64);
            for e in &n.entries {
                w.u64(e.id.ms);
                w.u64(e.id.seq);
                w.u8(e.deleted as u8);
                w.u64(e.fields.len() as u64);
                for (f, v) in &e.fields {
                    w.blob(f);
                    w.blob(v);
                }
            }
        }
        w.u64(self.groups.len() as u64);
        for (name, g) in &self.groups {
            w.blob(name);
            w.u64(g.last_id.ms);
            w.u64(g.last_id.seq);
            w.u64(g.entries_read as u64);
            w.u64(g.pel.len() as u64);
            for (id, n) in &g.pel {
                w.u64(id.ms);
                w.u64(id.seq);
                w.blob(&n.consumer);
                w.u64(n.delivery_time as u64);
                w.u64(n.delivery_count);
            }
            w.u64(g.consumers.len() as u64);
            for (cname, c) in &g.consumers {
                w.blob(cname);
                w.u64(c.seen_time as u64);
                w.u64(c.active_time as u64);
            }
        }
        w.0
    }

    pub fn decode(b: &[u8]) -> Option<Stream> {
        let mut r = Reader { b, at: 0 };
        if r.take(5)? != b"PGRS1" {
            return None;
        }
        let mut s = Stream {
            length: r.u64()?,
            last_id: StreamId {
                ms: r.u64()?,
                seq: r.u64()?,
            },
            first_id: StreamId {
                ms: r.u64()?,
                seq: r.u64()?,
            },
            max_deleted_id: StreamId {
                ms: r.u64()?,
                seq: r.u64()?,
            },
            entries_added: r.u64()?,
            ..Default::default()
        };
        for _ in 0..r.u64()? {
            let bytes = r.u64()? as usize;
            let mf = (0..r.u64()?)
                .map(|_| r.blob())
                .collect::<Option<Vec<_>>>()?;
            let mut entries = Vec::new();
            for _ in 0..r.u64()? {
                let id = StreamId {
                    ms: r.u64()?,
                    seq: r.u64()?,
                };
                let deleted = r.u8()? != 0;
                let fields = (0..r.u64()?)
                    .map(|_| Some((r.blob()?, r.blob()?)))
                    .collect::<Option<Vec<_>>>()?;
                entries.push(Entry {
                    id,
                    fields,
                    deleted,
                });
            }
            s.nodes.push(Node {
                master_fields: mf,
                entries,
                bytes,
            });
        }
        for _ in 0..r.u64()? {
            let name = r.blob()?;
            let last_id = StreamId {
                ms: r.u64()?,
                seq: r.u64()?,
            };
            let entries_read = r.u64()? as i64;
            let mut pel = BTreeMap::new();
            for _ in 0..r.u64()? {
                let id = StreamId {
                    ms: r.u64()?,
                    seq: r.u64()?,
                };
                pel.insert(
                    id,
                    Nack {
                        consumer: r.blob()?,
                        delivery_time: r.u64()? as i64,
                        delivery_count: r.u64()?,
                    },
                );
            }
            let mut consumers = BTreeMap::new();
            for _ in 0..r.u64()? {
                let cname = r.blob()?;
                consumers.insert(
                    cname,
                    Consumer {
                        seen_time: r.u64()? as i64,
                        active_time: r.u64()? as i64,
                        pel: Default::default(),
                    },
                );
            }
            // Each consumer's PEL is the group PEL's entries naming it.
            for (id, n) in &pel {
                if let Some(c) = consumers.get_mut(&n.consumer) {
                    c.pel.insert(*id);
                }
            }
            s.groups.insert(
                name,
                Group {
                    last_id,
                    entries_read,
                    pel,
                    consumers,
                },
            );
        }
        Some(s)
    }
}

/// A stream as Redis's RDB holds it (`RDB_TYPE_STREAM_LISTPACKS_3`), with the
/// listpacks still as their element lists: `rdb.rs` owns the byte encodings,
/// this module owns what the elements mean.
pub struct RdbStream {
    pub nodes: Vec<RdbNode>,
    pub length: u64,
    pub last_id: StreamId,
    /// `first_id`, `max_deleted_entry_id` and `entries_added`: absent from
    /// payloads before format 11, and derived on load as Redis derives them.
    pub meta: Option<(StreamId, StreamId, u64)>,
    pub groups: Vec<RdbGroup>,
}

/// One radix-tree node: its key (the master ID) and its listpack's elements.
pub struct RdbNode {
    pub master: StreamId,
    pub elements: Vec<Vec<u8>>,
    /// The listpack's size in bytes.
    pub bytes: usize,
}

pub struct RdbGroup {
    pub name: Vec<u8>,
    pub last_id: StreamId,
    /// The read counter, `u64::MAX` when unknown; absent before format 11.
    pub entries_read: Option<u64>,
    /// (ID, delivery time, delivery count), in ID order.
    pub pel: Vec<(StreamId, i64, u64)>,
    pub consumers: Vec<RdbConsumer>,
}

pub struct RdbConsumer {
    pub name: Vec<u8>,
    pub seen_time: i64,
    /// Absent before format 11, where it is taken to be the seen time.
    pub active_time: Option<i64>,
    pub pel: Vec<StreamId>,
}

// The entry flags of t_stream.c.
const ITEM_FLAG_DELETED: i64 = 1;
const ITEM_FLAG_SAMEFIELDS: i64 = 2;

/// A listpack element read as the integer it spells, as `lpGetIntegerIfValid`
/// reads it.
fn lp_number(e: &[u8]) -> Option<i64> {
    let t = std::str::from_utf8(e).ok()?;
    let digits = t.strip_prefix('-').unwrap_or(t);
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    t.parse().ok()
}

impl Stream {
    /// The stream in RDB form: each node's listpack laid out as
    /// `streamAppendItem` builds it — the master entry (live count, deleted
    /// count, the master fields, a zero), then every entry, tombstones
    /// included, as flags, ID deltas from the master, its fields or only its
    /// values when they match the master's, and its element count.
    pub fn to_rdb(&self) -> RdbStream {
        let int = |v: i64| v.to_string().into_bytes();
        let nodes = self
            .nodes
            .iter()
            .filter_map(|n| {
                let master = n.entries.first()?.id;
                let live = n.live();
                let mut el = vec![
                    int(live as i64),
                    int((n.entries.len() - live) as i64),
                    int(n.master_fields.len() as i64),
                ];
                el.extend(n.master_fields.iter().cloned());
                el.push(int(0));
                for e in &n.entries {
                    let same = n.master_fields.len() == e.fields.len()
                        && n.master_fields
                            .iter()
                            .zip(&e.fields)
                            .all(|(m, (f, _))| m == f);
                    let mut flags = 0;
                    if e.deleted {
                        flags |= ITEM_FLAG_DELETED;
                    }
                    if same {
                        flags |= ITEM_FLAG_SAMEFIELDS;
                    }
                    el.push(int(flags));
                    el.push(int(e.id.ms.wrapping_sub(master.ms) as i64));
                    el.push(int(e.id.seq.wrapping_sub(master.seq) as i64));
                    if same {
                        el.extend(e.fields.iter().map(|(_, v)| v.clone()));
                        el.push(int(e.fields.len() as i64 + 3));
                    } else {
                        el.push(int(e.fields.len() as i64));
                        for (f, v) in &e.fields {
                            el.push(f.clone());
                            el.push(v.clone());
                        }
                        el.push(int(e.fields.len() as i64 * 2 + 4));
                    }
                }
                Some(RdbNode {
                    master,
                    elements: el,
                    bytes: n.bytes,
                })
            })
            .collect();
        let groups = self
            .groups
            .iter()
            .map(|(name, g)| RdbGroup {
                name: name.clone(),
                last_id: g.last_id,
                entries_read: Some(g.entries_read as u64),
                pel: g
                    .pel
                    .iter()
                    .map(|(id, n)| (*id, n.delivery_time, n.delivery_count))
                    .collect(),
                consumers: g
                    .consumers
                    .iter()
                    .map(|(cname, c)| RdbConsumer {
                        name: cname.clone(),
                        seen_time: c.seen_time,
                        active_time: Some(c.active_time),
                        pel: c.pel.iter().copied().collect(),
                    })
                    .collect(),
            })
            .collect();
        RdbStream {
            nodes,
            length: self.length,
            last_id: self.last_id,
            meta: Some((self.first_id, self.max_deleted_id, self.entries_added)),
            groups,
        }
    }

    /// The reverse of [`to_rdb`](Self::to_rdb), over any of the three stream
    /// formats. `None` for a payload whose parts contradict each other: the
    /// checks are those of Redis's deep sanitisation (`streamValidate
    /// ListpackIntegrity`, a PEL entry with no consumer) plus the invariants
    /// this model holds — IDs ascending across nodes and within the last ID,
    /// the length matching the live entries — which a payload Redis built
    /// always meets.
    pub fn from_rdb(img: RdbStream) -> Option<Stream> {
        let mut nodes = Vec::with_capacity(img.nodes.len());
        let mut prev: Option<StreamId> = None;
        let mut live_total = 0u64;
        for n in img.nodes {
            let mut it = n.elements.into_iter();
            let mut num = || lp_number(&it.next()?);
            let count = num()?;
            let deleted = num()?;
            let nfields = num()?;
            if count < 0 || deleted < 0 || nfields < 0 || count + deleted == 0 {
                return None;
            }
            let master_fields: Vec<Vec<u8>> =
                (0..nfields).map(|_| it.next()).collect::<Option<_>>()?;
            if lp_number(&it.next()?)? != 0 {
                return None;
            }
            let mut entries = Vec::with_capacity((count + deleted) as usize);
            for _ in 0..count + deleted {
                let flags = lp_number(&it.next()?)?;
                let id = StreamId {
                    ms: n.master.ms.wrapping_add(lp_number(&it.next()?)? as u64),
                    seq: n.master.seq.wrapping_add(lp_number(&it.next()?)? as u64),
                };
                let (fields, expect) = if flags & ITEM_FLAG_SAMEFIELDS != 0 {
                    let fields: Fields = master_fields
                        .iter()
                        .map(|f| Some((f.clone(), it.next()?)))
                        .collect::<Option<_>>()?;
                    let expect = fields.len() as i64 + 3;
                    (fields, expect)
                } else {
                    let k = lp_number(&it.next()?)?;
                    if k < 0 {
                        return None;
                    }
                    let fields: Fields = (0..k)
                        .map(|_| Some((it.next()?, it.next()?)))
                        .collect::<Option<_>>()?;
                    (fields, k * 2 + 4)
                };
                if lp_number(&it.next()?)? != expect {
                    return None;
                }
                // The first entry is the one the node is keyed by.
                if entries.is_empty() && id != n.master {
                    return None;
                }
                if prev.is_some_and(|p| id <= p) {
                    return None;
                }
                prev = Some(id);
                entries.push(Entry {
                    id,
                    fields,
                    deleted: flags & ITEM_FLAG_DELETED != 0,
                });
            }
            if it.next().is_some() {
                return None;
            }
            let node = Node {
                master_fields,
                entries,
                bytes: n.bytes,
            };
            if node.live() as i64 != count {
                return None;
            }
            live_total += count as u64;
            nodes.push(node);
        }
        if live_total != img.length || prev.is_some_and(|p| p > img.last_id) {
            return None;
        }
        let mut s = Stream {
            nodes,
            length: img.length,
            last_id: img.last_id,
            ..Default::default()
        };
        match img.meta {
            Some((first, max_deleted, added)) => {
                s.first_id = first;
                s.max_deleted_id = max_deleted;
                s.entries_added = added;
            }
            // What rdbLoadObject assumes of a stream saved before format 11:
            // nothing deleted, everything ever added still there, and the
            // first ID the first live entry's (`streamGetEdgeID`).
            None => {
                s.entries_added = s.length;
                let first = s.iter().next().map_or(StreamId::MAX, |e| e.id);
                s.first_id = first;
            }
        }
        for g in img.groups {
            let entries_read = match g.entries_read {
                Some(v) => v as i64,
                None => s.estimate_distance(g.last_id),
            };
            let mut pel = BTreeMap::new();
            for (id, delivery_time, delivery_count) in g.pel {
                let nack = Nack {
                    consumer: Vec::new(),
                    delivery_time,
                    delivery_count,
                };
                if pel.insert(id, nack).is_some() {
                    return None;
                }
            }
            let mut consumers = BTreeMap::new();
            // Every PEL entry is owned by exactly one consumer.
            let mut owned = std::collections::BTreeSet::new();
            for c in g.consumers {
                let mut cpel = std::collections::BTreeSet::new();
                for id in c.pel {
                    let nack = pel.get_mut(&id)?;
                    if !owned.insert(id) {
                        return None;
                    }
                    nack.consumer = c.name.clone();
                    cpel.insert(id);
                }
                let consumer = Consumer {
                    seen_time: c.seen_time,
                    active_time: c.active_time.unwrap_or(c.seen_time),
                    pel: cpel,
                };
                if consumers.insert(c.name, consumer).is_some() {
                    return None;
                }
            }
            if owned.len() != pel.len() {
                return None;
            }
            let group = Group {
                last_id: g.last_id,
                entries_read,
                pel,
                consumers,
            };
            if s.groups.insert(g.name, group).is_some() {
                return None;
            }
        }
        Some(s)
    }
}

#[derive(Default)]
struct Writer(Vec<u8>);
impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.0.extend_from_slice(b);
    }
    fn blob(&mut self, b: &[u8]) {
        self.u64(b.len() as u64);
        self.bytes(b);
    }
}

struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}
impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let s = self.b.get(self.at..self.at.checked_add(n)?)?;
        self.at += n;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn blob(&mut self) -> Option<Vec<u8>> {
        let n = self.u64()? as usize;
        Some(self.take(n)?.to_vec())
    }
}

// ─────────────────────────────── arguments ──────────────────────────────────

const INVALID_ID: &str = "ERR Invalid stream ID specified as stream command argument";

fn ull(s: &[u8]) -> Option<u64> {
    let t = std::str::from_utf8(s).ok()?;
    if t.is_empty() || !t.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    t.parse().ok()
}

fn ll(s: &[u8]) -> Result<i64, String> {
    std::str::from_utf8(s)
        .ok()
        .and_then(|t| t.parse().ok())
        .ok_or_else(|| "ERR value is not an integer or out of range".to_string())
}

/// Redis's `streamGenericParseIDOrReply`. The bool is whether the sequence
/// was given (false for `ms-*`, which only XADD accepts).
fn parse_id(
    s: &[u8],
    missing_seq: u64,
    strict: bool,
    allow_seq_star: bool,
) -> Result<(StreamId, bool), String> {
    if s.len() > 127 {
        return Err(INVALID_ID.into());
    }
    if strict && (s == b"-" || s == b"+") {
        return Err(INVALID_ID.into());
    }
    if s == b"-" {
        return Ok((StreamId::ZERO, true));
    }
    if s == b"+" {
        return Ok((StreamId::MAX, true));
    }
    let (ms, rest) = match s.iter().position(|&c| c == b'-') {
        Some(p) => (&s[..p], Some(&s[p + 1..])),
        None => (s, None),
    };
    let ms = ull(ms).ok_or(INVALID_ID)?;
    match rest {
        None => Ok((
            StreamId {
                ms,
                seq: missing_seq,
            },
            true,
        )),
        Some(b"*") if allow_seq_star => Ok((StreamId { ms, seq: 0 }, false)),
        Some(seq) => Ok((
            StreamId {
                ms,
                seq: ull(seq).ok_or(INVALID_ID)?,
            },
            true,
        )),
    }
}

fn strict_id(s: &[u8]) -> Result<StreamId, String> {
    parse_id(s, 0, true, false).map(|(id, _)| id)
}

/// A range bound, `(` for exclusive — Redis's `streamParseIntervalIDOrReply`.
fn interval(s: &[u8], missing_seq: u64) -> Result<(StreamId, bool), String> {
    if s.len() > 1 && s[0] == b'(' {
        Ok((parse_id(&s[1..], missing_seq, true, false)?.0, true))
    } else {
        Ok((parse_id(s, missing_seq, false, false)?.0, false))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TrimBy {
    MaxLen(u64),
    MinId(StreamId),
}

#[derive(Debug, Clone, Copy)]
pub struct Trim {
    by: TrimBy,
    approx: bool,
    limit: i64,
}

/// The tunables a stream reads from CONFIG.
pub struct Cfg {
    pub node_max_bytes: usize,
    pub node_max_entries: usize,
}

impl Cfg {
    pub fn current() -> Cfg {
        Cfg {
            node_max_bytes: crate::config::get_i64("stream-node-max-bytes").max(0) as usize,
            node_max_entries: crate::config::get_i64("stream-node-max-entries").max(0) as usize,
        }
    }
}

/// What `parse_trim` read: the trim, `NOMKSTREAM`, XADD's ID (and whether it
/// was given with a sequence), and where the ID sits.
type TrimArgs = (Option<Trim>, bool, Option<(StreamId, bool)>, usize);

/// Redis's `streamParseAddOrTrimArgsOrReply`: the trim options, and for
/// XADD the position of the ID.
fn parse_trim(args: &[Vec<u8>], xadd: bool) -> Result<TrimArgs, String> {
    let mut i = 1; // after the key
    let (mut by, mut approx, mut limit, mut limit_given, mut nomk) =
        (None, false, 0i64, false, false);
    let mut id = None;
    while i < args.len() {
        let more = args.len() - 1 - i;
        let opt = &args[i];
        if xadd && opt.as_slice() == b"*" {
            break;
        } else if opt.eq_ignore_ascii_case(b"MAXLEN") && more > 0 {
            if by.is_some() {
                return Err("ERR syntax error, MAXLEN and MINID options at the same time are not compatible".into());
            }
            approx = false;
            if more >= 2 && args[i + 1].as_slice() == b"~" {
                approx = true;
                i += 1;
            } else if more >= 2 && args[i + 1].as_slice() == b"=" {
                i += 1;
            }
            let n = ll(&args[i + 1])?;
            if n < 0 {
                return Err("ERR The MAXLEN argument must be >= 0.".into());
            }
            by = Some(TrimBy::MaxLen(n as u64));
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"MINID") && more > 0 {
            if by.is_some() {
                return Err("ERR syntax error, MAXLEN and MINID options at the same time are not compatible".into());
            }
            approx = false;
            if more >= 2 && args[i + 1].as_slice() == b"~" {
                approx = true;
                i += 1;
            } else if more >= 2 && args[i + 1].as_slice() == b"=" {
                i += 1;
            }
            by = Some(TrimBy::MinId(strict_id(&args[i + 1])?));
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"LIMIT") && more > 0 {
            limit = ll(&args[i + 1])?;
            if limit < 0 {
                return Err("ERR The LIMIT argument must be >= 0.".into());
            }
            limit_given = true;
            i += 1;
        } else if xadd && opt.eq_ignore_ascii_case(b"NOMKSTREAM") {
            nomk = true;
        } else if xadd {
            id = Some(parse_id(opt, 0, true, true)?);
            break;
        } else {
            return Err("ERR syntax error".into());
        }
        i += 1;
    }
    if limit != 0 && by.is_none() {
        return Err(
            "ERR syntax error, LIMIT cannot be used without specifying a trimming strategy".into(),
        );
    }
    if !xadd && by.is_none() {
        return Err("ERR syntax error, XTRIM must be called with a trimming strategy".into());
    }
    if limit_given {
        if !approx {
            return Err(
                "ERR syntax error, LIMIT cannot be used without the special ~ option".into(),
            );
        }
    } else if approx {
        let entries = crate::config::get_i64("stream-node-max-entries");
        limit = (100 * entries).clamp(1, 1_000_000);
        if entries <= 0 {
            limit = 10000;
        }
    } else {
        limit = 0;
    }
    Ok((by.map(|by| Trim { by, approx, limit }), nomk, id, i))
}

// ───────────────────────────────── storage ──────────────────────────────────

/// Where streams live. Each half implements it over its own storage.
pub trait StreamStore {
    /// The stream at `key`: `Ok(None)` when there is no key, `Err` (WRONGTYPE)
    /// when the key holds another type.
    fn load(&mut self, key: &[u8]) -> Result<Option<Stream>, Response>;
    /// Store `s` at `key`, creating the key if needed and keeping its TTL.
    fn store(&mut self, key: &[u8], s: &Stream) -> Result<(), Response>;
}

fn wrongtype() -> Response {
    Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

pub fn not_stream() -> Response {
    wrongtype()
}

fn err(m: impl Into<String>) -> Response {
    Response::Error(m.into())
}

fn entry_value(e: &Entry) -> Value {
    Value::Array(vec![
        e.id.value(),
        Value::Array(
            e.fields
                .iter()
                .flat_map(|(f, v)| [Value::Bulk(f.clone()), Value::Bulk(v.clone())])
                .collect(),
        ),
    ])
}

fn entries_value(es: &[&Entry]) -> Value {
    Value::Array(es.iter().map(|e| entry_value(e)).collect())
}

// ──────────────────────────────── commands ──────────────────────────────────

/// A stream command, as parsed. Everything is validated at run time against
/// Redis's own error order, so this keeps the arguments.
#[derive(Debug, Clone)]
pub struct StreamCmd {
    pub name: String,
    pub args: Vec<Vec<u8>>,
}

const NAMES: &[&str] = &[
    "XADD",
    "XLEN",
    "XRANGE",
    "XREVRANGE",
    "XDEL",
    "XTRIM",
    "XREAD",
    "XREADGROUP",
    "XGROUP",
    "XACK",
    "XPENDING",
    "XCLAIM",
    "XAUTOCLAIM",
    "XINFO",
    "XSETID",
];

impl StreamCmd {
    pub fn parse(name: &str, args: &[Vec<u8>]) -> Option<Result<StreamCmd, String>> {
        if !NAMES.contains(&name) {
            return None;
        }
        Some(Ok(StreamCmd {
            name: name.to_string(),
            args: args.to_vec(),
        }))
    }

    /// The key a single-key command works on; XREAD names several.
    pub fn keys(&self) -> Vec<&[u8]> {
        match self.name.as_str() {
            "XREAD" | "XREADGROUP" => self.read_keys().unwrap_or_default(),
            "XGROUP" | "XINFO" => self
                .args
                .get(1)
                .map(|k| vec![k.as_slice()])
                .unwrap_or_default(),
            _ => self
                .args
                .first()
                .map(|k| vec![k.as_slice()])
                .unwrap_or_default(),
        }
    }

    fn read_keys(&self) -> Option<Vec<&[u8]>> {
        let at = self
            .args
            .iter()
            .position(|a| a.eq_ignore_ascii_case(b"STREAMS"))?
            + 1;
        let rest = &self.args[at..];
        Some(rest[..rest.len() / 2].iter().map(Vec::as_slice).collect())
    }

    /// Whether this command can change the stream.
    pub fn writes(&self) -> bool {
        !matches!(
            self.name.as_str(),
            "XLEN" | "XRANGE" | "XREVRANGE" | "XREAD" | "XINFO" | "XPENDING"
        )
    }

    pub fn run(&self, st: &mut dyn StreamStore) -> Response {
        let r = match self.name.as_str() {
            "XADD" => xadd(&self.args, st),
            "XLEN" => match st.load(&self.args[0]) {
                Ok(s) => Ok(Response::Integer(s.map_or(0, |s| s.length as i64))),
                Err(e) => Err(e),
            },
            "XRANGE" => xrange(&self.args, st, false),
            "XREVRANGE" => xrange(&self.args, st, true),
            "XDEL" => xdel(&self.args, st),
            "XTRIM" => xtrim(&self.args, st),
            "XREAD" | "XREADGROUP" => {
                match xread(self.name == "XREADGROUP", &self.args, st, None) {
                    Ok(ReadOutcome::Reply(r)) => Ok(r),
                    Ok(ReadOutcome::Wait(_)) => Ok(Response::NullArray),
                    Err(e) => Err(e),
                }
            }
            "XGROUP" => xgroup(&self.args, st),
            "XACK" => xack(&self.args, st),
            "XPENDING" => xpending(&self.args, st),
            "XCLAIM" => xclaim(&self.args, st),
            "XAUTOCLAIM" => xautoclaim(&self.args, st),
            "XINFO" => xinfo(&self.args, st),
            "XSETID" => xsetid(&self.args, st),
            _ => Err(err("ERR unknown stream command")),
        };
        r.unwrap_or_else(|e| e)
    }
}

type R = Result<Response, Response>;

fn xadd(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    let (trim, nomk, id, idpos) = parse_trim(args, true).map_err(err)?;
    let field_pos = idpos + 1;
    if args.len() < field_pos + 2 || !(args.len() - field_pos).is_multiple_of(2) {
        return Err(err("ERR wrong number of arguments for 'xadd' command"));
    }
    if let Some((id, true)) = id
        && id.is_zero()
    {
        return Err(err("ERR The ID specified in XADD must be greater than 0-0"));
    }
    let mut s = match st.load(&args[0])? {
        Some(s) => s,
        None if nomk => return Ok(Response::Null),
        None => Stream::default(),
    };
    if s.last_id == StreamId::MAX {
        return Err(err(
            "ERR The stream has exhausted the last possible ID, unable to add more items",
        ));
    }
    let fields: Fields = args[field_pos..]
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    let cfg = Cfg::current();
    let added = s.append(fields, id, &cfg).map_err(err)?;
    let trimmed = trim.map_or(0, |t| s.trim(&t));
    st.store(&args[0], &s)?;
    crate::notify::raise(crate::notify::STREAM, "xadd", &args[0]);
    if trimmed > 0 {
        crate::notify::raise(crate::notify::STREAM, "xtrim", &args[0]);
    }
    Ok(Response::BulkString(added.render().into_bytes()))
}

fn xrange(args: &[Vec<u8>], st: &mut dyn StreamStore, rev: bool) -> R {
    let (sa, ea) = if rev {
        (&args[2], &args[1])
    } else {
        (&args[1], &args[2])
    };
    let (mut start, sx) = interval(sa, 0).map_err(err)?;
    if sx {
        start = start
            .incr()
            .ok_or_else(|| err("ERR invalid start ID for the interval"))?;
    }
    let (mut end, ex) = interval(ea, u64::MAX).map_err(err)?;
    if ex {
        end = end
            .decr()
            .ok_or_else(|| err("ERR invalid end ID for the interval"))?;
    }
    let mut count: i64 = -1;
    let mut j = 3;
    while j < args.len() {
        if args[j].eq_ignore_ascii_case(b"COUNT") && j + 1 < args.len() {
            count = ll(&args[j + 1]).map_err(err)?.max(0);
            j += 2;
        } else {
            return Err(err("ERR syntax error"));
        }
    }
    let Some(s) = st.load(&args[0])? else {
        return Ok(Response::Value(Value::Array(vec![])));
    };
    if count == 0 {
        return Ok(Response::NullArray);
    }
    let es = s.range(start, end, rev, if count < 0 { 0 } else { count as usize });
    Ok(Response::Value(entries_value(&es)))
}

fn xdel(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    let Some(mut s) = st.load(&args[0])? else {
        return Ok(Response::Integer(0));
    };
    let ids = args[1..]
        .iter()
        .map(|a| strict_id(a))
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    let mut deleted = 0;
    let mut first_entry = false;
    for id in ids {
        if s.delete(id) {
            if id == s.first_id {
                first_entry = true;
            }
            if id > s.max_deleted_id {
                s.max_deleted_id = id;
            }
            deleted += 1;
        }
    }
    if deleted > 0 {
        if s.length == 0 {
            s.first_id = StreamId::ZERO;
        } else if first_entry {
            s.first_id = s.first_valid_id();
        }
        st.store(&args[0], &s)?;
        crate::notify::raise(crate::notify::STREAM, "xdel", &args[0]);
    }
    Ok(Response::Integer(deleted))
}

fn xtrim(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    let (trim, _, _, _) = parse_trim(args, false).map_err(err)?;
    let Some(mut s) = st.load(&args[0])? else {
        return Ok(Response::Integer(0));
    };
    let deleted = s.trim(&trim.expect("XTRIM requires a strategy"));
    if deleted > 0 {
        st.store(&args[0], &s)?;
        crate::notify::raise(crate::notify::STREAM, "xtrim", &args[0]);
    }
    Ok(Response::Integer(deleted as i64))
}

/// What XREAD found: a reply, or nothing yet — with the IDs it would wait
/// from, `$` resolved, so a blocked retry keeps the original point.
pub enum ReadOutcome {
    Reply(Response),
    Wait(Vec<Vec<u8>>),
}

/// XREAD and XREADGROUP. `resolved` replaces the ID arguments on a retry of a
/// blocked read, so `$` keeps meaning the last ID when the read first ran.
pub fn xread(
    group_read: bool,
    args: &[Vec<u8>],
    st: &mut dyn StreamStore,
    resolved: Option<&[Vec<u8>]>,
) -> Result<ReadOutcome, Response> {
    let cmd = if group_read { "XREADGROUP" } else { "XREAD" };
    let mut count: i64 = 0;
    let mut block = false;
    let mut noack = false;
    let mut group: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut streams_at = None;
    let mut i = 0;
    while i < args.len() {
        let more = args.len() - i - 1;
        let o = &args[i];
        if o.eq_ignore_ascii_case(b"BLOCK") && more > 0 {
            crate::blocking::parse_timeout_ms(&args[i + 1]).map_err(err)?;
            block = true;
            i += 1;
        } else if o.eq_ignore_ascii_case(b"COUNT") && more > 0 {
            count = ll(&args[i + 1]).map_err(err)?.max(0);
            i += 1;
        } else if o.eq_ignore_ascii_case(b"STREAMS") && more > 0 {
            let n = args.len() - (i + 1);
            if !n.is_multiple_of(2) {
                let sym = if group_read { '>' } else { '$' };
                return Err(err(format!(
                    "ERR Unbalanced '{}' list of streams: for each stream key an ID or '{sym}' must be specified.",
                    cmd.to_lowercase()
                )));
            }
            streams_at = Some(i + 1);
            break;
        } else if o.eq_ignore_ascii_case(b"GROUP") && more >= 2 {
            if !group_read {
                return Err(err(
                    "ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.",
                ));
            }
            group = Some((args[i + 1].clone(), args[i + 2].clone()));
            i += 2;
        } else if o.eq_ignore_ascii_case(b"NOACK") {
            if !group_read {
                return Err(err(
                    "ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.",
                ));
            }
            noack = true;
        } else {
            return Err(err("ERR syntax error"));
        }
        i += 1;
    }
    let Some(at) = streams_at else {
        return Err(err("ERR syntax error"));
    };
    if group_read && group.is_none() {
        return Err(err("ERR Missing GROUP option for XREADGROUP"));
    }
    let n = (args.len() - at) / 2;
    let keys = &args[at..at + n];
    let id_args: Vec<Vec<u8>> = match resolved {
        Some(r) => r.to_vec(),
        None => args[at + n..].to_vec(),
    };

    // Resolve every ID first, as Redis does, so a bad one fails the call.
    let mut loaded: Vec<Option<Stream>> = Vec::with_capacity(n);
    let mut ids: Vec<StreamId> = Vec::with_capacity(n);
    let mut resolved_out: Vec<Vec<u8>> = Vec::with_capacity(n);
    for (k, key) in keys.iter().enumerate() {
        let s = st.load(key)?;
        if let Some((g, _)) = &group
            && s.as_ref().is_none_or(|s| !s.groups.contains_key(g))
        {
            return Err(err(format!(
                "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(g)
            )));
        }
        let a = &id_args[k];
        let id = if a.as_slice() == b"$" {
            if group_read {
                return Err(err(
                    "ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.",
                ));
            }
            s.as_ref().map_or(StreamId::ZERO, |s| s.last_id)
        } else if a.as_slice() == b">" {
            if !group_read {
                return Err(err(
                    "ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.",
                ));
            }
            StreamId::MAX
        } else if a.as_slice() == b"+" && !group_read {
            // `+` is the last entry: Redis 7.4 added it; 7.2 refuses it.
            return Err(err(INVALID_ID));
        } else {
            strict_id(a).map_err(err)?
        };
        resolved_out.push(if a.as_slice() == b">" {
            b">".to_vec()
        } else {
            id.render().into_bytes()
        });
        ids.push(id);
        loaded.push(s);
    }

    let now = now_ms();
    let mut out: Vec<Value> = Vec::new();
    for (k, key) in keys.iter().enumerate() {
        let Some(s) = loaded[k].as_mut() else {
            continue;
        };
        let mut gt = ids[k];
        let mut serve = false;
        let mut history = false;
        let mut changed = false;
        if let Some((gname, cname)) = &group {
            let gr = s.groups.get(gname).expect("checked above").clone();
            if gt != StreamId::MAX {
                serve = true;
                history = true;
            } else if s.length > 0 && s.last_valid_id() > gr.last_id {
                serve = true;
                gt = gr.last_id;
            }
            let g = s.groups.get_mut(gname).expect("checked above");
            if !g.consumers.contains_key(cname.as_slice()) {
                crate::notify::raise(crate::notify::STREAM, "xgroup-createconsumer", key);
            }
            g.consumers
                .entry(cname.clone())
                .or_insert_with(|| Consumer {
                    seen_time: now,
                    active_time: -1,
                    pel: Default::default(),
                })
                .seen_time = now;
            changed = true;
        } else if s.length > 0 && s.last_valid_id() > gt {
            serve = true;
        }
        if serve {
            let start = gt.incr().unwrap_or(StreamId::MAX);
            let entries = if let Some((gname, cname)) = &group {
                if history {
                    read_history(s, gname, cname, start, count as usize, now)
                } else {
                    read_new(s, gname, cname, start, count as usize, noack, now)
                }
            } else {
                let es = s.range(start, StreamId::MAX, false, count as usize);
                entries_value(&es)
            };
            out.push(Value::Array(vec![Value::Bulk(key.clone()), entries]));
            changed |= group.is_some();
        }
        if changed {
            let snapshot = s.clone();
            st.store(key, &snapshot)?;
        }
    }
    if !out.is_empty() {
        return Ok(ReadOutcome::Reply(Response::Value(Value::Array(out))));
    }
    if block {
        Ok(ReadOutcome::Wait(resolved_out))
    } else {
        Ok(ReadOutcome::Reply(Response::NullArray))
    }
}

/// `streamReplyWithRange` for a group read of new entries: advance the
/// group's last ID and read counter, and record each delivery in the PEL.
fn read_new(
    s: &mut Stream,
    gname: &[u8],
    cname: &[u8],
    start: StreamId,
    count: usize,
    noack: bool,
    now: i64,
) -> Value {
    let ids: Vec<StreamId> = s
        .range(start, StreamId::MAX, false, count)
        .iter()
        .map(|e| e.id)
        .collect();
    let mut out = Vec::new();
    for id in ids {
        let tomb = s.range_has_tombstones(Some(id), None);
        let est = s.estimate_distance(id);
        let added = s.entries_added;
        let g = s.groups.get_mut(gname).expect("group checked");
        if id > g.last_id {
            if g.entries_read != -1 && !tomb {
                g.entries_read += 1;
            } else if added > 0 {
                g.entries_read = est;
            }
            g.last_id = id;
        }
        if !noack {
            if let Some(old) = g.pel.get(&id).map(|n| n.consumer.clone())
                && let Some(c) = g.consumers.get_mut(&old)
            {
                c.pel.remove(&id);
            }
            g.pel.insert(
                id,
                Nack {
                    consumer: cname.to_vec(),
                    delivery_time: now,
                    delivery_count: 1,
                },
            );
            let c = g.consumers.get_mut(cname).expect("consumer created");
            c.pel.insert(id);
            c.active_time = now;
        }
        out.push(entry_value(s.find(id).expect("in range")));
    }
    Value::Array(out)
}

/// `streamReplyWithRangeFromConsumerPEL`: a consumer re-reading what it was
/// delivered and has not acknowledged. A deleted entry shows as a nil.
fn read_history(
    s: &mut Stream,
    gname: &[u8],
    cname: &[u8],
    start: StreamId,
    count: usize,
    now: i64,
) -> Value {
    let pel: Vec<StreamId> = s.groups[gname].consumers[cname]
        .pel
        .range(start..)
        .take(if count == 0 { usize::MAX } else { count })
        .copied()
        .collect();
    let mut out = Vec::new();
    for id in pel {
        match s.find(id).map(entry_value) {
            Some(v) => {
                out.push(v);
                if let Some(n) = s.groups.get_mut(gname).and_then(|g| g.pel.get_mut(&id)) {
                    n.delivery_time = now;
                    n.delivery_count += 1;
                }
            }
            None => out.push(Value::Array(vec![id.value(), Value::NilArray])),
        }
    }
    Value::Array(out)
}

fn subcmd_syntax(sub: &[u8], cmd: &str) -> Response {
    err(format!(
        "ERR unknown subcommand or wrong number of arguments for '{}'. Try {cmd} HELP.",
        String::from_utf8_lossy(sub)
    ))
}

fn xgroup(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    let opt = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    if opt == "HELP" && args.len() == 1 {
        return Ok(help(&[
            "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "CREATE <key> <groupname> <id|$> [option]",
            "    Create a new consumer group. Options are:",
            "    * MKSTREAM",
            "      Create the empty stream if it does not exist.",
            "    * ENTRIESREAD entries_read",
            "      Set the group's entries_read counter (internal use).",
            "CREATECONSUMER <key> <groupname> <consumer>",
            "    Create a new consumer in the specified group.",
            "DELCONSUMER <key> <groupname> <consumer>",
            "    Remove the specified consumer.",
            "DESTROY <key> <groupname>",
            "    Remove the specified group.",
            "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
            "    Set the current group ID and entries_read counter.",
            "HELP",
            "    Print this help.",
        ]));
    }
    if args.len() < 3 {
        return Err(subcmd_syntax(&args[0], "XGROUP"));
    }
    let (mut mkstream, mut entries_read) = (false, -1i64);
    let mut i = 4;
    let create = opt == "CREATE";
    let setid = opt == "SETID";
    while i < args.len() {
        if create && args[i].eq_ignore_ascii_case(b"MKSTREAM") {
            mkstream = true;
            i += 1;
        } else if (create || setid)
            && args[i].eq_ignore_ascii_case(b"ENTRIESREAD")
            && i + 1 < args.len()
        {
            entries_read = ll(&args[i + 1]).map_err(err)?;
            if entries_read < 0 && entries_read != -1 {
                return Err(err("ERR value for ENTRIESREAD must be positive or -1"));
            }
            i += 2;
        } else {
            return Err(subcmd_syntax(&args[0], "XGROUP"));
        }
    }
    let key = &args[1];
    let gname = args[2].clone();
    let loaded = st.load(key)?;
    if !mkstream && loaded.is_none() {
        return Err(err(
            "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
        ));
    }
    let needs_group = matches!(opt.as_str(), "SETID" | "CREATECONSUMER" | "DELCONSUMER");
    if needs_group
        && loaded
            .as_ref()
            .is_some_and(|s| !s.groups.contains_key(&gname))
    {
        return Err(err(format!(
            "NOGROUP No such consumer group '{}' for key name '{}'",
            String::from_utf8_lossy(&gname),
            String::from_utf8_lossy(key)
        )));
    }
    let mut s = loaded.unwrap_or_default();
    let event: &str;
    let reply = match opt.as_str() {
        "CREATE" if (4..=7).contains(&args.len()) => {
            let id = if args[3].as_slice() == b"$" {
                s.last_id
            } else {
                strict_id(&args[3]).map_err(err)?
            };
            if s.groups.contains_key(&gname) {
                return Err(err("BUSYGROUP Consumer Group name already exists"));
            }
            s.groups.insert(
                gname,
                Group {
                    last_id: id,
                    entries_read,
                    pel: BTreeMap::new(),
                    consumers: BTreeMap::new(),
                },
            );
            event = "xgroup-create";
            Response::Ok
        }
        "SETID" if args.len() == 4 || args.len() == 6 => {
            let id = if args[3].as_slice() == b"$" {
                s.last_id
            } else {
                parse_id(&args[3], 0, false, false).map_err(err)?.0
            };
            let g = s.groups.get_mut(&gname).expect("checked");
            g.last_id = id;
            g.entries_read = entries_read;
            event = "xgroup-setid";
            Response::Ok
        }
        "DESTROY" if args.len() == 3 => {
            if s.groups.remove(&gname).is_some() {
                event = "xgroup-destroy";
                Response::Integer(1)
            } else {
                return Ok(Response::Integer(0));
            }
        }
        "CREATECONSUMER" if args.len() == 4 => {
            let g = s.groups.get_mut(&gname).expect("checked");
            if g.consumers.contains_key(&args[3]) {
                return Ok(Response::Integer(0));
            }
            g.consumers.insert(
                args[3].clone(),
                Consumer {
                    seen_time: now_ms(),
                    active_time: -1,
                    pel: Default::default(),
                },
            );
            event = "xgroup-createconsumer";
            Response::Integer(1)
        }
        "DELCONSUMER" if args.len() == 4 => {
            let g = s.groups.get_mut(&gname).expect("checked");
            match g.consumers.remove(&args[3]) {
                None => return Ok(Response::Integer(0)),
                Some(c) => {
                    for id in &c.pel {
                        g.pel.remove(id);
                    }
                    event = "xgroup-delconsumer";
                    Response::Integer(c.pel.len() as i64)
                }
            }
        }
        _ => return Err(subcmd_syntax(&args[0], "XGROUP")),
    };
    st.store(key, &s)?;
    crate::notify::raise(crate::notify::STREAM, event, key);
    Ok(reply)
}

fn xack(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    let Some(mut s) = st.load(&args[0])? else {
        return Ok(Response::Integer(0));
    };
    let Some(g) = s.groups.get_mut(&args[1]) else {
        return Ok(Response::Integer(0));
    };
    let ids = args[2..]
        .iter()
        .map(|a| strict_id(a))
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    let mut n = 0;
    for id in ids {
        if let Some(nack) = g.pel.remove(&id) {
            if let Some(c) = g.consumers.get_mut(&nack.consumer) {
                c.pel.remove(&id);
            }
            n += 1;
        }
    }
    if n > 0 {
        st.store(&args[0], &s)?;
    }
    Ok(Response::Integer(n))
}

fn nogroup(key: &[u8], g: &[u8]) -> Response {
    err(format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(g)
    ))
}

fn xpending(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    let argc = args.len() + 1;
    if argc != 3 && !(6..=9).contains(&argc) {
        return Err(err("ERR syntax error"));
    }
    let mut range = None;
    let mut minidle = 0i64;
    let mut consumer: Option<&[u8]> = None;
    if argc >= 6 {
        let mut si = 2;
        if args[2].eq_ignore_ascii_case(b"IDLE") {
            minidle = ll(&args[3]).map_err(err)?;
            if argc < 8 {
                return Err(err("ERR syntax error"));
            }
            si += 2;
        }
        let count = ll(&args[si + 2]).map_err(err)?.max(0);
        let (mut start, sx) = interval(&args[si], 0).map_err(err)?;
        if sx {
            start = start
                .incr()
                .ok_or_else(|| err("ERR invalid start ID for the interval"))?;
        }
        let (mut end, ex) = interval(&args[si + 1], u64::MAX).map_err(err)?;
        if ex {
            end = end
                .decr()
                .ok_or_else(|| err("ERR invalid end ID for the interval"))?;
        }
        if si + 3 < args.len() {
            consumer = Some(&args[si + 3]);
        }
        range = Some((start, end, count));
    }
    let s = st.load(&args[0])?;
    let Some(g) = s.as_ref().and_then(|s| s.groups.get(&args[1])) else {
        return Err(nogroup(&args[0], &args[1]));
    };
    let now = now_ms();
    match range {
        None => {
            if g.pel.is_empty() {
                return Ok(Response::Value(Value::Array(vec![
                    Value::Int(0),
                    Value::Nil,
                    Value::Nil,
                    Value::NilArray,
                ])));
            }
            let first = g.pel.keys().next().expect("non-empty");
            let last = g.pel.keys().next_back().expect("non-empty");
            let consumers = g
                .consumers
                .iter()
                .filter(|(_, c)| !c.pel.is_empty())
                .map(|(n, c)| {
                    Value::Array(vec![
                        Value::Bulk(n.clone()),
                        Value::Bulk(c.pel.len().to_string().into_bytes()),
                    ])
                })
                .collect();
            Ok(Response::Value(Value::Array(vec![
                Value::Int(g.pel.len() as i64),
                first.value(),
                last.value(),
                Value::Array(consumers),
            ])))
        }
        Some((start, end, mut count)) => {
            let ids: Vec<StreamId> = match consumer {
                Some(c) => match g.consumers.get(c) {
                    None => return Ok(Response::Value(Value::Array(vec![]))),
                    Some(c) => c.pel.range(start..=end.max(start)).copied().collect(),
                },
                None => g
                    .pel
                    .range(start..=end.max(start))
                    .map(|(k, _)| *k)
                    .collect(),
            };
            let mut out = Vec::new();
            for id in ids {
                if count == 0 || id > end {
                    break;
                }
                let n = &g.pel[&id];
                if minidle > 0 && now - n.delivery_time < minidle {
                    continue;
                }
                count -= 1;
                out.push(Value::Array(vec![
                    id.value(),
                    Value::Bulk(n.consumer.clone()),
                    Value::Int((now - n.delivery_time).max(0)),
                    Value::Int(n.delivery_count as i64),
                ]));
            }
            Ok(Response::Value(Value::Array(out)))
        }
    }
}

fn xclaim(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    let Some(mut s) = st.load(&args[0])? else {
        return Err(nogroup(&args[0], &args[1]));
    };
    if !s.groups.contains_key(&args[1]) {
        return Err(nogroup(&args[0], &args[1]));
    }
    let minidle = ll(&args[3])
        .map_err(|_| err("ERR Invalid min-idle-time argument for XCLAIM"))?
        .max(0);
    let mut ids = Vec::new();
    let mut j = 4;
    while j < args.len() {
        match strict_id(&args[j]) {
            Ok(id) => ids.push(id),
            Err(_) => break,
        }
        j += 1;
    }
    let now = now_ms();
    let (mut force, mut justid, mut retry, mut delivery) = (false, false, -1i64, -1i64);
    let mut last_id = StreamId::ZERO;
    while j < args.len() {
        let more = args.len() - 1 - j;
        let o = &args[j];
        if o.eq_ignore_ascii_case(b"FORCE") {
            force = true;
        } else if o.eq_ignore_ascii_case(b"JUSTID") {
            justid = true;
        } else if o.eq_ignore_ascii_case(b"IDLE") && more > 0 {
            j += 1;
            delivery = now
                - ll(&args[j]).map_err(|_| err("ERR Invalid IDLE option argument for XCLAIM"))?;
        } else if o.eq_ignore_ascii_case(b"TIME") && more > 0 {
            j += 1;
            delivery =
                ll(&args[j]).map_err(|_| err("ERR Invalid TIME option argument for XCLAIM"))?;
        } else if o.eq_ignore_ascii_case(b"RETRYCOUNT") && more > 0 {
            j += 1;
            retry = ll(&args[j])
                .map_err(|_| err("ERR Invalid RETRYCOUNT option argument for XCLAIM"))?;
        } else if o.eq_ignore_ascii_case(b"LASTID") && more > 0 {
            j += 1;
            last_id = strict_id(&args[j]).map_err(err)?;
        } else {
            return Err(err(format!(
                "ERR Unrecognized XCLAIM option '{}'",
                String::from_utf8_lossy(o)
            )));
        }
        j += 1;
    }
    let delivery = if delivery != -1 && (0..=now).contains(&delivery) {
        delivery
    } else {
        now
    };
    let cname = args[2].clone();
    let mut out = Vec::new();
    let existing: Vec<bool> = ids.iter().map(|id| s.exists(*id)).collect();
    let values: Vec<Option<Value>> = ids.iter().map(|id| s.find(*id).map(entry_value)).collect();
    {
        let g = s.groups.get_mut(&args[1]).expect("checked");
        if last_id > g.last_id {
            g.last_id = last_id;
        }
        if !g.consumers.contains_key(cname.as_slice()) {
            crate::notify::raise(crate::notify::STREAM, "xgroup-createconsumer", &args[0]);
        }
        g.consumers
            .entry(cname.clone())
            .or_insert_with(|| Consumer {
                seen_time: now,
                active_time: -1,
                pel: Default::default(),
            })
            .seen_time = now;
        for (k, id) in ids.iter().enumerate() {
            if !existing[k] {
                if let Some(n) = g.pel.remove(id)
                    && let Some(c) = g.consumers.get_mut(&n.consumer)
                {
                    c.pel.remove(id);
                }
                continue;
            }
            if force && !g.pel.contains_key(id) {
                g.pel.insert(
                    *id,
                    Nack {
                        consumer: vec![],
                        delivery_time: now,
                        delivery_count: 0,
                    },
                );
            }
            let Some(n) = g.pel.get(id).cloned() else {
                continue;
            };
            let owned = !n.consumer.is_empty();
            if owned && minidle > 0 && now - n.delivery_time < minidle {
                continue;
            }
            if n.consumer != cname
                && let Some(c) = g.consumers.get_mut(&n.consumer)
            {
                c.pel.remove(id);
            }
            let mut n = n;
            n.delivery_time = delivery;
            if retry >= 0 {
                n.delivery_count = retry as u64;
            } else if !justid {
                n.delivery_count += 1;
            }
            n.consumer = cname.clone();
            g.pel.insert(*id, n);
            let c = g.consumers.get_mut(&cname).expect("created");
            c.pel.insert(*id);
            c.active_time = now;
            out.push(if justid {
                id.value()
            } else {
                values[k].clone().expect("exists")
            });
        }
    }
    st.store(&args[0], &s)?;
    Ok(Response::Value(Value::Array(out)))
}

fn xautoclaim(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    let minidle = ll(&args[3])
        .map_err(|_| err("ERR Invalid min-idle-time argument for XAUTOCLAIM"))?
        .max(0);
    let (mut start, sx) = interval(&args[4], 0).map_err(err)?;
    if sx {
        start = start
            .incr()
            .ok_or_else(|| err("ERR invalid start ID for the interval"))?;
    }
    let mut count: i64 = 100;
    let mut justid = false;
    let mut j = 5;
    while j < args.len() {
        let more = args.len() - 1 - j;
        if args[j].eq_ignore_ascii_case(b"COUNT") && more > 0 {
            let max = i64::MAX / 16;
            count = ll(&args[j + 1])
                .ok()
                .filter(|c| (1..=max).contains(c))
                .ok_or_else(|| err("ERR COUNT must be > 0"))?;
            j += 1;
        } else if args[j].eq_ignore_ascii_case(b"JUSTID") {
            justid = true;
        } else {
            return Err(err("ERR syntax error"));
        }
        j += 1;
    }
    let Some(mut s) = st.load(&args[0])? else {
        return Err(nogroup(&args[0], &args[1]));
    };
    if !s.groups.contains_key(&args[1]) {
        return Err(nogroup(&args[0], &args[1]));
    }
    let now = now_ms();
    let cname = args[2].clone();
    let pel_ids: Vec<StreamId> = s.groups[&args[1]]
        .pel
        .range(start..)
        .map(|(k, _)| *k)
        .collect();
    let mut attempts = count * 10;
    let mut out = Vec::new();
    let mut deleted = Vec::new();
    let mut idx = 0;
    let mut stopped_at = None;
    {
        let values: BTreeMap<StreamId, Value> = pel_ids
            .iter()
            .filter_map(|id| s.find(*id).map(|e| (*id, entry_value(e))))
            .collect();
        let g = s.groups.get_mut(&args[1]).expect("checked");
        if !g.consumers.contains_key(cname.as_slice()) {
            crate::notify::raise(crate::notify::STREAM, "xgroup-createconsumer", &args[0]);
        }
        g.consumers
            .entry(cname.clone())
            .or_insert_with(|| Consumer {
                seen_time: now,
                active_time: -1,
                pel: Default::default(),
            })
            .seen_time = now;
        loop {
            if attempts == 0 {
                stopped_at = Some(idx);
                break;
            }
            attempts -= 1;
            if count == 0 {
                stopped_at = Some(idx);
                break;
            }
            let Some(&id) = pel_ids.get(idx) else { break };
            idx += 1;
            let Some(v) = values.get(&id) else {
                if let Some(n) = g.pel.remove(&id)
                    && let Some(c) = g.consumers.get_mut(&n.consumer)
                {
                    c.pel.remove(&id);
                }
                deleted.push(id.value());
                count -= 1;
                continue;
            };
            let n = g.pel.get(&id).cloned().expect("from the PEL");
            if minidle > 0 && now - n.delivery_time < minidle {
                continue;
            }
            if n.consumer != cname
                && let Some(c) = g.consumers.get_mut(&n.consumer)
            {
                c.pel.remove(&id);
            }
            let mut n = n;
            n.delivery_time = now;
            if !justid {
                n.delivery_count += 1;
            }
            n.consumer = cname.clone();
            g.pel.insert(id, n);
            let c = g.consumers.get_mut(&cname).expect("created");
            c.pel.insert(id);
            c.active_time = now;
            out.push(if justid { id.value() } else { v.clone() });
            count -= 1;
        }
    }
    // The cursor: the next PEL entry after the last one looked at.
    let next = stopped_at
        .and_then(|i| pel_ids.get(i))
        .copied()
        .unwrap_or(StreamId::ZERO);
    st.store(&args[0], &s)?;
    Ok(Response::Value(Value::Array(vec![
        next.value(),
        Value::Array(out),
        Value::Array(deleted),
    ])))
}

fn help(lines: &[&str]) -> Response {
    Response::Value(Value::Array(
        lines
            .iter()
            .map(|l| Value::Simple((*l).to_string()))
            .collect(),
    ))
}

fn xinfo(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    if args[0].eq_ignore_ascii_case(b"HELP") {
        return Ok(help(&[
            "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "CONSUMERS <key> <groupname>",
            "    Show consumers of <groupname>.",
            "GROUPS <key>",
            "    Show the stream consumer groups.",
            "STREAM <key> [FULL [COUNT <count>]",
            "    Show information about the stream.",
            "HELP",
            "    Print this help.",
        ]));
    }
    if args.len() < 2 {
        return Err(subcmd_syntax(&args[0], "XINFO"));
    }
    let Some(s) = st.load(&args[1])? else {
        return Err(err("ERR no such key"));
    };
    let opt = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let now = now_ms();
    let kv = |k: &str, v: Value| (Value::str(k), v);
    match opt.as_str() {
        "CONSUMERS" if args.len() == 3 => {
            let Some(g) = s.groups.get(&args[2]) else {
                return Err(err(format!(
                    "NOGROUP No such consumer group '{}' for key name '{}'",
                    String::from_utf8_lossy(&args[2]),
                    String::from_utf8_lossy(&args[1])
                )));
            };
            Ok(Response::Value(Value::Array(
                g.consumers
                    .iter()
                    .map(|(n, c)| {
                        let inactive = if c.active_time != -1 {
                            now - c.active_time
                        } else {
                            -1
                        };
                        Value::Map(vec![
                            kv("name", Value::Bulk(n.clone())),
                            kv("pending", Value::Int(c.pel.len() as i64)),
                            kv("idle", Value::Int((now - c.seen_time).max(0))),
                            kv("inactive", Value::Int(inactive)),
                        ])
                    })
                    .collect(),
            )))
        }
        "GROUPS" if args.len() == 2 => Ok(Response::Value(Value::Array(
            s.groups
                .iter()
                .map(|(n, g)| {
                    Value::Map(vec![
                        kv("name", Value::Bulk(n.clone())),
                        kv("consumers", Value::Int(g.consumers.len() as i64)),
                        kv("pending", Value::Int(g.pel.len() as i64)),
                        kv("last-delivered-id", g.last_id.value()),
                        kv(
                            "entries-read",
                            if g.entries_read != -1 {
                                Value::Int(g.entries_read)
                            } else {
                                Value::Nil
                            },
                        ),
                        kv("lag", s.lag(g)),
                    ])
                })
                .collect(),
        ))),
        "STREAM" => {
            let opts = &args[2..];
            let (full, count) = match opts {
                [] => (false, 10),
                [f] if f.eq_ignore_ascii_case(b"FULL") => (true, 10),
                [f, c, n]
                    if f.eq_ignore_ascii_case(b"FULL") && c.eq_ignore_ascii_case(b"COUNT") =>
                {
                    let n = ll(n).map_err(err)?;
                    (true, if n < 0 { 10 } else { n as usize })
                }
                _ => return Err(subcmd_syntax(&args[0], "XINFO")),
            };
            let mut m = vec![
                kv("length", Value::Int(s.length as i64)),
                kv("radix-tree-keys", Value::Int(s.nodes.len() as i64)),
                kv("radix-tree-nodes", Value::Int(s.nodes.len() as i64 + 1)),
                kv("last-generated-id", s.last_id.value()),
                kv("max-deleted-entry-id", s.max_deleted_id.value()),
                kv("entries-added", Value::Int(s.entries_added as i64)),
                kv("recorded-first-entry-id", s.first_id.value()),
            ];
            if !full {
                m.push(kv("groups", Value::Int(s.groups.len() as i64)));
                let first = s.range(StreamId::ZERO, StreamId::MAX, false, 1);
                let last = s.range(StreamId::ZERO, StreamId::MAX, true, 1);
                m.push(kv(
                    "first-entry",
                    first.first().map_or(Value::Nil, |e| entry_value(e)),
                ));
                m.push(kv(
                    "last-entry",
                    last.first().map_or(Value::Nil, |e| entry_value(e)),
                ));
            } else {
                let es = s.range(StreamId::ZERO, StreamId::MAX, false, count);
                m.push(kv("entries", entries_value(&es)));
                let lim = |n: usize| if count == 0 { n } else { n.min(count) };
                let groups = s
                    .groups
                    .iter()
                    .map(|(n, g)| {
                        let pending = g
                            .pel
                            .iter()
                            .take(lim(g.pel.len()))
                            .map(|(id, nk)| {
                                Value::Array(vec![
                                    id.value(),
                                    Value::Bulk(nk.consumer.clone()),
                                    Value::Int(nk.delivery_time),
                                    Value::Int(nk.delivery_count as i64),
                                ])
                            })
                            .collect();
                        let consumers = g
                            .consumers
                            .iter()
                            .map(|(cn, c)| {
                                let cp = c
                                    .pel
                                    .iter()
                                    .take(lim(c.pel.len()))
                                    .map(|id| {
                                        let nk = &g.pel[id];
                                        Value::Array(vec![
                                            id.value(),
                                            Value::Int(nk.delivery_time),
                                            Value::Int(nk.delivery_count as i64),
                                        ])
                                    })
                                    .collect();
                                Value::Map(vec![
                                    kv("name", Value::Bulk(cn.clone())),
                                    kv("seen-time", Value::Int(c.seen_time)),
                                    kv("active-time", Value::Int(c.active_time)),
                                    kv("pel-count", Value::Int(c.pel.len() as i64)),
                                    kv("pending", Value::Array(cp)),
                                ])
                            })
                            .collect();
                        Value::Map(vec![
                            kv("name", Value::Bulk(n.clone())),
                            kv("last-delivered-id", g.last_id.value()),
                            kv(
                                "entries-read",
                                if g.entries_read != -1 {
                                    Value::Int(g.entries_read)
                                } else {
                                    Value::Nil
                                },
                            ),
                            kv("lag", s.lag(g)),
                            kv("pel-count", Value::Int(g.pel.len() as i64)),
                            kv("pending", Value::Array(pending)),
                            kv("consumers", Value::Array(consumers)),
                        ])
                    })
                    .collect();
                m.push(kv("groups", Value::Array(groups)));
            }
            Ok(Response::Value(Value::Map(m)))
        }
        _ => Err(subcmd_syntax(&args[0], "XINFO")),
    }
}

fn xsetid(args: &[Vec<u8>], st: &mut dyn StreamStore) -> R {
    let id = strict_id(&args[1]).map_err(err)?;
    let (mut entries_added, mut max_xdel) = (-1i64, StreamId::ZERO);
    let mut i = 2;
    while i < args.len() {
        let more = args.len() - 1 - i;
        if args[i].eq_ignore_ascii_case(b"ENTRIESADDED") && more > 0 {
            entries_added = ll(&args[i + 1]).map_err(err)?;
            if entries_added < 0 {
                return Err(err("ERR entries_added must be positive"));
            }
            i += 2;
        } else if args[i].eq_ignore_ascii_case(b"MAXDELETEDID") && more > 0 {
            max_xdel = strict_id(&args[i + 1]).map_err(err)?;
            if id < max_xdel {
                return Err(err(
                    "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                ));
            }
            i += 2;
        } else {
            return Err(err("ERR syntax error"));
        }
    }
    let Some(mut s) = st.load(&args[0])? else {
        return Err(err("ERR no such key"));
    };
    if id < s.max_deleted_id {
        return Err(err(
            "ERR The ID specified in XSETID is smaller than current max_deleted_entry_id",
        ));
    }
    if s.length > 0 {
        if id < s.last_valid_id() {
            return Err(err(
                "ERR The ID specified in XSETID is smaller than the target stream top item",
            ));
        }
        if entries_added != -1 && s.length > entries_added as u64 {
            return Err(err(
                "ERR The entries_added specified in XSETID is smaller than the target stream length",
            ));
        }
    }
    s.last_id = id;
    if entries_added != -1 {
        s.entries_added = entries_added as u64;
    }
    if !max_xdel.is_zero() {
        s.max_deleted_id = max_xdel;
    }
    st.store(&args[0], &s)?;
    crate::notify::raise(crate::notify::STREAM, "xsetid", &args[0]);
    Ok(Response::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Default)]
    struct Mem(HashMap<Vec<u8>, Vec<u8>>);
    impl StreamStore for Mem {
        fn load(&mut self, key: &[u8]) -> Result<Option<Stream>, Response> {
            Ok(self
                .0
                .get(key)
                .map(|b| Stream::decode(b).expect("round-trips")))
        }
        fn store(&mut self, key: &[u8], s: &Stream) -> Result<(), Response> {
            self.0.insert(key.to_vec(), s.encode());
            Ok(())
        }
    }

    fn run(st: &mut Mem, argv: &[&str]) -> Response {
        let args: Vec<Vec<u8>> = argv[1..].iter().map(|s| s.as_bytes().to_vec()).collect();
        StreamCmd {
            name: argv[0].to_string(),
            args,
        }
        .run(st)
    }

    fn int(r: Response) -> i64 {
        match r {
            Response::Integer(n) => n,
            other => panic!("expected an integer, got {other:?}"),
        }
    }

    fn restored(payload: &[u8]) -> Stream {
        match crate::rdb::deserialize(payload) {
            Ok(crate::commands::KeyDump::Stream(v)) => Stream::decode(&v).expect("decodes"),
            _ => panic!("refused"),
        }
    }

    /// A stream through DUMP and RESTORE in Redis's format comes back as it
    /// went — entries, tombstones, node layout, groups, PELs, consumer times —
    /// and each node's accounted size is the size of the listpack written.
    #[test]
    fn the_rdb_form_round_trips() {
        let mut st = Mem::default();
        let cfg_entries = Cfg::current().node_max_entries.max(1);
        for i in 1..=(cfg_entries + 5) {
            let v = match i % 4 {
                0 => "-5000".to_string(),
                1 => "99999999999".to_string(),
                2 => "x".repeat(70),
                _ => i.to_string(),
            };
            let fields: &[&str] = if i % 3 == 0 {
                &["other", "f"]
            } else {
                &["a", "b"]
            };
            let id = format!("{i}-{}", i % 2);
            int_or_bulk(run(
                &mut st,
                &["XADD", "s", &id, fields[0], &v, fields[1], "007"],
            ));
        }
        run(&mut st, &["XDEL", "s", "2-0", "4-0"]);
        run(&mut st, &["XGROUP", "CREATE", "s", "g", "0"]);
        run(&mut st, &["XGROUP", "CREATE", "s", "h", "$"]);
        run(
            &mut st,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "alice",
                "COUNT",
                "3",
                "STREAMS",
                "s",
                ">",
            ],
        );
        run(
            &mut st,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "",
                "COUNT",
                "2",
                "STREAMS",
                "s",
                ">",
            ],
        );
        run(&mut st, &["XGROUP", "CREATECONSUMER", "s", "g", "bob"]);
        let before = st.0.get(b"s".as_slice()).unwrap().clone();
        let s = Stream::decode(&before).unwrap();
        assert!(s.nodes.len() > 1, "the test should span nodes");
        let payload = crate::rdb::serialize(&crate::commands::KeyDump::Stream(before.clone()));
        assert_eq!(restored(&payload).encode(), before);
    }

    fn int_or_bulk(r: Response) {
        assert!(!matches!(r, Response::Error(_)), "{r:?}");
    }

    /// Redis's own fixtures (stream-cgroups.tcl): a 5.0 payload, format 15,
    /// with tombstones and PEL entries for trimmed IDs, whose read counters
    /// and first ID are derived on load.
    #[test]
    fn a_redis_5_stream_payload_restores() {
        let s = restored(PAYLOAD_V5);
        assert_eq!(s.length, 2);
        assert_eq!(s.entries_added, 2);
        assert_eq!(s.max_deleted_id, StreamId::ZERO);
        assert_eq!(s.first_id, StreamId { ms: 5, seq: 0 });
        let g1 = &s.groups[b"g1".as_slice()];
        assert_eq!(g1.entries_read, 1);
        assert!(matches!(s.lag(g1), Value::Int(1)));
        assert_eq!(g1.pel.len(), 4);
        assert_eq!(g1.consumers[b"c11".as_slice()].pel.len(), 4);
        let g2 = &s.groups[b"g2".as_slice()];
        assert_eq!(g2.entries_read, 0);
        assert!(matches!(s.lag(g2), Value::Int(2)));
    }

    /// A 7.0 payload, format 19: the consumer's active time is its seen time.
    #[test]
    fn a_redis_7_0_stream_payload_restores() {
        let s = restored(PAYLOAD_V7_0);
        let c = &s.groups[b"g".as_slice()].consumers[b"Alice".as_slice()];
        assert_eq!(c.seen_time, c.active_time);
        assert_eq!(s.length, 1);
        // Written back, the node's listpack is the very bytes Redis wrote:
        // after the type byte and the 16-byte node key, a 29-byte string.
        let redis_lp = &PAYLOAD_V7_0[20..49];
        assert_eq!(PAYLOAD_V7_0[19] as usize, redis_lp.len());
        let ours = crate::rdb::serialize(&crate::commands::KeyDump::Stream(s.encode()));
        assert!(ours.windows(redis_lp.len()).any(|w| w == redis_lp));
    }

    /// A consumer claiming an ID the group's PEL lacks is refused, as Redis
    /// refuses it ("Consumer entry not found in group global PEL").
    #[test]
    fn contradictory_stream_payloads_are_refused() {
        let mut st = Mem::default();
        run(&mut st, &["XADD", "s", "1-0", "a", "1"]);
        run(&mut st, &["XGROUP", "CREATE", "s", "g", "0"]);
        run(
            &mut st,
            &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
        );
        let s = Stream::decode(st.0.get(b"s".as_slice()).unwrap()).unwrap();
        let mut img = s.to_rdb();
        img.groups[0].consumers[0]
            .pel
            .push(StreamId { ms: 9, seq: 9 });
        assert!(Stream::from_rdb(img).is_none());
        let mut img = s.to_rdb();
        img.groups[0].consumers[0].pel.clear();
        assert!(
            Stream::from_rdb(img).is_none(),
            "a PEL entry with no consumer"
        );
        let mut img = s.to_rdb();
        img.length = 2;
        assert!(Stream::from_rdb(img).is_none());
    }

    const PAYLOAD_V5: &[u8] = b"\x0F\x01\x10\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\xC3\x40\x4A\x40\x57\x16\x57\x00\x00\x00\x23\x00\x02\x01\x04\x01\x01\x01\x84\x64\x61\x74\x61\x05\x00\x01\x03\x01\x00\x20\x01\x03\x81\x61\x02\x04\x20\x0A\x00\x01\x40\x0A\x00\x62\x60\x0A\x00\x02\x40\x0A\x00\x63\x60\x0A\x40\x22\x01\x81\x64\x20\x0A\x40\x39\x20\x0A\x00\x65\x60\x0A\x00\x05\x40\x0A\x00\x66\x20\x0A\x00\xFF\x02\x06\x00\x02\x02\x67\x31\x05\x00\x04\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x3E\xF7\x83\x43\x7A\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x3E\xF7\x83\x43\x7A\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x3E\xF7\x83\x43\x7A\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x3E\xF7\x83\x43\x7A\x01\x00\x00\x01\x01\x03\x63\x31\x31\x3E\xF7\x83\x43\x7A\x01\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x02\x67\x32\x00\x00\x00\x00\x09\x00\x3D\x52\xEF\x68\x67\x52\x1D\xFA";
    const PAYLOAD_V7_0: &[u8] = b"\x13\x01\x10\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x1D\x1D\x00\x00\x00\x0A\x00\x01\x01\x00\x01\x01\x01\x81\x66\x02\x00\x01\x02\x01\x00\x01\x00\x01\x81\x76\x02\x04\x01\xFF\x01\x01\x01\x01\x01\x00\x00\x01\x01\x01\x67\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\xF5\x5A\x71\xC7\x84\x01\x00\x00\x01\x01\x05\x41\x6C\x69\x63\x65\xF5\x5A\x71\xC7\x84\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x0B\x00\xA7\xA9\x14\xA5\x27\xFF\x9B\x9B";

    #[test]
    fn ids_parse_like_redis() {
        assert_eq!(
            parse_id(b"5", 7, false, false).unwrap().0,
            StreamId { ms: 5, seq: 7 }
        );
        assert_eq!(
            parse_id(b"5-*", 0, true, true).unwrap(),
            (StreamId { ms: 5, seq: 0 }, false)
        );
        assert!(parse_id(b"-", 0, true, false).is_err());
        assert!(parse_id(b"1-x", 0, false, false).is_err());
        assert_eq!(
            interval(b"(1-1", 0).unwrap(),
            (StreamId { ms: 1, seq: 1 }, true)
        );
    }

    #[test]
    fn xadd_xrange_xdel_and_trim() {
        let mut st = Mem::default();
        for i in 1..=5 {
            run(&mut st, &["XADD", "s", &format!("{i}-0"), "f", "v"]);
        }
        assert!(
            matches!(run(&mut st, &["XADD", "s", "3-0", "f", "v"]), Response::Error(e) if e.contains("equal or smaller"))
        );
        assert!(
            matches!(run(&mut st, &["XADD", "s", "0-0", "f", "v"]), Response::Error(e) if e.contains("greater than 0-0"))
        );
        assert_eq!(int(run(&mut st, &["XLEN", "s"])), 5);
        assert_eq!(int(run(&mut st, &["XDEL", "s", "2-0", "9-0"])), 1);
        assert_eq!(int(run(&mut st, &["XTRIM", "s", "MAXLEN", "2"])), 2);
        let Response::Value(Value::Array(es)) = run(&mut st, &["XRANGE", "s", "-", "+"]) else {
            panic!()
        };
        assert_eq!(es.len(), 2);
        assert_eq!(
            es[0],
            entry_value(&Entry {
                id: StreamId { ms: 4, seq: 0 },
                fields: vec![(b"f".to_vec(), b"v".to_vec())],
                deleted: false
            })
        );
        // ms-* takes the next sequence of the same millisecond.
        let r = run(&mut st, &["XADD", "s", "5-*", "f", "v"]);
        assert!(matches!(r, Response::BulkString(ref b) if b == b"5-1"));
    }

    #[test]
    fn approximate_trim_keeps_whole_nodes() {
        let mut st = Mem::default();
        // stream-node-max-entries defaults to 100; 250 entries is 3 nodes.
        for i in 1..=250 {
            run(&mut st, &["XADD", "s", &format!("{i}-0"), "f", "v"]);
        }
        assert_eq!(
            int(run(&mut st, &["XTRIM", "s", "MAXLEN", "~", "120"])),
            100
        );
        assert_eq!(int(run(&mut st, &["XLEN", "s"])), 150);
    }

    #[test]
    fn groups_deliver_ack_and_claim() {
        let mut st = Mem::default();
        run(&mut st, &["XADD", "s", "1-0", "a", "1"]);
        run(&mut st, &["XADD", "s", "2-0", "b", "2"]);
        assert!(matches!(
            run(&mut st, &["XGROUP", "CREATE", "s", "g", "0"]),
            Response::Ok
        ));
        assert!(
            matches!(run(&mut st, &["XGROUP", "CREATE", "s", "g", "0"]), Response::Error(e) if e.starts_with("BUSYGROUP"))
        );
        let r = run(
            &mut st,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "alice",
                "COUNT",
                "1",
                "STREAMS",
                "s",
                ">",
            ],
        );
        assert!(matches!(r, Response::Value(_)));
        let Response::Value(Value::Array(p)) = run(&mut st, &["XPENDING", "s", "g"]) else {
            panic!()
        };
        assert_eq!(p[0], Value::Int(1));
        assert_eq!(int(run(&mut st, &["XACK", "s", "g", "1-0"])), 1);
        run(
            &mut st,
            &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", ">"],
        );
        let Response::Value(Value::Array(c)) =
            run(&mut st, &["XCLAIM", "s", "g", "bob", "0", "2-0", "JUSTID"])
        else {
            panic!()
        };
        assert_eq!(c, vec![StreamId { ms: 2, seq: 0 }.value()]);
        let Response::Value(Value::Array(g)) = run(&mut st, &["XINFO", "GROUPS", "s"]) else {
            panic!()
        };
        let Value::Map(m) = &g[0] else { panic!() };
        assert_eq!(m[5].1, Value::Int(0)); // lag
    }
}
