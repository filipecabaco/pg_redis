//! Redis's `DUMP`/`RESTORE` payload format: an RDB-serialised value, a
//! two-byte format version, and a CRC-64 of everything before it.
//!
//! The two directions are deliberately asymmetric. What we *emit* is the
//! narrowest set of encodings a real Redis 7.2 accepts — raw strings, plain
//! sets and hashes, binary-double sorted sets, lists as quicklists of plain
//! nodes, and streams as Redis lays them out — because a payload only has to
//! be valid, not identical to the one Redis itself would build. Byte identity
//! is not even possible: a hashtable-backed set serialises in iteration
//! order, which is seeded per process. What we *accept* is wider: everything
//! 7.2 emits, which for small collections means listpacks, plus intsets,
//! LZF-compressed and integer-packed strings, the ASCII-double sorted sets of
//! older servers, and the stream formats of 5.0 and 7.0.
//!
//! The version footer is 11 — what Redis 7.2.5, the version this server
//! reports, writes — and anything newer is refused, exactly as 7.2.5 refuses
//! it. So a payload from a 7.4 server fails here with the same error it would
//! get replaying into 7.2, and one of ours restores into a real 7.2
//! unmodified. Formats 7.2 accepts only for pre-7 compatibility — ziplists,
//! zipmaps, v1 quicklists — are refused as a bad format rather than
//! half-supported; the error is honest and the encodings have not been
//! written by any server since 6.x.
#![forbid(unsafe_code)]

use crate::commands::KeyDump;

/// What Redis 7.2.5 stamps on every payload it emits, and the newest this
/// module admits.
const RDB_VERSION: u16 = 11;

// The object-type bytes this module speaks, by their names in rdb.h.
const TYPE_STRING: u8 = 0;
/// The type byte earlier pg_redis builds wrote for a stream, unused by
/// Redis: the payload is the stream's own serialised value (`stream.rs`).
/// Still read, so those payloads restore; no longer written.
const TYPE_PG_REDIS_STREAM: u8 = 0xF5;
const TYPE_SET: u8 = 2;
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;
const TYPE_ZSET_2: u8 = 5;
const TYPE_SET_INTSET: u8 = 11;
const TYPE_STREAM_LISTPACKS: u8 = 15;
const TYPE_HASH_LISTPACK: u8 = 16;
const TYPE_ZSET_LISTPACK: u8 = 17;
const TYPE_LIST_QUICKLIST_2: u8 = 18;
const TYPE_STREAM_LISTPACKS_2: u8 = 19;
const TYPE_SET_LISTPACK: u8 = 20;
const TYPE_STREAM_LISTPACKS_3: u8 = 21;

const QUICKLIST_NODE_PLAIN: u64 = 1;
const QUICKLIST_NODE_PACKED: u64 = 2;

/// The two ways `RESTORE` refuses a payload, named by the reply each one
/// earns. Redis distinguishes a malformed object from a bad envelope, and a
/// client retrying the right one depends on the distinction.
pub enum RestoreError {
    /// "DUMP payload version or checksum are wrong"
    Envelope,
    /// "Bad data format"
    Format,
}

// ───────────────────────────── CRC-64 ───────────────────────────────────────

/// CRC-64/XZ as Redis computes it: the Jones polynomial, reflected, no final
/// xor. The table form of what crc64.c unrolls.
static CRC64_TABLE: std::sync::LazyLock<[u64; 256]> = std::sync::LazyLock::new(|| {
    // 0xad93d23594c935a9 bit-reversed, as the reflected algorithm wants it.
    const POLY: u64 = 0x95ac9329ac4bc9b5;
    let mut table = [0u64; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
});

fn crc64(data: &[u8]) -> u64 {
    let mut crc = 0u64;
    for &b in data {
        crc = CRC64_TABLE[((crc ^ b as u64) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

// ───────────────────────────── Writing ──────────────────────────────────────

/// RDB length encoding: 6 bits in one byte, 14 in two, else a marker byte and
/// a big-endian word.
fn write_len(out: &mut Vec<u8>, n: u64) {
    if n < 1 << 6 {
        out.push(n as u8);
    } else if n < 1 << 14 {
        out.push(0x40 | (n >> 8) as u8);
        out.push(n as u8);
    } else if n <= u32::MAX as u64 {
        out.push(0x80);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    } else {
        out.push(0x81);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

/// Always the raw form. Redis packs small integers and LZF-compresses long
/// values; both are optimisations of size, not of validity, and every reader
/// accepts the raw spelling.
fn write_string(out: &mut Vec<u8>, s: &[u8]) {
    write_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// A stream ID as the radix tree keys it, and as PELs store it: 128 bits,
/// big-endian, so byte order is ID order.
fn write_id(out: &mut Vec<u8>, id: crate::stream::StreamId) {
    out.extend_from_slice(&id.ms.to_be_bytes());
    out.extend_from_slice(&id.seq.to_be_bytes());
}

/// `rdbSaveObject`'s stream branch, format 11: the nodes as (master ID,
/// listpack) pairs, the metadata, then each group with its PEL and its
/// consumers, each consumer with the IDs it owns.
fn write_stream(out: &mut Vec<u8>, s: &crate::stream::RdbStream) {
    write_len(out, s.nodes.len() as u64);
    for n in &s.nodes {
        let mut key = Vec::with_capacity(16);
        write_id(&mut key, n.master);
        write_string(out, &key);
        write_string(out, &listpack_build(&n.elements));
    }
    write_len(out, s.length);
    write_len(out, s.last_id.ms);
    write_len(out, s.last_id.seq);
    let (first, max_deleted, added) = s.meta.unwrap_or_default();
    write_len(out, first.ms);
    write_len(out, first.seq);
    write_len(out, max_deleted.ms);
    write_len(out, max_deleted.seq);
    write_len(out, added);
    write_len(out, s.groups.len() as u64);
    for g in &s.groups {
        write_string(out, &g.name);
        write_len(out, g.last_id.ms);
        write_len(out, g.last_id.seq);
        write_len(out, g.entries_read.unwrap_or(u64::MAX));
        write_len(out, g.pel.len() as u64);
        for (id, delivery_time, delivery_count) in &g.pel {
            write_id(out, *id);
            out.extend_from_slice(&delivery_time.to_le_bytes());
            write_len(out, *delivery_count);
        }
        write_len(out, g.consumers.len() as u64);
        for c in &g.consumers {
            write_string(out, &c.name);
            out.extend_from_slice(&c.seen_time.to_le_bytes());
            out.extend_from_slice(&c.active_time.unwrap_or(c.seen_time).to_le_bytes());
            write_len(out, c.pel.len() as u64);
            for id in &c.pel {
                write_id(out, *id);
            }
        }
    }
}

/// The integer a listpack stores a string as, when it spells one exactly as
/// `lpStringToInt64` demands: no sign but `-`, no leading zero, no `-0`.
fn lp_int_value(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    let t = std::str::from_utf8(s).ok()?;
    let digits = t.strip_prefix('-').unwrap_or(t);
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if (digits.len() > 1 && digits.starts_with('0')) || t == "-0" {
        return None;
    }
    t.parse().ok()
}

/// The bytes `lpEncodeBacklen` spends on an entry of `l` bytes.
fn backlen_size(l: usize) -> usize {
    match l {
        0..=127 => 1,
        128..=16382 => 2,
        16383..=2097150 => 3,
        2097151..=268435454 => 4,
        _ => 5,
    }
}

/// A listpack of `elements`, each in the encoding `lpAppend` picks for it:
/// integers in the narrowest width that holds them, strings behind the
/// shortest length prefix, every entry followed by its own length.
fn listpack_build(elements: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::new();
    for e in elements {
        let start = body.len();
        match lp_int_value(e) {
            Some(v) if (0..=127).contains(&v) => body.push(v as u8),
            Some(v) if (-4096..=4095).contains(&v) => {
                let u = (v as u16) & 0x1fff;
                body.push(0xc0 | (u >> 8) as u8);
                body.push(u as u8);
            }
            Some(v) if i16::try_from(v).is_ok() => {
                body.push(0xf1);
                body.extend_from_slice(&(v as i16).to_le_bytes());
            }
            Some(v) if (-(1 << 23)..(1 << 23)).contains(&v) => {
                body.push(0xf2);
                body.extend_from_slice(&(v as i32).to_le_bytes()[..3]);
            }
            Some(v) if i32::try_from(v).is_ok() => {
                body.push(0xf3);
                body.extend_from_slice(&(v as i32).to_le_bytes());
            }
            Some(v) => {
                body.push(0xf4);
                body.extend_from_slice(&v.to_le_bytes());
            }
            None if e.len() < 64 => {
                body.push(0x80 | e.len() as u8);
                body.extend_from_slice(e);
            }
            None if e.len() < 4096 => {
                body.push(0xe0 | (e.len() >> 8) as u8);
                body.push(e.len() as u8);
                body.extend_from_slice(e);
            }
            None => {
                body.push(0xf0);
                body.extend_from_slice(&(e.len() as u32).to_le_bytes());
                body.extend_from_slice(e);
            }
        }
        let l = body.len() - start;
        let n = backlen_size(l);
        for i in (0..n).rev() {
            let b = ((l >> (7 * i)) & 127) as u8;
            body.push(if i == n - 1 { b } else { b | 128 });
        }
    }
    let mut out = Vec::with_capacity(body.len() + 7);
    out.extend_from_slice(&((body.len() + 7) as u32).to_le_bytes());
    out.extend_from_slice(&(elements.len().min(u16::MAX as usize) as u16).to_le_bytes());
    out.extend_from_slice(&body);
    out.push(0xff);
    out
}

/// One key's value as a `RESTORE`-ready payload: object, version, CRC.
pub fn serialize(dump: &KeyDump) -> Vec<u8> {
    let mut out = Vec::new();
    match dump {
        KeyDump::String(v) => {
            out.push(TYPE_STRING);
            write_string(&mut out, v);
        }
        // A quicklist whose every node is plain — the one list shape a 7.0
        // loader takes without a listpack in it.
        KeyDump::List(items) => {
            out.push(TYPE_LIST_QUICKLIST_2);
            write_len(&mut out, items.len() as u64);
            for item in items {
                write_len(&mut out, QUICKLIST_NODE_PLAIN);
                write_string(&mut out, item);
            }
        }
        KeyDump::Set(items) => {
            out.push(TYPE_SET);
            write_len(&mut out, items.len() as u64);
            for item in items {
                write_string(&mut out, item);
            }
        }
        KeyDump::Hash(pairs) => {
            out.push(TYPE_HASH);
            write_len(&mut out, pairs.len() as u64);
            for (f, v) in pairs {
                write_string(&mut out, f);
                write_string(&mut out, v);
            }
        }
        KeyDump::Stream(v) => {
            out.push(TYPE_STREAM_LISTPACKS_3);
            let s = crate::stream::Stream::decode(v).expect("a stored stream decodes");
            write_stream(&mut out, &s.to_rdb());
        }
        KeyDump::Zset(pairs) => {
            out.push(TYPE_ZSET_2);
            write_len(&mut out, pairs.len() as u64);
            for (m, score) in pairs {
                write_string(&mut out, m);
                out.extend_from_slice(&score.to_le_bytes());
            }
        }
    }
    out.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let crc = crc64(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

// ───────────────────────────── Reading ──────────────────────────────────────

/// A cursor over the payload body. Every read is bounds-checked and a `None`
/// anywhere becomes `RestoreError::Format` at the top — the payload passed
/// its checksum, so a truncated object is malformed, not corrupt.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

/// An RDB length is either a number or a marker that the bytes that follow
/// are one of the special string encodings.
enum Len {
    Plain(u64),
    Special(u8),
}

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Option<u8> {
        let b = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.data.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }

    fn len(&mut self) -> Option<Len> {
        let b = self.u8()?;
        match b >> 6 {
            0 => Some(Len::Plain(b as u64 & 0x3f)),
            1 => {
                let lo = self.u8()?;
                Some(Len::Plain(((b as u64 & 0x3f) << 8) | lo as u64))
            }
            2 => match b {
                0x80 => Some(Len::Plain(
                    u32::from_be_bytes(self.take(4)?.try_into().ok()?) as u64,
                )),
                0x81 => Some(Len::Plain(u64::from_be_bytes(
                    self.take(8)?.try_into().ok()?,
                ))),
                _ => None,
            },
            _ => Some(Len::Special(b & 0x3f)),
        }
    }

    fn plain_len(&mut self) -> Option<u64> {
        match self.len()? {
            Len::Plain(n) => Some(n),
            Len::Special(_) => None,
        }
    }

    /// A string object: raw, a packed integer, or an LZF-compressed run.
    fn string(&mut self) -> Option<Vec<u8>> {
        match self.len()? {
            Len::Plain(n) => Some(self.take(n as usize)?.to_vec()),
            Len::Special(enc) => match enc {
                0 => Some(itoa(self.u8()? as i8 as i64)),
                1 => Some(itoa(
                    i16::from_le_bytes(self.take(2)?.try_into().ok()?) as i64
                )),
                2 => Some(itoa(
                    i32::from_le_bytes(self.take(4)?.try_into().ok()?) as i64
                )),
                3 => {
                    let clen = self.plain_len()? as usize;
                    let ulen = self.plain_len()? as usize;
                    lzf_decompress(self.take(clen)?, ulen)
                }
                _ => None,
            },
        }
    }

    /// The ASCII double of `RDB_TYPE_ZSET`: one length byte, three sentinel
    /// values for the non-finite cases, digits otherwise.
    fn ascii_double(&mut self) -> Option<f64> {
        match self.u8()? {
            255 => Some(f64::NEG_INFINITY),
            254 => Some(f64::INFINITY),
            253 => Some(f64::NAN),
            n => std::str::from_utf8(self.take(n as usize)?)
                .ok()?
                .parse()
                .ok(),
        }
    }

    fn binary_double(&mut self) -> Option<f64> {
        Some(f64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    /// `rdbLoadMillisecondTime`: eight little-endian bytes.
    fn millis(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn raw_id(&mut self) -> Option<crate::stream::StreamId> {
        let b = self.take(16)?;
        Some(crate::stream::StreamId {
            ms: u64::from_be_bytes(b[..8].try_into().ok()?),
            seq: u64::from_be_bytes(b[8..].try_into().ok()?),
        })
    }

    fn len_id(&mut self) -> Option<crate::stream::StreamId> {
        Some(crate::stream::StreamId {
            ms: self.plain_len()?,
            seq: self.plain_len()?,
        })
    }

    /// `rdbLoadObject`'s stream branch, for any of the three formats: 15
    /// lacks the deletion metadata and the group read counters, and 15 and
    /// 19 the consumers' active times.
    fn stream(&mut self, t: u8) -> Option<crate::stream::RdbStream> {
        use crate::stream::{RdbConsumer, RdbGroup, RdbNode, RdbStream};
        let v2 = t >= TYPE_STREAM_LISTPACKS_2;
        let mut nodes = Vec::new();
        for _ in 0..self.plain_len()? {
            let key = self.string()?;
            if key.len() != 16 {
                return None;
            }
            let master = Reader { data: &key, pos: 0 }.raw_id()?;
            let lp = self.string()?;
            let elements = listpack_entries(&lp)?;
            if elements.is_empty() {
                return None;
            }
            nodes.push(RdbNode {
                master,
                elements,
                bytes: lp.len(),
            });
        }
        let length = self.plain_len()?;
        let last_id = self.len_id()?;
        let meta = if v2 {
            Some((self.len_id()?, self.len_id()?, self.plain_len()?))
        } else {
            None
        };
        let mut groups = Vec::new();
        for _ in 0..self.plain_len()? {
            let name = self.string()?;
            let last_id = self.len_id()?;
            let entries_read = if v2 { Some(self.plain_len()?) } else { None };
            let mut pel = Vec::new();
            for _ in 0..self.plain_len()? {
                pel.push((self.raw_id()?, self.millis()?, self.plain_len()?));
            }
            let mut consumers = Vec::new();
            for _ in 0..self.plain_len()? {
                let name = self.string()?;
                let seen_time = self.millis()?;
                let active_time = if t >= TYPE_STREAM_LISTPACKS_3 {
                    Some(self.millis()?)
                } else {
                    None
                };
                let mut ids = Vec::new();
                for _ in 0..self.plain_len()? {
                    ids.push(self.raw_id()?);
                }
                consumers.push(RdbConsumer {
                    name,
                    seen_time,
                    active_time,
                    pel: ids,
                });
            }
            groups.push(RdbGroup {
                name,
                last_id,
                entries_read,
                pel,
                consumers,
            });
        }
        Some(RdbStream {
            nodes,
            length,
            last_id,
            meta,
            groups,
        })
    }
}

fn itoa(n: i64) -> Vec<u8> {
    n.to_string().into_bytes()
}

/// LibLZF's decompressor, which is all of the format Redis uses: a control
/// byte under 32 is a literal run of that many plus one bytes, anything else
/// is a back-reference into what has already been produced.
fn lzf_decompress(input: &[u8], expected: usize) -> Option<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(expected);
    let mut i = 0usize;
    while i < input.len() {
        let ctrl = input[i] as usize;
        i += 1;
        if ctrl < 32 {
            let n = ctrl + 1;
            out.extend_from_slice(input.get(i..i + n)?);
            i += n;
        } else {
            let mut len = ctrl >> 5;
            if len == 7 {
                len += *input.get(i)? as usize;
                i += 1;
            }
            let back = ((ctrl & 0x1f) << 8) + *input.get(i)? as usize + 1;
            i += 1;
            let start = out.len().checked_sub(back)?;
            // Byte at a time on purpose: a reference may overlap the bytes
            // it is producing, which is how LZF spells a run.
            for j in 0..len + 2 {
                let b = *out.get(start + j)?;
                out.push(b);
            }
        }
    }
    (out.len() == expected).then_some(out)
}

/// Every element of a listpack, integers rendered back to their decimal
/// spelling — which is what they abbreviate.
fn listpack_entries(blob: &[u8]) -> Option<Vec<Vec<u8>>> {
    // 4 bytes total-bytes, 2 bytes element count, entries, 0xFF.
    let total = u32::from_le_bytes(blob.get(0..4)?.try_into().ok()?) as usize;
    if total != blob.len() {
        return None;
    }
    let mut out = Vec::new();
    let mut p = 6usize;
    loop {
        let b = *blob.get(p)?;
        if b == 0xff {
            break;
        }
        // (element, bytes the encoding and payload took)
        let (element, entry_len) = if b & 0x80 == 0 {
            (itoa(b as i64), 1)
        } else if b & 0xc0 == 0x80 {
            let n = (b & 0x3f) as usize;
            (blob.get(p + 1..p + 1 + n)?.to_vec(), 1 + n)
        } else if b & 0xe0 == 0xc0 {
            let raw = (((b & 0x1f) as i64) << 8) | *blob.get(p + 1)? as i64;
            // Sign-extend from 13 bits.
            (itoa((raw << 51) >> 51), 2)
        } else {
            match b {
                0xf1 => (
                    itoa(i16::from_le_bytes(blob.get(p + 1..p + 3)?.try_into().ok()?) as i64),
                    3,
                ),
                0xf2 => {
                    let s = blob.get(p + 1..p + 4)?;
                    let raw = (s[0] as i64) | (s[1] as i64) << 8 | (s[2] as i64) << 16;
                    (itoa((raw << 40) >> 40), 4)
                }
                0xf3 => (
                    itoa(i32::from_le_bytes(blob.get(p + 1..p + 5)?.try_into().ok()?) as i64),
                    5,
                ),
                0xf4 => (
                    itoa(i64::from_le_bytes(blob.get(p + 1..p + 9)?.try_into().ok()?)),
                    9,
                ),
                0xf0 => {
                    let n = u32::from_le_bytes(blob.get(p + 1..p + 5)?.try_into().ok()?) as usize;
                    (blob.get(p + 5..p + 5 + n)?.to_vec(), 5 + n)
                }
                _ if b & 0xf0 == 0xe0 => {
                    let n = (((b & 0x0f) as usize) << 8) | *blob.get(p + 1)? as usize;
                    (blob.get(p + 2..p + 2 + n)?.to_vec(), 2 + n)
                }
                _ => return None,
            }
        };
        out.push(element);
        // Entries end with their own length, so the list walks backwards
        // too; readers going forwards only have to skip it.
        p += entry_len + backlen_size(entry_len);
    }
    Some(out)
}

/// An intset: a fixed-width array of little-endian integers, back to their
/// decimal spellings.
fn intset_entries(blob: &[u8]) -> Option<Vec<Vec<u8>>> {
    let width = u32::from_le_bytes(blob.get(0..4)?.try_into().ok()?) as usize;
    let count = u32::from_le_bytes(blob.get(4..8)?.try_into().ok()?) as usize;
    if !matches!(width, 2 | 4 | 8) || blob.len() != 8 + width * count {
        return None;
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let s = &blob[8 + i * width..8 + (i + 1) * width];
        let v = match width {
            2 => i16::from_le_bytes(s.try_into().ok()?) as i64,
            4 => i32::from_le_bytes(s.try_into().ok()?) as i64,
            _ => i64::from_le_bytes(s.try_into().ok()?),
        };
        out.push(itoa(v));
    }
    Some(out)
}

fn pairs_of(mut items: Vec<Vec<u8>>) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    if !items.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(items.len() / 2);
    for [k, v] in items.as_chunks_mut::<2>().0 {
        out.push((std::mem::take(k), std::mem::take(v)));
    }
    Some(out)
}

fn scored_of(items: Vec<Vec<u8>>) -> Option<Vec<(Vec<u8>, f64)>> {
    pairs_of(items)?
        .into_iter()
        .map(|(m, s)| {
            let score = match s.as_slice() {
                b"inf" | b"+inf" => f64::INFINITY,
                b"-inf" => f64::NEG_INFINITY,
                b"nan" => f64::NAN,
                _ => std::str::from_utf8(&s).ok()?.parse().ok()?,
            };
            Some((m, score))
        })
        .collect()
}

fn object(r: &mut Reader<'_>) -> Option<KeyDump> {
    let dump = match r.u8()? {
        TYPE_STRING => KeyDump::String(r.string()?),
        TYPE_PG_REDIS_STREAM => {
            let v = r.string()?;
            crate::stream::Stream::decode(&v)?;
            KeyDump::Stream(v)
        }
        TYPE_SET => {
            let n = r.plain_len()?;
            let mut items = Vec::with_capacity(n.min(4096) as usize);
            for _ in 0..n {
                items.push(r.string()?);
            }
            KeyDump::Set(items)
        }
        TYPE_HASH => {
            let n = r.plain_len()?;
            let mut pairs = Vec::with_capacity(n.min(4096) as usize);
            for _ in 0..n {
                let f = r.string()?;
                let v = r.string()?;
                pairs.push((f, v));
            }
            KeyDump::Hash(pairs)
        }
        t @ (TYPE_ZSET | TYPE_ZSET_2) => {
            let n = r.plain_len()?;
            let mut pairs = Vec::with_capacity(n.min(4096) as usize);
            for _ in 0..n {
                let m = r.string()?;
                let score = match t {
                    TYPE_ZSET_2 => r.binary_double()?,
                    _ => r.ascii_double()?,
                };
                pairs.push((m, score));
            }
            KeyDump::Zset(pairs)
        }
        TYPE_SET_INTSET => KeyDump::Set(intset_entries(&r.string()?)?),
        TYPE_SET_LISTPACK => KeyDump::Set(listpack_entries(&r.string()?)?),
        t @ (TYPE_STREAM_LISTPACKS | TYPE_STREAM_LISTPACKS_2 | TYPE_STREAM_LISTPACKS_3) => {
            let s = crate::stream::Stream::from_rdb(r.stream(t)?)?;
            KeyDump::Stream(s.encode())
        }
        TYPE_HASH_LISTPACK => KeyDump::Hash(pairs_of(listpack_entries(&r.string()?)?)?),
        TYPE_ZSET_LISTPACK => KeyDump::Zset(scored_of(listpack_entries(&r.string()?)?)?),
        TYPE_LIST_QUICKLIST_2 => {
            let nodes = r.plain_len()?;
            let mut items = Vec::new();
            for _ in 0..nodes {
                match r.plain_len()? {
                    QUICKLIST_NODE_PLAIN => items.push(r.string()?),
                    QUICKLIST_NODE_PACKED => items.extend(listpack_entries(&r.string()?)?),
                    _ => return None,
                }
            }
            KeyDump::List(items)
        }
        _ => return None,
    };
    // The whole body must be consumed — trailing bytes mean the object lied
    // about its shape — and a collection with nothing in it is a key that
    // does not exist, which Redis also refuses to restore.
    if r.pos != r.data.len() {
        return None;
    }
    let empty = match &dump {
        KeyDump::String(_) => false,
        KeyDump::List(v) => v.is_empty(),
        KeyDump::Set(v) => v.is_empty(),
        KeyDump::Hash(v) => v.is_empty(),
        KeyDump::Zset(v) => v.is_empty(),
        KeyDump::Stream(_) => false,
    };
    (!empty).then_some(dump)
}

/// The reverse of `serialize`, over anything a Redis up to 7.2 emits.
pub fn deserialize(payload: &[u8]) -> Result<KeyDump, RestoreError> {
    if payload.len() < 11 {
        return Err(RestoreError::Envelope);
    }
    let body_end = payload.len() - 10;
    let version = u16::from_le_bytes(payload[body_end..body_end + 2].try_into().unwrap());
    let crc = u64::from_le_bytes(payload[body_end + 2..].try_into().unwrap());
    if version > RDB_VERSION || crc != crc64(&payload[..body_end + 2]) {
        return Err(RestoreError::Envelope);
    }
    let mut r = Reader {
        data: &payload[..body_end],
        pos: 0,
    };
    object(&mut r).ok_or(RestoreError::Format)
}

// ───────────────────────────── Functions ────────────────────────────────────

/// `RDB_OPCODE_FUNCTION2`: one library's code follows as a string.
const OPCODE_FUNCTION2: u8 = 245;
/// The opcode of the format 7.0's release candidates wrote.
const OPCODE_FUNCTION_PRE_GA: u8 = 246;

/// `FUNCTION DUMP`'s payload: each library's code behind its opcode, then the
/// version and CRC that close every DUMP payload.
pub fn functions_dump(codes: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for code in codes {
        out.push(OPCODE_FUNCTION2);
        write_string(&mut out, code);
    }
    out.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let crc = crc64(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// The library codes in a `FUNCTION RESTORE` payload, or the error Redis
/// answers with.
pub fn functions_restore(payload: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let envelope = || "ERR DUMP payload version or checksum are wrong".to_string();
    if payload.len() < 10 {
        return Err(envelope());
    }
    let body_end = payload.len() - 10;
    let version = u16::from_le_bytes(payload[body_end..body_end + 2].try_into().unwrap());
    let crc = u64::from_le_bytes(payload[body_end + 2..].try_into().unwrap());
    if version > RDB_VERSION || crc != crc64(&payload[..body_end + 2]) {
        return Err(envelope());
    }
    let mut r = Reader {
        data: &payload[..body_end],
        pos: 0,
    };
    let mut codes = Vec::new();
    while r.pos < body_end {
        match r.u8() {
            Some(OPCODE_FUNCTION2) => {}
            Some(OPCODE_FUNCTION_PRE_GA) => {
                return Err("ERR Pre-GA function format not supported".into());
            }
            Some(_) => return Err("ERR given type is not a function".into()),
            None => return Err("ERR can not read data type".into()),
        }
        codes.push(
            r.string()
                .ok_or("ERR failed loading the given functions payload")?,
        );
    }
    Ok(codes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn functions_payload_round_trips() {
        let codes = vec![b"#!lua name=a\n".to_vec(), vec![b'x'; 300]];
        assert_eq!(functions_restore(&functions_dump(&codes)).ok(), Some(codes));
        let mut bad = functions_dump(&[b"x".to_vec()]);
        bad[1] ^= 1;
        assert!(functions_restore(&bad).unwrap_err().contains("checksum"));
    }

    /// The check-value every CRC-64/XZ implementation agrees on. A wrong
    /// polynomial, direction or final xor all fail here.
    #[test]
    fn crc64_matches_the_reference_check_value() {
        assert_eq!(crc64(b"123456789"), 0xe9c6d914c4b8d9ca);
        assert_eq!(crc64(b""), 0);
    }

    fn round_trips(dump: KeyDump) -> KeyDump {
        match deserialize(&serialize(&dump)) {
            Ok(d) => d,
            Err(_) => panic!("round trip refused its own payload"),
        }
    }

    #[test]
    fn every_type_round_trips() {
        match round_trips(KeyDump::String(b"hello".to_vec())) {
            KeyDump::String(v) => assert_eq!(v, b"hello"),
            _ => panic!("came back as another type"),
        }
        let items = vec![b"a".to_vec(), b"".to_vec(), vec![0u8, 255, 1]];
        match round_trips(KeyDump::List(items.clone())) {
            KeyDump::List(v) => assert_eq!(v, items),
            _ => panic!("came back as another type"),
        }
        match round_trips(KeyDump::Set(items.clone())) {
            KeyDump::Set(v) => assert_eq!(v, items),
            _ => panic!("came back as another type"),
        }
        let pairs = vec![(b"f".to_vec(), b"v".to_vec()), (b"g".to_vec(), vec![])];
        match round_trips(KeyDump::Hash(pairs.clone())) {
            KeyDump::Hash(v) => assert_eq!(v, pairs),
            _ => panic!("came back as another type"),
        }
        let scored = vec![
            (b"m".to_vec(), 1.5),
            (b"n".to_vec(), f64::INFINITY),
            (b"o".to_vec(), -0.0),
        ];
        match round_trips(KeyDump::Zset(scored.clone())) {
            KeyDump::Zset(v) => assert_eq!(v, scored),
            _ => panic!("came back as another type"),
        }
    }

    #[test]
    fn a_flipped_bit_fails_the_checksum_and_a_newer_version_is_refused() {
        let mut payload = serialize(&KeyDump::String(b"x".to_vec()));
        payload[0] ^= 0x01;
        assert!(matches!(deserialize(&payload), Err(RestoreError::Envelope)));

        // Version 12 with its checksum recomputed: the envelope is sound and
        // still refused, exactly as 7.2.5 refuses payloads from 7.4.
        let mut payload = serialize(&KeyDump::String(b"x".to_vec()));
        let body_end = payload.len() - 10;
        payload[body_end] = 12;
        let crc = crc64(&payload[..body_end + 2]);
        let crc_at = body_end + 2;
        payload[crc_at..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(deserialize(&payload), Err(RestoreError::Envelope)));
    }

    #[test]
    fn truncated_and_trailing_bytes_are_a_bad_format() {
        // A set that claims three members and carries two.
        let mut body = vec![TYPE_SET, 3];
        for s in [b"a", b"b"] {
            body.push(1);
            body.extend_from_slice(s);
        }
        body.extend_from_slice(&RDB_VERSION.to_le_bytes());
        let crc = crc64(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        assert!(matches!(deserialize(&body), Err(RestoreError::Format)));

        // A sound string object with a spare byte after it.
        let mut body = vec![TYPE_STRING, 1, b'x', 0u8];
        body.extend_from_slice(&RDB_VERSION.to_le_bytes());
        let crc = crc64(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        assert!(matches!(deserialize(&body), Err(RestoreError::Format)));
    }

    #[test]
    fn an_empty_collection_is_a_bad_format() {
        let mut body = vec![TYPE_SET, 0];
        body.extend_from_slice(&RDB_VERSION.to_le_bytes());
        let crc = crc64(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        assert!(matches!(deserialize(&body), Err(RestoreError::Format)));
    }

    /// A listpack built by hand, one entry per encoding the reader knows.
    #[test]
    fn every_listpack_encoding_reads_back() {
        let mut entries: Vec<u8> = Vec::new();
        let mut push = |enc: &[u8]| {
            entries.extend_from_slice(enc);
            assert!(enc.len() <= 127);
            entries.push(enc.len() as u8);
        };
        push(&[5]); // 7-bit uint: "5"
        push(&[0x82, b'a', b'b']); // 6-bit string: "ab"
        push(&[0xc0 | 0x1f, 0xff]); // 13-bit int, all ones: "-1"
        push(&[0xe0, 3, b'x', b'y', b'z']); // 12-bit string: "xyz"
        push(&[0xf1, 0x2e, 0xfb]); // int16 LE: "-1234"
        push(&[0xf2, 0x00, 0x00, 0x80]); // int24 LE: "-8388608"
        push(&[0xf3, 0x40, 0xe2, 0x01, 0x00]); // int32 LE: "123456"
        push(&[0xf4, 0, 0, 0, 0, 0, 0, 0, 0x80]); // int64 LE: i64::MIN
        push(&[0xf0, 2, 0, 0, 0, b'h', b'i']); // 32-bit string: "hi"

        let mut blob = Vec::new();
        blob.extend_from_slice(&((6 + entries.len() + 1) as u32).to_le_bytes());
        blob.extend_from_slice(&9u16.to_le_bytes());
        blob.extend_from_slice(&entries);
        blob.push(0xff);

        let got = listpack_entries(&blob).expect("a well-formed listpack");
        let want: Vec<Vec<u8>> = [
            "5",
            "ab",
            "-1",
            "xyz",
            "-1234",
            "-8388608",
            "123456",
            &i64::MIN.to_string(),
        ]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .chain([b"hi".to_vec()])
        .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn intsets_read_back_at_every_width() {
        for (width, bytes, want) in [
            (2u32, vec![0x01u8, 0x00, 0xff, 0xff], vec!["1", "-1"]),
            (4, vec![0x40, 0xe2, 0x01, 0x00], vec!["123456"]),
            (8, (-2i64).to_le_bytes().to_vec(), vec!["-2"]),
        ] {
            let mut blob = Vec::new();
            blob.extend_from_slice(&width.to_le_bytes());
            blob.extend_from_slice(&(bytes.len() as u32 / width).to_le_bytes());
            blob.extend_from_slice(&bytes);
            let got = intset_entries(&blob).expect("a well-formed intset");
            let want: Vec<Vec<u8>> = want.iter().map(|s| s.as_bytes().to_vec()).collect();
            assert_eq!(got, want);
        }
    }

    #[test]
    fn lzf_literals_and_overlapping_references_decompress() {
        // "aaaaaaaaaa": literal 'a', then a reference of length 9 back 1 —
        // the overlapping-copy case. Length is spelled 7-in-the-control plus
        // an extension byte of 0, plus the format's implicit 2.
        let compressed = [0x00, b'a', 0xe0, 0x00, 0x00];
        assert_eq!(
            lzf_decompress(&compressed, 10),
            Some(b"aaaaaaaaaa".to_vec())
        );
        assert_eq!(lzf_decompress(&compressed, 11), None);
    }
}
