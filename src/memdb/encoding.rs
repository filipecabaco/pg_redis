//! Redis's object encodings for hashes, sets and sorted sets, as a state each
//! collection carries.
//!
//! Redis stores a small collection packed (a listpack, or an intset for a set
//! of integers) and converts it to a hash table or skiplist once it outgrows
//! a threshold. The conversion is one way: a collection that shrinks keeps
//! the encoding it grew into. The encoding is visible to clients twice over,
//! in `OBJECT ENCODING` and in the order elements come back: a listpack keeps
//! insertion order and deletes in place, an intset is sorted, and a hash
//! table's order is its own. So the state is kept per collection, by both
//! halves, and moves with the key.
//!
//! Every rule here is a transition taken from Redis 7.2's sources, named
//! after the function it follows. Nothing here stores anything: callers hold
//! the state and pass it in.
#![forbid(unsafe_code)]

use std::collections::HashMap;

use super::parse_stored_int;

/// A collection's encoding. The numbers are what the durable half stores in
/// `redis.encoding_N`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Enc {
    Listpack = 1,
    Intset = 2,
    Hashtable = 3,
    Skiplist = 4,
}

impl Enc {
    /// The name `OBJECT ENCODING` answers with.
    pub fn name(self) -> &'static str {
        match self {
            Enc::Listpack => "listpack",
            Enc::Intset => "intset",
            Enc::Hashtable => "hashtable",
            Enc::Skiplist => "skiplist",
        }
    }

    pub fn code(self) -> i16 {
        self as i16
    }

    pub fn from_code(c: i16) -> Option<Enc> {
        Some(match c {
            1 => Enc::Listpack,
            2 => Enc::Intset,
            3 => Enc::Hashtable,
            4 => Enc::Skiplist,
            _ => return None,
        })
    }

    /// Packed: `SCAN` returns all of it in one reply, and its order is fixed.
    pub fn is_packed(self) -> bool {
        matches!(self, Enc::Listpack | Enc::Intset)
    }
}

/// How many thresholds there are, and their `CONFIG` names, in the order
/// [`Limits::from_values`] takes them.
pub const LIMIT_COUNT: usize = 8;
pub const LIMIT_NAMES: [&str; LIMIT_COUNT] = [
    "hash-max-listpack-entries",
    "hash-max-listpack-value",
    "set-max-intset-entries",
    "set-max-listpack-entries",
    "set-max-listpack-value",
    "zset-max-listpack-entries",
    "zset-max-listpack-value",
    "list-max-listpack-size",
];

/// The thresholds, as `CONFIG SET` last left them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub hash_entries: usize,
    pub hash_value: usize,
    pub set_intset: usize,
    pub set_entries: usize,
    pub set_value: usize,
    pub zset_entries: usize,
    pub zset_value: usize,
    /// `list-max-listpack-size`: negative for a size class, positive for a
    /// count of elements.
    pub list_size: i64,
}

impl Limits {
    /// Redis 7.2's compiled-in defaults, which are what a server started
    /// without a config file runs with. (The sample `redis.conf` sets
    /// `hash-max-listpack-entries` to 128; the default is 512.)
    pub const DEFAULT: Limits = Limits {
        hash_entries: 512,
        hash_value: 64,
        set_intset: 512,
        set_entries: 128,
        set_value: 64,
        zset_entries: 128,
        zset_value: 64,
        list_size: -2,
    };

    pub fn from_values(v: [i64; LIMIT_COUNT]) -> Limits {
        let n = |x: i64| usize::try_from(x).unwrap_or(0);
        Limits {
            hash_entries: n(v[0]),
            hash_value: n(v[1]),
            set_intset: n(v[2]),
            set_entries: n(v[3]),
            set_value: n(v[4]),
            zset_entries: n(v[5]),
            zset_value: n(v[6]),
            list_size: v[7],
        }
    }

    /// The thresholds in force now.
    pub fn current() -> Limits {
        crate::server::enc_limits().map_or(Limits::DEFAULT, Limits::from_values)
    }

    /// `intsetMaxEntries`: an intset holds at most 2^30 whatever the setting.
    fn intset_max(&self) -> usize {
        self.set_intset.min(1 << 30)
    }
}

fn is_int(m: &[u8]) -> bool {
    parse_stored_int(m).is_some()
}

/// Sort an intset's members the way it keeps them: by value.
pub fn sort_ints<T>(items: &mut [T], member: impl Fn(&T) -> &[u8]) {
    items.sort_by_cached_key(|t| parse_stored_int(member(t)).unwrap_or(0));
}

// ───────────────────────────────── hashes ──────────────────────────────────

/// `hashTypeTryConversion`: before a write of `pairs` field-value pairs (as
/// the command named them, repeats included) whose lengths are `lens`.
pub fn hash_presize(
    enc: Enc,
    pairs: usize,
    lens: impl IntoIterator<Item = usize>,
    l: &Limits,
) -> Enc {
    if enc != Enc::Listpack {
        return enc;
    }
    if pairs > l.hash_entries || lens.into_iter().any(|n| n > l.hash_value) {
        Enc::Hashtable
    } else {
        Enc::Listpack
    }
}

/// `hashTypeSet`'s check once the hash holds `len` fields.
pub fn hash_grown(enc: Enc, len: usize, l: &Limits) -> Enc {
    if enc == Enc::Listpack && len > l.hash_entries {
        Enc::Hashtable
    } else {
        enc
    }
}

/// What loading a hash from a plain RDB payload gives: `len` fields, the
/// longest field or value `widest` bytes.
pub fn hash_shape(len: usize, widest: usize, l: &Limits) -> Enc {
    if len > l.hash_entries || widest > l.hash_value {
        Enc::Hashtable
    } else {
        Enc::Listpack
    }
}

// ────────────────────────────────── sets ───────────────────────────────────

/// `setTypeCreate`: a new set for a command adding `hint` members, the first
/// of them `first`.
pub fn set_create(first: &[u8], hint: usize, l: &Limits) -> Enc {
    if is_int(first) && hint <= l.set_intset {
        Enc::Intset
    } else if hint <= l.set_entries {
        Enc::Listpack
    } else {
        Enc::Hashtable
    }
}

/// `setTypeMaybeConvert`: an existing set about to have `hint` members added.
pub fn set_presize(enc: Enc, hint: usize, l: &Limits) -> Enc {
    match enc {
        Enc::Listpack if hint > l.set_entries => Enc::Hashtable,
        Enc::Intset if hint > l.set_intset => Enc::Hashtable,
        e => e,
    }
}

/// `setTypeAddAux`: the encoding once `member`, not already there, joins a
/// set of `len`. `widest` is the longest member as text — asked only when an
/// intset is converting, which is the one rule that measures its members.
pub fn set_add(
    enc: Enc,
    len: usize,
    member: &[u8],
    widest: impl FnOnce() -> usize,
    l: &Limits,
) -> Enc {
    match enc {
        Enc::Intset if is_int(member) => {
            if len + 1 > l.intset_max() {
                Enc::Hashtable
            } else {
                Enc::Intset
            }
        }
        Enc::Intset => {
            let fits = len < l.set_entries
                && member.len() <= l.set_value
                && (len == 0 || widest() <= l.set_value);
            if fits { Enc::Listpack } else { Enc::Hashtable }
        }
        Enc::Listpack => {
            if len < l.set_entries && member.len() <= l.set_value {
                Enc::Listpack
            } else {
                Enc::Hashtable
            }
        }
        _ => Enc::Hashtable,
    }
}

/// A set's encoding from its members alone: the encoding a set that never
/// converted is in. What the durable half reads for a set with no encoding
/// row.
pub fn set_natural<'a>(members: impl IntoIterator<Item = &'a [u8]>, l: &Limits) -> Enc {
    let (mut n, mut ints, mut widest) = (0usize, true, 0usize);
    for m in members {
        n += 1;
        ints &= is_int(m);
        widest = widest.max(m.len());
    }
    if ints && n <= l.intset_max() {
        Enc::Intset
    } else if n <= l.set_entries && widest <= l.set_value {
        Enc::Listpack
    } else {
        Enc::Hashtable
    }
}

/// A set being built the way Redis builds one: the result of `SUNION`,
/// `SDIFF` and `SINTERSTORE`, a set loaded from a payload, what `SPOP`
/// leaves. Members in the order the encoding keeps them.
pub struct SetBuilder {
    enc: Enc,
    items: Vec<Vec<u8>>,
    pos: HashMap<Vec<u8>, usize>,
    limits: Limits,
}

impl SetBuilder {
    pub fn new(enc: Enc, limits: Limits) -> SetBuilder {
        SetBuilder {
            enc,
            items: Vec::new(),
            pos: HashMap::new(),
            limits,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn enc(&self) -> Enc {
        self.enc
    }

    fn reindex(&mut self) {
        self.pos.clear();
        for (i, m) in self.items.iter().enumerate() {
            self.pos.insert(m.clone(), i);
        }
    }

    /// Add `m`; whether it was new.
    pub fn add(&mut self, m: &[u8]) -> bool {
        if self.pos.contains_key(m) {
            return false;
        }
        let items = &self.items;
        let widest = || items.iter().map(Vec::len).max().unwrap_or(0);
        let next = set_add(self.enc, self.items.len(), m, widest, &self.limits);
        if self.enc == Enc::Intset && next != Enc::Intset {
            // An intset converts in its own order: sorted.
            sort_ints(&mut self.items, |v| v);
            self.reindex();
        }
        self.enc = next;
        self.pos.insert(m.to_vec(), self.items.len());
        self.items.push(m.to_vec());
        true
    }

    /// Remove `m`; whether it was there. A listpack closes the gap, keeping
    /// the order of the rest.
    pub fn remove(&mut self, m: &[u8]) -> bool {
        let Some(i) = self.pos.remove(m) else {
            return false;
        };
        if self.enc == Enc::Hashtable {
            self.items.swap_remove(i);
            if let Some(moved) = self.items.get(i) {
                self.pos.insert(moved.clone(), i);
            }
        } else {
            self.items.remove(i);
            for p in self.pos.values_mut() {
                if *p > i {
                    *p -= 1;
                }
            }
        }
        true
    }

    /// `maybeConvertToIntset`: once every member is known to be an integer.
    pub fn intset_if_all_ints(&mut self) {
        if self.enc != Enc::Intset
            && self.items.len() <= self.limits.intset_max()
            && self.items.iter().all(|m| is_int(m))
        {
            self.enc = Enc::Intset;
        }
    }

    /// The members in the encoding's order, and the encoding.
    pub fn finish(mut self) -> (Vec<Vec<u8>>, Enc) {
        if self.enc == Enc::Intset {
            sort_ints(&mut self.items, |v| v);
        }
        (self.items, self.enc)
    }
}

/// What a set operation needs of a source set.
pub trait SetView {
    fn size(&self) -> usize;
    fn has(&self, m: &[u8]) -> bool;
    /// The members in the set's own order.
    fn members(&self) -> Vec<&[u8]>;
}

/// `sunionDiffGenericCommand`'s union: every member of every set, into a set
/// that starts as an intset. Missing keys are `None`.
pub fn union<S: SetView>(sets: &[Option<S>], l: Limits) -> SetBuilder {
    let mut out = SetBuilder::new(Enc::Intset, l);
    for s in sets.iter().flatten() {
        for m in s.members() {
            out.add(m);
        }
    }
    out
}

/// `sunionDiffGenericCommand`'s difference, with the algorithm Redis picks
/// by size: the first set filtered, or the first set with the others removed
/// from it. Both give the same members; they can leave different encodings.
/// `same_as_first` is whether a later key names the first set again, which
/// makes the result empty.
pub fn diff<S: SetView>(sets: &[Option<S>], same_as_first: bool, l: Limits) -> SetBuilder {
    let mut out = SetBuilder::new(Enc::Intset, l);
    let Some(Some(first)) = sets.first() else {
        return out;
    };
    if same_as_first {
        return out;
    }
    let rest: Vec<&S> = sets[1..].iter().flatten().collect();
    let present = 1 + rest.len();
    let one = first.size() * present / 2;
    let two: usize = first.size() + rest.iter().map(|s| s.size()).sum::<usize>();
    if one <= two {
        for m in first.members() {
            if !rest.iter().any(|s| s.has(m)) {
                out.add(m);
            }
        }
    } else {
        for m in first.members() {
            out.add(m);
        }
        for s in rest {
            for m in s.members() {
                out.remove(m);
            }
            if out.is_empty() {
                break;
            }
        }
    }
    out
}

/// The order `SINTER` takes its sets in: smallest first, ties in the order
/// the command named them. The first is the one it walks.
pub fn inter_order(sizes: &[usize]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..sizes.len()).collect();
    order.sort_by_key(|&i| sizes[i]);
    order
}

/// `SINTER`: the smallest set's members, in its order, that every other set
/// holds. Empty when a key is missing.
pub fn inter<S: SetView>(sets: &[Option<S>]) -> Vec<&[u8]> {
    let Some(sets) = sets.iter().map(Option::as_ref).collect::<Option<Vec<&S>>>() else {
        return Vec::new();
    };
    let order = inter_order(&sets.iter().map(|s| s.size()).collect::<Vec<_>>());
    let Some(&first) = order.first() else {
        return Vec::new();
    };
    sets[first]
        .members()
        .into_iter()
        .filter(|m| order[1..].iter().all(|&j| sets[j].has(m)))
        .collect()
}

/// `SINTERSTORE`'s result set: built in the order `SINTER` visits, starting
/// as an intset when the set walked is one and a listpack otherwise, and an
/// intset in the end if every member is an integer.
pub fn inter_store<'a>(
    walked: Enc,
    members: impl IntoIterator<Item = &'a [u8]>,
    l: Limits,
) -> SetBuilder {
    let start = if walked == Enc::Intset {
        Enc::Intset
    } else {
        Enc::Listpack
    };
    let mut out = SetBuilder::new(start, l);
    for m in members {
        out.add(m);
    }
    out.intset_if_all_ints();
    out
}

/// A set loaded from a plain RDB set payload of `len` members: an intset
/// that converts as members arrive, unless it is too long to start as one.
pub fn set_loaded<'a>(
    members: impl IntoIterator<Item = &'a [u8]>,
    len: usize,
    l: Limits,
) -> SetBuilder {
    let start = if len > l.intset_max() {
        Enc::Hashtable
    } else {
        Enc::Intset
    };
    let mut out = SetBuilder::new(start, l);
    for m in members {
        out.add(m);
    }
    out
}

// ─────────────────────────────── sorted sets ───────────────────────────────

/// `zsetTypeCreate`: a new sorted set for a command adding `hint` members,
/// the first of them `first_len` bytes long.
pub fn zset_create(hint: usize, first_len: usize, l: &Limits) -> Enc {
    if hint <= l.zset_entries && first_len <= l.zset_value {
        Enc::Listpack
    } else {
        Enc::Skiplist
    }
}

/// `zsetTypeMaybeConvert`: an existing sorted set about to take `hint`
/// members, new or not.
pub fn zset_presize(enc: Enc, hint: usize, l: &Limits) -> Enc {
    if enc == Enc::Listpack && hint > l.zset_entries {
        Enc::Skiplist
    } else {
        enc
    }
}

/// `zsetAdd`: a member of `member_len` bytes, not already there, joining a
/// sorted set of `len`.
pub fn zset_add(enc: Enc, len: usize, member_len: usize, l: &Limits) -> Enc {
    if enc == Enc::Listpack && (len + 1 > l.zset_entries || member_len > l.zset_value) {
        Enc::Skiplist
    } else {
        enc
    }
}

/// `zsetConvertToListpackIfNeeded`, and loading a plain payload: the
/// encoding a sorted set of `len` members, the longest `widest` bytes, is
/// built in.
pub fn zset_shape(len: usize, widest: usize, l: &Limits) -> Enc {
    if len <= l.zset_entries && widest <= l.zset_value {
        Enc::Listpack
    } else {
        Enc::Skiplist
    }
}

// ────────────────────────────────── lists ──────────────────────────────────

/// `optimization_level`: the byte limit of each negative
/// `list-max-listpack-size`.
const LIST_SIZE_LEVELS: [usize; 5] = [4096, 8192, 16384, 32768, 65536];
/// `SIZE_SAFETY_LIMIT`: the byte limit when the setting counts elements.
const LIST_SAFETY_LIMIT: usize = 8192;

/// A list's encoding from its elements: a listpack while it fits one
/// quicklist node, by bytes as a listpack would store them. Redis converts a
/// shrinking quicklist back only once it is down to one node of half the
/// limit, which this does not track.
pub fn list_encoding<'a>(items: impl IntoIterator<Item = &'a [u8]>, l: &Limits) -> &'static str {
    let (mut bytes, mut count) = (crate::rdb::LISTPACK_OVERHEAD, 0usize);
    for e in items {
        bytes += crate::rdb::listpack_entry_len(e);
        count += 1;
    }
    let fits = if l.list_size >= 0 {
        count <= (l.list_size.max(1) as usize) && bytes <= LIST_SAFETY_LIMIT
    } else {
        let level = (l.list_size.unsigned_abs() - 1).min(LIST_SIZE_LEVELS.len() as u64 - 1);
        bytes <= LIST_SIZE_LEVELS[level as usize]
    };
    if fits { "listpack" } else { "quicklist" }
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Limits = Limits::DEFAULT;

    struct V(Vec<Vec<u8>>);

    impl SetView for V {
        fn size(&self) -> usize {
            self.0.len()
        }
        fn has(&self, m: &[u8]) -> bool {
            self.0.iter().any(|x| x == m)
        }
        fn members(&self) -> Vec<&[u8]> {
            self.0.iter().map(Vec::as_slice).collect()
        }
    }

    fn v(items: &[&str]) -> V {
        V(items.iter().map(|s| s.as_bytes().to_vec()).collect())
    }

    fn strs(items: Vec<Vec<u8>>) -> Vec<String> {
        items
            .into_iter()
            .map(|b| String::from_utf8(b).unwrap())
            .collect()
    }

    #[test]
    fn hashes_convert_once_and_stay_converted() {
        let l = Limits::DEFAULT;
        assert_eq!(hash_presize(Enc::Listpack, 512, [64, 1], &l), Enc::Listpack);
        assert_eq!(hash_presize(Enc::Listpack, 513, [1], &l), Enc::Hashtable);
        assert_eq!(hash_presize(Enc::Listpack, 1, [65], &l), Enc::Hashtable);
        assert_eq!(hash_grown(Enc::Listpack, 512, &l), Enc::Listpack);
        assert_eq!(hash_grown(Enc::Listpack, 513, &l), Enc::Hashtable);
        // Shrinking never converts back.
        assert_eq!(hash_grown(Enc::Hashtable, 1, &l), Enc::Hashtable);
        assert_eq!(hash_presize(Enc::Hashtable, 1, [1], &l), Enc::Hashtable);
        assert_eq!(hash_shape(3, 65, &l), Enc::Hashtable);
    }

    #[test]
    fn sets_start_by_their_first_member_and_size_hint() {
        assert_eq!(set_create(b"7", 1, &L), Enc::Intset);
        assert_eq!(set_create(b"7", 513, &L), Enc::Hashtable);
        assert_eq!(set_create(b"a", 128, &L), Enc::Listpack);
        assert_eq!(set_create(b"a", 129, &L), Enc::Hashtable);
        // Not what string2ll takes.
        for m in ["007", "-0", "+1", " 1", "1.0", "9223372036854775808"] {
            assert_eq!(set_create(m.as_bytes(), 1, &L), Enc::Listpack, "{m}");
        }
        assert_eq!(set_presize(Enc::Listpack, 129, &L), Enc::Hashtable);
        assert_eq!(set_presize(Enc::Intset, 512, &L), Enc::Intset);
        assert_eq!(set_presize(Enc::Intset, 513, &L), Enc::Hashtable);
    }

    #[test]
    fn an_intset_takes_a_string_as_a_listpack_and_past_512_as_a_hashtable() {
        assert_eq!(set_add(Enc::Intset, 3, b"x", || 3, &L), Enc::Listpack);
        assert_eq!(set_add(Enc::Intset, 128, b"x", || 3, &L), Enc::Hashtable);
        assert_eq!(set_add(Enc::Intset, 511, b"1", || 3, &L), Enc::Intset);
        // Past 512 integers it skips the listpack.
        assert_eq!(set_add(Enc::Intset, 512, b"1", || 3, &L), Enc::Hashtable);
        assert_eq!(set_add(Enc::Listpack, 127, b"x", || 0, &L), Enc::Listpack);
        assert_eq!(set_add(Enc::Listpack, 128, b"x", || 0, &L), Enc::Hashtable);
        assert_eq!(
            set_add(Enc::Listpack, 1, &[b'x'; 65], || 0, &L),
            Enc::Hashtable
        );
        // The integers' width counts when the value limit is small.
        let narrow = Limits { set_value: 3, ..L };
        assert_eq!(
            set_add(Enc::Intset, 2, b"abc", || 4, &narrow),
            Enc::Hashtable
        );
        assert_eq!(
            set_add(Enc::Intset, 2, b"abc", || 3, &narrow),
            Enc::Listpack
        );
    }

    #[test]
    fn a_built_set_sorts_its_integers_then_appends() {
        let mut b = SetBuilder::new(Enc::Intset, L);
        for m in ["30", "10", "20"] {
            assert!(b.add(m.as_bytes()));
        }
        assert!(!b.add(b"10"));
        assert_eq!(b.enc(), Enc::Intset);
        b.add(b"x");
        b.add(b"5");
        assert_eq!(b.enc(), Enc::Listpack);
        assert!(b.remove(b"20"));
        let (items, enc) = b.finish();
        assert_eq!(
            (strs(items), enc),
            (
                strs(vec![
                    b"10".to_vec(),
                    b"30".to_vec(),
                    b"x".to_vec(),
                    b"5".to_vec()
                ]),
                Enc::Listpack
            )
        );
    }

    #[test]
    fn union_and_diff_follow_the_result_sets_encoding() {
        // Observed on Redis 7.2: SUNION of an intset and a listpack.
        let i = v(&["1", "3", "9"]);
        let a = v(&["c", "a", "b", "3", "1"]);
        let (items, enc) = union(&[Some(i), Some(a)], L).finish();
        assert_eq!(strs(items), ["1", "3", "9", "c", "a", "b"]);
        assert_eq!(enc, Enc::Listpack);
        // A listpack of integers comes back sorted: the result is an intset.
        let (items, enc) = union(&[Some(v(&["30", "10", "20"]))], L).finish();
        assert_eq!(
            (strs(items), enc),
            (
                strs(vec![b"10".to_vec(), b"20".to_vec(), b"30".to_vec()]),
                Enc::Intset
            )
        );
        let (items, _) = diff(
            &[
                Some(v(&["c", "a", "b", "3", "1"])),
                Some(v(&["b", "c", "z"])),
            ],
            false,
            L,
        )
        .finish();
        assert_eq!(strs(items), ["a", "3", "1"]);
        assert!(diff(&[Some(v(&["a"])), None], true, L).is_empty());
        assert!(diff::<V>(&[None, Some(v(&["a"]))], false, L).is_empty());
    }

    #[test]
    fn inter_walks_the_smallest_set_ties_in_argument_order() {
        let sets = [Some(v(&["c", "b", "a"])), Some(v(&["a", "b", "c"]))];
        assert_eq!(inter(&sets), [b"c" as &[u8], b"b", b"a"]);
        let sets = [Some(v(&["a", "b", "c", "d"])), Some(v(&["c", "b"]))];
        assert_eq!(inter(&sets), [b"c" as &[u8], b"b"]);
        assert!(inter(&[Some(v(&["a"])), None]).is_empty());
        // Stored, integers from a listpack become an intset.
        let (items, enc) = inter_store(Enc::Listpack, [b"30" as &[u8], b"10", b"20"], L).finish();
        assert_eq!(
            (strs(items), enc),
            (
                strs(vec![b"10".to_vec(), b"20".to_vec(), b"30".to_vec()]),
                Enc::Intset
            )
        );
    }

    #[test]
    fn sorted_sets_convert_by_size_and_member_length() {
        assert_eq!(zset_create(128, 64, &L), Enc::Listpack);
        assert_eq!(zset_create(129, 1, &L), Enc::Skiplist);
        assert_eq!(zset_create(1, 65, &L), Enc::Skiplist);
        assert_eq!(zset_presize(Enc::Listpack, 129, &L), Enc::Skiplist);
        assert_eq!(zset_add(Enc::Listpack, 127, 1, &L), Enc::Listpack);
        assert_eq!(zset_add(Enc::Listpack, 128, 1, &L), Enc::Skiplist);
        assert_eq!(zset_add(Enc::Skiplist, 1, 1, &L), Enc::Skiplist);
        assert_eq!(zset_shape(28, 3, &L), Enc::Listpack);
    }

    #[test]
    fn lists_are_listpacks_while_they_fit_a_node() {
        let short: Vec<Vec<u8>> = (0..200).map(|i| format!("a{i}").into_bytes()).collect();
        assert_eq!(
            list_encoding(short.iter().map(Vec::as_slice), &L),
            "listpack"
        );
        let long = vec![vec![b'x'; 100]; 90];
        assert_eq!(
            list_encoding(long.iter().map(Vec::as_slice), &L),
            "quicklist"
        );
        assert_eq!(
            list_encoding(long[..31].iter().map(Vec::as_slice), &L),
            "listpack"
        );
        // 1,000 integers of three bytes each fit.
        let ints = vec![b"1000000".to_vec(); 1000];
        assert_eq!(
            list_encoding(ints.iter().map(Vec::as_slice), &L),
            "listpack"
        );
        let counted = Limits { list_size: 3, ..L };
        assert_eq!(
            list_encoding(short[..3].iter().map(Vec::as_slice), &counted),
            "listpack"
        );
        assert_eq!(
            list_encoding(short[..4].iter().map(Vec::as_slice), &counted),
            "quicklist"
        );
    }
}
