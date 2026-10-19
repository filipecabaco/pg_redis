//! Redis's HyperLogLog, byte for byte.
//!
//! A HyperLogLog is a string whose bytes are Redis's own `hllhdr` layout — the
//! `HYLL` magic, an encoding byte, a cached cardinality — followed by either the
//! sparse run-length opcodes or the dense 6-bit registers. Keeping that layout
//! exactly, rather than any HyperLogLog, is what makes `GET`, `DUMP`,
//! `RESTORE` and a real Redis agree on the value, and what Redis's own tests
//! inspect. Ported from `hyperloglog.c` (Redis 7.2); the comments there apply.
#![forbid(unsafe_code)]

const HDR: usize = 16;
const P: u32 = 14;
const Q: u32 = 64 - P;
pub const REGISTERS: usize = 1 << P;
const P_MASK: u64 = REGISTERS as u64 - 1;
const BITS: usize = 6;
const REGISTER_MAX: u8 = (1 << BITS) - 1;
pub const DENSE_SIZE: usize = HDR + (REGISTERS * BITS).div_ceil(8);
const DENSE: u8 = 0;
const SPARSE: u8 = 1;
const ALPHA_INF: f64 = 0.721_347_520_444_481_7;

const SPARSE_VAL_MAX_VALUE: u8 = 32;
const SPARSE_VAL_MAX_LEN: usize = 4;
const SPARSE_ZERO_MAX_LEN: usize = 64;
const SPARSE_XZERO_MAX_LEN: usize = 16384;

/// Redis's `hll-sparse-max-bytes` default: past it, a sparse HLL turns dense.
pub const DEFAULT_SPARSE_MAX_BYTES: usize = 3000;

pub const INVALID: &str = "INVALIDOBJ Corrupted HLL object detected";
pub const NOT_HLL: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";

/// The HLL is corrupt: a run walks past the last register, or stops short.
#[derive(Debug, PartialEq)]
pub struct Corrupt;

fn is_zero(b: u8) -> bool {
    b & 0xc0 == 0
}
fn is_xzero(b: u8) -> bool {
    b & 0xc0 == 0x40
}
fn zero_len(b: u8) -> usize {
    (b & 0x3f) as usize + 1
}
fn xzero_len(b0: u8, b1: u8) -> usize {
    ((((b0 & 0x3f) as usize) << 8) | b1 as usize) + 1
}
fn val_value(b: u8) -> u8 {
    ((b >> 2) & 0x1f) + 1
}
fn val_len(b: u8) -> usize {
    (b & 0x3) as usize + 1
}
fn val_op(val: u8, len: usize) -> u8 {
    (((val - 1) << 2) | (len as u8 - 1)) | 0x80
}
fn zero_op(len: usize) -> u8 {
    (len - 1) as u8
}
fn xzero_op(len: usize) -> [u8; 2] {
    let l = len - 1;
    [((l >> 8) as u8) | 0x40, (l & 0xff) as u8]
}

/// A new, empty HLL: sparse, every register zero.
pub fn new_sparse() -> Vec<u8> {
    let mut v = vec![0u8; HDR];
    v[..4].copy_from_slice(b"HYLL");
    v[4] = SPARSE;
    let mut left = REGISTERS;
    while left > 0 {
        let run = left.min(SPARSE_XZERO_MAX_LEN);
        v.extend_from_slice(&xzero_op(run));
        left -= run;
    }
    v
}

/// Redis's `isHLLObjectOrReply` checks on the bytes alone.
pub fn is_valid(v: &[u8]) -> bool {
    v.len() >= HDR
        && &v[..4] == b"HYLL"
        && v[4] <= SPARSE
        && (v[4] != DENSE || v.len() == DENSE_SIZE)
}

pub fn is_sparse(v: &[u8]) -> bool {
    v[4] == SPARSE
}

pub fn murmur64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let mut h = seed ^ (key.len() as u64).wrapping_mul(M);
    let mut chunks = key.chunks_exact(8);
    for c in &mut chunks {
        let mut k = u64::from_le_bytes(c.try_into().expect("8 bytes"));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        for (i, &b) in rest.iter().enumerate() {
            h ^= (b as u64) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// The register an element lands in, and the length of its run of zeroes
/// plus one — the value that register takes if it is larger.
pub fn pat_len(ele: &[u8]) -> (usize, u8) {
    let mut hash = murmur64a(ele, 0xadc83b19);
    let index = (hash & P_MASK) as usize;
    hash >>= P;
    hash |= 1u64 << Q;
    (index, hash.trailing_zeros() as u8 + 1)
}

fn dense_get(regs: &[u8], i: usize) -> u8 {
    let byte = i * BITS / 8;
    let fb = (i * BITS) & 7;
    let b0 = regs[byte] as u16;
    let b1 = regs.get(byte + 1).copied().unwrap_or(0) as u16;
    (((b0 >> fb) | (b1 << (8 - fb))) & REGISTER_MAX as u16) as u8
}

fn dense_set_raw(regs: &mut [u8], i: usize, val: u8) {
    let byte = i * BITS / 8;
    let fb = (i * BITS) & 7;
    let v = val as u16;
    regs[byte] &= !((REGISTER_MAX as u16) << fb) as u8;
    regs[byte] |= (v << fb) as u8;
    if byte + 1 < regs.len() {
        regs[byte + 1] &= !((REGISTER_MAX as u16) >> (8 - fb)) as u8;
        regs[byte + 1] |= (v >> (8 - fb)) as u8;
    }
}

/// Raise register `i` to `count` if that is larger; whether it changed.
fn dense_set(regs: &mut [u8], i: usize, count: u8) -> bool {
    if count > dense_get(regs, i) {
        dense_set_raw(regs, i, count);
        true
    } else {
        false
    }
}

pub fn invalidate_cache(v: &mut [u8]) {
    v[15] |= 1 << 7;
}

pub fn cached(v: &[u8]) -> Option<u64> {
    (v[15] & 0x80 == 0).then(|| u64::from_le_bytes(v[8..16].try_into().expect("8 bytes")))
}

pub fn set_cache(v: &mut [u8], card: u64) {
    v[8..16].copy_from_slice(&card.to_le_bytes());
}

/// The same HLL, dense. The header — magic and cached cardinality — carries
/// over, as Redis's `*hdr = *oldhdr` does.
pub fn sparse_to_dense(v: &[u8]) -> Result<Vec<u8>, Corrupt> {
    if v[4] == DENSE {
        return Ok(v.to_vec());
    }
    let mut d = vec![0u8; DENSE_SIZE];
    d[..HDR].copy_from_slice(&v[..HDR]);
    d[4] = DENSE;
    let regs = &mut d[HDR..];
    let (mut idx, mut p) = (0usize, HDR);
    while p < v.len() {
        let b = v[p];
        if is_zero(b) {
            idx += zero_len(b);
            p += 1;
        } else if is_xzero(b) {
            idx += xzero_len(b, *v.get(p + 1).ok_or(Corrupt)?);
            p += 2;
        } else {
            let (run, val) = (val_len(b), val_value(b));
            if run + idx > REGISTERS {
                break;
            }
            for _ in 0..run {
                dense_set_raw(regs, idx, val);
                idx += 1;
            }
            p += 1;
        }
    }
    if idx != REGISTERS {
        return Err(Corrupt);
    }
    Ok(d)
}

/// Redis's `hllSparseSet`: raise register `index` to `count` in the sparse
/// opcodes, splitting a run where needed, and turn the whole HLL dense when
/// the value no longer fits a VAL opcode or the encoding outgrows
/// `max_bytes`. Whether a register changed.
pub fn sparse_set(
    v: &mut Vec<u8>,
    index: usize,
    count: u8,
    max_bytes: usize,
) -> Result<bool, Corrupt> {
    if count > SPARSE_VAL_MAX_VALUE {
        return promote(v, index, count);
    }
    // Step 1: find the opcode covering `index`.
    let end = v.len();
    let (mut p, mut first, mut span) = (HDR, 0usize, 0usize);
    let mut prev: Option<usize> = None;
    while p < end {
        let b = v[p];
        let oplen;
        if is_zero(b) {
            span = zero_len(b);
            oplen = 1;
        } else if b & 0x80 != 0 {
            span = val_len(b);
            oplen = 1;
        } else {
            span = xzero_len(b, *v.get(p + 1).ok_or(Corrupt)?);
            oplen = 2;
        }
        if index < first + span {
            break;
        }
        prev = Some(p);
        p += oplen;
        first += span;
    }
    if span == 0 || p >= end {
        return Err(Corrupt);
    }
    let b = v[p];
    let (zero, xzero, val) = (is_zero(b), is_xzero(b), b & 0x80 != 0);
    let runlen = if zero {
        zero_len(b)
    } else if xzero {
        xzero_len(b, v[p + 1])
    } else {
        val_len(b)
    };

    // Step 2: the trivial updates in place.
    let trivial = if val {
        if val_value(b) >= count {
            return Ok(false);
        }
        runlen == 1
    } else {
        zero && runlen == 1
    };
    if trivial {
        v[p] = val_op(count, 1);
    } else {
        // The general case: split the run into up to three opcodes.
        let last = first + span - 1;
        let mut seq: Vec<u8> = Vec::with_capacity(5);
        let zeros = |seq: &mut Vec<u8>, len: usize| {
            if len > SPARSE_ZERO_MAX_LEN {
                seq.extend_from_slice(&xzero_op(len));
            } else {
                seq.push(zero_op(len));
            }
        };
        if zero || xzero {
            if index != first {
                zeros(&mut seq, index - first);
            }
            seq.push(val_op(count, 1));
            if index != last {
                zeros(&mut seq, last - index);
            }
        } else {
            let cur = val_value(b);
            if index != first {
                seq.push(val_op(cur, index - first));
            }
            seq.push(val_op(count, 1));
            if index != last {
                seq.push(val_op(cur, last - index));
            }
        }
        let oldlen = if xzero { 2 } else { 1 };
        // Step 3: splice it in, unless that outgrows the sparse limit.
        if seq.len() > oldlen && v.len() + seq.len() - oldlen > max_bytes {
            return promote(v, index, count);
        }
        v.splice(p..p + oldlen, seq);
    }

    // Step 4: merge adjacent VAL opcodes of equal value, near the change.
    let mut q = prev.unwrap_or(HDR);
    let mut scan = 5;
    while q < v.len() && scan > 0 {
        scan -= 1;
        let b = v[q];
        if is_xzero(b) {
            q += 2;
            continue;
        }
        if is_zero(b) {
            q += 1;
            continue;
        }
        if q + 1 < v.len() && v[q + 1] & 0x80 != 0 {
            let (v1, v2) = (val_value(b), val_value(v[q + 1]));
            let len = val_len(b) + val_len(v[q + 1]);
            if v1 == v2 && len <= SPARSE_VAL_MAX_LEN {
                v[q + 1] = val_op(v1, len);
                v.remove(q);
                continue;
            }
        }
        q += 1;
    }
    invalidate_cache(v);
    Ok(true)
}

fn promote(v: &mut Vec<u8>, index: usize, count: u8) -> Result<bool, Corrupt> {
    *v = sparse_to_dense(v)?;
    dense_set(&mut v[HDR..], index, count);
    Ok(true)
}

/// Add one element; whether any register changed.
pub fn add(v: &mut Vec<u8>, ele: &[u8], max_bytes: usize) -> Result<bool, Corrupt> {
    let (index, count) = pat_len(ele);
    if v[4] == DENSE {
        Ok(dense_set(&mut v[HDR..], index, count))
    } else {
        sparse_set(v, index, count, max_bytes)
    }
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let (mut y, mut z) = (1.0, x);
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let (mut y, mut z) = (1.0, 1.0 - x);
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prime == z {
            return z / 3.0;
        }
    }
}

/// Ertl's estimator over a register histogram, as Redis 7 computes it.
fn estimate(histo: &[u32; 64]) -> u64 {
    let m = REGISTERS as f64;
    let mut z = m * tau((m - histo[(Q + 1) as usize] as f64) / m);
    for j in (1..=Q as usize).rev() {
        z += histo[j] as f64;
        z *= 0.5;
    }
    z += m * sigma(histo[0] as f64 / m);
    (ALPHA_INF * m * m / z).round() as u64
}

/// The estimated cardinality, from the registers whatever the encoding.
pub fn count(v: &[u8]) -> Result<u64, Corrupt> {
    let mut histo = [0u32; 64];
    if v[4] == DENSE {
        for i in 0..REGISTERS {
            histo[dense_get(&v[HDR..], i) as usize] += 1;
        }
    } else {
        let (mut idx, mut p) = (0usize, HDR);
        while p < v.len() {
            let b = v[p];
            if is_zero(b) {
                idx += zero_len(b);
                histo[0] += zero_len(b) as u32;
                p += 1;
            } else if is_xzero(b) {
                let run = xzero_len(b, *v.get(p + 1).ok_or(Corrupt)?);
                idx += run;
                histo[0] += run as u32;
                p += 2;
            } else {
                idx += val_len(b);
                histo[val_value(b) as usize] += val_len(b) as u32;
                p += 1;
            }
        }
        if idx != REGISTERS {
            return Err(Corrupt);
        }
    }
    Ok(estimate(&histo))
}

/// Cardinality of raw byte-per-register registers, as a multi-key PFCOUNT
/// builds them.
pub fn count_raw(regs: &[u8; REGISTERS]) -> u64 {
    let mut histo = [0u32; 64];
    for &r in regs.iter() {
        histo[r as usize & 63] += 1;
    }
    estimate(&histo)
}

/// `max[i] = max(max[i], register i of v)`.
pub fn merge_into(max: &mut [u8; REGISTERS], v: &[u8]) -> Result<(), Corrupt> {
    if v[4] == DENSE {
        for (i, m) in max.iter_mut().enumerate() {
            *m = (*m).max(dense_get(&v[HDR..], i));
        }
        return Ok(());
    }
    let (mut i, mut p) = (0usize, HDR);
    while p < v.len() {
        let b = v[p];
        if is_zero(b) {
            i += zero_len(b);
            p += 1;
        } else if is_xzero(b) {
            i += xzero_len(b, *v.get(p + 1).ok_or(Corrupt)?);
            p += 2;
        } else {
            let (run, val) = (val_len(b), val_value(b));
            if run + i > REGISTERS {
                break;
            }
            for _ in 0..run {
                max[i] = max[i].max(val);
                i += 1;
            }
            p += 1;
        }
    }
    if i != REGISTERS {
        return Err(Corrupt);
    }
    Ok(())
}

/// Write merged registers into `v` — dense first when any input was dense,
/// as `PFMERGE` does — and invalidate its cached cardinality.
pub fn apply_registers(
    v: &mut Vec<u8>,
    max: &[u8; REGISTERS],
    use_dense: bool,
    max_bytes: usize,
) -> Result<(), Corrupt> {
    if use_dense {
        *v = sparse_to_dense(v)?;
    }
    for (j, &m) in max.iter().enumerate() {
        if m == 0 {
            continue;
        }
        if v[4] == DENSE {
            dense_set(&mut v[HDR..], j, m);
        } else {
            sparse_set(v, j, m, max_bytes)?;
        }
    }
    invalidate_cache(v);
    Ok(())
}

/// Every register, dense or sparse.
pub fn registers(v: &[u8]) -> Result<Vec<u8>, Corrupt> {
    let d = sparse_to_dense(v)?;
    Ok((0..REGISTERS).map(|i| dense_get(&d[HDR..], i)).collect())
}

/// `PFDEBUG DECODE`: the sparse opcodes, spelled out.
pub fn decode(v: &[u8]) -> String {
    let mut out = Vec::new();
    let mut p = HDR;
    while p < v.len() {
        let b = v[p];
        if is_zero(b) {
            out.push(format!("z:{}", zero_len(b)));
            p += 1;
        } else if is_xzero(b) {
            out.push(format!(
                "Z:{}",
                xzero_len(b, v.get(p + 1).copied().unwrap_or(0))
            ));
            p += 2;
        } else {
            out.push(format!("v:{},{}", val_value(b), val_len(b)));
            p += 1;
        }
    }
    out.join(" ")
}

/// `PFSELFTEST`: register access, and the estimate's error over growing
/// cardinalities with dense and sparse agreeing — Redis's own checks.
pub fn self_test(max_bytes: usize) -> Result<(), String> {
    let rand = || crate::memdb::fast_random();
    let mut dense = vec![0u8; DENSE_SIZE];
    dense[..4].copy_from_slice(b"HYLL");
    for _ in 0..1000 {
        let vals: Vec<u8> = (0..REGISTERS)
            .map(|_| (rand() as u8) & REGISTER_MAX)
            .collect();
        for (i, &r) in vals.iter().enumerate() {
            dense_set_raw(&mut dense[HDR..], i, r);
        }
        for (i, &r) in vals.iter().enumerate() {
            let got = dense_get(&dense[HDR..], i);
            if got != r {
                return Err(format!(
                    "TESTFAILED Register {i} should be {r} but is {got}"
                ));
            }
        }
    }
    dense[HDR..].fill(0);
    let mut sparse = new_sparse();
    let relerr = 1.04 / (REGISTERS as f64).sqrt();
    let seed = rand();
    let mut checkpoint: u64 = 1;
    for j in 1..=10_000_000u64 {
        let ele = (j ^ seed).to_le_bytes();
        let (index, count_) = pat_len(&ele);
        dense_set(&mut dense[HDR..], index, count_);
        add(&mut sparse, &ele, max_bytes).map_err(|_| INVALID.to_string())?;
        if j == checkpoint {
            if (j as usize) < max_bytes / 2 && !is_sparse(&sparse) {
                return Err("TESTFAILED sparse encoding not used".to_string());
            }
            let d = count(&dense).map_err(|_| INVALID.to_string())?;
            if d != count(&sparse).map_err(|_| INVALID.to_string())? {
                return Err("TESTFAILED dense/sparse disagree".to_string());
            }
            let maxerr = if j == 10 {
                1
            } else {
                (relerr * 6.0 * checkpoint as f64).ceil() as u64
            };
            let abserr = checkpoint.abs_diff(d);
            if abserr > maxerr {
                return Err(format!(
                    "TESTFAILED Too big error. card:{checkpoint} abserr:{abserr}"
                ));
            }
            checkpoint *= 10;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_hll_is_redis_bytes() {
        let v = new_sparse();
        // What `PFADD k` leaves in a real Redis 7.2: header and one XZERO run.
        assert_eq!(&v[..5], b"HYLL\x01");
        assert_eq!(&v[HDR..], &[0x7f, 0xff]);
        assert!(is_valid(&v));
        assert_eq!(count(&v), Ok(0));
    }

    #[test]
    fn hashing_and_encoding_match_redis_bytes() {
        // Read off a real Redis 7.2: `PFADD hll a; PFDEBUG GETREG hll` has
        // register 12711 at 2 and every other register at 0.
        assert_eq!(pat_len(b"a"), (12711, 2));
        // `PFADD h2 foo bar zap; PFCOUNT h2; GET h2` on Redis 7.2, verbatim.
        let mut v = new_sparse();
        for e in [b"foo".as_slice(), b"bar", b"zap"] {
            add(&mut v, e, DEFAULT_SPARSE_MAX_BYTES).unwrap();
        }
        assert_eq!(count(&v), Ok(3));
        set_cache(&mut v, 3);
        assert_eq!(
            v,
            b"HYLL\x01\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x5c\xb3\x90\x42\x07\x84\x48\x58\x80\x58\xe7"
                .to_vec()
        );
    }

    #[test]
    fn sparse_and_dense_agree_and_promote() {
        let mut s = new_sparse();
        for i in 0..2000u32 {
            add(&mut s, &i.to_le_bytes(), DEFAULT_SPARSE_MAX_BYTES).unwrap();
        }
        let est = count(&s).unwrap();
        let d = sparse_to_dense(&s).unwrap();
        assert_eq!(count(&d).unwrap(), est);
        assert!(
            (est as f64 - 2000.0).abs() < 2000.0 * 0.05,
            "estimate {est}"
        );
        // Enough elements outgrow the sparse limit.
        for i in 2000..20000u32 {
            add(&mut s, &i.to_le_bytes(), DEFAULT_SPARSE_MAX_BYTES).unwrap();
        }
        assert!(!is_sparse(&s));
        assert_eq!(s.len(), DENSE_SIZE);
    }

    #[test]
    fn merge_takes_register_maxima() {
        let (mut a, mut b) = (new_sparse(), new_sparse());
        for i in 0..100u32 {
            add(&mut a, &i.to_le_bytes(), DEFAULT_SPARSE_MAX_BYTES).unwrap();
            add(&mut b, &(i + 50).to_le_bytes(), DEFAULT_SPARSE_MAX_BYTES).unwrap();
        }
        let mut max = [0u8; REGISTERS];
        merge_into(&mut max, &a).unwrap();
        merge_into(&mut max, &b).unwrap();
        let est = count_raw(&max);
        assert!((140..=160).contains(&est), "union estimate {est}");
    }

    #[test]
    fn a_truncated_sparse_hll_is_corrupt() {
        let mut v = new_sparse();
        v.truncate(v.len() - 1);
        assert_eq!(count(&v), Err(Corrupt));
    }
}
