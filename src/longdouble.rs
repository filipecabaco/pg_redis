//! The `long double` arithmetic behind `INCRBYFLOAT` and `HINCRBYFLOAT`.
//!
//! Redis parses both operands with `strtold`, adds them as C `long double`
//! and prints the sum with `%.17Lf`. On x86-64 a `long double` is the x87
//! 80-bit extended format, with a 64-bit significand, so Redis keeps digits a
//! 64-bit double drops: `3 + 1.000000000000000005` is `4.00000000000000001`.
//! x86-64 Redis is the reference here; arm64 builds use a 128-bit
//! `long double` and print different digits.
//!
//! - [`LongDouble::parse`] is Redis's `string2ld`: glibc's `strtold`
//!   grammar (decimal, hexadecimal, `inf`/`infinity`), the whole string, no
//!   leading space, at most 5119 bytes, with NaN, overflow and underflow to
//!   zero refused. Decimal and hexadecimal significands are rounded to the
//!   nearest 80-bit value, ties to even, by `rustc_apfloat`.
//! - [`LongDouble::add`] is the x87 sum, rounded to nearest, ties to even.
//! - [`LongDouble::to_human`] is `ld2string(…, LD_STR_HUMAN)`: the exact
//!   binary value rounded to 17 decimal places as glibc's `printf` rounds it
//!   (to nearest, ties to even), trailing zeros and a bare `.` dropped, and
//!   `-0` written `0`.
//!
//! Both halves, the memory engine and the logged tables, go through
//! [`increment`], so they agree on every digit.
#![forbid(unsafe_code)]

use rustc_apfloat::ieee::X87DoubleExtended;
use rustc_apfloat::{Float, Round};

use crate::store::Num;

/// Redis's `MAX_LONG_DOUBLE_CHARS`: the buffer `string2ld` copies into, so a
/// string must be shorter than this.
pub const MAX_LONG_DOUBLE_CHARS: usize = 5 * 1024;

/// A C `long double` as x86-64 has it: the x87 80-bit extended format.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LongDouble(X87DoubleExtended);

impl LongDouble {
    pub const ZERO: LongDouble = LongDouble(X87DoubleExtended::ZERO);

    /// Redis's `string2ld`. `None` where Redis refuses the string.
    pub fn parse(bytes: &[u8]) -> Option<LongDouble> {
        if bytes.is_empty() || bytes.len() >= MAX_LONG_DOUBLE_CHARS {
            return None;
        }
        // Every spelling strtold takes is ASCII.
        let s = std::str::from_utf8(bytes).ok()?;
        let (negative, body) = match s.as_bytes()[0] {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        if body.eq_ignore_ascii_case("inf") || body.eq_ignore_ascii_case("infinity") {
            let inf = X87DoubleExtended::INFINITY;
            return Some(LongDouble(if negative { -inf } else { inf }));
        }
        let hex = body.len() > 2 && (body.starts_with("0x") || body.starts_with("0X"));
        let (text, nonzero) = if hex {
            let (has_exponent, nonzero) = scan(&body[2..], 16, b"pP")?;
            // rustc_apfloat wants a binary exponent on every hex float;
            // strtold does not.
            let text = if has_exponent {
                body.to_string()
            } else {
                format!("{body}p0")
            };
            (text, nonzero)
        } else {
            let (_, nonzero) = scan(body, 10, b"eE")?;
            (body.to_string(), nonzero)
        };
        let magnitude = X87DoubleExtended::from_str_r(&text, Round::NearestTiesToEven)
            .ok()?
            .value;
        // strtold's ERANGE: a value too large becomes HUGE_VALL, one too
        // small for even a subnormal becomes zero, and Redis refuses both.
        // A zero written as zero is fine.
        if magnitude.is_infinite() || magnitude.is_nan() || (magnitude.is_zero() && nonzero) {
            return None;
        }
        Some(LongDouble(if negative { -magnitude } else { magnitude }))
    }

    /// `a + b` in x87 extended precision, rounded to nearest, ties to even.
    pub fn add(self, other: LongDouble) -> LongDouble {
        LongDouble(self.0.add_r(other.0, Round::NearestTiesToEven).value)
    }

    pub fn is_finite(self) -> bool {
        self.0.is_finite()
    }

    /// Redis's `ld2string(…, LD_STR_HUMAN)` for a finite value: `%.17Lf`,
    /// then the trailing zeros after the point, the point if nothing is
    /// left after it, and the sign of a negative zero all dropped.
    pub fn to_human(self) -> String {
        debug_assert!(self.is_finite());
        let bits = self.0.to_bits();
        let negative = (bits >> 79) & 1 == 1;
        let field = ((bits >> 64) & 0x7fff) as i32;
        let significand = bits as u64;
        // value = significand × 2^exp; a subnormal has field 0 and the
        // exponent of field 1.
        let exp = field.max(1) - 16383 - 63;

        let mut text = String::new();
        if negative {
            text.push('-');
        }
        if exp >= 0 {
            // An integer: no fractional digits to print.
            text.push_str(&big_decimal(significand, exp as u32));
        } else {
            text.push_str(&fixed17(significand, exp.unsigned_abs()));
        }
        if text.contains('.') {
            let kept = text.trim_end_matches('0').trim_end_matches('.').len();
            text.truncate(kept);
        }
        if text == "-0" {
            text = "0".to_string();
        }
        text
    }
}

/// What `INCRBYFLOAT` and `HINCRBYFLOAT` store and reply: `current` is the
/// stored value, `None` for a missing key or field (zero).
///
/// `Num::NotNumber` when the stored value does not parse, `Num::Overflow`
/// when the sum is infinite or NaN; the callers spell the errors, which
/// differ between the two commands.
pub fn increment(current: Option<&[u8]>, delta: LongDouble) -> Num<String> {
    let base = match current {
        None => LongDouble::ZERO,
        Some(bytes) => match LongDouble::parse(bytes) {
            Some(v) => v,
            None => return Num::NotNumber,
        },
    };
    let sum = base.add(delta);
    if !sum.is_finite() {
        return Num::Overflow;
    }
    Num::Value(sum.to_human())
}

/// Check `s` against strtold's grammar for a significand in `radix`, then an
/// optional exponent after one of `marks`: digits with at most one point,
/// at least one digit, and an exponent of an optional sign and at least one
/// decimal digit. Returns whether there is an exponent and whether any
/// significand digit is non-zero, or `None` if strtold would stop short of
/// the end (which Redis refuses).
fn scan(s: &str, radix: u32, marks: &[u8]) -> Option<(bool, bool)> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut digits = 0;
    let mut nonzero = false;
    let mut dot = false;
    while i < b.len() {
        let c = b[i];
        if c == b'.' && !dot {
            dot = true;
        } else if let Some(d) = (c as char).to_digit(radix) {
            digits += 1;
            nonzero |= d != 0;
        } else {
            break;
        }
        i += 1;
    }
    if digits == 0 {
        return None;
    }
    if i == b.len() {
        return Some((false, nonzero));
    }
    if !marks.contains(&b[i]) {
        return None;
    }
    i += 1;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    let exponent = &b[i..];
    if exponent.is_empty() || !exponent.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some((true, nonzero))
}

/// `significand × 2^-shift` printed with 17 decimal places, rounded to
/// nearest with ties to even: `round(significand × 10^17 / 2^shift)`, then
/// the point placed 17 digits from the right.
fn fixed17(significand: u64, shift: u32) -> String {
    // Below 2^64 × 10^17 < 2^121: fits a u128.
    let n = significand as u128 * 100_000_000_000_000_000u128;
    let (mut q, half, rest) = if shift >= 128 {
        (0, false, n != 0)
    } else {
        let q = n >> shift;
        let half = (n >> (shift - 1)) & 1 == 1;
        let rest = n & ((1u128 << (shift - 1)) - 1) != 0;
        (q, half, rest)
    };
    if half && (rest || q & 1 == 1) {
        q += 1;
    }
    let digits = format!("{q:018}");
    let (int, frac) = digits.split_at(digits.len() - 17);
    format!("{int}.{frac}")
}

/// The decimal digits of `significand × 2^shift`, which can run to nearly
/// 5000 digits: the value is built in base-2^32 limbs by shifting, then
/// divided by 10^9 until nothing is left, nine digits at a time.
fn big_decimal(significand: u64, shift: u32) -> String {
    const BASE: u64 = 1_000_000_000;
    // Little-endian base-2^32 limbs of the binary value.
    let mut limbs: Vec<u32> = vec![0; (shift / 32) as usize];
    let bit = shift % 32;
    let wide = (significand as u128) << bit;
    limbs.extend([wide as u32, (wide >> 32) as u32, (wide >> 64) as u32]);
    while limbs.last() == Some(&0) {
        limbs.pop();
    }
    // Repeated division by 10^9, most significant limb first.
    let mut chunks = Vec::new();
    while !limbs.is_empty() {
        let mut rem = 0u64;
        for limb in limbs.iter_mut().rev() {
            let cur = (rem << 32) | *limb as u64;
            *limb = (cur / BASE) as u32;
            rem = cur % BASE;
        }
        chunks.push(rem as u32);
        while limbs.last() == Some(&0) {
            limbs.pop();
        }
    }
    let mut text = match chunks.pop() {
        Some(top) => top.to_string(),
        None => "0".to_string(),
    };
    for chunk in chunks.iter().rev() {
        text.push_str(&format!("{chunk:09}"));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Option<String> {
        LongDouble::parse(s.as_bytes()).map(|v| v.to_human())
    }

    fn sum(a: &str, b: &str) -> String {
        match increment(Some(a.as_bytes()), LongDouble::parse(b.as_bytes()).unwrap()) {
            Num::Value(s) => s,
            Num::NotNumber => "not a number".into(),
            Num::Overflow => "overflow".into(),
        }
    }

    #[test]
    fn parses_signs_points_and_exponents() {
        assert_eq!(p("1.5").as_deref(), Some("1.5"));
        assert_eq!(p("+1.5").as_deref(), Some("1.5"));
        assert_eq!(p("-1.5").as_deref(), Some("-1.5"));
        assert_eq!(p(".5").as_deref(), Some("0.5"));
        assert_eq!(p("5.").as_deref(), Some("5"));
        assert_eq!(p("5.0e3").as_deref(), Some("5000"));
        assert_eq!(p("1E+2").as_deref(), Some("100"));
        assert_eq!(p("25e-1").as_deref(), Some("2.5"));
        assert_eq!(p("007").as_deref(), Some("7"));
        assert_eq!(p("-0").as_deref(), Some("0"));
        assert_eq!(p("0e-99999999999").as_deref(), Some("0"));
    }

    #[test]
    fn parses_hex_floats() {
        assert_eq!(p("0x1.8p3").as_deref(), Some("12"));
        assert_eq!(p("0X10").as_deref(), Some("16"));
        assert_eq!(p("-0x.8").as_deref(), Some("-0.5"));
        assert_eq!(p("0x1P-2").as_deref(), Some("0.25"));
        assert_eq!(
            p("0xffffffffffffffff").as_deref(),
            Some("18446744073709551615")
        );
        // 65 significant bits: the last one is a tie, to even.
        assert_eq!(
            p("0x1ffffffffffffffffp0").as_deref(),
            Some("36893488147419103232")
        );
        assert_eq!(p("0x0p99999999999").as_deref(), Some("0"));
    }

    #[test]
    fn infinity_parses_and_nan_does_not() {
        for s in [
            "inf",
            "INF",
            "-inf",
            "+Inf",
            "infinity",
            "-Infinity",
            "INFINITY",
        ] {
            assert!(
                LongDouble::parse(s.as_bytes()).is_some_and(|v| !v.is_finite()),
                "{s}"
            );
        }
        for s in ["nan", "NaN", "-nan", "nan(1)", "infinit", "infinityy", "in"] {
            assert_eq!(p(s), None, "{s}");
        }
    }

    #[test]
    fn refuses_what_strtold_stops_short_of() {
        for s in [
            "",
            " 1",
            "1 ",
            "\t1",
            "1\n",
            "abc",
            "1e",
            "1e+",
            "1e-",
            ".",
            "+",
            "-",
            "+-1",
            "--1",
            "1..2",
            "1.2.3",
            "0x",
            "0x.",
            "0x.p1",
            "0x1p",
            "0x1p+",
            "0xg",
            "1_000",
            "1d",
            "1f",
            "0b101",
            "1e5.5",
            "0x1.8p3e1",
            "\u{ff11}",
        ] {
            assert_eq!(p(s), None, "{s:?}");
        }
        assert_eq!(p("1\0"), None);
        let long = "1".repeat(MAX_LONG_DOUBLE_CHARS);
        assert_eq!(p(&long), None);
        let longest = format!("0.{}", "0".repeat(MAX_LONG_DOUBLE_CHARS - 4)) + "1";
        assert_eq!(longest.len(), MAX_LONG_DOUBLE_CHARS - 1);
        assert_eq!(p(&longest), None, "underflows to zero");
    }

    #[test]
    fn overflow_and_underflow_to_zero_are_refused_subnormals_are_not() {
        assert_eq!(p("1e5000"), None);
        assert_eq!(p("-1e5000"), None);
        assert_eq!(p("0x1p16384"), None);
        assert!(p("1.18973149535723176502e4932").is_some());
        assert_eq!(
            p("1.18973149535723176509e4932"),
            None,
            "rounds up to infinity"
        );
        assert_eq!(p("1e-5000"), None);
        assert_eq!(
            p("0x1p-16446"),
            None,
            "half the smallest subnormal: a tie, to zero"
        );
        assert!(
            p("0x1.000001p-16446").is_some(),
            "just over half: the smallest subnormal"
        );
        assert!(p("0x1p-16445").is_some());
        assert!(p("3.6e-4951").is_some());
        assert!(p("1e-4940").is_some());
    }

    #[test]
    fn formats_seventeen_places_trimmed() {
        assert_eq!(p("10.5").as_deref(), Some("10.5"));
        assert_eq!(sum("10.5", "0.1"), "10.6");
        assert_eq!(sum("3", "1.000000000000000005"), "4.00000000000000001");
        assert_eq!(sum("5.0e3", "200"), "5200");
        assert_eq!(sum("1", "-1"), "0");
        assert_eq!(sum("-0", "-0"), "0");
        assert_eq!(p("-1e-30").as_deref(), Some("0"), "rounds to -0, written 0");
        assert_eq!(p("0.1").as_deref(), Some("0.1"));
        assert_eq!(p("1e-17").as_deref(), Some("0.00000000000000001"));
        assert_eq!(p("4e-18").as_deref(), Some("0"));
        assert_eq!(p("6e-18").as_deref(), Some("0.00000000000000001"));
    }

    #[test]
    fn formatting_ties_go_to_even() {
        // m × 2^-18 for odd m: exactly 18 decimal places, the last a 5.
        // 1 × 2^-18 = 0.000003814697265625
        assert_eq!(p("0x1p-18").as_deref(), Some("0.00000381469726562"));
        // 3 × 2^-18 = 0.000011444091796875
        assert_eq!(p("0x3p-18").as_deref(), Some("0.00001144409179688"));
        assert_eq!(p("-0x3p-18").as_deref(), Some("-0.00001144409179688"));
    }

    #[test]
    fn formats_very_large_and_very_small_values() {
        let max = p("0x1.fffffffffffffffep16383").unwrap();
        assert_eq!(max.len(), 4933);
        assert!(max.starts_with("118973149535723176502126385303097020516906332229462420"));
        assert!(max.ends_with("19552086811989770240"));
        assert_eq!(p("0x1p64").as_deref(), Some("18446744073709551616"));
        assert_eq!(p("0x1p63").as_deref(), Some("9223372036854775808"));
        assert_eq!(p("-1e20").as_deref(), Some("-100000000000000000000"));
        assert_eq!(p("0x1p-16445").as_deref(), Some("0"));
        assert_eq!(sum("1e4932", "1e4932"), "overflow");
        assert_eq!(sum("inf", "-inf"), "overflow");
        assert_eq!(sum("abc", "1"), "not a number");
    }

    #[test]
    fn missing_is_zero() {
        let one = LongDouble::parse(b"1.25").unwrap();
        assert!(matches!(increment(None, one), Num::Value(s) if s == "1.25"));
        let inf = LongDouble::parse(b"inf").unwrap();
        assert!(matches!(increment(None, inf), Num::Overflow));
    }

    /// Replies recorded from x86-64 Redis by scripts/gen_longdouble_cases.exs.
    const RECORDED: &str = include_str!("../tests/fixtures/longdouble-redis-7.2-x86_64.txt");

    fn unescape(field: &str) -> Vec<u8> {
        let b = field.as_bytes();
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'\\' {
                let hex = std::str::from_utf8(&b[i + 2..i + 4]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                i += 4;
            } else {
                out.push(b[i]);
                i += 1;
            }
        }
        out
    }

    /// What `INCRBYFLOAT` replies, as the store's dispatcher spells it.
    fn incrbyfloat(stored: &mut Option<Vec<u8>>, arg: &[u8]) -> Result<Vec<u8>, &'static str> {
        let Some(delta) = LongDouble::parse(arg) else {
            return Err("ERR value is not a valid float");
        };
        match increment(stored.as_deref(), delta) {
            Num::Value(text) => {
                *stored = Some(text.clone().into_bytes());
                Ok(text.into_bytes())
            }
            Num::NotNumber => Err("ERR value is not a valid float"),
            Num::Overflow => Err("ERR increment would produce NaN or Infinity"),
        }
    }

    /// What `HINCRBYFLOAT` replies: the increment is checked first, and an
    /// infinite one refused before the field is read.
    fn hincrbyfloat(stored: &mut Option<Vec<u8>>, arg: &[u8]) -> Result<Vec<u8>, &'static str> {
        let Some(delta) = LongDouble::parse(arg) else {
            return Err("ERR value is not a valid float");
        };
        if !delta.is_finite() {
            return Err("ERR value is NaN or Infinity");
        }
        match increment(stored.as_deref(), delta) {
            Num::Value(text) => {
                *stored = Some(text.clone().into_bytes());
                Ok(text.into_bytes())
            }
            Num::NotNumber => Err("ERR hash value is not a float"),
            Num::Overflow => Err("ERR increment would produce NaN or Infinity"),
        }
    }

    #[test]
    fn replays_what_x86_64_redis_replied() {
        let (mut string, mut field) = (None, None);
        let mut case = "";
        let (mut checked, mut failures) = (0, Vec::new());
        for line in RECORDED.lines().filter(|l| !l.starts_with('#')) {
            let mut parts = line.splitn(3, ' ');
            let op = parts.next().unwrap();
            let arg = unescape(parts.next().unwrap());
            match op {
                "case" => {
                    case = line;
                    (string, field) = (None, None);
                    continue;
                }
                "set" => {
                    string = Some(arg);
                    continue;
                }
                "hset" => {
                    field = Some(arg);
                    continue;
                }
                _ => {}
            }
            let want = parts.next().unwrap();
            let got = match op {
                "incr" => incrbyfloat(&mut string, &arg),
                "hincr" => hincrbyfloat(&mut field, &arg),
                other => panic!("unknown line {other}"),
            };
            let got = match got {
                Ok(v) => format!("={}", String::from_utf8(v).unwrap()),
                Err(e) => format!("!{e}"),
            };
            checked += 1;
            if unescape(want) != got.as_bytes() {
                failures.push(format!("{case}: {line}\n  got {got}"));
            }
        }
        assert!(checked > 4000, "only {checked} replies recorded");
        assert!(
            failures.is_empty(),
            "{} of {checked} differ from Redis:\n{}",
            failures.len(),
            failures[..failures.len().min(20)].join("\n")
        );
    }
}
