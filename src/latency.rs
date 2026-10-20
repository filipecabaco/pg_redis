//! Redis's latency monitor (`latency.c`): every operation that takes at
//! least `latency-monitor-threshold` milliseconds is a sample of its event,
//! kept by event in the last 160 seconds' worth, with the latest and the
//! highest seen. `LATENCY LATEST`, `HISTORY`, `RESET` and `GRAPH` read them.
//!
//! The samples live in `ServerCtl`, so every worker records into, and
//! reports from, the same history. A sample in the same second as the one
//! before replaces it if higher, as Redis merges them.
#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};

use crate::commands::{Response, Value};

/// The events this server records, by the names Redis gives them.
pub const EVENTS: [&str; 4] = ["command", "fast-command", "expire-cycle", "eviction-cycle"];
pub const COMMAND: usize = 0;
pub const FAST_COMMAND: usize = 1;
pub const EXPIRE_CYCLE: usize = 2;
pub const EVICTION_CYCLE: usize = 3;

/// `LATENCY_TS_LEN`.
const SAMPLES: usize = 160;

#[repr(C)]
struct Sample {
    /// Unix seconds; 0 for none.
    time: AtomicI64,
    ms: AtomicU32,
}

#[repr(C)]
struct Series {
    /// Where the next sample goes.
    next: AtomicU32,
    max: AtomicU32,
    samples: [Sample; SAMPLES],
}

/// Every event's series, behind one lock.
#[repr(C)]
pub struct LatencyCtl {
    lock: AtomicU32,
    series: [Series; EVENTS.len()],
}

struct Held<'a>(&'a AtomicU32);

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Release);
    }
}

fn hold(l: &LatencyCtl) -> Held<'_> {
    while l
        .lock
        .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        std::hint::spin_loop();
    }
    Held(&l.lock)
}

fn ctl() -> Option<&'static LatencyCtl> {
    crate::server::ctl().map(|c| &c.latency)
}

impl Series {
    /// The samples, oldest first.
    fn ordered(&self) -> Vec<(i64, u32)> {
        let next = self.next.load(Ordering::Relaxed) as usize;
        (0..SAMPLES)
            .map(|k| &self.samples[(next + k) % SAMPLES])
            .map(|s| (s.time.load(Ordering::Relaxed), s.ms.load(Ordering::Relaxed)))
            .filter(|(t, _)| *t != 0)
            .collect()
    }

    fn clear(&self) {
        self.next.store(0, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
        for s in &self.samples {
            s.time.store(0, Ordering::Relaxed);
            s.ms.store(0, Ordering::Relaxed);
        }
    }
}

/// `latencyAddSampleIfNeeded`: `ms` for `event`, when the monitor is on and
/// it reached the threshold.
pub fn record(event: usize, ms: u64) {
    let threshold = crate::config::get_i64("latency-monitor-threshold");
    if threshold <= 0 || (ms as i64) < threshold {
        return;
    }
    let Some(l) = ctl() else { return };
    let ms = ms.min(u64::from(u32::MAX)) as u32;
    let now = crate::server::now_ms() / 1000;
    let _held = hold(l);
    let s = &l.series[event];
    s.max.fetch_max(ms, Ordering::Relaxed);
    let next = s.next.load(Ordering::Relaxed) as usize;
    let prev = &s.samples[(next + SAMPLES - 1) % SAMPLES];
    if prev.time.load(Ordering::Relaxed) == now {
        prev.ms.fetch_max(ms, Ordering::Relaxed);
        return;
    }
    s.samples[next].time.store(now, Ordering::Relaxed);
    s.samples[next].ms.store(ms, Ordering::Relaxed);
    s.next
        .store(((next + 1) % SAMPLES) as u32, Ordering::Relaxed);
}

fn event_index(name: &[u8]) -> Option<usize> {
    EVENTS
        .iter()
        .position(|e| e.as_bytes().eq_ignore_ascii_case(name))
}

/// `LATENCY LATEST`: each event with samples, its latest and its highest.
pub fn latest() -> Response {
    let Some(l) = ctl() else {
        return Response::Value(Value::Array(vec![]));
    };
    let _held = hold(l);
    let rows = EVENTS
        .iter()
        .zip(&l.series)
        .filter_map(|(name, s)| {
            let (time, ms) = *s.ordered().last()?;
            Some(Value::Array(vec![
                Value::str(name),
                Value::Int(time),
                Value::Int(i64::from(ms)),
                Value::Int(i64::from(s.max.load(Ordering::Relaxed))),
            ]))
        })
        .collect();
    Response::Value(Value::Array(rows))
}

/// `LATENCY HISTORY event`: its samples, oldest first.
pub fn history(event: &[u8]) -> Response {
    let rows = match (ctl(), event_index(event)) {
        (Some(l), Some(i)) => {
            let _held = hold(l);
            l.series[i]
                .ordered()
                .into_iter()
                .map(|(t, ms)| Value::Array(vec![Value::Int(t), Value::Int(i64::from(ms))]))
                .collect()
        }
        _ => vec![],
    };
    Response::Value(Value::Array(rows))
}

/// `LATENCY RESET [event ...]`: how many events with samples were reset.
pub fn reset(events: &[Vec<u8>]) -> Response {
    let Some(l) = ctl() else {
        return Response::Integer(0);
    };
    let _held = hold(l);
    let mut n = 0;
    for (i, s) in l.series.iter().enumerate() {
        let named = events.is_empty() || events.iter().any(|e| event_index(e) == Some(i));
        if named && !s.ordered().is_empty() {
            s.clear();
            n += 1;
        }
    }
    Response::Integer(n)
}

/// `LATENCY GRAPH event`: `latencyCommandGenSparkeline`'s header, then one
/// column per sample, its height the sample's share of the highest.
pub fn graph(event: &[u8]) -> Response {
    let name = String::from_utf8_lossy(event);
    let samples = match (ctl(), event_index(event)) {
        (Some(l), Some(i)) => {
            let _held = hold(l);
            Some((
                l.series[i].ordered(),
                l.series[i].max.load(Ordering::Relaxed),
            ))
        }
        _ => None,
    };
    let Some((samples, max)) = samples.filter(|(s, _)| !s.is_empty()) else {
        return Response::Error(format!("ERR No samples available for event '{name}'"));
    };
    let high = samples.iter().map(|s| s.1).max().unwrap_or(0);
    let low = samples.iter().map(|s| s.1).min().unwrap_or(0);
    let mut out = format!(
        "{name} - high {high} ms, low {low} ms (all time high {max} ms)\n{}\n",
        "-".repeat(80)
    );
    const ROWS: u32 = 4;
    for row in (1..=ROWS).rev() {
        let line: String = samples
            .iter()
            .map(|(_, ms)| {
                let height = if high == 0 {
                    0
                } else {
                    (ms * ROWS).div_ceil(high)
                };
                if height >= row { '#' } else { ' ' }
            })
            .collect();
        out.push_str(line.trim_end());
        out.push('\n');
    }
    Response::BulkString(out.into_bytes())
}
