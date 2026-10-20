//! Redis's event loop metrics (`INFO stats` and `INFO debug`), and the
//! instantaneous rates sampled from its cron.
//!
//! Redis has one event loop: each cycle serves what clients sent, then runs
//! the cron, `hz` times a second. Here every worker has a dispatcher loop, so
//! one of them, whichever claims it first, stands for Redis's: its cycles are
//! the `eventloop_cycles`, its ticks the cron, and it samples the rates every
//! 100 ms, as `serverCron` does at the default `hz` of 10. Commands every
//! worker runs add to the durations, as they would all have run on Redis's
//! one loop.
#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

/// `STATS_METRIC_SAMPLES`.
const SAMPLES: usize = 16;

/// How often the owning loop wakes with nothing to do: `1000 / hz`.
pub const CRON: Duration = Duration::from_millis(100);

#[repr(C)]
struct Sample {
    ms: AtomicU64,
    cycles: AtomicU64,
    duration: AtomicU64,
    ops: AtomicU64,
    net_in: AtomicU64,
    net_out: AtomicU64,
}

#[repr(C)]
pub struct EventLoopCtl {
    /// The process id of the worker whose loop is the event loop; 0 for none.
    owner: AtomicU32,
    cycles: AtomicU64,
    /// Microseconds spent in cycles, the commands' and the cron's.
    duration_sum: AtomicU64,
    cmd_sum: AtomicU64,
    cron_sum: AtomicU64,
    duration_max: AtomicU64,
    cmd_per_cycle_max: AtomicU64,
    last_sample_ms: AtomicU64,
    /// Where the next sample goes, and how many are held.
    next: AtomicU32,
    held: AtomicU32,
    samples: [Sample; SAMPLES],
}

fn ctl() -> Option<&'static EventLoopCtl> {
    crate::server::ctl().map(|c| &c.eventloop)
}

/// Whether this worker's loop is the event loop, claiming it when nobody
/// does.
pub fn claim() -> bool {
    let Some(c) = ctl() else { return false };
    let me = std::process::id();
    match c.owner.compare_exchange(0, me, Relaxed, Relaxed) {
        Ok(_) => true,
        Err(owner) => owner == me,
    }
}

/// On a worker's way out: the next worker to tick takes the loop over.
pub fn release() {
    if let Some(c) = ctl() {
        let _ = c
            .owner
            .compare_exchange(std::process::id(), 0, Relaxed, Relaxed);
    }
}

/// One command's time, wherever it ran: connection threads answer many
/// commands themselves, which Redis's loop would have run.
pub fn command(usec: u64) {
    if let Some(c) = ctl() {
        c.duration_sum.fetch_add(usec, Relaxed);
        c.cmd_sum.fetch_add(usec, Relaxed);
    }
}

/// One cycle of a dispatcher loop: `commands` run in `cmd_usec` (already
/// counted by [`command`]), then the cron in `cron_usec`, counted only on
/// the owning loop.
pub fn cycle(owner: bool, commands: u64, cmd_usec: u64, cron_usec: u64) {
    let Some(c) = ctl() else { return };
    c.cmd_per_cycle_max.fetch_max(commands, Relaxed);
    if !owner {
        return;
    }
    let total = cmd_usec + cron_usec;
    c.cycles.fetch_add(1, Relaxed);
    c.duration_sum.fetch_add(cron_usec, Relaxed);
    c.cron_sum.fetch_add(cron_usec, Relaxed);
    c.duration_max.fetch_max(total, Relaxed);
    let now = crate::server::now_ms().max(0) as u64;
    if now.saturating_sub(c.last_sample_ms.load(Relaxed)) >= CRON.as_millis() as u64 {
        c.last_sample_ms.store(now, Relaxed);
        sample(c, now);
    }
}

/// `trackInstantaneousMetric` for every metric at once.
fn sample(c: &EventLoopCtl, now: u64) {
    let (ops, net_in, net_out) = crate::server::ctl().map_or((0, 0, 0), |s| {
        (
            s.total_commands.load(Relaxed),
            s.net_input.load(Relaxed),
            s.net_output.load(Relaxed),
        )
    });
    let i = c.next.load(Relaxed) as usize % SAMPLES;
    let s = &c.samples[i];
    s.ms.store(now, Relaxed);
    s.cycles.store(c.cycles.load(Relaxed), Relaxed);
    s.duration.store(c.duration_sum.load(Relaxed), Relaxed);
    s.ops.store(ops, Relaxed);
    s.net_in.store(net_in, Relaxed);
    s.net_out.store(net_out, Relaxed);
    c.next.store(((i + 1) % SAMPLES) as u32, Relaxed);
    let held = c.held.load(Relaxed);
    if (held as usize) < SAMPLES {
        c.held.store(held + 1, Relaxed);
    }
}

/// `CONFIG RESETSTAT`: the sums from zero, and no samples.
pub fn reset() {
    let Some(c) = ctl() else { return };
    for a in [
        &c.cycles,
        &c.duration_sum,
        &c.cmd_sum,
        &c.cron_sum,
        &c.duration_max,
        &c.cmd_per_cycle_max,
    ] {
        a.store(0, Relaxed);
    }
    c.held.store(0, Relaxed);
}

/// The rates across the samples held, oldest to newest, as Redis averages
/// its samples: (ops per second, input KB/s, output KB/s, cycles per second,
/// microseconds per cycle).
fn rates(c: &EventLoopCtl) -> (u64, f64, f64, u64, u64) {
    let held = c.held.load(Relaxed) as usize;
    if held < 2 {
        return (0, 0.0, 0.0, 0, 0);
    }
    let next = c.next.load(Relaxed) as usize;
    let newest = &c.samples[(next + SAMPLES - 1) % SAMPLES];
    let oldest = &c.samples[(next + SAMPLES - held) % SAMPLES];
    let d = |f: fn(&Sample) -> &AtomicU64| {
        f(newest)
            .load(Relaxed)
            .saturating_sub(f(oldest).load(Relaxed))
    };
    let ms = d(|s| &s.ms).max(1);
    let per_sec = |n: u64| n * 1000 / ms;
    let cycles = d(|s| &s.cycles);
    (
        per_sec(d(|s| &s.ops)),
        d(|s| &s.net_in) as f64 * 1000.0 / ms as f64 / 1024.0,
        d(|s| &s.net_out) as f64 * 1000.0 / ms as f64 / 1024.0,
        per_sec(cycles),
        d(|s| &s.duration).checked_div(cycles).unwrap_or(0),
    )
}

/// `INFO stats`' rate lines, which Redis prints after
/// `total_commands_processed` and after the net totals.
pub fn ops_line() -> String {
    let ops = ctl().map_or(0, |c| rates(c).0);
    format!("instantaneous_ops_per_sec:{ops}")
}

pub fn kbps_lines() -> [String; 4] {
    let (_, input, output, _, _) = ctl().map_or((0, 0.0, 0.0, 0, 0), rates);
    [
        format!("instantaneous_input_kbps:{input:.2}"),
        format!("instantaneous_output_kbps:{output:.2}"),
        "instantaneous_input_repl_kbps:0.00".into(),
        "instantaneous_output_repl_kbps:0.00".into(),
    ]
}

/// `INFO stats`' event loop lines.
pub fn stats_lines() -> [String; 5] {
    let get = |f: fn(&EventLoopCtl) -> &AtomicU64| ctl().map_or(0, |c| f(c).load(Relaxed));
    let (_, _, _, cycles_per_sec, usec) = ctl().map_or((0, 0.0, 0.0, 0, 0), rates);
    [
        format!("eventloop_cycles:{}", get(|c| &c.cycles)),
        format!("eventloop_duration_sum:{}", get(|c| &c.duration_sum)),
        format!("eventloop_duration_cmd_sum:{}", get(|c| &c.cmd_sum)),
        format!("instantaneous_eventloop_cycles_per_sec:{cycles_per_sec}"),
        format!("instantaneous_eventloop_duration_usec:{usec}"),
    ]
}

/// `INFO debug`, which only naming it shows. No AOF is written.
pub fn debug_lines() -> [String; 4] {
    let get = |f: fn(&EventLoopCtl) -> &AtomicU64| ctl().map_or(0, |c| f(c).load(Relaxed));
    [
        "eventloop_duration_aof_sum:0".into(),
        format!("eventloop_duration_cron_sum:{}", get(|c| &c.cron_sum)),
        format!("eventloop_duration_max:{}", get(|c| &c.duration_max)),
        format!(
            "eventloop_cmd_per_cycle_max:{}",
            get(|c| &c.cmd_per_cycle_max)
        ),
    ]
}
