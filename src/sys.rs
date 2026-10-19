//! The operating-system calls the standard library does not wrap, made
//! through rustix's safe wrappers.
#![forbid(unsafe_code)]

use std::net::TcpStream;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

fn timespec(d: Duration) -> rustix::time::Timespec {
    rustix::time::Timespec {
        tv_sec: d.as_secs().min(i64::MAX as u64) as i64,
        tv_nsec: i64::from(d.subsec_nanos()),
    }
}

/// Whether the peer has hung up, without consuming what it sent: a
/// non-blocking peek that reads end-of-stream.
pub fn peer_closed(stream: &TcpStream) -> bool {
    let mut b = [std::mem::MaybeUninit::<u8>::uninit(); 1];
    let flags = libc::MSG_PEEK | libc::MSG_DONTWAIT;
    matches!(
        socket2::SockRef::from(stream).recv_with_flags(&mut b, flags),
        Ok(0)
    )
}

/// Wait up to `timeout` for `stream` to become readable. True once it is, or
/// has hung up or failed (the next read reports which); false when the time
/// ran out.
pub fn wait_readable(stream: &TcpStream, timeout: Duration) -> bool {
    let mut fds = [rustix::event::PollFd::new(
        stream,
        rustix::event::PollFlags::IN,
    )];
    !matches!(
        rustix::event::poll(&mut fds, Some(&timespec(timeout))),
        Ok(0)
    )
}

/// This process's user and system CPU time, in seconds: the 14th and 15th
/// fields of `/proc/self/stat`, in clock ticks. Zero where there is no
/// `/proc` — a development machine, not a server.
pub fn cpu_times() -> (f64, f64) {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return (0.0, 0.0);
    };
    // The command name, field 2, is parenthesised and may hold spaces: count
    // from the last `)` instead. What follows starts at field 3.
    let fields: Vec<&str> = stat
        .rfind(')')
        .map_or("", |at| &stat[at + 1..])
        .split_ascii_whitespace()
        .collect();
    let hz = rustix::param::clock_ticks_per_second() as f64;
    let ticks = |i: usize| {
        fields
            .get(i)
            .and_then(|f| f.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    (ticks(11) / hz, ticks(12) / hz)
}

/// Sleep until `word` stops holding `expected`, a wake on it, or `timeout`.
/// Shared rather than `PRIVATE` futex operations: the waiter and the waker are
/// different processes, the word is in shared memory. Returns at once if the
/// word has already moved, which is the race a waiter needs closed.
#[cfg(target_os = "linux")]
pub fn futex_wait(word: &AtomicU32, expected: u32, timeout: Duration) {
    let flags = rustix::thread::futex::Flags::empty();
    let _ = rustix::thread::futex::wait(word, flags, expected, Some(&timespec(timeout)));
}

/// Wake one process sleeping in [`futex_wait`] on `word`.
#[cfg(target_os = "linux")]
pub fn futex_wake(word: &AtomicU32) {
    let _ = rustix::thread::futex::wake(word, rustix::thread::futex::Flags::empty(), 1);
}

/// Without futexes the waiter sleeps a little and looks again.
#[cfg(not(target_os = "linux"))]
pub fn futex_wait(_word: &AtomicU32, _expected: u32, timeout: Duration) {
    std::thread::sleep(timeout.min(Duration::from_millis(5)));
}

#[cfg(not(target_os = "linux"))]
pub fn futex_wake(_word: &AtomicU32) {}
