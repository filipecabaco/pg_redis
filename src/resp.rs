#![forbid(unsafe_code)]
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

const MAX_BULK_LEN: usize = 512 * 1024 * 1024; // 512 MiB — matches Redis default
const MAX_ARRAY_LEN: usize = 65_536;
/// Redis's `PROTO_INLINE_MAX_SIZE`: the longest line accepted, inline command
/// or RESP header alike.
const MAX_INLINE_LEN: usize = 64 * 1024;
/// Bulk strings are read in chunks of this size rather than allocated up front,
/// so a client cannot reserve memory it never intends to send.
const BULK_CHUNK: usize = 64 * 1024;

/// Reads RESP commands from a client. Generic over the byte source so the
/// parser can be driven from memory (the fuzz harness below); the server only
/// ever builds it over a `TcpStream`.
pub struct RespParser<R = TcpStream> {
    reader: BufReader<R>,
    line_buf: Vec<u8>,
}

impl<R: Read> RespParser<R> {
    pub fn from_reader(source: R) -> Self {
        Self {
            reader: BufReader::new(source),
            line_buf: Vec::with_capacity(256),
        }
    }

    /// Whether a command is already waiting in the read buffer — a pipeline
    /// that arrived together.
    pub fn has_buffered(&self) -> bool {
        !self.reader.buffer().is_empty()
    }
}

impl RespParser<TcpStream> {
    pub fn new(stream: TcpStream) -> Self {
        Self::from_reader(stream)
    }

    pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        self.reader.get_ref().set_read_timeout(dur)
    }

    /// Whether the client has hung up, without consuming anything it sent:
    /// a peek that reads end-of-stream. Buffered input means it has not.
    pub fn peer_closed(&self) -> bool {
        !self.has_buffered() && crate::sys::peer_closed(self.reader.get_ref())
    }

    /// Wait up to `timeout` for the client to send something. True once the
    /// socket is readable, or has hung up or failed, which the next read
    /// reports; false when the time ran out.
    pub fn wait_readable(&self, timeout: Duration) -> bool {
        crate::sys::wait_readable(self.reader.get_ref(), timeout)
    }
}

impl<R: Read> RespParser<R> {
    fn read_line_bytes(&mut self) -> io::Result<&[u8]> {
        self.line_buf.clear();
        // `read_until` only returns at a newline or EOF, so it must be capped:
        // an unterminated stream would otherwise grow `line_buf` without limit.
        let n = (&mut self.reader)
            .take(MAX_INLINE_LEN as u64 + 1)
            .read_until(b'\n', &mut self.line_buf)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        if self.line_buf.last() != Some(&b'\n') {
            // Redis's wording, which names what the line was meant to be.
            let what = match self.line_buf.first() {
                Some(b'*') => "too big mbulk count string",
                Some(b'$') => "too big bulk count string",
                _ => "too big inline request",
            };
            return Err(protocol_error(what));
        }
        let end = self.line_buf.len();
        let end = if end >= 2 && self.line_buf[end - 2] == b'\r' {
            end - 2
        } else if end >= 1 {
            end - 1
        } else {
            end
        };
        Ok(&self.line_buf[..end])
    }

    pub fn read_command(&mut self) -> io::Result<Vec<Vec<u8>>> {
        let line = self.read_line_bytes()?;

        if line.first() == Some(&b'*') {
            let count: i64 = std::str::from_utf8(&line[1..])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| protocol_error("invalid multibulk length"))?;
            // A count of zero or less is no command at all, which Redis
            // skips rather than refuses.
            if count <= 0 {
                return Ok(vec![]);
            }
            if count as u64 > MAX_ARRAY_LEN as u64 {
                return Err(protocol_error("invalid multibulk length"));
            }
            let count = count as usize;
            let mut parts = Vec::with_capacity(count);
            for _ in 0..count {
                parts.push(self.read_bulk_string()?);
            }
            Ok(parts)
        } else {
            split_inline(line)
        }
    }

    fn read_bulk_string(&mut self) -> io::Result<Vec<u8>> {
        let line = self.read_line_bytes()?;
        match line.first() {
            Some(b'$') => {}
            other => {
                let got = other.map(|&b| b as char).unwrap_or(' ');
                return Err(protocol_error(&format!("expected '$', got '{got}'")));
            }
        }
        let len: i64 = std::str::from_utf8(&line[1..])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| protocol_error("invalid bulk length"))?;
        if len < 0 || len as u64 > MAX_BULK_LEN as u64 {
            return Err(protocol_error("invalid bulk length"));
        }
        let len = len as usize;
        // Grow as the bytes actually arrive. Trusting the announced length would
        // let a client commit up to MAX_BULK_LEN per connection from a 12-byte
        // header and then simply stall, holding the memory for the read timeout.
        let mut buf = Vec::with_capacity(len.min(BULK_CHUNK));
        while buf.len() < len {
            let end = (buf.len() + BULK_CHUNK).min(len);
            let start = buf.len();
            buf.resize(end, 0);
            self.reader.read_exact(&mut buf[start..])?;
        }
        let mut crlf = [0u8; 2];
        self.reader.read_exact(&mut crlf)?;
        Ok(buf)
    }
}

/// A malformed request. The worker answers it with `-ERR Protocol error: ...`
/// and closes the connection, as Redis does — the stream cannot be trusted
/// past the point it went wrong.
fn protocol_error(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason.to_string())
}

/// The text a protocol error is reported with, or `None` for an error that is
/// not one (a timeout, a hung-up client).
pub fn protocol_error_reason(e: &io::Error) -> Option<String> {
    (e.kind() == io::ErrorKind::InvalidData).then(|| format!("Protocol error: {e}"))
}

/// An inline command split as Redis's `sdssplitargs` does: whitespace between
/// arguments, double quotes with C escapes (`\n`, `\xHH`, ...), single quotes
/// with only `\'`. A closing quote must end the argument.
pub(crate) fn split_inline(line: &[u8]) -> io::Result<Vec<Vec<u8>>> {
    let unbalanced = || protocol_error("unbalanced quotes in request");
    let mut args = Vec::new();
    let mut i = 0;
    // Redis splits the line as a C string, so a NUL byte ends it.
    let n = line.iter().position(|&b| b == 0).unwrap_or(line.len());
    loop {
        while i < n && matches!(line[i], b' ' | b'\n' | b'\r' | b'\t' | 0x0b | 0x0c) {
            i += 1;
        }
        if i >= n {
            return Ok(args);
        }
        let mut cur = Vec::new();
        let (mut inq, mut insq) = (false, false);
        loop {
            if inq {
                if i >= n {
                    return Err(unbalanced());
                }
                let c = line[i];
                if c == b'\\' && i + 3 < n && line[i + 1] == b'x' {
                    let hex = |b: u8| (b as char).to_digit(16);
                    if let (Some(h), Some(l)) = (hex(line[i + 2]), hex(line[i + 3])) {
                        cur.push((h * 16 + l) as u8);
                        i += 4;
                        continue;
                    }
                }
                if c == b'\\' && i + 1 < n {
                    cur.push(match line[i + 1] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 0x08,
                        b'a' => 0x07,
                        other => other,
                    });
                    i += 2;
                    continue;
                }
                if c == b'"' {
                    if i + 1 < n && !line[i + 1].is_ascii_whitespace() {
                        return Err(unbalanced());
                    }
                    i += 1;
                    break;
                }
                cur.push(c);
                i += 1;
            } else if insq {
                if i >= n {
                    return Err(unbalanced());
                }
                let c = line[i];
                if c == b'\\' && i + 1 < n && line[i + 1] == b'\'' {
                    cur.push(b'\'');
                    i += 2;
                    continue;
                }
                if c == b'\'' {
                    if i + 1 < n && !line[i + 1].is_ascii_whitespace() {
                        return Err(unbalanced());
                    }
                    i += 1;
                    break;
                }
                cur.push(c);
                i += 1;
            } else {
                if i >= n {
                    break;
                }
                match line[i] {
                    b' ' | b'\n' | b'\r' | b'\t' => break,
                    b'"' => inq = true,
                    b'\'' => insq = true,
                    c => cur.push(c),
                }
                i += 1;
            }
        }
        args.push(cur);
    }
}

pub fn write_simple_string(w: &mut impl Write, s: &str) -> io::Result<()> {
    write!(w, "+{}\r\n", s)
}

thread_local! {
    /// Set by every error reply on this thread, so the connection loop can
    /// tell a failed command from a successful one for `INFO commandstats`.
    pub static REPLIED_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A RESP error, prefixed with `ERR` only when the message carries no code of
/// its own. Clients match on the code, so prefixing unconditionally turned an
/// OOM reply into `-ERR OOM …`, which no OOM handling recognises.
pub fn write_error(w: &mut impl Write, msg: &str) -> io::Result<()> {
    REPLIED_ERROR.with(|e| e.set(true));
    let code = msg
        .split(' ')
        .next()
        .filter(|c| ERROR_CODES.contains(c))
        .unwrap_or("ERR");
    crate::server::count_error(code);
    // An error is one line. Messages that echo client input — "unknown
    // command ... with args beginning with" — can carry a CR or LF, and a raw
    // one ends the line early and desyncs every reply after it. Redis maps
    // both to spaces for the same reason.
    let clean;
    let msg = if msg.contains(['\r', '\n']) {
        clean = msg.replace(['\r', '\n'], " ");
        clean.as_str()
    } else {
        msg
    };
    if has_error_code(msg) {
        write!(w, "-{}\r\n", msg)
    } else {
        write!(w, "-ERR {}\r\n", msg)
    }
}

/// The error codes Redis defines, so a message opening with one keeps it. A
/// list rather than "first word is uppercase": plenty of messages open with a
/// command name and still need the prefix.
const ERROR_CODES: &[&str] = &[
    "ERR",
    "WRONGTYPE",
    "NOAUTH",
    "WRONGPASS",
    "OOM",
    "EXECABORT",
    "NOSCRIPT",
    "LOADING",
    "BUSY",
    "BUSYKEY",
    "BUSYGROUP",
    "NOGROUP",
    "NOPERM",
    "READONLY",
    "NOPROTO",
    "INVALIDOBJ",
    "NOTBUSY",
    "UNBLOCKED",
    "TRYAGAIN",
    "MOVED",
    "CROSSSLOT",
    "NOREPLICAS",
    "MASTERDOWN",
    "MISCONF",
    "NOGOODSLAVE",
    "TESTFAILED",
];

/// Whether `msg` opens with one of those codes followed by a space.
/// An error passed through as given, its first word counted as its code — a
/// script's own `{err=...}` or `redis.error_reply`, whose codes Redis does not
/// vet.
pub fn write_error_verbatim(w: &mut impl Write, msg: &str) -> io::Result<()> {
    REPLIED_ERROR.with(|e| e.set(true));
    let msg = msg.replace(['\r', '\n'], " ");
    let msg = msg.trim_end();
    let code = msg
        .split(' ')
        .next()
        .filter(|c| !c.is_empty())
        .unwrap_or("ERR");
    crate::server::count_error(code);
    write!(w, "-{}\r\n", msg)
}

pub(crate) fn has_error_code(msg: &str) -> bool {
    matches!(msg.split_once(' '), Some((code, _)) if ERROR_CODES.contains(&code))
}

pub fn write_integer(w: &mut impl Write, n: i64) -> io::Result<()> {
    write!(w, ":{}\r\n", n)
}

pub fn write_bulk_string(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    write!(w, "${}\r\n", data.len())?;
    w.write_all(data)?;
    write!(w, "\r\n")
}

pub fn write_null_bulk(w: &mut impl Write) -> io::Result<()> {
    write!(w, "$-1\r\n")
}

pub fn write_null_array(w: &mut impl Write) -> io::Result<()> {
    write!(w, "*-1\r\n")
}

pub fn write_array_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, "*{}\r\n", count)
}

pub fn write_map_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, "%{}\r\n", count)
}

fn write_push_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, ">{}\r\n", count)
}

/// Write the header for a pub/sub frame: push (RESP3) or array (RESP2).
pub fn write_pubsub_header(w: &mut impl Write, count: usize, proto: u8) -> io::Result<()> {
    if proto == 3 {
        write_push_header(w, count)
    } else {
        write_array_header(w, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    /// Feed `data` to a parser over a real socket, so the parser exercises the
    /// same `BufReader<TcpStream>` path it uses in production.
    fn parser_for(data: Vec<u8>) -> RespParser {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            // A client that hangs up mid-command is normal; ignore write errors.
            conn.write_all(&data).ok();
        });
        RespParser::new(TcpStream::connect(addr).unwrap())
    }

    fn parse(data: &[u8]) -> io::Result<Vec<Vec<u8>>> {
        parser_for(data.to_vec()).read_command()
    }

    /// A writer under test, paired with the bytes it must produce.
    type WriteCase = (fn(&mut Vec<u8>) -> io::Result<()>, &'static [u8]);

    #[test]
    fn writers_emit_resp_wire_format() {
        let cases: &[WriteCase] = &[
            (|w| write_simple_string(w, "OK"), b"+OK\r\n"),
            (|w| write_simple_string(w, "PONG"), b"+PONG\r\n"),
            (|w| write_error(w, "bad"), b"-ERR bad\r\n"),
            // Already coded: prefixing again would give "-ERR ERR ..." for the
            // messages that carry their own code, and "-ERR OOM ..." for OOM.
            (|w| write_error(w, "ERR bad thing"), b"-ERR bad thing\r\n"),
            (
                |w| write_error(w, "WRONGTYPE wrong kind"),
                b"-WRONGTYPE wrong kind\r\n",
            ),
            (
                |w| write_error(w, "OOM command not allowed"),
                b"-OOM command not allowed\r\n",
            ),
            // Anything that is not a defined code is prose and still gets the
            // prefix — including messages opening with a command name.
            (|w| write_error(w, "not a code"), b"-ERR not a code\r\n"),
            (
                |w| write_error(w, "WATCH requires shared memory"),
                b"-ERR WATCH requires shared memory\r\n",
            ),
            (|w| write_integer(w, 42), b":42\r\n"),
            (|w| write_integer(w, -2), b":-2\r\n"),
            (|w| write_bulk_string(w, b"hello"), b"$5\r\nhello\r\n"),
            (|w| write_bulk_string(w, b""), b"$0\r\n\r\n"),
            (write_null_bulk, b"$-1\r\n"),
            (write_null_array, b"*-1\r\n"),
            (|w| write_array_header(w, 3), b"*3\r\n"),
            (|w| write_map_header(w, 7), b"%7\r\n"),
            (|w| write_push_header(w, 2), b">2\r\n"),
        ];
        for (write, expected) in cases {
            let mut buf = Vec::new();
            write(&mut buf).unwrap();
            assert_eq!(
                &buf,
                expected,
                "expected {:?}",
                String::from_utf8_lossy(expected)
            );
        }
    }

    /// A NUL byte ends an inline request, as it does in Redis. Found by the
    /// fuzzer: the splitter used to stop on a NUL without consuming it and
    /// pushed empty arguments until memory ran out.
    #[test]
    fn an_inline_request_ends_at_a_nul() {
        let cases: [(&[u8], &[&[u8]]); 3] = [
            (b"\0", &[]),
            (b"GET a\0b c", &[b"GET", b"a"]),
            (b"SET k v\0\0\0", &[b"SET", b"k", b"v"]),
        ];
        for (line, want) in cases {
            assert_eq!(split_inline(line).unwrap(), want, "{line:?}");
        }
        assert!(split_inline(b"GET \"a\0b\"").is_err());
    }

    /// An error that echoes client bytes stays one line: a CR or LF in it
    /// used to end the reply early and desync the connection.
    #[test]
    fn an_error_never_spans_lines() {
        let mut out = Vec::new();
        write_error(
            &mut out,
            "unknown command 'EVAL', with args beginning with: '\n x\r\n'",
        )
        .unwrap();
        assert_eq!(
            out,
            b"-ERR unknown command 'EVAL', with args beginning with: '  x  '\r\n".to_vec()
        );
    }

    #[test]
    fn pubsub_header_follows_negotiated_protocol() {
        let mut resp2 = Vec::new();
        write_pubsub_header(&mut resp2, 3, 2).unwrap();
        assert_eq!(resp2, b"*3\r\n");

        let mut resp3 = Vec::new();
        write_pubsub_header(&mut resp3, 3, 3).unwrap();
        assert_eq!(resp3, b">3\r\n");
    }

    #[test]
    fn parses_the_command_forms_clients_send() {
        let cases: &[(&[u8], &[&[u8]])] = &[
            // Inline commands, as sent by telnet and `redis-cli` pipe mode.
            (b"PING\r\n", &[b"PING"]),
            (b"SET k v\r\n", &[b"SET", b"k", b"v"]),
            // Bare LF, without the CR.
            (b"PING\n", &[b"PING"]),
            // RESP arrays, as sent by every real client library.
            (b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", &[b"GET", b"foo"]),
            (
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n",
                &[b"SET", b"key", b"val"],
            ),
            // Empty and binary bulk payloads must survive intact.
            (b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n", &[b"GET", b""]),
            (
                b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\n\x00\xff\x01\r\n",
                &[b"SET", b"k", b"\x00\xff\x01"],
            ),
            // Quoted inline arguments, split as Redis's sdssplitargs does.
            (b"SET \"a b\" 'c d'\r\n", &[b"SET", b"a b", b"c d"]),
            (b"SET k \"\\x41\\n\"\r\n", &[b"SET", b"k", b"A\n"]),
            (b"SET k 'it\\'s'\r\n", &[b"SET", b"k", b"it's"]),
            // A multibulk count of zero or less is skipped, not refused.
            (b"*-10\r\n", &[]),
            (b"*0\r\n", &[]),
        ];
        for (wire, expected) in cases {
            let parts = parse(wire).unwrap_or_else(|e| panic!("{:?} failed to parse: {e}", wire));
            let expected: Vec<Vec<u8>> = expected.iter().map(|p| p.to_vec()).collect();
            assert_eq!(parts, expected, "wire {:?}", String::from_utf8_lossy(wire));
        }
    }

    /// Every one of these is a way a client could otherwise make the parser
    /// commit memory it was never sent, or read past a declared limit.
    #[test]
    fn rejects_oversized_declarations() {
        let cases: &[(Vec<u8>, &str)] = &[
            (
                format!("*1\r\n${}\r\n", MAX_BULK_LEN + 1).into_bytes(),
                "invalid bulk length",
            ),
            (
                format!("*{}\r\n", MAX_ARRAY_LEN + 1).into_bytes(),
                "invalid multibulk length",
            ),
            (
                format!("{}\r\n", "A".repeat(MAX_INLINE_LEN + 1)).into_bytes(),
                "too big inline request",
            ),
            // No newline at all: `read_until` would happily buffer forever.
            (vec![b'A'; MAX_INLINE_LEN * 4], "too big inline request"),
            // The rest are Redis's protocol errors, worded as Redis words them.
            (b"*1\r\n$-1\r\n".to_vec(), "invalid bulk length"),
            (b"*1\r\n$abc\r\n".to_vec(), "invalid bulk length"),
            (b"*abc\r\n".to_vec(), "invalid multibulk length"),
            (b"*1\r\nfoo\r\n".to_vec(), "expected '$', got 'f'"),
            (b"set \"\"\"k\"\"\" v\r\n".to_vec(), "unbalanced quotes"),
            (b"set \"k v\r\n".to_vec(), "unbalanced quotes"),
        ];
        for (wire, expected) in cases {
            let err = parse(wire).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "for {expected}");
            assert!(
                err.to_string().contains(expected),
                "expected {expected:?}, got {err}"
            );
        }
    }

    #[test]
    fn accepts_an_inline_command_exactly_at_the_limit() {
        // MAX_INLINE_LEN bytes on the wire, terminator included.
        let line = format!("{}\r\n", "X".repeat(MAX_INLINE_LEN - 2)).into_bytes();
        let parts = parse(&line).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].len(), MAX_INLINE_LEN - 2);
    }

    #[test]
    fn bulk_string_spanning_several_chunks_round_trips() {
        // Larger than BULK_CHUNK, so the incremental read loop runs more than once.
        let payload = vec![b'z'; BULK_CHUNK * 2 + 7];
        let mut wire = format!("*1\r\n${}\r\n", payload.len()).into_bytes();
        wire.extend_from_slice(&payload);
        wire.extend_from_slice(b"\r\n");
        assert_eq!(parse(&wire).unwrap(), vec![payload]);
    }

    #[test]
    fn a_declared_bulk_length_alone_does_not_commit_memory() {
        // Announce 512 MiB, send nothing, and hang up. If the parser pre-allocated
        // on the header, this test would reserve half a gigabyte.
        let err = parse(b"*1\r\n$536870912\r\n").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
