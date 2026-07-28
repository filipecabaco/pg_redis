use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

const MAX_BULK_LEN: usize = 512 * 1024 * 1024; // 512 MiB — matches Redis default
const MAX_ARRAY_LEN: usize = 65_536;
const MAX_INLINE_LEN: usize = 8_192;
/// Bulk strings are read in chunks of this size rather than allocated up front,
/// so a client cannot reserve memory it never intends to send.
const BULK_CHUNK: usize = 64 * 1024;

pub struct RespParser {
    reader: BufReader<TcpStream>,
    line_buf: Vec<u8>,
}

impl RespParser {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            reader: BufReader::new(stream),
            line_buf: Vec::with_capacity(256),
        }
    }

    pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        self.reader.get_ref().set_read_timeout(dur)
    }

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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inline command too long",
            ));
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
            let count_str = std::str::from_utf8(&line[1..])
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad array count"))?;
            let count: usize = count_str
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad array count"))?;
            if count > MAX_ARRAY_LEN {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "array too large",
                ));
            }
            let mut parts = Vec::with_capacity(count);
            for _ in 0..count {
                parts.push(self.read_bulk_string()?);
            }
            Ok(parts)
        } else {
            Ok(line
                .split(|b| b.is_ascii_whitespace())
                .filter(|w| !w.is_empty())
                .map(<[u8]>::to_vec)
                .collect())
        }
    }

    fn read_bulk_string(&mut self) -> io::Result<Vec<u8>> {
        let line = self.read_line_bytes()?;
        if line.first() != Some(&b'$') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected bulk string",
            ));
        }
        let len_str = std::str::from_utf8(&line[1..])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad bulk len"))?;
        let len: i64 = len_str
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad bulk len"))?;
        if len < 0 {
            return Ok(vec![]);
        }
        let len = len as usize;
        if len > MAX_BULK_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bulk string too large",
            ));
        }
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

pub fn write_simple_string(w: &mut impl Write, s: &str) -> io::Result<()> {
    write!(w, "+{}\r\n", s)
}

/// A RESP error, prefixed with `ERR` only when the message carries no code of
/// its own. Clients match on the code, so prefixing unconditionally turned an
/// OOM reply into `-ERR OOM …`, which no OOM handling recognises.
pub fn write_error(w: &mut impl Write, msg: &str) -> io::Result<()> {
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
    "NOPERM",
    "READONLY",
    "NOPROTO",
];

/// Whether `msg` opens with one of those codes followed by a space.
fn has_error_code(msg: &str) -> bool {
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
            // A negative bulk length is the RESP null, decoded as empty.
            (b"*1\r\n$-1\r\n", &[b""]),
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
                "bulk string too large",
            ),
            (
                format!("*{}\r\n", MAX_ARRAY_LEN + 1).into_bytes(),
                "array too large",
            ),
            (
                format!("{}\r\n", "A".repeat(MAX_INLINE_LEN + 1)).into_bytes(),
                "inline command too long",
            ),
            // No newline at all: `read_until` would happily buffer forever.
            (vec![b'A'; MAX_INLINE_LEN * 4], "inline command too long"),
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
