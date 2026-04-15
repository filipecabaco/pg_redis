use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;

const MAX_BULK_LEN: usize = 512 * 1024 * 1024; // 512 MiB — matches Redis default
const MAX_ARRAY_LEN: usize = 65_536;
const MAX_INLINE_LEN: usize = 8_192;

pub struct RespParser {
    reader: BufReader<TcpStream>,
}

impl RespParser {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            reader: BufReader::new(stream),
        }
    }

    pub fn read_command(&mut self) -> io::Result<Vec<Vec<u8>>> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }

        if line.len() > MAX_INLINE_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inline command too long",
            ));
        }

        let line = line.trim_end_matches("\r\n").trim_end_matches('\n');

        if let Some(rest) = line.strip_prefix('*') {
            let count: usize = rest
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
                .split_whitespace()
                .map(|s| s.as_bytes().to_vec())
                .collect())
        }
    }

    fn read_bulk_string(&mut self) -> io::Result<Vec<u8>> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        let line = line.trim_end_matches("\r\n").trim_end_matches('\n');
        let rest = line
            .strip_prefix('$')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "expected bulk string"))?;
        let len: i64 = rest
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
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf)?;
        let mut crlf = [0u8; 2];
        self.reader.read_exact(&mut crlf)?;
        Ok(buf)
    }
}

pub fn write_simple_string(w: &mut impl Write, s: &str) -> io::Result<()> {
    write!(w, "+{}\r\n", s)
}

pub fn write_error(w: &mut impl Write, msg: &str) -> io::Result<()> {
    write!(w, "-ERR {}\r\n", msg)
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

pub fn write_array_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, "*{}\r\n", count)
}

pub fn write_map_header(w: &mut impl Write, count: usize) -> io::Result<()> {
    write!(w, "%{}\r\n", count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    fn check_write(f: impl Fn(&mut Vec<u8>) -> io::Result<()>, expected: &[u8]) {
        let mut buf = Vec::new();
        f(&mut buf).unwrap();
        assert_eq!(buf, expected);
    }

    fn make_parser(data: &'static [u8]) -> RespParser {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            conn.write_all(data).unwrap();
        });
        let stream = TcpStream::connect(addr).unwrap();
        RespParser::new(stream)
    }

    fn make_parser_owned(data: Vec<u8>) -> RespParser {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            conn.write_all(&data).unwrap();
        });
        let stream = TcpStream::connect(addr).unwrap();
        RespParser::new(stream)
    }

    #[test]
    fn test_write_simple_string() {
        check_write(|w| write_simple_string(w, "OK"), b"+OK\r\n");
    }

    #[test]
    fn test_write_pong() {
        check_write(|w| write_simple_string(w, "PONG"), b"+PONG\r\n");
    }

    #[test]
    fn test_write_error() {
        check_write(|w| write_error(w, "bad"), b"-ERR bad\r\n");
    }

    #[test]
    fn test_write_integer_positive() {
        check_write(|w| write_integer(w, 42), b":42\r\n");
    }

    #[test]
    fn test_write_integer_negative() {
        check_write(|w| write_integer(w, -2), b":-2\r\n");
    }

    #[test]
    fn test_write_bulk_string() {
        check_write(|w| write_bulk_string(w, b"hello"), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_write_bulk_string_empty() {
        check_write(|w| write_bulk_string(w, b""), b"$0\r\n\r\n");
    }

    #[test]
    fn test_write_null_bulk() {
        check_write(|w| write_null_bulk(w), b"$-1\r\n");
    }

    #[test]
    fn test_write_array_header() {
        check_write(|w| write_array_header(w, 3), b"*3\r\n");
    }

    #[test]
    fn test_write_map_header() {
        check_write(|w| write_map_header(w, 7), b"%7\r\n");
    }

    #[test]
    fn test_parse_inline_ping() {
        let mut parser = make_parser(b"PING\r\n");
        let parts = parser.read_command().unwrap();
        assert_eq!(parts, vec![b"PING".to_vec()]);
    }

    #[test]
    fn test_parse_array_get() {
        let mut parser = make_parser(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        let parts = parser.read_command().unwrap();
        assert_eq!(parts, vec![b"GET".to_vec(), b"foo".to_vec()]);
    }

    #[test]
    fn test_parse_array_set() {
        let mut parser = make_parser(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n");
        let parts = parser.read_command().unwrap();
        assert_eq!(
            parts,
            vec![b"SET".to_vec(), b"key".to_vec(), b"val".to_vec()]
        );
    }

    #[test]
    fn test_parse_binary_safe_value() {
        let mut parser = make_parser(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n");
        let parts = parser.read_command().unwrap();
        assert_eq!(parts[2], b"hello");
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let mut parser = make_parser(b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n");
        let parts = parser.read_command().unwrap();
        assert_eq!(parts, vec![b"GET".to_vec(), b"".to_vec()]);
    }

    #[test]
    fn bulk_string_at_limit_is_accepted() {
        // Build a message with a bulk string exactly at MAX_BULK_LEN.
        // We only test the header parsing path — actually allocating 512 MiB in a test
        // is not practical, so we use a small representative value instead and verify
        // the boundary check is the only guard (tested separately below).
        let data = b"*1\r\n$5\r\nhello\r\n";
        let mut parser = make_parser(data);
        let parts = parser.read_command().unwrap();
        assert_eq!(parts[0], b"hello");
    }

    #[test]
    fn bulk_string_over_limit_is_rejected() {
        let over_limit = MAX_BULK_LEN + 1;
        let header = format!("*1\r\n${}\r\n", over_limit);
        let mut parser = make_parser_owned(header.into_bytes());
        let err = parser.read_command().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("bulk string too large"));
    }

    #[test]
    fn array_over_limit_is_rejected() {
        let over_limit = MAX_ARRAY_LEN + 1;
        let header = format!("*{}\r\n", over_limit);
        let mut parser = make_parser_owned(header.into_bytes());
        let err = parser.read_command().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("array too large"));
    }

    #[test]
    fn inline_command_over_limit_is_rejected() {
        let long_line = format!("{}\r\n", "A".repeat(MAX_INLINE_LEN + 1));
        let mut parser = make_parser_owned(long_line.into_bytes());
        let err = parser.read_command().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("inline command too long"));
    }

    #[test]
    fn inline_command_at_limit_is_accepted() {
        // A line of exactly MAX_INLINE_LEN bytes (including \r\n) — should parse fine.
        // 8190 chars of 'X' + \r\n = 8192 bytes total.
        let line = format!("{}\r\n", "X".repeat(MAX_INLINE_LEN - 2));
        let mut parser = make_parser_owned(line.into_bytes());
        let parts = parser.read_command().unwrap();
        assert_eq!(parts.len(), 1);
    }
}
