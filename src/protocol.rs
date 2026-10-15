//! Postgres wire protocol (v3) framing.
//!
//! The proxy forwards almost everything byte-for-byte, so it works on raw frames
//! (tag + body) and only decodes the handful of messages it needs to inspect, using
//! pgwire's message types. Messages it builds itself (ReadyForQuery, errors, its own
//! extended-protocol queries) are encoded here.

use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use pgwire::messages::{extendedquery, simplequery, DecodeContext, Message};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Hard cap on a single message, to bound memory if a peer sends garbage.
pub const MAX_FRAME: usize = 1 << 30;

pub const SSL_REQUEST: i32 = 80877103;
pub const GSSENC_REQUEST: i32 = 80877104;
pub const CANCEL_REQUEST: i32 = 80877102;

/// One protocol message. `raw` is the whole message as on the wire (tag, length, body),
/// so forwarding never re-encodes; `body` is a view into it.
#[derive(Clone, Debug)]
pub struct Frame {
    pub tag: u8,
    pub body: Bytes,
    raw: Bytes,
}

impl Frame {
    pub fn new(tag: u8, body: impl Into<Bytes>) -> Self {
        let body: Bytes = body.into();
        let mut raw = BytesMut::with_capacity(5 + body.len());
        raw.put_u8(tag);
        raw.put_i32(body.len() as i32 + 4);
        raw.extend_from_slice(&body);
        let raw = raw.freeze();
        Self { tag, body: raw.slice(5..), raw }
    }

    pub fn wire_len(&self) -> usize {
        self.raw.len()
    }

    pub fn encode_into(&self, out: &mut BytesMut) {
        out.extend_from_slice(&self.raw);
    }

    pub fn to_bytes(&self) -> Bytes {
        self.raw.clone()
    }

    /// Decode with one of pgwire's message types.
    pub fn decode<M: Message>(&self) -> Option<M> {
        let mut buf = BytesMut::from(&self.raw[..]);
        M::decode(&mut buf, &decode_ctx()).ok().flatten()
    }
}

fn decode_ctx() -> DecodeContext {
    let mut c = DecodeContext::default();
    c.awaiting_frontend_ssl = false;
    c.awaiting_frontend_startup = false;
    c
}

/// Buffered frame reader.
pub struct FrameReader<R> {
    inner: R,
    buf: BytesMut,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner, buf: BytesMut::with_capacity(16 * 1024) }
    }

    async fn fill(&mut self, need: usize) -> io::Result<bool> {
        while self.buf.len() < need {
            if self.buf.capacity() - self.buf.len() < 8 * 1024 {
                self.buf.reserve(need.max(32 * 1024));
            }
            let n = self.inner.read_buf(&mut self.buf).await?;
            if n == 0 {
                if self.buf.is_empty() {
                    return Ok(false);
                }
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed mid-message"));
            }
        }
        Ok(true)
    }

    /// Next tagged message, or None on a clean EOF between messages.
    pub async fn read_frame(&mut self) -> io::Result<Option<Frame>> {
        if !self.fill(5).await? {
            return Ok(None);
        }
        let tag = self.buf[0];
        let len = i32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        if len < 4 || len as usize > MAX_FRAME {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("bad message length {len}")));
        }
        let total = 1 + len as usize;
        self.fill(total).await?;
        let raw = self.buf.split_to(total).freeze();
        Ok(Some(Frame { tag, body: raw.slice(5..), raw }))
    }

    /// Whether a complete message is already buffered (reading it will not wait).
    pub fn has_complete_frame(&self) -> bool {
        if self.buf.len() < 5 {
            return false;
        }
        let len = i32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        len >= 4 && self.buf.len() > len as usize
    }

    /// An untagged startup-phase packet (length-prefixed), returned whole.
    pub async fn read_startup(&mut self) -> io::Result<Option<Bytes>> {
        if !self.fill(4).await? {
            return Ok(None);
        }
        let len = i32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
        if !(8..=10_000).contains(&len) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad startup packet length"));
        }
        self.fill(len as usize).await?;
        Ok(Some(self.buf.split_to(len as usize).freeze()))
    }

    pub fn into_parts(self) -> (R, BytesMut) {
        (self.inner, self.buf)
    }

    pub fn from_parts(inner: R, buf: BytesMut) -> Self {
        Self { inner, buf }
    }
}

pub async fn write_frames<W: AsyncWrite + Unpin>(w: &mut W, frames: &[Frame]) -> io::Result<()> {
    if let [one] = frames {
        return w.write_all(&one.raw).await;
    }
    let mut out = BytesMut::with_capacity(frames.iter().map(Frame::wire_len).sum());
    for f in frames {
        f.encode_into(&mut out);
    }
    w.write_all(&out).await
}

/// Startup packet code (the second int32).
pub fn startup_code(pkt: &[u8]) -> i32 {
    i32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]])
}

/// Key/value parameters of a StartupMessage.
pub fn startup_params(pkt: &[u8]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut parts = pkt[8..].split(|b| *b == 0);
    while let (Some(k), Some(v)) = (parts.next(), parts.next()) {
        if k.is_empty() {
            break;
        }
        out.push((String::from_utf8_lossy(k).into_owned(), String::from_utf8_lossy(v).into_owned()));
    }
    out
}

// ---- messages the proxy builds -------------------------------------------------

pub fn ready_for_query(status: u8) -> Frame {
    Frame::new(b'Z', Bytes::copy_from_slice(&[status]))
}

/// ErrorResponse / NoticeResponse body from fields.
pub fn error_frame(severity: &str, code: &str, message: &str) -> Frame {
    let mut b = BytesMut::new();
    for (k, v) in [(b'S', severity), (b'V', severity), (b'C', code), (b'M', message)] {
        b.put_u8(k);
        b.extend_from_slice(v.as_bytes());
        b.put_u8(0);
    }
    b.put_u8(0);
    Frame::new(b'E', b.freeze())
}

fn cstr(b: &mut BytesMut, s: &str) {
    b.extend_from_slice(s.as_bytes());
    b.put_u8(0);
}

pub fn parse_msg(name: &str, sql: &str) -> Frame {
    let mut b = BytesMut::new();
    cstr(&mut b, name);
    cstr(&mut b, sql);
    b.put_i16(0);
    Frame::new(b'P', b.freeze())
}

/// Bind with text-format parameters and text results.
pub fn bind_msg(portal: &str, stmt: &str, params: &[Option<&str>]) -> Frame {
    let mut b = BytesMut::new();
    cstr(&mut b, portal);
    cstr(&mut b, stmt);
    b.put_i16(0); // all params text
    b.put_i16(params.len() as i16);
    for p in params {
        match p {
            None => b.put_i32(-1),
            Some(v) => {
                b.put_i32(v.len() as i32);
                b.extend_from_slice(v.as_bytes());
            }
        }
    }
    b.put_i16(0); // all results text
    Frame::new(b'B', b.freeze())
}

pub fn execute_msg(portal: &str) -> Frame {
    let mut b = BytesMut::new();
    cstr(&mut b, portal);
    b.put_i32(0);
    Frame::new(b'E', b.freeze())
}

pub fn close_msg(kind: u8, name: &str) -> Frame {
    let mut b = BytesMut::new();
    b.put_u8(kind);
    cstr(&mut b, name);
    Frame::new(b'C', b.freeze())
}

pub fn sync_msg() -> Frame {
    Frame::new(b'S', Bytes::new())
}

/// Text values of a DataRow.
pub fn data_row_values(body: &[u8]) -> Vec<Option<String>> {
    let mut out = Vec::new();
    if body.len() < 2 {
        return out;
    }
    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut i = 2;
    for _ in 0..n {
        if i + 4 > body.len() {
            break;
        }
        let len = i32::from_be_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
        i += 4;
        if len < 0 {
            out.push(None);
        } else {
            let end = (i + len as usize).min(body.len());
            out.push(Some(String::from_utf8_lossy(&body[i..end]).into_owned()));
            i = end;
        }
    }
    out
}

/// ErrorResponse "M" (message) and "C" (code) fields.
pub fn error_fields(body: &[u8]) -> (String, String) {
    let (mut code, mut msg) = (String::new(), String::new());
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let k = body[i];
        i += 1;
        let end = body[i..].iter().position(|b| *b == 0).map(|p| i + p).unwrap_or(body.len());
        let v = String::from_utf8_lossy(&body[i..end]).into_owned();
        match k {
            b'C' => code = v,
            b'M' => msg = v,
            _ => {}
        }
        i = end + 1;
    }
    (code, msg)
}

/// Text array literal for a Bind parameter, e.g. {"a","b",NULL}.
pub fn text_array(items: &[Option<&str>]) -> String {
    let mut s = String::from("{");
    for (i, it) in items.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        match it {
            None => s.push_str("NULL"),
            Some(v) => {
                s.push('"');
                for c in v.chars() {
                    if c == '"' || c == '\\' {
                        s.push('\\');
                    }
                    s.push(c);
                }
                s.push('"');
            }
        }
    }
    s.push('}');
    s
}

// ---- decoding the client messages the proxy inspects ---------------------------

pub type Parse = extendedquery::Parse;
pub type Bind = extendedquery::Bind;
pub type Describe = extendedquery::Describe;
pub type Execute = extendedquery::Execute;
pub type Close = extendedquery::Close;
pub type Query = simplequery::Query;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_roundtrip() {
        let frames = vec![parse_msg("s1", "SELECT $1"), bind_msg("", "s1", &[Some("x"), None]), sync_msg()];
        let mut buf = Vec::new();
        write_frames(&mut buf, &frames).await.unwrap();
        let mut r = FrameReader::new(&buf[..]);
        let p = r.read_frame().await.unwrap().unwrap();
        let parse: Parse = p.decode().unwrap();
        assert_eq!(parse.name.as_deref(), Some("s1"));
        assert_eq!(parse.query, "SELECT $1");
        let b: Bind = r.read_frame().await.unwrap().unwrap().decode().unwrap();
        assert_eq!(b.parameters.len(), 2);
        assert!(b.parameters[1].is_none());
        assert_eq!(r.read_frame().await.unwrap().unwrap().tag, b'S');
        assert!(r.read_frame().await.unwrap().is_none());
    }

    #[test]
    fn arrays_and_rows() {
        assert_eq!(text_array(&[Some("a\"b"), None, Some("c\\")]), r#"{"a\"b",NULL,"c\\"}"#);
        let mut b = BytesMut::new();
        b.put_i16(2);
        b.put_i32(3);
        b.extend_from_slice(b"abc");
        b.put_i32(-1);
        assert_eq!(data_row_values(&b), vec![Some("abc".to_string()), None]);
        let e = error_frame("ERROR", "42P01", "nope");
        assert_eq!(error_fields(&e.body), ("42P01".to_string(), "nope".to_string()));
    }
}
