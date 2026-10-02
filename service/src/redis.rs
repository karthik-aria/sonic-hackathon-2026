use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::db_layout::{Endpoint, Layout};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BULK_BYTES: usize = 64 * 1024 * 1024;
const MAX_ARRAY_ITEMS: usize = 1_000_000;
const MAX_RESP_DEPTH: usize = 32;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Reply {
    Simple(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Self>>),
}

enum Socket {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(buf),
            Self::Unix(stream) => stream.read(buf),
        }
    }
}

impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(buf),
            Self::Unix(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.flush(),
            Self::Unix(stream) => stream.flush(),
        }
    }
}

pub(crate) struct Connection {
    reader: BufReader<Socket>,
}

impl Connection {
    pub(crate) fn connect(endpoint: &Endpoint) -> Result<Self> {
        let socket =
            if let Some(path) = endpoint.socket.as_ref().filter(|path| Path::exists(path)) {
                Socket::Unix(UnixStream::connect(path).with_context(|| {
                    format!("connecting to Redis Unix socket {}", path.display())
                })?)
            } else {
                let address = (endpoint.host.as_str(), endpoint.port)
                    .to_socket_addrs()
                    .context("resolving Redis host")?
                    .next()
                    .context("Redis host resolved to no addresses")?;
                Socket::Tcp(
                    TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)
                        .with_context(|| format!("connecting to Redis at {address}"))?,
                )
            };
        Ok(Self {
            reader: BufReader::new(socket),
        })
    }

    pub(crate) fn authenticate(&mut self, endpoint: &Endpoint) -> Result<()> {
        let Some(path) = endpoint.password_file.as_ref() else {
            return Ok(());
        };
        let password = std::fs::read_to_string(path)
            .with_context(|| format!("reading Redis password file {}", path.display()))?;
        let password = password.trim();
        if password.is_empty() {
            bail!("Redis password file is empty");
        }
        drop(self.command(&["AUTH", password])?);
        Ok(())
    }

    pub(crate) fn select(&mut self, database_id: u32) -> Result<()> {
        drop(self.command(&["SELECT", &database_id.to_string()])?);
        Ok(())
    }

    pub(crate) fn command(&mut self, parts: &[&str]) -> Result<Reply> {
        write_command(self.reader.get_mut(), parts)?;
        read_reply(&mut self.reader)
    }

    pub(crate) fn start_monitor(&mut self) -> Result<()> {
        drop(self.command(&["MONITOR"])?);
        Ok(())
    }

    pub(crate) fn read_monitor_line(&mut self, line: &mut String) -> Result<bool> {
        line.clear();
        let read = self.reader.read_line(line)?;
        Ok(read != 0)
    }
}

pub(crate) fn write_command<W>(writer: &mut W, parts: &[&str]) -> io::Result<()>
where
    W: Write,
{
    let count = parts.len();
    write!(writer, "*{count}\r\n")?;
    for part in parts {
        let bytes = part.as_bytes();
        let length = bytes.len();
        write!(writer, "${length}\r\n")?;
        writer.write_all(bytes)?;
        writer.write_all(b"\r\n")?;
    }
    writer.flush()
}

pub(crate) fn read_reply<R>(reader: &mut R) -> Result<Reply>
where
    R: BufRead,
{
    read_reply_at_depth(reader, 0)
}

fn read_reply_at_depth<R>(reader: &mut R, depth: usize) -> Result<Reply>
where
    R: BufRead,
{
    if depth > MAX_RESP_DEPTH {
        bail!("Redis reply exceeded maximum nesting depth");
    }
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line)? == 0 {
        bail!("Redis closed the connection before replying");
    }
    if line.len() < 3 || !line.ends_with(b"\r\n") {
        bail!("Redis returned a malformed RESP line");
    }
    let Some(kind) = line.first().copied() else {
        bail!("Redis returned an empty RESP line");
    };
    let Some(body) = line.get(1..line.len().saturating_sub(2)) else {
        bail!("Redis returned a malformed RESP line");
    };
    match kind {
        b'+' => Ok(Reply::Simple(String::from_utf8(body.to_vec())?)),
        b'-' => bail!("Redis returned an error reply"),
        b':' => Ok(Reply::Integer(std::str::from_utf8(body)?.parse()?)),
        b'$' => {
            let length: i64 = std::str::from_utf8(body)?.parse()?;
            if length == -1 {
                return Ok(Reply::Bulk(None));
            }
            let length = usize::try_from(length)?;
            if length > MAX_BULK_BYTES {
                bail!("Redis bulk reply exceeded maximum size");
            }
            let mut bytes = vec![0; length];
            reader.read_exact(&mut bytes)?;
            let mut crlf = [0; 2];
            reader.read_exact(&mut crlf)?;
            if crlf != *b"\r\n" {
                bail!("Redis returned a malformed bulk reply");
            }
            Ok(Reply::Bulk(Some(bytes)))
        }
        b'*' => {
            let count: i64 = std::str::from_utf8(body)?.parse()?;
            if count == -1 {
                return Ok(Reply::Array(None));
            }
            let count = usize::try_from(count)?;
            if count > MAX_ARRAY_ITEMS {
                bail!("Redis array reply exceeded maximum item count");
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(read_reply_at_depth(reader, depth + 1)?);
            }
            Ok(Reply::Array(Some(items)))
        }
        _ => bail!("Redis returned an unknown RESP reply type"),
    }
}

pub(crate) fn bulk_strings(reply: Reply) -> Result<Vec<String>> {
    let Reply::Array(Some(items)) = reply else {
        bail!("Redis HGETALL reply was not an array");
    };
    items
        .into_iter()
        .map(|item| match item {
            Reply::Bulk(Some(value)) => {
                String::from_utf8(value).context("Redis value was not UTF-8")
            }
            Reply::Simple(_) | Reply::Integer(_) | Reply::Bulk(_) | Reply::Array(_) => {
                bail!("Redis HGETALL returned a non-bulk value");
            }
        })
        .collect()
}

pub(crate) fn read_oid_map(layout: &Layout) -> Result<BTreeMap<String, String>> {
    let Some(database) = layout.database("COUNTERS_DB") else {
        return Ok(BTreeMap::new());
    };
    let mut connection = Connection::connect(&database.endpoint)?;
    connection.authenticate(&database.endpoint)?;
    connection.select(database.id)?;
    let mut output = BTreeMap::new();
    for key in ["COUNTERS_PORT_NAME_MAP", "COUNTERS_RIF_NAME_MAP"] {
        let pairs = bulk_strings(connection.command(&["HGETALL", key])?)?;
        output.extend(oid_map_from_hash_pairs(key, &pairs)?);
    }
    Ok(output)
}

fn oid_map_from_hash_pairs(key: &str, pairs: &[String]) -> Result<BTreeMap<String, String>> {
    let (pairs, remainder) = pairs.as_chunks::<2>();
    if !remainder.is_empty() {
        bail!("COUNTERS_DB returned an odd number of values for {key}");
    }
    let mut output = BTreeMap::new();
    for [name, oid] in pairs {
        drop(output.insert(oid.clone(), name.clone()));
    }
    Ok(output)
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_resp_command_encoding() {
        let mut output = Vec::new();
        write_command(&mut output, &["HSET", "PORT|Ethernet1", "mtu", "9100"]).unwrap();
        assert_eq!(
            output,
            b"*4\r\n$4\r\nHSET\r\n$14\r\nPORT|Ethernet1\r\n$3\r\nmtu\r\n$4\r\n9100\r\n"
        );
    }

    #[test]
    fn test_oid_name_hash_is_inverted_to_oid_name_pairs() {
        assert_eq!(
            oid_map_from_hash_pairs(
                "COUNTERS_PORT_NAME_MAP",
                &[
                    "Ethernet0".to_owned(),
                    "oid:0x1".to_owned(),
                    "Ethernet4".to_owned(),
                    "oid:0x2".to_owned(),
                ],
            )
            .unwrap(),
            BTreeMap::from([
                ("oid:0x1".to_owned(), "Ethernet0".to_owned()),
                ("oid:0x2".to_owned(), "Ethernet4".to_owned()),
            ])
        );
    }

    #[test]
    fn test_oid_name_hash_rejects_odd_number_of_values() {
        assert_eq!(
            oid_map_from_hash_pairs("COUNTERS_PORT_NAME_MAP", &["Ethernet0".to_owned()])
                .unwrap_err()
                .to_string(),
            "COUNTERS_DB returned an odd number of values for COUNTERS_PORT_NAME_MAP"
        );
    }

    #[rstest::rstest]
    #[case::simple(b"+OK\r\n", Reply::Simple("OK".to_owned()))]
    #[case::integer(b":42\r\n", Reply::Integer(42))]
    #[case::bulk(b"$3\r\nfoo\r\n", Reply::Bulk(Some(b"foo".to_vec())))]
    #[case::nil(b"$-1\r\n", Reply::Bulk(None))]
    #[case::array(
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        Reply::Array(Some(vec![
            Reply::Bulk(Some(b"a".to_vec())),
            Reply::Bulk(Some(b"b".to_vec()))
        ]))
    )]
    fn test_resp_reply(#[case] input: &[u8], #[case] expected: Reply) {
        assert_eq!(read_reply(&mut Cursor::new(input)).unwrap(), expected);
    }
}
