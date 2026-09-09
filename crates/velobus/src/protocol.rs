use bytes::Bytes;
use std::fmt;

pub const MAX_FRAME: usize = 1_048_576;
pub const MAX_PAYLOAD: usize = 262_144;
pub const MAX_BATCH: usize = 256;
pub const MAX_FETCH_BYTES: usize = 524_288;
pub const MAX_SCAN: usize = 4096;
pub const PROTOCOL: u16 = 1;
pub const UNAUTHORIZED: u16 = 2;
pub const INVALID: u16 = 3;
pub const CAPACITY: u16 = 4;
pub const LIMIT: u16 = 5;
pub const STORAGE: u16 = 6;
pub const CURSOR: u16 = 7;
pub const NO_SERVICE: u16 = 8;
pub const OVERLOADED: u16 = 9;
pub const DEADLINE_EXCEEDED: u16 = 10;
pub const SERVICE_UNAVAILABLE: u16 = 11;
pub const HANDLER_ERROR: u16 = 12;

#[derive(Debug)]
pub struct Error {
    pub code: u16,
    pub message: String,
}

impl Error {
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Debug)]
pub struct Record {
    pub topic: String,
    pub key: String,
    pub payload: Bytes,
}

impl Record {
    /// Includes space for vector growth and allocator / shared-buffer overhead.
    pub fn retained_bytes(&self) -> u64 {
        // Topic/key are retained both in the shared input frame and in decoded Strings.
        256 + 2 * self.topic.len() as u64 + 2 * self.key.len() as u64 + self.payload.len() as u64
    }

    pub fn wire_bytes(&self) -> usize {
        16 + self.topic.len() + self.key.len() + self.payload.len()
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    pub sequence: u64,
    pub record: Record,
}

pub struct Fetch {
    pub topic: String,
    pub after: u64,
    pub limit: usize,
    pub max_bytes: usize,
    pub latest: bool,
}

pub enum Request {
    Hello {
        version: u16,
        token: String,
    },
    Publish(Vec<Record>),
    Fetch(Fetch),
    Stats,
    Ping,
    Register {
        route: String,
        concurrency: usize,
        queue_limit: usize,
    },
    Rpc {
        route: String,
        timeout_ms: u32,
        payload: Bytes,
    },
    Take {
        wait_ms: u32,
    },
    Complete {
        call_id: u64,
        failed: bool,
        payload: Bytes,
    },
}

pub struct Reader {
    data: Bytes,
    position: usize,
}

impl Reader {
    pub fn new(data: Bytes) -> Self {
        Self { data, position: 0 }
    }
    pub fn take(&mut self, length: usize) -> Result<Bytes, Error> {
        if length > self.data.len().saturating_sub(self.position) {
            return Err(Error::new(PROTOCOL, "truncated request body"));
        }
        let value = self.data.slice(self.position..self.position + length);
        self.position += length;
        Ok(value)
    }
    pub fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(
            self.take(2)?.as_ref().try_into().unwrap(),
        ))
    }
    pub fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(
            self.take(4)?.as_ref().try_into().unwrap(),
        ))
    }
    pub fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(
            self.take(8)?.as_ref().try_into().unwrap(),
        ))
    }
    pub fn string(&mut self, max: usize) -> Result<String, Error> {
        let length = self.u16()? as usize;
        if length > max {
            return Err(Error::new(INVALID, "string exceeds byte limit"));
        }
        let value = self.take(length)?;
        String::from_utf8(value.to_vec()).map_err(|_| Error::new(PROTOCOL, "invalid UTF-8"))
    }
    pub fn finish(self) -> Result<(), Error> {
        if self.position != self.data.len() {
            return Err(Error::new(PROTOCOL, "unexpected trailing bytes"));
        }
        Ok(())
    }
}

pub fn validate_topic(topic: &str, wildcard: bool) -> Result<(), Error> {
    if wildcard && topic == "*" {
        return Ok(());
    }
    if topic.is_empty()
        || topic.len() > 255
        || !topic
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(Error::new(
            INVALID,
            "topic must contain 1..255 ASCII letters, digits, dot, underscore or hyphen",
        ));
    }
    Ok(())
}

pub fn decode_records(reader: &mut Reader) -> Result<Vec<Record>, Error> {
    let count = reader.u16()? as usize;
    if !(1..=MAX_BATCH).contains(&count) {
        return Err(Error::new(INVALID, "publish count must be 1..256"));
    }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let topic = reader.string(255)?;
        validate_topic(&topic, false)?;
        let key = reader.string(1024)?;
        let length = reader.u32()? as usize;
        if length > MAX_PAYLOAD {
            return Err(Error::new(LIMIT, "payload exceeds 262144 bytes"));
        }
        records.push(Record {
            topic,
            key,
            payload: reader.take(length)?,
        });
    }
    Ok(records)
}

pub fn decode_request(opcode: u8, body: Bytes) -> Result<Request, Error> {
    let mut reader = Reader::new(body);
    let request = match opcode {
        1 => Request::Hello {
            version: reader.u16()?,
            token: reader.string(u16::MAX as usize)?,
        },
        2 => Request::Publish(decode_records(&mut reader)?),
        3 => {
            let topic = reader.string(255)?;
            validate_topic(&topic, true)?;
            let after = reader.u64()?;
            let limit = reader.u16()? as usize;
            let max_bytes = reader.u32()? as usize;
            let mode = reader.u8()?;
            if !(1..=MAX_BATCH).contains(&limit) || !(1..=MAX_FETCH_BYTES).contains(&max_bytes) {
                return Err(Error::new(INVALID, "invalid fetch count or byte budget"));
            }
            if mode > 1 {
                return Err(Error::new(PROTOCOL, "unknown fetch mode"));
            }
            Request::Fetch(Fetch {
                topic,
                after,
                limit,
                max_bytes,
                latest: mode == 1,
            })
        }
        4 => Request::Stats,
        5 => Request::Ping,
        6 => {
            let route = reader.string(255)?;
            validate_topic(&route, false)?;
            let concurrency = reader.u16()? as usize;
            let queue_limit = reader.u16()? as usize;
            if !(1..=256).contains(&concurrency) || !(1..=4096).contains(&queue_limit) {
                return Err(Error::new(
                    INVALID,
                    "invalid worker concurrency or queue limit",
                ));
            }
            Request::Register {
                route,
                concurrency,
                queue_limit,
            }
        }
        7 => {
            let route = reader.string(255)?;
            validate_topic(&route, false)?;
            let timeout_ms = reader.u32()?;
            if !(1..=30_000).contains(&timeout_ms) {
                return Err(Error::new(INVALID, "RPC timeout must be 1..30000 ms"));
            }
            let length = reader.u32()? as usize;
            if length > MAX_PAYLOAD {
                return Err(Error::new(LIMIT, "RPC payload exceeds 262144 bytes"));
            }
            Request::Rpc {
                route,
                timeout_ms,
                payload: reader.take(length)?,
            }
        }
        8 => {
            let wait_ms = reader.u32()?;
            if !(1..=1000).contains(&wait_ms) {
                return Err(Error::new(INVALID, "TAKE wait must be 1..1000 ms"));
            }
            Request::Take { wait_ms }
        }
        9 => {
            let call_id = reader.u64()?;
            let status = reader.u8()?;
            if status > 1 {
                return Err(Error::new(INVALID, "invalid completion status"));
            }
            let length = reader.u32()? as usize;
            if length > MAX_PAYLOAD || (status == 1 && length > 4096) {
                return Err(Error::new(LIMIT, "completion payload exceeds limit"));
            }
            let payload = reader.take(length)?;
            if status == 1 && std::str::from_utf8(&payload).is_err() {
                return Err(Error::new(PROTOCOL, "handler error must be UTF-8"));
            }
            Request::Complete {
                call_id,
                failed: status == 1,
                payload,
            }
        }
        _ => return Err(Error::new(PROTOCOL, "unknown opcode")),
    };
    reader.finish()?;
    Ok(request)
}

pub fn put_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

pub fn encode_record(out: &mut Vec<u8>, record: &Record) {
    put_string(out, &record.topic);
    put_string(out, &record.key);
    out.extend_from_slice(&(record.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&record.payload);
}

pub fn encode_records(records: &[Record]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + records.iter().map(|r| r.wire_bytes() - 8).sum::<usize>());
    out.extend_from_slice(&(records.len() as u16).to_be_bytes());
    for record in records {
        encode_record(&mut out, record);
    }
    out
}

pub fn envelope(opcode: u8, request_id: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + body.len());
    out.extend_from_slice(&((5 + body.len()) as u32).to_be_bytes());
    out.push(opcode);
    out.extend_from_slice(&request_id.to_be_bytes());
    out.extend_from_slice(body);
    out
}

pub fn error_body(error: &Error) -> Vec<u8> {
    let mut out = error.code.to_be_bytes().to_vec();
    put_string(&mut out, &error.message);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_decode_rejects_trailing_utf8_modes_and_empty_batches() {
        assert!(decode_request(5, Bytes::from_static(&[1])).is_err());
        assert!(decode_request(2, Bytes::from_static(&[0, 0])).is_err());
        assert!(decode_request(77, Bytes::new()).is_err());
        assert!(decode_request(1, Bytes::from_static(&[0, 1, 0, 1, 255])).is_err());
        let mut fetch = Vec::new();
        put_string(&mut fetch, "*");
        fetch.extend_from_slice(&0u64.to_be_bytes());
        fetch.extend_from_slice(&1u16.to_be_bytes());
        fetch.extend_from_slice(&100u32.to_be_bytes());
        fetch.push(2);
        assert!(decode_request(3, fetch.into()).is_err());
    }

    #[test]
    fn binary_payload_round_trip_and_limits() {
        let records = vec![Record {
            topic: "vehicle.position".into(),
            key: "ç".into(),
            payload: Bytes::from_static(&[0, 255, 1]),
        }];
        let Request::Publish(decoded) = decode_request(2, encode_records(&records).into()).unwrap()
        else {
            panic!()
        };
        assert_eq!(decoded[0].payload, records[0].payload);
        assert_eq!(decoded[0].key, "ç");
        assert!(validate_topic("topic/invalid", false).is_err());
        assert!(validate_topic("*", false).is_err());
        assert!(validate_topic("*", true).is_ok());
        let bad = vec![Record {
            payload: Bytes::from(vec![0; MAX_PAYLOAD + 1]),
            ..records[0].clone()
        }];
        assert_eq!(
            decode_request(2, encode_records(&bad).into())
                .err()
                .unwrap()
                .code,
            LIMIT
        );
    }
}
