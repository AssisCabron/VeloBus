use crate::protocol::{
    decode_records, encode_records, Error, Reader, Record, CAPACITY, MAX_FRAME, STORAGE,
};
use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"VLOWAL01";
const HEADER_BYTES: u64 = 12;

pub struct Wal {
    file: File,
    pub bytes: u64,
    max_bytes: u64,
    #[cfg(test)]
    pub fail_after_write: bool,
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::new(STORAGE, format!("WAL I/O: {error}"))
}

/// Synchronize each newly created directory entry, including nested parents.
fn ensure_directory(path: &Path) -> Result<(), Error> {
    if path.is_dir() {
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    ensure_directory(parent)?;
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(error) => return Err(storage(error)),
    }
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(storage)?;
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(storage)?;
    Ok(())
}

impl Wal {
    pub fn open(
        directory: &Path,
        max_bytes: u64,
        mut replay: impl FnMut(u64, Vec<Record>) -> Result<(), Error>,
    ) -> Result<Self, Error> {
        ensure_directory(directory)?;
        let path = directory.join("events.wal");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(storage)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|_| {
            Error::new(
                STORAGE,
                "data directory is already locked by another process",
            )
        })?;
        let mut length = file.metadata().map_err(storage)?.len();
        if length > max_bytes {
            return Err(Error::new(
                CAPACITY,
                "existing WAL exceeds configured max-wal-bytes",
            ));
        }
        if length == 0 {
            file.write_all(MAGIC).map_err(storage)?;
            file.sync_all().map_err(storage)?;
            length = MAGIC.len() as u64;
        }
        // Also covers a previous process that created the file but did not sync the directory.
        File::open(directory)
            .and_then(|dir| dir.sync_all())
            .map_err(storage)?;
        file.seek(SeekFrom::Start(0)).map_err(storage)?;
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)
            .map_err(|_| Error::new(STORAGE, "incomplete WAL file header"))?;
        if &magic != MAGIC {
            return Err(Error::new(STORAGE, "invalid WAL file header"));
        }
        let mut position = MAGIC.len() as u64;
        while position < length {
            if length - position < HEADER_BYTES {
                truncate_tail(&mut file, position)?;
                length = position;
                break;
            }
            let mut header = [0u8; HEADER_BYTES as usize];
            file.read_exact(&mut header).map_err(storage)?;
            let header_crc = u32::from_be_bytes(header[8..12].try_into().unwrap());
            if crc32fast::hash(&header[..8]) != header_crc {
                return Err(Error::new(
                    STORAGE,
                    format!("WAL header checksum mismatch at byte {position}"),
                ));
            }
            let body_length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
            let expected_crc = u32::from_be_bytes(header[4..8].try_into().unwrap());
            // firstSequence (8) plus a validated publish body (at most MAX_FRAME - 5).
            if !(19..=MAX_FRAME + 3).contains(&body_length) {
                return Err(Error::new(
                    STORAGE,
                    format!("invalid WAL batch length at byte {position}"),
                ));
            }
            let end = position + HEADER_BYTES + body_length as u64;
            if end > length {
                truncate_tail(&mut file, position)?;
                length = position;
                break;
            }
            let mut body = vec![0; body_length];
            file.read_exact(&mut body).map_err(storage)?;
            if crc32fast::hash(&body) != expected_crc {
                return Err(Error::new(
                    STORAGE,
                    format!("WAL batch checksum mismatch at byte {position}"),
                ));
            }
            let mut reader = Reader::new(Bytes::from(body));
            let first = reader.u64().map_err(storage)?;
            let records = decode_records(&mut reader).map_err(storage)?;
            reader.finish().map_err(storage)?;
            replay(first, records)?;
            position = end;
        }
        file.seek(SeekFrom::Start(length)).map_err(storage)?;
        Ok(Self {
            file,
            bytes: length,
            max_bytes,
            #[cfg(test)]
            fail_after_write: false,
        })
    }

    pub fn batch_bytes(records: &[Record]) -> u64 {
        HEADER_BYTES
            + 8
            + 2
            + records
                .iter()
                .map(|r| (r.wire_bytes() - 8) as u64)
                .sum::<u64>()
    }

    pub fn check_capacity(&self, records: &[Record]) -> Result<(), Error> {
        if Self::batch_bytes(records) > self.max_bytes.saturating_sub(self.bytes) {
            return Err(Error::new(
                CAPACITY,
                "WAL capacity reached; batch was not written",
            ));
        }
        Ok(())
    }

    pub fn append(&mut self, first: u64, records: &[Record]) -> Result<(), Error> {
        self.check_capacity(records)?;
        let record_bytes = encode_records(records);
        let mut body = Vec::with_capacity(8 + record_bytes.len());
        body.extend_from_slice(&first.to_be_bytes());
        body.extend_from_slice(&record_bytes);
        let mut frame = Vec::with_capacity(HEADER_BYTES as usize + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
        frame.extend_from_slice(&crc32fast::hash(&frame).to_be_bytes());
        frame.extend_from_slice(&body);
        self.file.write_all(&frame).map_err(storage)?;
        #[cfg(test)]
        if self.fail_after_write {
            return Err(storage("injected failure after write, before sync"));
        }
        self.file.sync_all().map_err(storage)?;
        self.bytes += frame.len() as u64;
        Ok(())
    }
}

fn truncate_tail(file: &mut File, length: u64) -> Result<(), Error> {
    file.set_len(length).map_err(storage)?;
    file.sync_all().map_err(storage)?;
    Ok(())
}
