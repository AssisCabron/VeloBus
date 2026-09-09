use crate::protocol::{
    encode_record, Error, Event, Fetch, Record, CAPACITY, CURSOR, LIMIT, MAX_SCAN, STORAGE,
};
use crate::wal::Wal;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

pub struct Store {
    events: Vec<Event>,
    retained_bytes: u64,
    max_retained_bytes: u64,
    max_wal_bytes: u64,
    wal: Option<Wal>,
    poisoned: bool,
    published_records: u64,
    fetched_records: u64,
    coalesced_records: u64,
    started: Instant,
}

pub struct FetchResult {
    pub cursor: u64,
    pub events: Vec<Event>,
    pub has_more: bool,
}

impl FetchResult {
    pub fn encode(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            11 + self
                .events
                .iter()
                .map(|e| e.record.wire_bytes())
                .sum::<usize>(),
        );
        out.extend_from_slice(&self.cursor.to_be_bytes());
        out.extend_from_slice(&(self.events.len() as u16).to_be_bytes());
        out.push(u8::from(self.has_more));
        for event in self.events {
            out.extend_from_slice(&event.sequence.to_be_bytes());
            encode_record(&mut out, &event.record);
        }
        out
    }
}

impl Store {
    pub fn memory(max_retained_bytes: u64, max_wal_bytes: u64) -> Self {
        Self {
            events: Vec::new(),
            retained_bytes: 0,
            max_retained_bytes,
            max_wal_bytes,
            wal: None,
            poisoned: false,
            published_records: 0,
            fetched_records: 0,
            coalesced_records: 0,
            started: Instant::now(),
        }
    }

    pub fn disk(
        directory: &Path,
        max_retained_bytes: u64,
        max_wal_bytes: u64,
    ) -> Result<Self, Error> {
        let mut store = Self::memory(max_retained_bytes, max_wal_bytes);
        let wal = Wal::open(directory, max_wal_bytes, |first, records| {
            if first != store.last_sequence() + 1 {
                return Err(Error::new(
                    STORAGE,
                    "nonconsecutive sequence numbers in WAL",
                ));
            }
            let bytes = store.check_retained_capacity(&records)?;
            store.commit(first, records, bytes);
            Ok(())
        })?;
        store.wal = Some(wal);
        Ok(store)
    }

    fn last_sequence(&self) -> u64 {
        self.events.len() as u64
    }

    fn check_retained_capacity(&self, records: &[Record]) -> Result<u64, Error> {
        let bytes: u64 = records.iter().map(Record::retained_bytes).sum();
        if bytes > self.max_retained_bytes.saturating_sub(self.retained_bytes) {
            return Err(Error::new(
                CAPACITY,
                "retained history capacity reached; batch was not written",
            ));
        }
        Ok(bytes)
    }

    fn commit(&mut self, first: u64, records: Vec<Record>, bytes: u64) {
        self.events.extend(
            records
                .into_iter()
                .enumerate()
                .map(|(index, record)| Event {
                    sequence: first + index as u64,
                    record,
                }),
        );
        self.retained_bytes += bytes;
    }

    pub fn publish(&mut self, records: Vec<Record>) -> Result<Vec<u8>, Error> {
        if self.poisoned {
            return Err(Error::new(
                STORAGE,
                "store is read-only after uncertain disk I/O; restart required",
            ));
        }
        let bytes = self.check_retained_capacity(&records)?;
        let count = records.len() as u64;
        let last = self
            .last_sequence()
            .checked_add(count)
            .ok_or_else(|| Error::new(CAPACITY, "sequence range exhausted"))?;
        let first = self.last_sequence() + 1;
        if let Some(wal) = self.wal.as_mut() {
            wal.check_capacity(&records)?;
            // Capacity failures are certain and do not poison the store; I/O failures do.
            if let Err(error) = wal.append(first, &records) {
                self.poisoned = true;
                return Err(error);
            }
        }
        self.commit(first, records, bytes);
        self.published_records = self.published_records.saturating_add(count);
        let mut out = first.to_be_bytes().to_vec();
        out.extend_from_slice(&last.to_be_bytes());
        out.extend_from_slice(&(count as u16).to_be_bytes());
        out.push(u8::from(self.wal.is_some()));
        Ok(out)
    }

    pub fn fetch(&mut self, request: Fetch) -> Result<FetchResult, Error> {
        let last = self.last_sequence();
        if request.after > last {
            return Err(Error::new(
                CURSOR,
                "cursor is beyond this log's high-water mark",
            ));
        }
        let mut cursor = request.after;
        let mut selected: Vec<&Event> = Vec::with_capacity(request.limit);
        let mut positions = HashMap::new();
        let mut wire_bytes = 0;
        let mut coalesced = 0;
        for event in self
            .events
            .iter()
            .skip(request.after as usize)
            .take(MAX_SCAN)
        {
            let record = &event.record;
            if request.topic != "*" && request.topic != record.topic {
                cursor = event.sequence;
                continue;
            }
            let key = (record.topic.as_str(), record.key.as_str());
            let replace = if request.latest && !record.key.is_empty() {
                positions.get(&key).copied()
            } else {
                None
            };
            let previous_bytes = replace
                .map(|index: usize| selected[index].record.wire_bytes())
                .unwrap_or(0);
            let candidate_bytes = wire_bytes - previous_bytes + record.wire_bytes();
            if candidate_bytes > request.max_bytes
                || (replace.is_none() && selected.len() == request.limit)
            {
                if selected.is_empty() && record.wire_bytes() > request.max_bytes {
                    return Err(Error::new(
                        LIMIT,
                        "first eligible event exceeds maxBytes; increase the fetch budget",
                    ));
                }
                break;
            }
            wire_bytes = candidate_bytes;
            if let Some(index) = replace {
                selected[index] = event;
                coalesced += 1;
            } else {
                if request.latest && !record.key.is_empty() {
                    positions.insert(key, selected.len());
                }
                selected.push(event);
            }
            cursor = event.sequence;
        }
        selected.sort_unstable_by_key(|event| event.sequence);
        let events = selected.into_iter().cloned().collect::<Vec<_>>();
        self.fetched_records = self.fetched_records.saturating_add(events.len() as u64);
        self.coalesced_records = self.coalesced_records.saturating_add(coalesced);
        Ok(FetchResult {
            cursor,
            events,
            has_more: cursor < last,
        })
    }

    pub fn stats(&self, connections: usize) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "version": env!("CARGO_PKG_VERSION"),
            "mode": if self.wal.is_some() { "disk" } else { "memory" },
            "records": self.events.len(), "retainedBytes": self.retained_bytes,
            "maxRetainedBytes": self.max_retained_bytes,
            "walBytes": self.wal.as_ref().map(|wal| wal.bytes).unwrap_or(0),
            "maxWalBytes": self.max_wal_bytes, "lastSequence": self.last_sequence().to_string(),
            "publishedRecords": self.published_records, "fetchedRecords": self.fetched_records,
            "coalescedRecords": self.coalesced_records, "connections": connections,
            "uptimeSeconds": self.started.elapsed().as_secs_f64(), "writePoisoned": self.poisoned,
        }))
        .expect("serializing finite store stats")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::fs::{self, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const BUDGET: u64 = 16 * 1024 * 1024;
    fn record(topic: &str, key: &str, payload: &[u8]) -> Record {
        Record {
            topic: topic.into(),
            key: key.into(),
            payload: Bytes::copy_from_slice(payload),
        }
    }
    fn fetch(topic: &str, after: u64, latest: bool, limit: usize, max_bytes: usize) -> Fetch {
        Fetch {
            topic: topic.into(),
            after,
            latest,
            limit,
            max_bytes,
        }
    }
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "nodara-test-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            Self(path)
        }
        fn wal(&self) -> PathBuf {
            self.0.join("events.wal")
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn latest_coalesces_by_topic_and_key_but_preserves_empty_keys_and_order() {
        let mut store = Store::memory(BUDGET, BUDGET);
        store
            .publish(vec![
                record("a", "x", b"old"),
                record("b", "x", b"other"),
                record("a", "", b"one"),
                record("a", "x", b"new"),
                record("a", "", b"two"),
            ])
            .unwrap();
        let latest = store.fetch(fetch("*", 0, true, 256, 4096)).unwrap();
        assert_eq!(
            latest.events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            [2, 3, 4, 5]
        );
        assert_eq!(latest.cursor, 5);
        assert!(!latest.has_more);
        assert_eq!(
            store
                .fetch(fetch("*", 0, false, 256, 4096))
                .unwrap()
                .events
                .len(),
            5
        );
    }

    #[test]
    fn replacement_over_budget_retains_prior_event_and_does_not_skip() {
        let mut store = Store::memory(BUDGET, BUDGET);
        store
            .publish(vec![record("a", "x", b"1"), record("a", "x", &[0; 100])])
            .unwrap();
        let result = store.fetch(fetch("*", 0, true, 1, 20)).unwrap();
        assert_eq!(result.cursor, 1);
        assert_eq!(result.events[0].record.payload.as_ref(), b"1");
        assert!(result.has_more);
        assert_eq!(
            store.fetch(fetch("*", 1, true, 1, 20)).err().unwrap().code,
            LIMIT
        );
        assert_eq!(
            store.fetch(fetch("*", 1, true, 1, 200)).unwrap().events[0].sequence,
            2
        );
    }

    #[test]
    fn limit_can_replace_existing_keys_but_not_skip_new_keys() {
        let mut store = Store::memory(BUDGET, BUDGET);
        store
            .publish(vec![
                record("a", "x", b"1"),
                record("a", "x", b"2"),
                record("a", "y", b"3"),
            ])
            .unwrap();
        let result = store.fetch(fetch("*", 0, true, 1, 4096)).unwrap();
        assert_eq!(result.cursor, 2);
        assert_eq!(result.events[0].sequence, 2);
        assert!(result.has_more);
    }

    #[test]
    fn nonmatching_scans_are_bounded_and_future_cursors_fail() {
        let mut store = Store::memory(BUDGET, BUDGET);
        for _ in 0..17 {
            store.publish(vec![record("a", "", b"1"); 256]).unwrap();
        }
        let result = store.fetch(fetch("b", 0, false, 256, 4096)).unwrap();
        assert_eq!(result.cursor, 4096);
        assert!(result.events.is_empty());
        assert!(result.has_more);
        assert_eq!(
            store
                .fetch(fetch("*", 5000, false, 256, 4096))
                .err()
                .unwrap()
                .code,
            CURSOR
        );
    }

    #[test]
    fn capacity_rejection_is_atomic_in_memory_and_on_disk() {
        let temp = Temp::new();
        let one = record("a", "x", b"1");
        let mut store = Store::disk(&temp.0, one.retained_bytes(), BUDGET).unwrap();
        assert_eq!(
            store
                .publish(vec![one.clone(), one.clone()])
                .err()
                .unwrap()
                .code,
            CAPACITY
        );
        assert_eq!(store.last_sequence(), 0);
        assert_eq!(fs::metadata(temp.wal()).unwrap().len(), 8);
        store.publish(vec![one]).unwrap();
        assert_eq!(store.last_sequence(), 1);
        drop(store);
        let restored = Store::disk(&temp.0, BUDGET, BUDGET).unwrap();
        assert_eq!(restored.last_sequence(), 1);
    }

    #[test]
    fn wal_limit_rejects_whole_batch_without_poisoning_or_writing() {
        let temp = Temp::new();
        let one = record("a", "x", b"1");
        let max_wal = 8 + Wal::batch_bytes(std::slice::from_ref(&one));
        let mut store = Store::disk(&temp.0, BUDGET, max_wal).unwrap();
        assert_eq!(
            store
                .publish(vec![one.clone(), one.clone()])
                .err()
                .unwrap()
                .code,
            CAPACITY
        );
        assert!(!store.poisoned);
        assert_eq!(fs::metadata(temp.wal()).unwrap().len(), 8);
        store.publish(vec![one]).unwrap();
        assert_eq!(fs::metadata(temp.wal()).unwrap().len(), max_wal);
    }

    #[test]
    fn exclusive_lock_and_recovery_preserve_sequence_and_payload() {
        let temp = Temp::new();
        let mut store = Store::disk(&temp.0, BUDGET, BUDGET).unwrap();
        assert!(Store::disk(&temp.0, BUDGET, BUDGET).is_err());
        let receipt = store
            .publish(vec![record("a", "x", &[0, 255]), record("a", "", b"next")])
            .unwrap();
        assert_eq!(receipt[18], 1);
        drop(store);
        let mut store = Store::disk(&temp.0, BUDGET, BUDGET).unwrap();
        assert_eq!(store.last_sequence(), 2);
        assert_eq!(store.published_records, 0);
        let result = store.fetch(fetch("*", 0, false, 256, 4096)).unwrap();
        assert_eq!(result.events[0].record.payload.as_ref(), &[0, 255]);
        store.publish(vec![record("b", "", b"3")]).unwrap();
        assert_eq!(store.last_sequence(), 3);
    }

    #[test]
    fn partial_header_and_partial_body_recovery_truncate_only_tail() {
        for partial in [1usize, 8, 12, 16] {
            let temp = Temp::new();
            let mut store = Store::disk(&temp.0, BUDGET, BUDGET).unwrap();
            store.publish(vec![record("a", "", b"first")]).unwrap();
            let valid = fs::metadata(temp.wal()).unwrap().len();
            store.publish(vec![record("a", "", b"second")]).unwrap();
            drop(store);
            OpenOptions::new()
                .write(true)
                .open(temp.wal())
                .unwrap()
                .set_len(valid + partial as u64)
                .unwrap();
            let restored = Store::disk(&temp.0, BUDGET, BUDGET).unwrap();
            assert_eq!(restored.last_sequence(), 1);
            assert_eq!(fs::metadata(temp.wal()).unwrap().len(), valid);
        }
    }

    #[test]
    fn header_length_and_body_corruption_fail_without_truncating() {
        for offset in [8u64, 12, 16, 20, 30] {
            let temp = Temp::new();
            let mut store = Store::disk(&temp.0, BUDGET, BUDGET).unwrap();
            store.publish(vec![record("a", "x", b"payload")]).unwrap();
            drop(store);
            let length = fs::metadata(temp.wal()).unwrap().len();
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(temp.wal())
                .unwrap();
            file.seek(SeekFrom::Start(offset)).unwrap();
            let mut byte = [0];
            file.read_exact(&mut byte).unwrap();
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&[byte[0] ^ 1]).unwrap();
            drop(file);
            assert!(Store::disk(&temp.0, BUDGET, BUDGET).is_err());
            assert_eq!(fs::metadata(temp.wal()).unwrap().len(), length);
        }
    }

    #[test]
    fn invalid_complete_length_even_with_valid_header_crc_is_corruption() {
        let temp = Temp::new();
        drop(Store::disk(&temp.0, BUDGET, BUDGET).unwrap());
        let mut header = 0u32.to_be_bytes().to_vec();
        header.extend_from_slice(&0u32.to_be_bytes());
        header.extend_from_slice(&crc32fast::hash(&header).to_be_bytes());
        OpenOptions::new()
            .append(true)
            .open(temp.wal())
            .unwrap()
            .write_all(&header)
            .unwrap();
        assert!(Store::disk(&temp.0, BUDGET, BUDGET).is_err());
        assert_eq!(fs::metadata(temp.wal()).unwrap().len(), 20);
    }

    #[test]
    fn uncertain_io_failure_poisoning_blocks_writes_and_restart_reconciles_log() {
        let temp = Temp::new();
        let mut store = Store::disk(&temp.0, BUDGET, BUDGET).unwrap();
        store.wal.as_mut().unwrap().fail_after_write = true;
        assert_eq!(
            store
                .publish(vec![record("a", "", b"unknown outcome")])
                .err()
                .unwrap()
                .code,
            STORAGE
        );
        assert!(store.poisoned);
        assert_eq!(store.last_sequence(), 0);
        let uncertain_length = fs::metadata(temp.wal()).unwrap().len();
        assert!(uncertain_length > 8);
        assert_eq!(
            store
                .publish(vec![record("a", "", b"must not append")])
                .err()
                .unwrap()
                .code,
            STORAGE
        );
        assert_eq!(fs::metadata(temp.wal()).unwrap().len(), uncertain_length);
        drop(store);
        let mut restored = Store::disk(&temp.0, BUDGET, BUDGET).unwrap();
        assert_eq!(restored.last_sequence(), 1);
        let result = restored.fetch(fetch("*", 0, false, 256, 4096)).unwrap();
        assert_eq!(result.events[0].record.payload.as_ref(), b"unknown outcome");
        restored.publish(vec![record("a", "", b"next")]).unwrap();
        assert_eq!(restored.last_sequence(), 2);
    }
}
