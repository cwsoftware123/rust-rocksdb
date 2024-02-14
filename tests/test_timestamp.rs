mod util;

use {
    rocksdb::{AsColumnFamilyRef, IteratorMode, Options, ReadOptions, WriteBatch, DB},
    std::{cmp::Ordering, convert::TryInto},
    util::DBPath,
};

// we do this test in a `test` column family.
const CF_NAME_TEST: &str = "test";

/// RocksDB doesn't "enshrine" a specific timestamp format; it's up for choice
/// by applications. For testing purpose, we use a 8-byte timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct U64Timestamp([u8; Self::SIZE]);

impl U64Timestamp {
    pub const SIZE: usize = 8;
}

impl AsRef<[u8]> for U64Timestamp {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<u64> for U64Timestamp {
    fn from(value: u64) -> Self {
        // note: use little endian encoding
        Self(value.to_le_bytes())
    }
}

impl From<&[u8]> for U64Timestamp {
    fn from(bytes: &[u8]) -> Self {
        // note: panic if slice is not exactly 8 bytes
        debug_assert_eq!(bytes.len(), Self::SIZE, "incorrect length: {}", bytes.len());
        Self(bytes.try_into().unwrap())
    }
}

impl PartialOrd for U64Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for U64Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        // note: use little endian encoding
        let a = u64::from_le_bytes(self.0);
        let b = u64::from_le_bytes(other.0);
        a.cmp(&b)
    }
}

/// Comparator that goes together with `U64Timestamp`.
///
/// This comparator behaves identically to RocksDB's built-in comparator, also
/// using the same name: "leveldb.BytewiseComparator.u64ts".
///
/// Adapted from:
/// - https://github.com/facebook/rocksdb/blob/main/util/comparator.cc#L238
/// - https://github.com/linxGnu/grocksdb/blob/master/db_ts_test.go#L167
/// - https://github.com/sei-protocol/sei-db/blob/main/ss/rocksdb/comparator.go
struct U64Comparator;

impl U64Comparator {
    /// Quote from SeiDB:
    /// > We also use the same builtin comparator name so the builtin tools
    /// > `ldb`/`sst_dump` can work with the database.
    pub const NAME: &'static str = "leveldb.BytewiseComparator.u64ts";

    /// Compares two internal keys with timestamp suffix, larger timestamp
    /// comes first.
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        // first, compare the keys without timestamps. if the keys are different
        // then we don't have to consider timestamps at all.
        let ord = Self::compare_without_ts(a, true, b, true);
        if ord != Ordering::Equal {
            return ord;
        }

        // the keys are the same, now we compare the timestamps.
        // the larger (newer) timestamp should come first, meaning seek operation
        // will try to find a version less than or equal to the target version.
        Self::compare_ts(
            extract_timestamp_from_user_key(a),
            extract_timestamp_from_user_key(b),
        )
        .reverse()
    }

    /// Compares timestamps as little endian encoded integers.
    fn compare_ts(bz1: &[u8], bz2: &[u8]) -> Ordering {
        let ts1 = U64Timestamp::from(bz1);
        let ts2 = U64Timestamp::from(bz2);
        ts1.cmp(&ts2)
    }

    // Compares two internal keys without the timestamp part.
    fn compare_without_ts(mut a: &[u8], a_has_ts: bool, mut b: &[u8], b_has_ts: bool) -> Ordering {
        if a_has_ts {
            a = strip_timestamp_from_user_key(a);
        }
        if b_has_ts {
            b = strip_timestamp_from_user_key(b);
        }
        a.cmp(b)
    }
}

#[inline]
fn extract_timestamp_from_user_key(key: &[u8]) -> &[u8] {
    &key[(key.len() - U64Timestamp::SIZE)..]
}

#[inline]
fn strip_timestamp_from_user_key(key: &[u8]) -> &[u8] {
    &key[..(key.len() - U64Timestamp::SIZE)]
}

fn new_db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts
}

fn new_cf_options() -> Options {
    let mut opts = Options::default();
    // must use a timestamp-enabled comparator
    opts.set_comparator_with_ts(
        U64Comparator::NAME,
        U64Timestamp::SIZE,
        Box::new(U64Comparator::compare),
        Box::new(U64Comparator::compare_ts),
        Box::new(U64Comparator::compare_without_ts),
    );
    opts
}

fn new_read_options_with_ts<T: AsRef<[u8]>>(ts: T) -> ReadOptions {
    let mut opts = ReadOptions::default();
    opts.set_timestamp(ts);
    opts
}

#[test]
fn write_read_and_iterate_with_timestamp() {
    let path = DBPath::new("_rust_rocksdb_write_read_and_iterate_with_timestamp");
    let opts = new_db_options();
    let db = DB::open_cf_with_opts(&opts, &path, [(CF_NAME_TEST, new_cf_options())]).unwrap();
    let cf = db.cf_handle(CF_NAME_TEST).unwrap();

    // write a first batch
    let ts1 = U64Timestamp::from(1);
    {
        let mut batch = WriteBatch::default();
        batch.put_cf_with_ts(cf, "donald", ts1, "trump");
        batch.put_cf_with_ts(cf, "jake", ts1, "shepherd");
        batch.put_cf_with_ts(cf, "joe", ts1, "biden");
        batch.put_cf_with_ts(cf, "larry", ts1, "engineer");
        db.write(batch).unwrap();
    }

    // write a second batch
    // this should 1) delete a key from the previous batch, 2) overwrite a
    // value from the previous batch, 3) write a new key not present in the
    // previous batch.
    let ts2 = U64Timestamp::from(2);
    {
        let mut batch = WriteBatch::default();
        batch.put_cf_with_ts(cf, "donald", ts2, "duck");
        batch.delete_cf_with_ts(cf, "joe", ts2);
        batch.put_cf_with_ts(cf, "pumpkin", ts2, "cat");
        db.write(batch).unwrap();
    }

    assert_data_at_timestamp(
        &db,
        &cf,
        ts1,
        [
            ("donald", Some("trump")),
            ("jake", Some("shepherd")),
            ("joe", Some("biden")),
            ("larry", Some("engineer")),
            ("pumpkin", None),
        ],
    );
    assert_data_at_timestamp(
        &db,
        &cf,
        ts2,
        [
            ("donald", Some("duck")),
            ("jake", Some("shepherd")),
            ("joe", None),
            ("larry", Some("engineer")),
            ("pumpkin", Some("cat")),
        ],
    );
}

fn assert_data_at_timestamp<const N: usize>(
    db: &DB,
    cf: &impl AsColumnFamilyRef,
    timestamp: U64Timestamp,
    expected: [(&str, Option<&str>); N],
) {
    let opts = new_read_options_with_ts(timestamp);

    // get
    for (k, v) in expected {
        let value = db.get_cf_opt(cf, k, &opts).unwrap();
        assert_eq!(value.as_deref(), v.map(|v| v.as_bytes()));
    }

    // iterate
    for (item, (k, v)) in db
        .iterator_cf_opt(cf, opts, IteratorMode::Start)
        .zip(expected.iter().filter_map(|(k, v)| v.map(|v| (k, v))))
    {
        let (key, value) = item.unwrap();
        assert_eq!(key.as_ref(), k.as_bytes());
        assert_eq!(value.as_ref(), v.as_bytes());
    }
}
