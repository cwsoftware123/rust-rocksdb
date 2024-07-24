mod util;

use {
    rocksdb::{Options, ReadOptions, DB},
    util::DBPath,
};

#[test]
fn timestamping_works() {
    let path = DBPath::new("_rust_rocksdb_timestamping_works");

    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    db_opts.set_comparator_with_ts("cname", Box::new(|a, b| a.cmp(b)));

    let db = DB::open(&db_opts, &path).unwrap();

    // Write a batch at timestamp 1
    let ts1 = 1_u64.to_be_bytes();
    db.put_with_ts("fish", ts1, "tuna").unwrap();

    // Write a batch at timestamp 2
    let ts2 = 2_u64.to_be_bytes();
    db.put_with_ts("fish", ts2, "sardine").unwrap();

    // Read at timestamp 1
    let mut read_opts = ReadOptions::default();
    read_opts.set_timestamp(ts1);

    let value = String::from_utf8(db.get_opt("fish", &read_opts).unwrap().unwrap()).unwrap();
    assert_eq!(value, "tuna");
}
