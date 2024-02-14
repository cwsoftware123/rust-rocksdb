//! Put all our added stuff in a single file so it's easy to manage.
//!
//! Our changes involve adding support for the timestamped methods (e.g. put_with_ts, get_with_ts).
//! We don't add support for every single one of them; just the ones we need.

use {
    crate::{
        comparator::CompareFn, ffi, AsColumnFamilyRef, CStrLike, Options, ReadOptions, WriteBatch,
    },
    libc::{c_char, c_uchar, c_void, size_t},
    std::{cmp::Ordering, ffi::CString, os::raw::c_int, slice},
};

pub type CompareTsFn = dyn Fn(&[u8], &[u8]) -> Ordering;

pub type CompareWithoutTsFn = dyn Fn(&[u8], bool, &[u8], bool) -> Ordering;

pub struct ComparatorWithTs {
    pub name: CString,
    pub compare_fn: Box<CompareFn>,
    pub compare_ts_fn: Box<CompareTsFn>,
    pub compare_without_ts_fn: Box<CompareWithoutTsFn>,
}

impl ComparatorWithTs {
    pub unsafe extern "C" fn destructor_callback(raw_cb: *mut c_void) {
        drop(Box::from_raw(raw_cb as *mut ComparatorWithTs));
    }

    pub unsafe extern "C" fn name_callback(raw_cb: *mut c_void) -> *const c_char {
        let cb: &mut ComparatorWithTs = &mut *(raw_cb as *mut ComparatorWithTs);
        let ptr = cb.name.as_ptr();
        ptr as *const c_char
    }

    pub unsafe extern "C" fn compare_callback(
        raw_cb: *mut c_void,
        a_raw: *const c_char,
        a_len: size_t,
        b_raw: *const c_char,
        b_len: size_t,
    ) -> c_int {
        let cb: &mut ComparatorWithTs = &mut *(raw_cb as *mut ComparatorWithTs);
        let a: &[u8] = slice::from_raw_parts(a_raw as *const u8, a_len);
        let b: &[u8] = slice::from_raw_parts(b_raw as *const u8, b_len);
        (cb.compare_fn)(a, b) as c_int
    }

    pub unsafe extern "C" fn compare_ts_callback(
        raw_cb: *mut c_void,
        a_ts_raw: *const c_char,
        a_ts_len: size_t,
        b_ts_raw: *const c_char,
        b_ts_len: size_t,
    ) -> c_int {
        let cb: &mut ComparatorWithTs = &mut *(raw_cb as *mut ComparatorWithTs);
        let a_ts: &[u8] = slice::from_raw_parts(a_ts_raw as *const u8, a_ts_len);
        let b_ts: &[u8] = slice::from_raw_parts(b_ts_raw as *const u8, b_ts_len);
        (cb.compare_ts_fn)(a_ts, b_ts) as c_int
    }

    pub unsafe extern "C" fn compare_without_ts_callback(
        raw_cb: *mut c_void,
        a_raw: *const c_char,
        a_len: size_t,
        a_has_ts_raw: c_uchar,
        b_raw: *const c_char,
        b_len: size_t,
        b_has_ts_raw: c_uchar,
    ) -> c_int {
        let cb: &mut ComparatorWithTs = &mut *(raw_cb as *mut ComparatorWithTs);
        let a: &[u8] = slice::from_raw_parts(a_raw as *const u8, a_len);
        let a_has_ts = a_has_ts_raw != 0;
        let b: &[u8] = slice::from_raw_parts(b_raw as *const u8, b_len);
        let b_has_ts = b_has_ts_raw != 0;
        (cb.compare_without_ts_fn)(a, a_has_ts, b, b_has_ts) as c_int
    }
}

impl Options {
    pub fn set_comparator_with_ts(
        &mut self,
        name: &str,
        timestamp_size: usize,
        compare_fn: Box<CompareFn>,
        compare_ts_fn: Box<CompareTsFn>,
        compare_without_ts_fn: Box<CompareWithoutTsFn>,
    ) {
        let cb = Box::new(ComparatorWithTs {
            name: name.into_c_string().unwrap(),
            compare_fn,
            compare_ts_fn,
            compare_without_ts_fn,
        });

        unsafe {
            // TODO: destroy the existing comparator?
            // https://github.com/linxGnu/grocksdb/blob/v1.8.12/options.go#L195
            let cmp = ffi::rocksdb_comparator_with_ts_create(
                Box::into_raw(cb).cast::<c_void>(),
                Some(ComparatorWithTs::destructor_callback),
                Some(ComparatorWithTs::compare_callback),
                Some(ComparatorWithTs::compare_ts_callback),
                Some(ComparatorWithTs::compare_without_ts_callback),
                Some(ComparatorWithTs::name_callback),
                timestamp_size as size_t,
            );
            ffi::rocksdb_options_set_comparator(self.inner, cmp);
        }
    }
}

impl ReadOptions {
    pub fn set_timestamp<T: AsRef<[u8]>>(&mut self, ts: T) {
        // we need to make sure the timestamp bytes live as long as the ReadOptions.
        // make a copy of it and let it owned by the ReadOptions.
        let ts = ts.as_ref().to_owned();
        let ptr = ts.as_ptr();
        let len = ts.len();
        self.timestamp = Some(ts);

        unsafe {
            ffi::rocksdb_readoptions_set_timestamp(self.inner, ptr as *const c_char, len as size_t);
        }
    }
}

impl WriteBatch {
    pub fn put_cf_with_ts<K, T, V>(&mut self, cf: &impl AsColumnFamilyRef, key: K, ts: T, value: V)
    where
        K: AsRef<[u8]>,
        T: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let ts = ts.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_put_cf_with_ts(
                self.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                ts.as_ptr() as *const c_char,
                ts.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
        }
    }

    pub fn delete_cf_with_ts<K, T>(&mut self, cf: &impl AsColumnFamilyRef, key: K, ts: T)
    where
        K: AsRef<[u8]>,
        T: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let ts = ts.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_delete_cf_with_ts(
                self.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                ts.as_ptr() as *const c_char,
                ts.len() as size_t,
            );
        }
    }
}
