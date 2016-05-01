//! This module contains various utilities/helper functions

use itertools::Itertools;
use clap;
use std::io::{BufRead, Write, BufWriter,};
use std::ptr;
use std::borrow::Cow;
use super::Joinkit;

/// Recognized datatypes
#[derive(Debug, PartialEq, Eq,)]
pub enum DataType {
    /// Signed integer 64
    I,
    /// Unisigned integer 64
    U,
    /// String
    S,
}

/// Union of numeric and character types
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VarData {
    /// Contains a number represented by `i64`
    I(i64),
    /// Contains a number represented by `u64`
    U(u64),
    /// Contains a string
    S(String),
}

/// Converts a record separator to a single byte
pub fn rec_sep_as_byte(rec_str: &str) -> Result<u8, clap::Error> {
    let bytes = rec_str.as_bytes();
    if bytes.len() == 1 {
        return Ok(bytes[0]);
    } else {
        let e = clap::Error {message: "Error: input record separator must be encodable to 1 byte \
        exactly!".to_owned(),
                             kind: clap::ErrorKind::ValueValidation,
                             info: None};
        return Err(e);
    }
}

/// Converts a slice containing the fields indices in base1 along with optional data type into
/// vector of 3-element tuples.
///
/// Each tuple contains the parsed field index, its position, both in base0 and their corresponding
/// data type annotation. The association with position is necessary, since the field indices will
/// be sorted (to facilitate the key extraction in `extract_key()` function) and thus might loose
/// the info about their correct position.
///
/// The resulting vector is sorted on the field indices. The error is returned if the input string
/// contains duplicate field indices or the provided data type is not recognized.
///
/// # Example
/// ```
/// use joinkit::util::{self, DataType};
///
/// // does not need to be ordered
/// let field_vec = vec!["1", "3-i", "6-u", "4"];
/// let field_idx = util::fields_to_idx(field_vec).unwrap();
///
/// // this reads as follows: 
/// // the first field goes to the first position with a default data type `String`, 
/// // the third field goes to the second position with an explicit data type `i64 , 
/// // the fourth field goes to the fourth position with an explicit data type `u64`,
/// // and the sixth field goes to the third position with a default data type `String`
/// assert_eq!(vec![(0, 0, DataType::S), 
///                 (2, 1, DataType::I),
///                 (3, 3, DataType::S),
///                 (5, 2, DataType::U)], field_idx);
pub fn fields_to_idx(f: Vec<&str>) -> Result<Vec<(usize, isize, DataType)>, clap::Error> {
    let mut idx: Vec<(usize, isize, DataType)> = Vec::new();
    let it = f.iter()
              .enumerate()
              .flat_map(|(i0, s)| s.split('-')
                                   .enumerate()
                                   .take(2)
                                   .map(move |(i1, s)| (i0, i1, s)));
    for (i0, i1, s) in it {
        // parse index
        if i1 == 0 {
            match s.parse::<usize>() {
                // convert from base 1 to base 0 and assign default data type
                Ok(u) => idx.push((u - 1, i0 as isize, DataType::S)),
                Err(_) => return Err(clap::Error {message: "Error: could not parse integer fields!".to_owned(),
                                                  kind: clap::ErrorKind::ValueValidation,
                                                  info: None}),
                
            }
        } else { // parse data_type
            let dt = match s {
                "i" => DataType::I,
                "u" => DataType::U,
                _ => return Err(clap::Error {message: format!("Error: '{}' is not a valid data type!", s),
                                             kind: clap::ErrorKind::ValueValidation,
                                             info: None}),
            };

            // update data type
            unsafe {
                // we cannot get here without first pushing to vector, so this is safe
                idx.get_unchecked_mut(i0).2 = dt;
            }
        }
    }
    idx.sort_by(|a, b| a.0.cmp(&b.0));
    // check if there are duplicates
    {
        let mut it = idx.iter();
        let mut previous = match it.next() {
            Some(t) => t,
            None => {
                let e = clap::Error {message: "Error: at least one key field expected!".to_owned(),
                                     kind: clap::ErrorKind::ValueValidation,
                                     info: None};
                return Err(e);
            },
        };
        for current in it {
            if previous.0 == current.0 {
                let e = clap::Error {message: "Error: the key fields must be unique!".to_owned(),
                                     kind: clap::ErrorKind::ValueValidation,
                                     info: None};
                return Err(e);
            }
            previous = current;
        }
    }
    Ok(idx)
}

/// Extracts a key from the record.
///
/// # Safety
///
/// You should always use the `key_idx` parameter generated by `fields_to_idx()` function, unless
/// you know, what you're doing ;)
///
/// # Example
/// ```
/// use joinkit::util::{self, DataType, VarData};
///
/// let rec = "a;b;1";
/// let field_sep = ";";
/// // this reads as follows: the first field goes to the second position with data type `String`
/// // and the third field goes to the first position with data type `i64`.
/// let key_idx = [(0, 1, DataType::S), (2, 0, DataType::I)];
/// unsafe {
///     let key = util::extract_key(rec, field_sep, &key_idx);
///     assert_eq!(vec![VarData::I(1), 
///                     VarData::S("a".to_owned())], key);
/// }
pub unsafe fn extract_key(record: &str, 
                   field_sep: &str,
                   key_idx: &[(usize, isize, DataType)]) -> Vec<VarData> { 
    let keys_len = key_idx.len();
    let mut keys: Vec<VarData> = Vec::with_capacity(keys_len);
    let mut actual_len = 0usize;
    {
        let ptr = keys.as_mut_ptr();
        let key_idx_it = key_idx.iter();
        let key_fields_it = record.split(field_sep)
            .enumerate()
            // join on enumerated value and key_idx
            .merge_join_inner_by(key_idx_it, |l, r| Ord::cmp(&l.0, &r.0));
        for ((_, k), &(_, i, ref dt)) in key_fields_it {
            let data = match dt {
                &DataType::I => {
                    VarData::I(k.parse::<i64>()
                                .expect(&format!("Error while parsing the \
                                                  key number {}: the value '{}' \
                                                  cannot be converted into 'i64'", k,
                                                  i + 1)))
                }
                &DataType::U => {
                    VarData::U(k.parse::<u64>()
                                .expect(&format!("Error while parsing the \
                                                  key number {}: the value '{}' \
                                                  cannot be converted into 'u64'", k,
                                                  i + 1)))
                }
                &DataType::S => VarData::S(k.to_owned()),
            };

            ptr::write(ptr.offset(i), data);
            actual_len += 1;
            keys.set_len(actual_len);
        }
        if actual_len != keys_len {
            panic!("Error during the key extraction: the key index exceeds the number of fields
                   in the record!");
        }
    }
    keys
}

/// Extracts a key from the record and returns a tuple of the key and the record.
///
/// # Safety
///
/// You should always use the `key_idx` parameter generated by `fields_to_idx()` function, unless
/// you know, what you're doing ;)
///
/// # Example
/// ```
/// use std::borrow::Cow;
/// use joinkit::util::{self, DataType, VarData};
///
/// let rec = "a;b;1";
/// let field_sep = ";";
/// // this reads as follows: the first field goes to the second position with data type `String`
/// // and the third field goes to the first position with data type `i64`.
/// let key_idx = [(0, 1, DataType::S), (2, 0, DataType::I)];
/// unsafe {
///     let key_val = util::extract_key_value(rec, field_sep, &key_idx);
///     assert_eq!((vec![VarData::I(1), 
///                      VarData::S("a".to_owned())], 
///                 Cow::Borrowed("a;b;1")), key_val);
/// }
pub unsafe fn extract_key_value<'a, C>(record: C, 
                                field_sep: &str,
                                key_idx: &[(usize, isize, DataType)]) -> (Vec<VarData>, Cow<'a, str>) 
    where C: Into<Cow<'a, str>>,
{ 
    let record = record.into();
    let key = extract_key(&record, field_sep, key_idx);
    (key, record)
}

/// Returns a number of fields in the record.
///
/// #Example
/// ```
/// use joinkit::util;
///
/// let rec = "a;b;c;d";
/// let field_sep = ";";
/// let n = util::num_fields(rec, field_sep);
///
/// assert_eq!(4, n);
pub fn num_fields(record: &str, 
                  field_sep: &str,) -> usize {
    record.split(field_sep).count()
}

/// Writes both, the left value and the right value into output stream. 
///
/// The values are separated by the field separator and the record separator is appended at the
/// end.
pub fn write_both<W: Write>(stream: &mut BufWriter<W>, lv: &str, rv: &str, fs: &[u8], rs: &[u8]) {
    stream.write(lv.as_bytes()).expect("Error: could not write into output stream!");
    stream.write(fs).expect("Error: could not write into output stream!");
    stream.write(rv.as_bytes()).expect("Error: could not write into output stream!");
    stream.write(rs).expect("Error: could not write into output stream!");
}

/// Writes only the left value with padded field separators in place of missing right value. 
pub fn write_left<W: Write>(stream: &mut BufWriter<W>, lv: &str, r_len: usize, fs: &[u8], rs: &[u8]) {
    stream.write(lv.as_bytes()).expect("Error: could not write into output stream!");
    // pad field separators for empty fields
    for _ in 0..r_len {
        stream.write(fs).expect("Error: could not write into output stream!");
    }
    stream.write(rs).expect("Error: could not write into output stream!");
}

/// Writes only the right value with padded field separators in place of missing left value. 
pub fn write_right<W: Write>(stream: &mut BufWriter<W>, rv: &str, l_len: usize, fs: &[u8], rs: &[u8]) {
    // pad field separators for empty fields
    for _ in 0..l_len {
        stream.write(fs).expect("Error: could not write into output stream!");
    }
    stream.write(rv.as_bytes()).expect("Error: could not write into output stream!");
    stream.write(rs).expect("Error: could not write into output stream!");
}

