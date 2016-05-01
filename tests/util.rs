extern crate joinkit;

use joinkit::util::{self, DataType};

#[test]
fn extract_key_single_eq() {
    unsafe {
        let rec0 = "20;a;b";
        let rec1 = "20;a;b";
        let key_idx = [(0, 0, DataType::U)];
        let k0 = util::extract_key(rec0, ";", &key_idx);
        let k1 = util::extract_key(rec1, ";", &key_idx);

        assert_eq!(k0, k1);
    }
}

#[test]
fn extract_key_single_ne() {
    unsafe {
        let rec0 = "20;a;b";
        let rec1 = "2;a;b";
        let key_idx = [(0, 0, DataType::U)];
        let k0 = util::extract_key(rec0, ";", &key_idx);
        let k1 = util::extract_key(rec1, ";", &key_idx);

        assert!(k0 > k1);
    }
}

#[test]
fn extract_key_multiple_eq() {
    unsafe {
        let rec0 = "20;a;b";
        let rec1 = "20;a;b";
        let key_idx = [(0, 1, DataType::U), (2, 0, DataType::S)];
        let k0 = util::extract_key(rec0, ";", &key_idx);
        let k1 = util::extract_key(rec1, ";", &key_idx);

        assert_eq!(k0, k1);
    }
}

#[test]
fn extract_key_multiple_ne() {
    unsafe {
        let rec0 = "20;a;b";
        let rec1 = "2;a;b";
        let key_idx = [(0, 1, DataType::U), (2, 0, DataType::S)];
        let k0 = util::extract_key(rec0, ";", &key_idx);
        let k1 = util::extract_key(rec1, ";", &key_idx);

        assert!(k0 > k1);
    }
}

