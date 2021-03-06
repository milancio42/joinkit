extern crate joinkit;
#[macro_use]
extern crate clap;
extern crate itertools;

use std::io::{self, BufRead, Write, BufWriter, stderr,};
use std::fs::File;
use std::process;
use joinkit::{Joinkit, util,};
use joinkit::EitherOrBoth::{Left, Both, Right};
use clap::{Arg, App,};
use itertools::Itertools;


fn main() {
    let join_modes = ["inner",
                      "left-excl",
                      "left-outer",
                      "right-excl",
                      "right-outer",
                      "full-outer",];
    let matches = App::new("mjoin")
        .version(&crate_version!()[..])
        .author("Milan Opath <milan.opath@gmail.com>")
        .about("Join records of two files using the Merge Join strategy.")
        .arg(Arg::with_name("FIELDS1")
            .help("Join on these comma-separated FIELDS of FILE1. \
                  The index starts with 1 and must not contain duplicates. \
                  It can optionally contain a flag to convert the given key to a number, e.g. \
                  '1-i,2,3-u'. \
                  Since strings are compared lexicographically, they are not suitable for numbers, \
                  e.g. `2` would be \
                  greater than `12`. \
                  The recognized flags are: -u: convert to unsigned int 64 \
                                            -i: convert to signed int 64.")
            .short("1")
            .default_value("1"))
            .arg(Arg::with_name("FIELDS2")
            .help("Join on these comma-separated FIELDS of FILE2. \
                  The index starts with 1 and must not contain duplicates. \
                  It can optionally contain a flag to convert the given key to a number, e.g. \
                  '1-i,2,3-u'. \
                  Since strings are compared lexicographically, they are not suitable for numbers, \
                  e.g. `2` would be \
                  greater than `12`. \
                  The recognized flags are: -u: convert to unsigned int 64 \
                                            -i: convert to signed int 64.")
            .short("2")
            .default_value("1"))
        .arg(Arg::with_name("in-rec-sep")
            .help("Input record separator - must be encodable as a single byte in utf8.")
            .short("R")
            .long("in-rec-sep")
            .takes_value(true))
        .arg(Arg::with_name("in-field-sep")
            .help("Input field separator - can be any string.")
            .short("F")
            .long("in-field-sep")
            .takes_value(true))
        .arg(Arg::with_name("in-rec-sep-left")
            .help("Left input file record separator - must be encodable as a single byte in utf8.")
            .long("in-rec-sep-left")
            .conflicts_with("in-rec-sep")
            .requires("in-rec-sep-right")
            .takes_value(true))
        .arg(Arg::with_name("in-field-sep-left")
            .help("Left input file field separator - can be any string.")
            .long("in-field-sep-left")
            .conflicts_with("in-field-sep")
            .requires("in-field-sep-right")
            .takes_value(true))
        .arg(Arg::with_name("in-rec-sep-right")
            .help("Right input file record separator - must be encodable as a single byte in utf8.")
            .long("in-rec-sep-right")
            .conflicts_with("in-rec-sep")
            .requires("in-rec-sep-left")
            .takes_value(true))
        .arg(Arg::with_name("in-field-sep-right")
            .help("Right input file field separator - can be any string.")
            .long("in-field-sep-right")
            .conflicts_with("in-field-sep")
            .requires("in-field-sep-left")
            .takes_value(true))
        .arg(Arg::with_name("out-rec-sep")
            .help("Output record separator - if not specified, it is equal to in-rec-sep.")
            .long("out-rec-sep")
            .takes_value(true))
        .arg(Arg::with_name("out-field-sep")
            .help("Output field separator - if not specified, it is equal to in-field-sep.")
            .long("out-field-sep")
            .takes_value(true))
        .arg(Arg::with_name("mode")
            .help("Join mode.")
            .short("m")
            .long("mode")
            .possible_values(&join_modes)
            .takes_value(true))
        .arg(Arg::with_name("FILE1")
            .help("The left input file.")
            .required(true)
            .index(1))
        .arg(Arg::with_name("FILE2")
            .help("The right input file.")
            .required(true)
            .index(2))
        .get_matches();

    let file_left: &str = matches.value_of("FILE1").unwrap();
    let file_right: &str = matches.value_of("FILE2").unwrap();
    
    let in_rec_sep: &str = matches.value_of("in-rec-sep").unwrap_or("\n");
    let in_rec_sep_left: &str = matches.value_of("in-rec-sep-left").unwrap_or(in_rec_sep);
    let in_rec_sep_left_u8: u8 = match util::rec_sep_as_byte(in_rec_sep_left) {
        Ok(b) => b,
        Err(e) => e.exit(),
    };
    let in_rec_sep_right: &str = matches.value_of("in-rec-sep-right").unwrap_or(in_rec_sep);
    let in_rec_sep_right_u8: u8 = match util::rec_sep_as_byte(in_rec_sep_right) {
        Ok(b) => b,
        Err(e) => e.exit(),
    };

    let in_field_sep: &str = matches.value_of("in-field-sep").unwrap_or(",");
    let in_field_sep_left: &str = matches.value_of("in-field-sep-left").unwrap_or(in_field_sep);
    let in_field_sep_right: &str = matches.value_of("in-field-sep-right").unwrap_or(in_field_sep);

    let out_rec_sep: &str = matches.value_of("out-rec-sep").unwrap_or(in_rec_sep);
    let out_rec_sep_u8: &[u8] = out_rec_sep.as_bytes();

    let out_field_sep: &str = matches.value_of("out-field-sep").unwrap_or(in_field_sep);
    let out_field_sep_u8: &[u8] = out_field_sep.as_bytes();

    let key_fields_idx_left: Vec<(usize, 
                                  isize, 
                                  util::DataType)> 
                             = match util::fields_to_idx(matches.values_of("FIELDS1")
                                                                .unwrap()
                                                                .collect::<Vec<_>>()) {
        Ok(v) => v,
        Err(e) => e.exit(),
    };
    let key_fields_idx_right: Vec<(usize, 
                                   isize, 
                                   util::DataType)> 
                             = match util::fields_to_idx(matches.values_of("FIELDS2")
                                                                .unwrap()
                                                                .collect::<Vec<_>>()) {
        Ok(v) => v,
        Err(e) => e.exit(),
    };

    let file_left = match File::open(file_left) {
        Ok(f) => f,
        Err(_) => {
            writeln!(&mut stderr(), "Erro: could not open FILE1").unwrap();
            process::exit(1);
        },

    };
    let stream_left = io::BufReader::new(file_left);
    let mut records_left = stream_left.split(in_rec_sep_left_u8)
        .map(|r| match r {
            Ok(v) => v,
            Err(_) => {
                writeln!(&mut stderr(), "Error: could not read the record in FILE1").unwrap();
                process::exit(1);
            },
        })
        .map(|v| String::from_utf8(v))
        .map(|r| match r {
            Ok(s) => s,
            Err(_) => {
                writeln!(&mut stderr(), "Error: could not convert the record bytes into string").unwrap();
                process::exit(1);
            },
        })
        .group_by(|s| unsafe {util::extract_key(s, in_field_sep_left, &key_fields_idx_left)})
        .peekable();


    let file_right = match File::open(file_right) {
        Ok(f) => f,
        Err(_) => {
            writeln!(&mut stderr(), "Error: could not open FILE2").unwrap();
            process::exit(1);
        },
    };
    let stream_right = io::BufReader::new(file_right);
    let mut records_right = stream_right.split(in_rec_sep_right_u8)
        .map(|r| match r {
            Ok(v) => v,
            Err(_) => {
                writeln!(&mut stderr(), "Error: could not read the record in FILE2").unwrap();
                process::exit(1);
            },
        })
        .map(|v| String::from_utf8(v))
        .map(|r| match r {
            Ok(s) => s,
            Err(_) => {
                writeln!(&mut stderr(), "Error: could not convert the record bytes into string").unwrap();
                process::exit(1);
            },
        })
        .group_by(|s| unsafe {util::extract_key(s, in_field_sep_right, &key_fields_idx_right)})
        .peekable();

    let mut out_stream = BufWriter::new(io::stdout());

    let mode = matches.value_of("mode").unwrap_or("inner");

    match mode {
        "inner" => {
            let join = records_left.merge_join_inner_by(records_right, |l, r| Ord::cmp(&l.0, &r.0));
            for ((_, lvv), (_, rvv)) in join {
                for lv in lvv {
                    for rv in &rvv {
                        util::write_both(&mut out_stream, &lv, &rv, out_field_sep_u8, out_rec_sep_u8);
                    }
                }
            }
        },
        "left-excl" => {
            let join = records_left.merge_join_left_excl_by(records_right, |l, r| Ord::cmp(&l.0, &r.0));
            for (_, lvv) in join {
                for lv in lvv {
                    util::write_left(&mut out_stream, &lv, 0, out_field_sep_u8, out_rec_sep_u8);
                }
            }
        },
        "left-outer" => {
            // take the first record and find the number of fields
            let right_num_fields = match records_right.peek() {
                Some(ref t) => (t.0).len(),
                None => 0,
            };
            let join = records_left.merge_join_left_outer_by(records_right, |l, r| Ord::cmp(&l.0, &r.0));
            for e in join {
                match e {
                    Left((_, lvv)) => for lv in lvv {
                        util::write_left(&mut out_stream, &lv, right_num_fields, out_field_sep_u8, out_rec_sep_u8);
                    },
                    Both((_, lvv), (_, rvv)) => for lv in lvv {
                        for rv in &rvv {
                            util::write_both(&mut out_stream, &lv, &rv, out_field_sep_u8, out_rec_sep_u8);
                        }
                    },
                    _ => unreachable!(),
                }

            }
        },
        "right-excl" => {
            // left-excl with inverted input
            let join = records_right.merge_join_left_excl_by(records_left, |l, r| Ord::cmp(&l.0, &r.0));
            for (_, lvv) in join {
                for lv in lvv {
                    util::write_right(&mut out_stream, &lv, 0, out_field_sep_u8, out_rec_sep_u8);
                }
            }
        },
        "right-outer" => {
            // take the first record and find the number of fields
            let left_num_fields = match records_left.peek() {
                Some(ref t) => (t.0).len(),
                None => 0,
            };
            // left-outer with inverted input
            let join = records_right.merge_join_left_outer_by(records_left, |l, r| Ord::cmp(&l.0, &r.0));
            for e in join {
                match e {
                    Left((_, lvv)) => for lv in lvv {
                        util::write_right(&mut out_stream, &lv, left_num_fields, out_field_sep_u8, out_rec_sep_u8);
                    },
                    Both((_, lvv), (_, rvv)) => for lv in lvv {
                        for rv in &rvv {
                            util::write_both(&mut out_stream, &lv, &rv, out_field_sep_u8, out_rec_sep_u8);
                        }
                    },
                    _ => unreachable!(),
                }

            }
        },
        "full-outer" => {
            // take the first record and find the number of fields
            let left_num_fields = match records_left.peek() {
                Some(ref t) => (t.0).len(),
                None => 0,
            };
            let right_num_fields = match records_right.peek() {
                Some(ref t) => (t.0).len(),
                None => 0,
            };
            let join = records_left.merge_join_full_outer_by(records_right, |l, r| Ord::cmp(&l.0, &r.0));
            for e in join {
                match e {
                    Left((_, lvv)) => for lv in lvv {
                        util::write_left(&mut out_stream, &lv, right_num_fields, out_field_sep_u8, out_rec_sep_u8);
                    },
                    Right((_, rvv)) => for rv in rvv {
                        util::write_right(&mut out_stream, &rv, left_num_fields, out_field_sep_u8, out_rec_sep_u8);
                    },
                    Both((_, lvv), (_, rvv)) => for lv in lvv {
                        for rv in &rvv {
                            util::write_both(&mut out_stream, &lv, &rv, out_field_sep_u8, out_rec_sep_u8);
                        }
                    },
                }

            }
        },
        _ => unreachable!(),
    }
}


