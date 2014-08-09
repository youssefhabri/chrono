// This is a part of rust-chrono.
// Copyright (c) 2014, Kang Seonghoon.
// See README.md and LICENSE.txt for details.

#![allow(missing_doc)]

use std::i64;
use std::io::{IoResult, IoError, InvalidInput};

#[deriving(Show, Clone)]
pub struct Timezone {
    pub local_minus_utc: i32,
    pub dst: bool,
    pub name: String,
}

#[deriving(Show, Clone)]
pub struct TzFile {
    transitions: Vec<(i64, Timezone)>,
    leap_transitions: Vec<(i64, i32)>,
    future_rules: Option<String>,
}

fn invalid_input<T>(desc: &'static str) -> IoResult<T> {
    Err(IoError { kind: InvalidInput, desc: desc, detail: None })
}

/// Returns the first index `i` such that `v[i]` is no `Less` than the target,
/// or `v.len()` if there is no such `i`.
/// Similar to `vec::bsearch` but `v[i]` (if any) needs not be `Equal` to the target.
fn bsearch_no_less<T>(v: &[T], f: |&T| -> Ordering) -> uint {
    let mut base = 0;
    let mut limit = v.len();
    while limit != 0 { // invariant: v[base-1] (if any) < target <= v[base+limit] (if any)
        let ix = base + (limit >> 1);
        if f(&v[ix]) == Less {
            base = ix + 1;
            limit -= 1;
        }
        limit >>= 1;
    }
    base
}

impl TzFile {
    pub fn read(r: &mut Reader) -> IoResult<TzFile> {
        let magic = try!(r.read_be_u32());
        if magic != 0x545a6966 /*TZif*/ { return invalid_input("invalid tzfile magic"); }

        let version = try!(r.read_u8());
        let timewidth = match version {
            b'\0' => 4,
            b'2' | b'3' => 8,
            _ => return invalid_input("invalid tzfile version"),
        };
        try!(r.read_exact(15));

        // for the format version 2 or 3, skip the first data and the second magic.
        if timewidth == 8 {
            let ttisgmtcnt = try!(r.read_be_u32()) as uint;
            let ttisstdcnt = try!(r.read_be_u32()) as uint;
            let leapcnt = try!(r.read_be_u32()) as uint;
            let timecnt = try!(r.read_be_u32()) as uint;
            let typecnt = try!(r.read_be_u32()) as uint;
            let charcnt = try!(r.read_be_u32()) as uint;

            let skip = timecnt * 5 + typecnt * 6 + charcnt + leapcnt * 8 + ttisgmtcnt + ttisstdcnt;
            try!(r.read_exact(skip));

            let magic_ = try!(r.read_be_u32());
            if magic_ != 0x545a6966 /*TZif*/ { return invalid_input("invalid tzfile magic"); }
            let version_ = try!(r.read_u8());
            if version_ != version { return invalid_input("invalid tzfile version"); }
            try!(r.read_exact(15));
        }

        let ttisgmtcnt = try!(r.read_be_u32()) as uint;
        let ttisstdcnt = try!(r.read_be_u32()) as uint;
        let leapcnt = try!(r.read_be_u32()) as uint;
        let timecnt = try!(r.read_be_u32()) as uint;
        let typecnt = try!(r.read_be_u32()) as uint;
        let charcnt = try!(r.read_be_u32()) as uint;

        // sanity check
        if typecnt == 0 || !(ttisstdcnt == 0 || ttisstdcnt == typecnt) ||
                           !(ttisgmtcnt == 0 || ttisgmtcnt == typecnt) {
            return invalid_input("invalid tzfile header");
        }

        let mut transitions: Vec<(i64, Timezone)> = Vec::new();
        let mut leap_transitions: Vec<(i64, i32)> = Vec::new();

        let mut ttpoints = Vec::new();
        let mut ttindices = Vec::new();
        let mut ttinfos0 = Vec::new();
        for i in range(0, timecnt) {
            ttpoints.push(try!(r.read_be_int_n(timewidth)));
        }
        for i in range(0, timecnt) {
            ttindices.push(try!(r.read_u8()) as uint);
        }
        for i in range(0, typecnt) {
            let gmtoff = try!(r.read_be_i32());
            let isdst = try!(r.read_u8());
            let abbrind = try!(r.read_u8()) as uint;
            ttinfos0.push((gmtoff, isdst, abbrind));
        }
        let charpool = match String::from_utf8(try!(r.read_exact(charcnt as uint))) {
            Ok(pool) => pool,
            Err(_) => return invalid_input("invalid tzfile abbreviation pool"),
        };
        for i in range(0, leapcnt) {
            let leapsince = try!(r.read_be_int_n(timewidth));
            let leaptotal = try!(r.read_be_i32());
            if leap_transitions.last().map_or(false, |&(since, _)| since >= leapsince) {
                return invalid_input("unsorted tzfile entires");
            }
            leap_transitions.push((leapsince, leaptotal));
        }

        // we don't use the standard/wall and UTC/local indicators, so simply ignore them.
        //
        // they are used as a template to the POSIX-style TZ environment variable
        // without DST rules (e.g. `CET-2CEST`), in which case POSIX (or, more accurately,
        // IEEE Std 1003.1-1996 [1]; I'm yet to find the corresponding parts in 1003.1-2001)
        // requires the implementation not to fail but allows it to use any default.
        //
        // the US rules (`M4.1.0,M10.5.0`) seem to be a common default according to tzcode,
        // and as rust-chrono implements [2] the future-proof implementation of TZ rules,
        // there is no need for handling the additional template information for tzfile.
        //
        // [1] http://mm.icann.org/pipermail/tz/1999-May/010546.html
        // [2] implementation planned but pending
        try!(r.read_exact(ttisstdcnt));
        try!(r.read_exact(ttisgmtcnt));

        // read the POSIX-style TZ rules for later dates
        let tzrules;
        if version >= b'2' {
            if try!(r.read_u8()) != b'\n' { return invalid_input("missing tzfile TZ string"); }
            let mut rules = Vec::new();
            loop {
                match try!(r.read_u8()) {
                    b'\n' => break,
                    ch => { rules.push(ch); }
                }
            }
            tzrules = if rules.is_empty() {
                None
            } else {
                match String::from_utf8(rules) {
                    Ok(rules) => Some(rules),
                    Err(_) => return invalid_input("invalid tzfile TZ string"),
                }
            };
        } else {
            tzrules = None;
        }

        let mut ttinfos = Vec::new();
        for (gmtoff, isdst, abbrind) in ttinfos0.move_iter() {
            let isdst = match isdst {
                0 => false,
                1 => true,
                _ => return invalid_input("invalid tzfile dst flag"),
            };
            let abbrev = if abbrind < charpool.len() {
                let abbrev = charpool.as_slice().slice_from(abbrind);
                match abbrev.find('\0') {
                    Some(idx) => abbrev.slice_to(idx).to_string(),
                    None => return invalid_input("invalid tzfile abbreviation index"),
                }
            } else {
                return invalid_input("invalid tzfile abbreviation index");
            };
            ttinfos.push(Timezone { local_minus_utc: gmtoff, dst: isdst, name: abbrev });
        }

        transitions.push((i64::MIN, ttinfos[0].clone()));
        for (ttpoint, ttindex) in ttpoints.move_iter().zip(ttindices.move_iter()) {
            if transitions.last().map_or(false, |&(since, _)| since >= ttpoint) {
                return invalid_input("unsorted tzfile entires");
            }
            transitions.push((ttpoint, ttinfos[ttindex].clone()));
        }

        Ok(TzFile { transitions: transitions, leap_transitions: leap_transitions,
                    future_rules: tzrules, })
    }

    pub fn transitions<'a>(&'a self) -> &'a [(i64, Timezone)] {
        self.transitions.as_slice()
    }

    pub fn leap_transitions<'a>(&'a self) -> &'a [(i64, i32)] {
        self.leap_transitions.as_slice()
    }

    pub fn timezone_at<'a>(&'a self, at: i64) -> &'a Timezone {
        let transitions = self.transitions.as_slice();
        let next = bsearch_no_less(transitions, |&(since, _)| since.cmp(&at));
        assert!(next > 0);
        transitions[next-1].ref1()
    }

    pub fn total_leap_seconds_at(&self, at: i64) -> i32 {
        let transitions = self.leap_transitions.as_slice();
        let next = bsearch_no_less(transitions, |&(since, _)| since.cmp(&at));
        if next > 0 {
            transitions[next-1].val1()
        } else {
            0
        }
    }

    pub fn future_rules<'a>(&'a self) -> Option<&'a str> {
        self.future_rules.as_ref().map(|s| s.as_slice())
    }
}

