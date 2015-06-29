//! SQL-like join implementation of two (non-sorted) iterators.
//!
//! The hash join strategy requires the right iterator can be collected to a `HashMap`. The left
//! iterator can be arbitrarily long. It is therefore asymmetric (intput iterators cannot be
//! swapped to obtain right_outer join from left_outer join), as distinct from [the merge join
//! strategy](merge_join/index.html), which is symmetric.
//!
//! The fact that iterators do not need to be sorted makes it very efficient and particularly
//! suitable for [star schema](https://en.wikipedia.org/wiki/Star_schema) or [snowflake
//! schema](https://en.wikipedia.org/wiki/Snowflake_schema) joins.
//!
//! The supported join types:
//!
//! * [`INNER JOIN`](trait.Joinkit.html#method.hash_join_inner) - an intersection between the
//! left and the right iterator.
//! * [`LEFT EXCL JOIN`](trait.Joinkit.html#method.hash_join_left_excl) - a difference
//! between the left and the right iterator (not directly in SQL).
//! * [`LEFT OUTER JOIN`](trait.Joinkit.html#method.hash_join_left_outer) - a union of `INNER
//! JOIN` and `LEFT EXCL JOIN`.
//! * [`RIGHT EXCL JOIN`](trait.Joinkit.html#method.hash_join_right_excl) - a difference
//! between the right and the left iterator (not directly in SQL).
//! * [`RIGHT OUTER JOIN`](trait.Joinkit.html#method.hash_join_right_outer) - a union of `INNER
//! JOIN` and `RIGHT EXCL JOIN`.
//! * [`FULL OUTER JOIN`](trait.Joinkit.html#method.hash_join_full_outer) - a union of `INNER
//! JOIN`, `LEFT EXCL JOIN` and `RIGHT EXCL JOIN`.

use std::collections::hash_map::{HashMap, IntoIter,};
use std::collections::hash_set::{HashSet,};
use std::mem;
use std::hash::Hash;
use super::EitherOrBoth::{self, Right, Left, Both};

/// See [`hash_join_inner()`](trait.Joinkit.html#method.hash_join_inner) for the description and
/// examples.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct HashJoinInner<L, K, RV> {
    left: L,
    map: HashMap<K, Vec<RV>>,
}

impl<L, K, RV> HashJoinInner<L, K, RV> 
    where K: Hash + Eq,
{
    /// Create a `HashJoinInner` iterator.
    pub fn new<LI, RI>(left: LI, right: RI) -> Self
        where L: Iterator<Item=LI::Item>,
              LI: IntoIterator<IntoIter=L>,
              RI: IntoIterator<Item=(K, RV)>
    {
        let mut map: HashMap<K, Vec<RV>> = HashMap::new();
        for (k, v) in right.into_iter() {
            let values = map.entry(k).or_insert(Vec::with_capacity(1));
            values.push(v);
        }
        HashJoinInner {
            left: left.into_iter(),
            map: map,
        }
    }
}

impl<L, K, LV, RV> Iterator for HashJoinInner<L, K, RV> 
    where L: Iterator<Item=(K, LV)>,
          K: Hash + Eq,
          RV: Clone,
{
    type Item = (LV, Vec<RV>);
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.left.next() {
                Some((lk, lv)) => match self.map.get(&lk) {
                    Some(rvv) => return Some((lv, rvv.clone())),
                    None => continue,
                },
                None => return None,
            }
        }
    }
}

/// See [`hash_join_left_excl()`](trait.Joinkit.html#method.hash_join_left_excl) for the
/// description and examples.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct HashJoinLeftExcl<L, K> {
    left: L,
    set: HashSet<K>,
}

impl<L, K> HashJoinLeftExcl<L, K> 
    where K: Hash + Eq,
{
    /// Create a `HashJoinLeftExcl` iterator.
    pub fn new<LI, RI, RV>(left: LI, right: RI) -> Self
        where L: Iterator<Item=LI::Item>,
              LI: IntoIterator<IntoIter=L>,
              RI: IntoIterator<Item=(K, RV)>
    {
        let mut set: HashSet<K> = HashSet::new();
        for (k, _) in right.into_iter() {
            set.insert(k);
        }
        HashJoinLeftExcl {
            left: left.into_iter(),
            set: set,
        }
    }
}

impl<L, K, LV> Iterator for HashJoinLeftExcl<L, K> 
    where L: Iterator<Item=(K, LV)>,
          K: Hash + Eq,
{
    type Item = LV;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.left.next() {
                Some((lk, lv)) => {
                    if self.set.contains(&lk) {
                        continue;
                    } else {
                        return Some(lv);
                    }
                },
                None => return None,
            }
        }
    }
}

/// See [`hash_join_left_outer()`](trait.Joinkit.html#method.hash_join_left_outer) for the
/// description and examples.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct HashJoinLeftOuter<L, K, RV> {
    left: L,
    map: HashMap<K, Vec<RV>>,
}

impl<L, K, RV> HashJoinLeftOuter<L, K, RV> 
    where K: Hash + Eq,
{
    /// Create a `HashJoinLeftOuter` iterator.
    pub fn new<LI, RI>(left: LI, right: RI) -> Self
        where L: Iterator<Item=LI::Item>,
              LI: IntoIterator<IntoIter=L>,
              RI: IntoIterator<Item=(K, RV)>
    {
        let mut map: HashMap<K, Vec<RV>> = HashMap::new();
        for (k, v) in right.into_iter() {
            let values = map.entry(k).or_insert(Vec::with_capacity(1));
            values.push(v);
        }
        HashJoinLeftOuter {
            left: left.into_iter(),
            map: map,
        }
    }
}

impl<L, K, LV, RV> Iterator for HashJoinLeftOuter<L, K, RV> 
    where L: Iterator<Item=(K, LV)>,
          K: Hash + Eq,
          RV: Clone,
{
    type Item = EitherOrBoth<LV, Vec<RV>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.left.next() {
                Some((lk, lv)) => match self.map.get(&lk) {
                    Some(rvv) => return Some(Both(lv, rvv.clone())),
                    None => return Some(Left(lv)),
                },
                None => return None,
            }
        }
    }
}

/// See [`hash_join_right_excl()`](trait.Joinkit.html#method.hash_join_right_excl) for the
/// description and examples.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct HashJoinRightExcl<L, K, RV> {
    left: L,
    map: HashMap<K, (Vec<RV>, bool)>,
    /// exclusion iterator - yields the unmatched values from the map. It is created once the left
    /// iterator is exhausted
    excl_iter: Option<IntoIter<K, (Vec<RV>, bool)>>,
}

impl<L, K, RV> HashJoinRightExcl<L, K, RV> 
    where K: Hash + Eq,
{
    /// Create a `HashJoinRightExcl` iterator.
    pub fn new<LI, RI>(left: LI, right: RI) -> Self
        where L: Iterator<Item=LI::Item>,
              LI: IntoIterator<IntoIter=L>,
              RI: IntoIterator<Item=(K, RV)>
    {
        let mut map: HashMap<K, (Vec<RV>, bool)> = HashMap::new();
        for (k, v) in right.into_iter() {
            let values = map.entry(k).or_insert((Vec::with_capacity(1), false));
            values.0.push(v);
        }
        HashJoinRightExcl {
            left: left.into_iter(),
            map: map,
            excl_iter: None,
        }
    }

    /// Moves the map to `self.excl_iter`
    ///
    /// Once the left iterator is exhausted, the info about which keys were matched is complete.
    /// To be able to iterate over map's values we need to move it into its `IntoIter`.
    fn set_excl_iter(&mut self) {
        let map = mem::replace(&mut self.map, HashMap::<K, (Vec<RV>, bool)>::new());
        self.excl_iter = Some(map.into_iter());
    }
}

impl<L, K, LV, RV> Iterator for HashJoinRightExcl<L, K, RV> 
    where L: Iterator<Item=(K, LV)>,
          K: Hash + Eq,
{
    type Item = Vec<RV>;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.excl_iter {
                // the left iterator is not yet exhausted
                None => match self.left.next() {
                    Some((lk, _)) => match self.map.get_mut(&lk) {
                        Some(rt) => {
                            rt.1 = true; // flag as matched
                        },
                        None => continue, // not interested in unmatched left value
                    },
                    // the left iterator is exhausted so move the map into `self.excl_iter`.
                    None => self.set_excl_iter(),
                },
                // iterate over unmatched values
                Some(ref mut r) => match r.next() {
                    Some((_, (rvv, matched))) => {
                        if !matched {
                            return Some(rvv);
                        } else {
                            continue;
                        }
                    },
                    None => return None,
                }
            }
        }
    }
}

/// See [`hash_join_right_outer()`](trait.Joinkit.html#method.hash_join_right_outer) for the
/// description and examples.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct HashJoinRightOuter<L, K, RV> {
    left: L,
    map: HashMap<K, (Vec<RV>, bool)>,
    /// exclusion iterator - yields the unmatched values from the map. It is created once the left
    /// iterator is exhausted
    excl_iter: Option<IntoIter<K, (Vec<RV>, bool)>>,
}

impl<L, K, RV> HashJoinRightOuter<L, K, RV> 
    where K: Hash + Eq,
{
    /// Create a `HashJoinRightOuter` iterator.
    pub fn new<LI, RI>(left: LI, right: RI) -> Self
        where L: Iterator<Item=LI::Item>,
              LI: IntoIterator<IntoIter=L>,
              RI: IntoIterator<Item=(K, RV)>
    {
        let mut map: HashMap<K, (Vec<RV>, bool)> = HashMap::new();
        for (k, v) in right.into_iter() {
            let values = map.entry(k).or_insert((Vec::with_capacity(1), false));
            values.0.push(v);
        }
        HashJoinRightOuter {
            left: left.into_iter(),
            map: map,
            excl_iter: None,
        }
    }

    /// Moves the map to `self.excl_iter`
    ///
    /// Once the left iterator is exhausted, the info about which keys were matched is complete.
    /// To be able to iterate over map's values we need to move it into its `IntoIter`.
    fn set_excl_iter(&mut self) {
        let map = mem::replace(&mut self.map, HashMap::<K, (Vec<RV>, bool)>::new());
        self.excl_iter = Some(map.into_iter());
    }
}

impl<L, K, LV, RV> Iterator for HashJoinRightOuter<L, K, RV> 
    where L: Iterator<Item=(K, LV)>,
          K: Hash + Eq,
          RV: Clone,
{
    type Item = EitherOrBoth<LV, Vec<RV>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.excl_iter {
                // the left iterator is not yet exhausted
                None => match self.left.next() {
                    Some((lk, lv)) => match self.map.get_mut(&lk) {
                        Some(rt) => {
                            rt.1 = true; // flag as matched
                            return Some(Both(lv, rt.0.clone()))
                        },
                        None => continue, // not interested in unmatched left value
                    },
                    // the left iterator is exhausted so move the map into `self.excl_iter`.
                    None => self.set_excl_iter(),
                },
                // iterate over unmatched values
                Some(ref mut r) => match r.next() {
                    Some((_, (rvv, matched))) => {
                        if !matched {
                            return Some(Right(rvv));
                        } else {
                            continue;
                        }
                    },
                    None => return None,
                }
            }
        }
    }
}

/// See [`hash_join_full_outer()`](trait.Joinkit.html#method.hash_join_full_outer) for the
/// description and examples.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct HashJoinFullOuter<L, K, RV> {
    left: L,
    map: HashMap<K, (Vec<RV>, bool)>,
    /// exclusion iterator - yields the unmatched values from the map. It is created once the left
    /// iterator is exhausted
    excl_iter: Option<IntoIter<K, (Vec<RV>, bool)>>,
}

impl<L, K, RV> HashJoinFullOuter<L, K, RV> 
    where K: Hash + Eq,
{
    /// Create a `HashJoinFullOuter` iterator.
    pub fn new<LI, RI>(left: LI, right: RI) -> Self
        where L: Iterator<Item=LI::Item>,
              LI: IntoIterator<IntoIter=L>,
              RI: IntoIterator<Item=(K, RV)>
    {
        let mut map: HashMap<K, (Vec<RV>, bool)> = HashMap::new();
        for (k, v) in right.into_iter() {
            let values = map.entry(k).or_insert((Vec::with_capacity(1), false));
            values.0.push(v);
        }
        HashJoinFullOuter {
            left: left.into_iter(),
            map: map,
            excl_iter: None,
        }
    }

    /// Moves the map to `self.excl_iter`
    ///
    /// Once the left iterator is exhausted, the info about which keys were matched is complete.
    /// To be able to iterate over map's values we need to move it into its `IntoIter`.
    fn set_excl_iter(&mut self) {
        let map = mem::replace(&mut self.map, HashMap::<K, (Vec<RV>, bool)>::new());
        self.excl_iter = Some(map.into_iter());
    }
}

impl<L, K, LV, RV> Iterator for HashJoinFullOuter<L, K, RV> 
    where L: Iterator<Item=(K, LV)>,
          K: Hash + Eq,
          RV: Clone,
{
    type Item = EitherOrBoth<LV, Vec<RV>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.excl_iter {
                // the left iterator is not yet exhausted
                None => match self.left.next() {
                    Some((lk, lv)) => match self.map.get_mut(&lk) {
                        Some(rt) => {
                            rt.1 = true; // flag as matched
                            return Some(Both(lv, rt.0.clone()))
                        },
                        None => return Some(Left(lv)),
                    },
                    // the left iterator is exhausted so move the map into `self.excl_iter`.
                    None => self.set_excl_iter(),
                },
                // iterate over unmatched values
                Some(ref mut r) => match r.next() {
                    Some((_, (rvv, matched))) => {
                        if !matched {
                            return Some(Right(rvv));
                        } else {
                            continue;
                        }
                    },
                    None => return None,
                }
            }
        }
    }
}
