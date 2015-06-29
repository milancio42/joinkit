#![warn(missing_docs)]
#![crate_name="joinkit"]

//! Joinkit provides iterator adaptors for efficient SQL-like joins.
//! 
//! # Strategies
//!
//! There are two join strategies, which fit different scenarios:
//! - **Hash Join** - a shorter data stream is loaded entirely into memory (`HashMap`), while the
//! longer can be arbitrarily large and is matched against `HashMap` sequentially. The greatest
//! advantage is that data do not need to be sorted and it has amortized O(n) complexity, therefore
//! it is very efficient.  This is the right choice if data is not sorted and the smaller stream
//! fits into memory. 
//! - **Merge Join** - the data streams *must* be sorted, but can be *both* arbitrarily large. This
//! is the right choice if the data is already sorted, as in this case it is slightly more
//! efficient than Hash Join. 
//!
//! To use the iterator adaptors in this crate, import `Joinkit trait`:
//!
//! ```
//! use joinkit::Joinkit;
//! ```
//!
//! The crate contains also 2 binaries `hjoin` and `mjoin`, which can be used to perform `Hash
//! Join` and `Merge Join` on command line. 

#[macro_use]
extern crate clap;
extern crate itertools;

use std::iter::{IntoIterator};
use std::cmp::Ordering;
use std::hash::Hash;

pub use merge_join::{MergeJoinInner, MergeJoinLeftExcl, MergeJoinLeftOuter, MergeJoinFullOuter};
pub use hash_join::{HashJoinInner, HashJoinLeftExcl, HashJoinLeftOuter, HashJoinRightExcl,
HashJoinRightOuter, HashJoinFullOuter};

pub mod util;
mod merge_join;
mod hash_join;

/// A value yielded by `merge_join` and `hash_join` outer iterators.
/// Contains one or two values, depending on which input iterator is exhausted.
///
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum EitherOrBoth<L, R> {
    /// Neither input iterator is exhausted yet, yielding two values.
    Both(L, R),
    /// The parameter iterator is exhausted, only yielding a value from the `self` iterator.
    Left(L),
    /// The `self` iterator is exhausted, only yielding a value from the parameter iterator.
    Right(R),
}

/// Trait `Joinkit` provides the extra iterator adaptors for efficient SQL-like joins.
pub trait Joinkit : Iterator {
    /// Return an iterator adaptor that [inner
    /// joins](https://en.wikipedia.org/wiki/Join_%28SQL%29#Inner_join) the two input iterators in
    /// ascending order. The resulting iterator is the intersection of the two input iterators.
    ///
    /// The both input iterators must be sorted and unique on the join key (e.g. by
    /// [grouping](http://bluss.github.io/rust-itertools/doc/itertools/trait.Itertools.html#method.group_by)
    /// them, if necessary) to produce the correct results.
    ///
    /// Iterator element type is `(L::Item, R::Item)`.
    ///
    /// ```
    /// use joinkit::Joinkit;
    ///
    /// // tuples of (key, [value,...]), where the key is extracted from the value
    /// // notice the values are grouped by the key
    /// let l = vec![("0", vec!["0;A"]), ("1", vec!["1;B"])].into_iter();
    /// let r = vec![("1", vec!["1;X", "1;Y"]), ("2", vec!["2;Z"])].into_iter();
    /// let mut it = l.merge_join_inner_by(r, |x, y| Ord::cmp(&x.0, &y.0));
    ///
    /// assert_eq!(it.next(), Some((("1", vec!["1;B"]), ("1", vec!["1;X", "1;Y"]))));
    /// assert_eq!(it.next(), None);
    /// ```
    fn merge_join_inner_by<R, F>(self, other: R, cmp: F) -> MergeJoinInner<Self, R::IntoIter, F> 
        where Self: Sized,
              R: IntoIterator,
              F: FnMut(&Self::Item, &R::Item) -> Ordering
    {
        MergeJoinInner::new(self, other.into_iter(), cmp)
    }

    /// Return an iterator adaptor that *left exclusive joins* the two input iterators in
    /// ascending order. The resulting iterator contains only those records from the left input
    /// iterator, which do not match the right input iterator. There is no direct equivalent in
    /// SQL.
    /// 
    /// The both input iterators must be sorted and unique on the join key (e.g. by
    /// [grouping](http://bluss.github.io/rust-itertools/doc/itertools/trait.Itertools.html#method.group_by)
    /// them, if necessary) to produce the correct results.
    ///
    /// Iterator element type is `L::Item`.
    ///
    /// ```
    /// use joinkit::Joinkit;
    ///
    /// // tuples of (key, [value,...]), where the key is extracted from the value
    /// // notice the values are grouped by the key
    /// let l = vec![("0", vec!["0;A"]), ("1", vec!["1;B"])].into_iter();
    /// let r = vec![("1", vec!["1;X", "1;Y"]), ("2", vec!["2;Z"])].into_iter();
    /// let mut it = l.merge_join_left_excl_by(r, |x, y| Ord::cmp(&x.0, &y.0));
    ///
    /// assert_eq!(it.next(), Some(("0", vec!["0;A"])));
    /// assert_eq!(it.next(), None);
    /// ```
    fn merge_join_left_excl_by<R, F>(self, other: R, cmp: F) 
                                        -> MergeJoinLeftExcl<Self, R::IntoIter, F> 
        where Self: Sized,
              R: IntoIterator,
              F: FnMut(&Self::Item, &R::Item) -> Ordering
    {
        MergeJoinLeftExcl::new(self, other.into_iter(), cmp)
    }

    /// Return an iterator adaptor that [left outer
    /// joins](https://en.wikipedia.org/wiki/Join_%28SQL%29#Left_outer_join) the two input iterators
    /// in ascending order. The resulting iterator contains all the records from the left input
    /// iterator, even if they do not match the right input iterator.
    ///
    /// The both input iterators must be sorted and unique on the join key (e.g. by
    /// [grouping](http://bluss.github.io/rust-itertools/doc/itertools/trait.Itertools.html#method.group_by)
    /// them, if necessary) to produce the correct results.
    ///
    /// Iterator element type is [`EitherOrBoth<L::Item, R::Item>`](enum.EitherOrBoth.html).
    ///
    /// ```
    /// use joinkit::Joinkit;
    /// use joinkit::EitherOrBoth::{Left, Both, Right};
    ///
    /// // tuples of (key, [value,...]), where the key is extracted from the value
    /// // notice the values are grouped by the key
    /// let l = vec![("0", vec!["0;A"]), ("1", vec!["1;B"])].into_iter();
    /// let r = vec![("1", vec!["1;X", "1;Y"]), ("2", vec!["2;Z"])].into_iter();
    /// let mut it = l.merge_join_left_outer_by(r, |x, y| Ord::cmp(&x.0, &y.0));
    ///
    /// assert_eq!(it.next(), Some(Left(("0", vec!["0;A"]))));
    /// assert_eq!(it.next(), Some(Both(("1", vec!["1;B"]), ("1", vec!["1;X", "1;Y"]))));
    /// assert_eq!(it.next(), None);
    /// ```
    fn merge_join_left_outer_by<R, F>(self, other: R, cmp: F) 
                                         -> MergeJoinLeftOuter<Self, R::IntoIter, F> 
        where Self: Sized,
              R: IntoIterator,
              F: FnMut(&Self::Item, &R::Item) -> Ordering
    {
        MergeJoinLeftOuter::new(self, other.into_iter(), cmp)
    }

    /// Return an iterator adaptor that [full outer
    /// joins](https://en.wikipedia.org/wiki/Join_%28SQL%29#Full_outer_join) the two input iterators
    /// in ascending order. The resulting iterator contains all the records from the both input
    /// iterators.
    ///
    /// The both input iterators must be sorted and unique on the join key (e.g. by
    /// [grouping](http://bluss.github.io/rust-itertools/doc/itertools/trait.Itertools.html#method.group_by)
    /// them, if necessary) to produce the correct results.
    ///
    /// Iterator element type is [`EitherOrBoth<L::Item, R::Item>`](enum.EitherOrBoth.html).
    ///
    /// ```
    /// use joinkit::Joinkit;
    /// use joinkit::EitherOrBoth::{Left, Both, Right};
    ///
    ///
    /// // tuples of (key, [value,...]), where the key is extracted from the value
    /// // notice the values are grouped by the key
    /// let l = vec![("0",vec!["0;A"]), ("1", vec!["1;B"])].into_iter();
    /// let r = vec![("1",vec!["1;X", "1;Y"]), ("2", vec!["2;Z"])].into_iter();
    /// let mut it = l.merge_join_full_outer_by(r, |x, y| Ord::cmp(&x.0, &y.0));
    ///
    /// assert_eq!(it.next(), Some(Left(("0", vec!["0;A"]))));
    /// assert_eq!(it.next(), Some(Both(("1", vec!["1;B"]), ("1", vec!["1;X", "1;Y"]))));
    /// assert_eq!(it.next(), Some(Right(("2", vec!["2;Z"]))));
    /// assert_eq!(it.next(), None);
    /// ```
    fn merge_join_full_outer_by<R, F>(self, other: R, cmp: F) 
                                         -> MergeJoinFullOuter<Self, R::IntoIter, F> 
        where Self: Sized,
              R: IntoIterator,
              F: FnMut(&Self::Item, &R::Item) -> Ordering
    {
        MergeJoinFullOuter::new(self, other.into_iter(), cmp)
    }

    /// Return an iterator adaptor that [inner
    /// joins](https://en.wikipedia.org/wiki/Join_%28SQL%29#Inner_join) the two input iterators in
    /// ascending order. The resulting iterator is the intersection of the two input iterators.
    ///
    /// The input iterators do *not* need to be sorted. The right input iterator is loaded into
    /// `HashMap` and grouped by the key automatically. Neither the left input iterator need to be
    /// unique on the key.
    ///
    /// The left input iterator element type must be `(K, LV)`, where `K: Hash + Eq`. 
    /// The right input iterator element type must be `(K, RV)`, where `K: Hash + Eq` and `RV:
    /// Clone`.
    ///
    /// When the join adaptor is created, the right iterator is **consumed** into `HashMap`.
    ///
    /// Iterator element type is `(LV, vec![RV,...])`. 
    /// The `RV` is cloned from `HashMap` for each joined value. A single `RV` can be expected to
    /// be joined (and cloned) multiple times to `LV`. To increase performance, consider wrapping
    /// `RV` into `std::rc::Rc` pointer to avoid unnecessary allocations.
    ///
    /// ```
    /// use joinkit::Joinkit;
    ///
    /// // tuples of (key, value), where the key is extracted from the value
    /// let l = vec![("0", "0;A"), ("1", "1;B")].into_iter();
    /// let r = vec![("1", "1;X"), ("2", "2;Z"), ("1", "1;Y")].into_iter();
    /// let mut it = l.hash_join_inner(r);
    ///
    /// // notice the grouped right values
    /// assert_eq!(it.next(), Some(("1;B", vec!["1;X", "1;Y"])));
    /// assert_eq!(it.next(), None);
    /// ```
    fn hash_join_inner<K, RI, RV>(self, other: RI) -> HashJoinInner<Self, K, RV> 
        where Self: Sized,
              K: Hash + Eq,
              RV: Clone,
              RI: IntoIterator<Item=(K, RV)>
    {
        HashJoinInner::new(self, other)
    }

    /// Return an iterator adaptor that *left exclusive joins* the two input iterators. The
    /// resulting iterator contains only those records from the left input iterator, which do not
    /// match the right input iterator. There is no direct equivalent in SQL.
    ///
    /// The input iterators do *not* need to be sorted. The right input iterator is loaded into
    /// `HashMap` and grouped by the key automatically. Neither the left input iterator need to be
    /// unique on the key.
    ///
    /// The left input iterator element type must be `(K, LV)`, where `K: Hash + Eq`. 
    /// The right input iterator element type must be `(K, RV)`, where `K: Hash + Eq`.
    ///
    /// When the join adaptor is created, the right iterator is **consumed** into `HashMap`.
    ///
    /// Iterator element type is `LV`.
    ///
    /// ```
    /// use joinkit::Joinkit;
    ///
    /// // tuples of (key, value), where the key is extracted from the value
    /// let l = vec![("0", "0;A"), ("1", "1;B")].into_iter();
    /// let r = vec![("1", "1;X"), ("2", "2;Z"), ("1", "1;Y")].into_iter();
    /// let mut it = l.hash_join_left_excl(r);
    ///
    /// assert_eq!(it.next(), Some("0;A"));
    /// assert_eq!(it.next(), None);
    /// ```
    fn hash_join_left_excl<K, RI, RV>(self, other: RI) -> HashJoinLeftExcl<Self, K> 
        where Self: Sized,
              K: Hash + Eq,
              RI: IntoIterator<Item=(K, RV)>
    {
        HashJoinLeftExcl::new(self, other)
    }

    /// Return an iterator adaptor that [left outer
    /// joins](https://en.wikipedia.org/wiki/Join_%28SQL%29#Left_outer_join) the two input
    /// iterators.  The resulting iterator contains all the records from the left input iterator,
    /// even if they do not match the right input iterator.
    ///
    /// The input iterators do *not* need to be sorted. The right input iterator is loaded into
    /// `HashMap` and grouped by the key automatically. Neither the left input iterator need to be
    /// unique on the key.
    ///
    /// The left input iterator element type must be `(K, LV)`, where `K: Hash + Eq`. 
    /// The right input iterator element type must be `(K, RV)`, where `K: Hash + Eq` and `RV:
    /// Clone`.
    ///
    /// When the join adaptor is created, the right iterator is **consumed** into `HashMap`.
    ///
    /// Iterator element type is [`EitherOrBoth<LV, RV>`](enum.EitherOrBoth.html).
    /// The `RV` is cloned from `HashMap` for each joined value. It is expected a single `RV` will
    /// be joined (and cloned) multiple times to `LV`. To increase performance, consider wrapping
    /// `RV` into `std::rc::Rc` pointer to avoid unnecessary allocations.
    ///
    /// ```
    /// use joinkit::Joinkit;
    /// use joinkit::EitherOrBoth::{Left, Both, Right};
    ///
    /// // tuples of (key, value), where the key is extracted from the value
    /// let l = vec![("0", "0;A"), ("1", "1;B")].into_iter();
    /// let r = vec![("1", "1;X"), ("2", "2;Z"), ("1", "1;Y")].into_iter();
    /// let mut it = l.hash_join_left_outer(r);
    ///
    /// // notice the grouped right values
    /// assert_eq!(it.next(), Some(Left("0;A")));
    /// assert_eq!(it.next(), Some(Both("1;B", vec!["1;X", "1;Y"])));
    /// assert_eq!(it.next(), None);
    /// ```
    fn hash_join_left_outer<K, RI, RV>(self, other: RI) -> HashJoinLeftOuter<Self, K, RV> 
        where Self: Sized,
              K: Hash + Eq,
              RV: Clone,
              RI: IntoIterator<Item=(K, RV)>
    {
        HashJoinLeftOuter::new(self, other)
    }

    /// Return an iterator adaptor that *right exclusive joins* the two input iterators. The resulting
    /// iterator contains only those records from the right input iterator, which do not match the
    /// left input iterator. There is no direct equivalent in SQL.
    ///
    /// The input iterators do *not* need to be sorted. The right input iterator is loaded into
    /// `HashMap` and grouped by the key automatically. Neither the left input iterator need to be
    /// unique on the key.
    ///
    /// The left input iterator element type must be `(K, LV)`, where `K: Hash + Eq`. 
    /// The right input iterator element type must be `(K, RV)`, where `K: Hash + Eq`.
    ///
    /// When the join adaptor is created, the right iterator is **consumed** into `HashMap`.
    ///
    /// Iterator element type is `vec![RV,...]`.
    ///
    /// ```
    /// use joinkit::Joinkit;
    ///
    /// // tuples of (key, value), where the key is extracted from the value
    /// let l = vec![("0", "0;A"), ("1", "1;B")].into_iter();
    /// let r = vec![("1", "1;X"), ("2", "2;Z"), ("1", "1;Y")].into_iter();
    /// let mut it = l.hash_join_right_excl(r);
    ///
    /// assert_eq!(it.next(), Some(vec!["2;Z"]));
    /// assert_eq!(it.next(), None);
    /// ```
    fn hash_join_right_excl<K, RI, RV>(self, other: RI) -> HashJoinRightExcl<Self, K, RV> 
        where Self: Sized,
              K: Hash + Eq,
              RI: IntoIterator<Item=(K, RV)>
    {
        HashJoinRightExcl::new(self, other)
    }

    /// Return an iterator adaptor that [right outer
    /// joins](https://en.wikipedia.org/wiki/Join_%28SQL%29#Right_outer_join) the two input
    /// iterators.  The resulting iterator contains all the records from the right input iterator,
    /// even if they do not match the left input iterator.
    ///
    /// The input iterators do *not* need to be sorted. The right input iterator is loaded into
    /// `HashMap` and grouped by the key automatically. Neither the left input iterator need to be
    /// unique on the key.
    ///
    /// The left input iterator element type must be `(K, LV)`, where `K: Hash + Eq`. 
    /// The right input iterator element type must be `(K, RV)`, where `K: Hash + Eq` and `RV:
    /// Clone`.
    ///
    /// When the join adaptor is created, the right iterator is **consumed** into `HashMap`.
    ///
    /// Iterator element type is [`EitherOrBoth<LV, RV>`](enum.EitherOrBoth.html).
    /// The `RV` is cloned from `HashMap` for each joined value. It is expected a single `RV` will
    /// be joined (and cloned) multiple times to `LV`. To increase performance, consider wrapping
    /// `RV` into `std::rc::Rc` pointer to avoid unnecessary allocations.
    ///
    /// ```
    /// use joinkit::Joinkit;
    /// use joinkit::EitherOrBoth::{Left, Both, Right};
    ///
    /// // tuples of (key, value), where the key is extracted from the value
    /// let l = vec![("0", "0;A"), ("1", "1;B")].into_iter();
    /// let r = vec![("1", "1;X"), ("2", "2;Z"), ("1", "1;Y")].into_iter();
    /// let mut it = l.hash_join_right_outer(r);
    ///
    /// // notice the grouped right values
    /// assert_eq!(it.next(), Some(Both("1;B", vec!["1;X", "1;Y"])));
    /// assert_eq!(it.next(), Some(Right(vec!["2;Z"])));
    /// assert_eq!(it.next(), None);
    /// ```
    fn hash_join_right_outer<K, RI, RV>(self, other: RI) -> HashJoinRightOuter<Self, K, RV> 
        where Self: Sized,
              K: Hash + Eq,
              RV: Clone,
              RI: IntoIterator<Item=(K, RV)>
    {
        HashJoinRightOuter::new(self, other)
    }

    /// Return an iterator adaptor that [full outer
    /// joins](https://en.wikipedia.org/wiki/Join_%28SQL%29#Full_outer_join) the two input
    /// iterators.  The resulting iterator contains all the records from the both input iterators.
    ///
    /// The input iterators do *not* need to be sorted. The right input iterator is loaded into
    /// `HashMap` and grouped by the key automatically. Neither the left input iterator need to be
    /// unique on the key.
    ///
    /// The left input iterator element type must be `(K, LV)`, where `K: Hash + Eq`. 
    /// The right input iterator element type must be `(K, RV)`, where `K: Hash + Eq` and `RV:
    /// Clone`.
    ///
    /// When the join adaptor is created, the right iterator is **consumed** into `HashMap`.
    ///
    /// Iterator element type is [`EitherOrBoth<LV, RV>`](enum.EitherOrBoth.html).
    /// The `RV` is cloned from `HashMap` for each joined value. It is expected a single `RV` will
    /// be joined (and cloned) multiple times to `LV`. To increase performance, consider wrapping
    /// `RV` into `std::rc::Rc` pointer to avoid unnecessary allocations.
    ///
    /// ```
    /// use joinkit::Joinkit;
    /// use joinkit::EitherOrBoth::{Left, Both, Right};
    ///
    /// // tuples of (key, value), where the key is extracted from the value
    /// let l = vec![("0", "0;A"), ("1", "1;B")].into_iter();
    /// let r = vec![("1", "1;X"), ("2", "2;Z"), ("1", "1;Y")].into_iter();
    /// let mut it = l.hash_join_full_outer(r);
    ///
    /// // notice the grouped right values
    /// assert_eq!(it.next(), Some(Left("0;A")));
    /// assert_eq!(it.next(), Some(Both("1;B", vec!["1;X", "1;Y"])));
    /// assert_eq!(it.next(), Some(Right(vec!["2;Z"])));
    /// assert_eq!(it.next(), None);
    /// ```
    fn hash_join_full_outer<K, RI, RV>(self, other: RI) -> HashJoinFullOuter<Self, K, RV> 
        where Self: Sized,
              K: Hash + Eq,
              RV: Clone,
              RI: IntoIterator<Item=(K, RV)>
    {
        HashJoinFullOuter::new(self, other)
    }
}

impl<T: ?Sized> Joinkit for T where T: Iterator { }
