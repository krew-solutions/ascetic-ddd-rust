//! The failure track: a list of errors that is never empty.

use alloc::vec::Vec;
use core::fmt;

/// One or more errors.
///
/// The OCaml source keeps `'err list` and a comment that it is never empty.
/// Here that is the type: an `Errors` is built from an error, grows by errors,
/// and merges with another `Errors`, and none of those can leave it empty.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Errors<E> {
    head: E,
    tail: Vec<E>,
}

impl<E> Errors<E> {
    /// A single error.
    pub fn new(error: E) -> Self {
        Errors {
            head: error,
            tail: Vec::new(),
        }
    }

    /// The errors of a list, if there are any.
    pub fn from_vec(errors: Vec<E>) -> Option<Self> {
        let mut errors = errors.into_iter();
        errors.next().map(|head| Errors {
            head,
            tail: errors.collect(),
        })
    }

    /// The first error.
    pub fn first(&self) -> &E {
        &self.head
    }

    /// How many errors there are; never zero.
    pub fn count(&self) -> usize {
        1 + self.tail.len()
    }

    /// The errors, in the order they were reported.
    pub fn iter(&self) -> impl Iterator<Item = &E> {
        core::iter::once(&self.head).chain(self.tail.iter())
    }

    /// These errors followed by one more.
    pub fn push(self, error: E) -> Self {
        Errors {
            head: self.head,
            tail: self
                .tail
                .into_iter()
                .chain(core::iter::once(error))
                .collect(),
        }
    }

    /// These errors followed by those. What `apply`, `both` and `plus` do on
    /// two failures.
    pub fn merge(self, other: Self) -> Self {
        Errors {
            head: self.head,
            tail: self.tail.into_iter().chain(other).collect(),
        }
    }

    /// The same errors, each transformed. Wlaschin's `doubleMap` on the failure
    /// track.
    pub fn map<F, U>(self, mut f: F) -> Errors<U>
    where
        F: FnMut(E) -> U,
    {
        Errors {
            head: f(self.head),
            tail: self.tail.into_iter().map(f).collect(),
        }
    }

    /// The errors as a plain list.
    pub fn into_vec(self) -> Vec<E> {
        self.into_iter().collect()
    }
}

/// A single error is one error. This is what lets `?` lift a plain `Result`
/// into a [`Rop`][crate::Rop] — OCaml's `of_result`.
impl<E> From<E> for Errors<E> {
    fn from(error: E) -> Self {
        Errors::new(error)
    }
}

impl<E> IntoIterator for Errors<E> {
    type Item = E;
    type IntoIter = core::iter::Chain<core::iter::Once<E>, alloc::vec::IntoIter<E>>;

    fn into_iter(self) -> Self::IntoIter {
        core::iter::once(self.head).chain(self.tail)
    }
}

impl<'a, E> IntoIterator for &'a Errors<E> {
    type Item = &'a E;
    type IntoIter = core::iter::Chain<core::iter::Once<&'a E>, core::slice::Iter<'a, E>>;

    fn into_iter(self) -> Self::IntoIter {
        core::iter::once(&self.head).chain(self.tail.iter())
    }
}

/// The errors joined by `; `, as Wlaschin's `&&&` joins them.
impl<E: fmt::Display> fmt::Display for Errors<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.head)?;
        for error in &self.tail {
            write!(f, "; {error}")?;
        }
        Ok(())
    }
}

impl<E: fmt::Debug + fmt::Display> core::error::Error for Errors<E> {}
