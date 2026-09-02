//! Dynamic value stored in [`WorkItemArguments`] and [`WorkResult`].
//!
//! Python stores `dict[str, Any]`, which happily holds both JSON-friendly data
//! (strings, numbers, nested dicts) and live objects such as a nested
//! [`RoutingSlip`] -- [`FallbackActivity`] and [`ParallelActivity`] rely on the
//! latter. Rust has no `Any`-typed literal, so the same duality is modelled
//! explicitly:
//!
//! * [`Value::Json`] -- serializable data, backed by [`serde_json::Value`];
//! * [`Value::Any`] -- an opaque object, kept alive by an [`Arc`] and recovered
//!   by downcasting.
//!
//! Only [`Value::Json`] survives serialization: attempting to marshal a
//! [`Value::Any`] fails, exactly as `json.dumps()` fails on a `RoutingSlip`.
//!
//! [`WorkItemArguments`]: crate::work_item_arguments::WorkItemArguments
//! [`WorkResult`]: crate::work_result::WorkResult
//! [`RoutingSlip`]: crate::routing_slip::RoutingSlip
//! [`FallbackActivity`]: crate::fallback_activity::FallbackActivity
//! [`ParallelActivity`]: crate::parallel_activity::ParallelActivity

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use serde::de::{Deserialize, Deserializer};
use serde::ser::{Error as _, Serialize, Serializer};

/// A value carried by a work item's arguments or a work log's result.
#[derive(Clone)]
pub enum Value {
    /// Serializable data.
    Json(serde_json::Value),
    /// An opaque object that never crosses the wire.
    Any(Arc<dyn Any + Send + Sync>),
}

impl Value {
    /// Wraps an arbitrary object as an opaque value.
    ///
    /// ```
    /// use ascetic_ddd_saga::Value;
    ///
    /// let value = Value::any(vec![1_u8, 2, 3]);
    /// assert_eq!(value.downcast_ref::<Vec<u8>>(), Some(&vec![1, 2, 3]));
    /// ```
    pub fn any<T: Any + Send + Sync>(value: T) -> Self {
        Value::Any(Arc::new(value))
    }

    /// Returns the wrapped JSON data, if this is a [`Value::Json`].
    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            Value::Json(value) => Some(value),
            Value::Any(_) => None,
        }
    }

    /// Returns the value as a string slice, if it holds a JSON string.
    pub fn as_str(&self) -> Option<&str> {
        self.as_json().and_then(serde_json::Value::as_str)
    }

    /// Returns the value as an integer, if it holds a JSON integer.
    pub fn as_i64(&self) -> Option<i64> {
        self.as_json().and_then(serde_json::Value::as_i64)
    }

    /// Returns the value as a float, if it holds a JSON number.
    pub fn as_f64(&self) -> Option<f64> {
        self.as_json().and_then(serde_json::Value::as_f64)
    }

    /// Returns the value as a boolean, if it holds a JSON boolean.
    pub fn as_bool(&self) -> Option<bool> {
        self.as_json().and_then(serde_json::Value::as_bool)
    }

    /// Borrows the wrapped object as `T`, if this is a [`Value::Any`] holding one.
    pub fn downcast_ref<T: Any + Send + Sync>(&self) -> Option<&T> {
        match self {
            Value::Any(value) => value.downcast_ref::<T>(),
            Value::Json(_) => None,
        }
    }

    /// True if the value can be serialized.
    pub fn is_serializable(&self) -> bool {
        matches!(self, Value::Json(_))
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Json(value) => fmt::Debug::fmt(value, f),
            Value::Any(_) => f.write_str("Any(<opaque>)"),
        }
    }
}

/// Two opaque values are equal only when they point at the same object,
/// mirroring Python's identity comparison for objects without `__eq__`.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Json(left), Value::Json(right)) => left == right,
            (Value::Any(left), Value::Any(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Value::Json(value) => value.serialize(serializer),
            Value::Any(_) => Err(S::Error::custom(
                "an opaque Value::Any cannot be serialized",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        serde_json::Value::deserialize(deserializer).map(Value::Json)
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        Value::Json(value)
    }
}

macro_rules! impl_from_into_json {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for Value {
                fn from(value: $ty) -> Self {
                    Value::Json(serde_json::Value::from(value))
                }
            }
        )*
    };
}

impl_from_into_json!(
    bool, i8, i16, i32, i64, isize, u8, u16, u32, u64, usize, f32, f64, &str, String
);

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(values: Vec<T>) -> Self {
        let values: Vec<Value> = values.into_iter().map(Into::into).collect();
        // Nested opaque values would be lost silently, so keep them opaque as a whole.
        if values.iter().all(Value::is_serializable) {
            Value::Json(serde_json::Value::Array(
                values
                    .into_iter()
                    .map(|value| value.as_json().cloned().unwrap_or(serde_json::Value::Null))
                    .collect(),
            ))
        } else {
            Value::any(values)
        }
    }
}

/// Generates a `dict[str, Any]`-like newtype.
///
/// Python declares `WorkItemArguments` and `WorkResult` as two empty `dict`
/// subclasses; this macro is the Rust equivalent of that duplication.
macro_rules! dict_newtype {
    (
        $(#[$meta:meta])*
        $name:ident
    ) => {
        $(#[$meta])*
        #[derive(Clone, Default, PartialEq, ::serde::Serialize, ::serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(::std::collections::BTreeMap<::std::string::String, $crate::value::Value>);

        impl $name {
            /// Creates an empty instance.
            pub fn new() -> Self {
                Self::default()
            }

            /// Inserts a value, returning the previous one under this key.
            pub fn insert(
                &mut self,
                key: impl Into<::std::string::String>,
                value: impl Into<$crate::value::Value>,
            ) -> ::std::option::Option<$crate::value::Value> {
                self.0.insert(key.into(), value.into())
            }

            /// Returns the value stored under `key`, or an error if it is absent.
            ///
            /// The counterpart of Python's `arguments["key"]`, which raises
            /// `KeyError` for a missing key.
            pub fn require(&self, key: &str) -> $crate::error::Result<&$crate::value::Value> {
                self.0
                    .get(key)
                    .ok_or_else(|| $crate::error::SagaError::missing_key(key))
            }

            /// Returns the string stored under `key`.
            pub fn get_str(&self, key: &str) -> $crate::error::Result<&str> {
                self.require(key)?.as_str().ok_or_else(|| {
                    $crate::error::SagaError::UnexpectedType {
                        key: key.to_owned(),
                        expected: "string",
                    }
                })
            }

            /// Returns the integer stored under `key`.
            pub fn get_i64(&self, key: &str) -> $crate::error::Result<i64> {
                self.require(key)?.as_i64().ok_or_else(|| {
                    $crate::error::SagaError::UnexpectedType {
                        key: key.to_owned(),
                        expected: "integer",
                    }
                })
            }

            /// Returns the boolean stored under `key`.
            pub fn get_bool(&self, key: &str) -> $crate::error::Result<bool> {
                self.require(key)?.as_bool().ok_or_else(|| {
                    $crate::error::SagaError::UnexpectedType {
                        key: key.to_owned(),
                        expected: "boolean",
                    }
                })
            }

            /// Borrows the opaque object stored under `key` as `T`.
            pub fn get_any<T: ::std::any::Any + Send + Sync>(
                &self,
                key: &str,
            ) -> $crate::error::Result<&T> {
                self.require(key)?.downcast_ref::<T>().ok_or_else(|| {
                    $crate::error::SagaError::UnexpectedType {
                        key: key.to_owned(),
                        expected: ::std::any::type_name::<T>(),
                    }
                })
            }

            /// Borrows the underlying map.
            pub fn as_map(
                &self,
            ) -> &::std::collections::BTreeMap<::std::string::String, $crate::value::Value> {
                &self.0
            }

            /// Mutably borrows the underlying map.
            pub fn as_map_mut(
                &mut self,
            ) -> &mut ::std::collections::BTreeMap<::std::string::String, $crate::value::Value> {
                &mut self.0
            }

            /// Consumes the wrapper and returns the underlying map.
            pub fn into_map(
                self,
            ) -> ::std::collections::BTreeMap<::std::string::String, $crate::value::Value> {
                self.0
            }
        }

        impl ::std::ops::Deref for $name {
            type Target =
                ::std::collections::BTreeMap<::std::string::String, $crate::value::Value>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl ::std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl ::std::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Debug::fmt(&self.0, f)
            }
        }

        impl
            ::std::convert::From<
                ::std::collections::BTreeMap<::std::string::String, $crate::value::Value>,
            > for $name
        {
            fn from(
                map: ::std::collections::BTreeMap<::std::string::String, $crate::value::Value>,
            ) -> Self {
                Self(map)
            }
        }

        impl<K, V, const N: usize> ::std::convert::From<[(K, V); N]> for $name
        where
            K: ::std::convert::Into<::std::string::String>,
            V: ::std::convert::Into<$crate::value::Value>,
        {
            fn from(entries: [(K, V); N]) -> Self {
                entries.into_iter().collect()
            }
        }

        impl<K, V> ::std::iter::FromIterator<(K, V)> for $name
        where
            K: ::std::convert::Into<::std::string::String>,
            V: ::std::convert::Into<$crate::value::Value>,
        {
            fn from_iter<I: ::std::iter::IntoIterator<Item = (K, V)>>(iterable: I) -> Self {
                Self(
                    iterable
                        .into_iter()
                        .map(|(key, value)| (key.into(), value.into()))
                        .collect(),
                )
            }
        }

        impl ::std::iter::IntoIterator for $name {
            type Item = (::std::string::String, $crate::value::Value);
            type IntoIter = ::std::collections::btree_map::IntoIter<
                ::std::string::String,
                $crate::value::Value,
            >;

            fn into_iter(self) -> Self::IntoIter {
                self.0.into_iter()
            }
        }

        impl<'a> ::std::iter::IntoIterator for &'a $name {
            type Item = (&'a ::std::string::String, &'a $crate::value::Value);
            type IntoIter = ::std::collections::btree_map::Iter<
                'a,
                ::std::string::String,
                $crate::value::Value,
            >;

            fn into_iter(self) -> Self::IntoIter {
                self.0.iter()
            }
        }
    };
}

pub(crate) use dict_newtype;
