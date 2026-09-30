//! [`Value`] as a parameter of `tokio-postgres`, so that the parameters of
//! a compiled [`Query`](super::Query) go to the driver as they are.
//!
//! Python's and Go's drivers take a value of any type and see what the
//! server wants of it. Rust's asks the value; this is `Value`'s answer. The
//! server has inferred the parameter's type from the query — `age >= $1`
//! makes `$1` whatever `age` is — and the value is written as that type if
//! it is of a kind that can be: an integer as any integer or float type it
//! fits, a float as a float, a text as any text type, a point in time as a
//! timestamp with or without zone, a date as a date, a span as an interval.
//! `numeric` is not
//! among them: its wire format is a decimal expansion this crate has no
//! other use for; compare a `numeric` column with `$1::bigint` or
//! `$1::float8`, or map the value in the application.
//!
//! Where the query gives the server nothing to infer a type from — every
//! operand of an operator a constant — the compiler says the type in the
//! text, and the value is written as that: see `param_type`.
//!
//! A text the server takes for a point in time, a date or a UUID — `"at" >
//! $1` with `$1` a string — is read here as the evaluator reads it
//! (ADR-0015): Python's and Go's drivers hand the text to the server, which
//! reads it by the column; this driver asks the value, so the value reads the
//! text itself, in the same subset. What the server would read beyond it,
//! `'yesterday'`, is an error here as it is in memory.

use std::error::Error;

use bytes::BytesMut;
use postgres_types::{IsNull, ToSql, Type, to_sql_checked};

use crate::domain::operand::Operand;
use crate::domain::reading;
use crate::domain::value::{Date, Value};

/// Microseconds from the Unix epoch to PostgreSQL's, 2000-01-01.
const EPOCH: i64 = 946_684_800_000_000;

/// Days from the Unix epoch to PostgreSQL's.
const EPOCH_DAYS: i32 = 10_957;

type Written = Result<IsNull, Box<dyn Error + Sync + Send>>;

/// Microseconds from PostgreSQL's epoch, as a timestamp is written.
fn micros(since_unix: i64, out: &mut BytesMut) -> Written {
    let micros = since_unix
        .checked_sub(EPOCH)
        .ok_or_else(|| -> Box<dyn Error + Sync + Send> { "a timestamp out of range".into() })?;
    out.extend_from_slice(&micros.to_be_bytes());
    Ok(IsNull::No)
}

/// Days from PostgreSQL's epoch, as a date is written.
fn days(date: Date, out: &mut BytesMut) -> Written {
    let days = date
        .as_days()
        .checked_sub(EPOCH_DAYS)
        .ok_or_else(|| -> Box<dyn Error + Sync + Send> { "a date out of range".into() })?;
    out.extend_from_slice(&days.to_be_bytes());
    Ok(IsNull::No)
}

/// A text that is not of the form `kind` is read in.
fn unreadable(text: &str, kind: &str, form: &str) -> Box<dyn Error + Sync + Send> {
    format!("'{text}' is not a {kind}: {form}").into()
}

impl ToSql for Value {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Written {
        let mismatch = || -> Box<dyn Error + Sync + Send> {
            format!("cannot write {} as {ty}", self.kind()).into()
        };
        match self {
            Value::Null => Ok(IsNull::Yes),
            Value::Bool(value) if *ty == Type::BOOL => value.to_sql(ty, out),
            Value::Int(value) if *ty == Type::INT8 => value.to_sql(ty, out),
            Value::Int(value) if *ty == Type::INT4 => i32::try_from(*value)
                .map_err(|_| mismatch())?
                .to_sql(ty, out),
            Value::Int(value) if *ty == Type::INT2 => i16::try_from(*value)
                .map_err(|_| mismatch())?
                .to_sql(ty, out),
            Value::Int(value) if *ty == Type::FLOAT8 => (*value as f64).to_sql(ty, out),
            Value::Int(value) if *ty == Type::FLOAT4 => (*value as f32).to_sql(ty, out),
            Value::Float(value) if *ty == Type::FLOAT8 => value.to_sql(ty, out),
            Value::Float(value) if *ty == Type::FLOAT4 => (*value as f32).to_sql(ty, out),
            Value::Text(value) if <&str as ToSql>::accepts(ty) => value.as_str().to_sql(ty, out),
            Value::Timestamp(value) if *ty == Type::TIMESTAMP || *ty == Type::TIMESTAMPTZ => {
                micros(value.as_micros(), out)
            }
            Value::Date(value) if *ty == Type::DATE => days(*value, out),
            Value::Text(text) if *ty == Type::TIMESTAMPTZ => {
                let moment = reading::point_in_time(text)
                    .ok_or_else(|| unreadable(text, "timestamptz", reading::POINT_IN_TIME))?;
                micros(moment.as_micros(), out)
            }
            Value::Text(text) if *ty == Type::TIMESTAMP => {
                let moment = reading::without_zone(text)
                    .ok_or_else(|| unreadable(text, "timestamp", reading::POINT_IN_TIME))?;
                micros(moment.as_micros(), out)
            }
            Value::Text(text) if *ty == Type::DATE => {
                let date = reading::calendar_date(text)
                    .ok_or_else(|| unreadable(text, "date", reading::POINT_IN_TIME))?;
                days(date, out)
            }
            Value::Text(text) if *ty == Type::UUID => {
                let id =
                    reading::uuid(text).ok_or_else(|| unreadable(text, "uuid", reading::UUID))?;
                out.extend_from_slice(id.as_bytes());
                Ok(IsNull::No)
            }
            Value::Interval(value) if *ty == Type::INTERVAL => {
                // Microseconds, then days, then months.
                out.extend_from_slice(&value.as_micros().to_be_bytes());
                out.extend_from_slice(&[0; 8]);
                Ok(IsNull::No)
            }
            Value::Uuid(value) if *ty == Type::UUID => {
                out.extend_from_slice(value.as_bytes());
                Ok(IsNull::No)
            }
            _ => Err(mismatch()),
        }
    }

    /// Whether a value fits depends on the value, not on `Value`: `to_sql`
    /// says.
    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}
