//! [`Value`] as a parameter of `tokio-postgres`, so that the parameters of
//! a compiled [`Query`](super::Query) go to the driver as they are.
//!
//! Python's and Go's drivers take a value of any type and see what the
//! server wants of it. Rust's asks the value; this is `Value`'s answer. The
//! server has inferred the parameter's type from the query — `age >= $1`
//! makes `$1` whatever `age` is — and the value is written as that type if
//! it is of a kind that can be: an integer as any integer or float type it
//! fits, a float as a float, a text as any text type, a point in time as a
//! timestamp with or without zone, a span as an interval. `numeric` is not
//! among them: its wire format is a decimal expansion this crate has no
//! other use for; compare a `numeric` column with `$1::bigint` or
//! `$1::float8`, or map the value in the application.
//!
//! Where the query gives the server nothing to infer a type from — every
//! operand of an operator a constant — the compiler says the type in the
//! text, and the value is written as that: see `param_type`.

use std::error::Error;

use bytes::BytesMut;
use postgres_types::{IsNull, ToSql, Type, to_sql_checked};

use crate::domain::operand::Operand;
use crate::domain::value::Value;

/// Microseconds from the Unix epoch to PostgreSQL's, 2000-01-01.
const EPOCH: i64 = 946_684_800_000_000;

type Written = Result<IsNull, Box<dyn Error + Sync + Send>>;

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
                let micros = value.as_micros().checked_sub(EPOCH).ok_or_else(mismatch)?;
                out.extend_from_slice(&micros.to_be_bytes());
                Ok(IsNull::No)
            }
            Value::Interval(value) if *ty == Type::INTERVAL => {
                // Microseconds, then days, then months.
                out.extend_from_slice(&value.as_micros().to_be_bytes());
                out.extend_from_slice(&[0; 8]);
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
