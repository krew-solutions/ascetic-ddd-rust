//! A string constant read as the kind of the value beside it.
//!
//! A template has the literals of RFC 9535 — a string, a number, `true`,
//! `false`, `null` — and no others: a point in time or a UUID in a template
//! is a string, `@.created_at > '2026-09-01'`. PostgreSQL reads an untyped
//! parameter by the type of the column beside it, and so does the evaluator:
//! a string compared with a value of a kind that has no literal of its own
//! is read as that kind first, [`Operand::read_beside`](super::operand::Operand::read_beside).
//! What is read is a subset of what the server reads, so that nothing the
//! evaluator accepts fails on the server. A string beside a number or a
//! boolean stays a string: those have literals, and a string there is the
//! author's choice. ADR-0015.

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, TimeZone};

use super::value::{Date, Timestamp};

/// The form a point in time is read in: ISO 8601, a date, or a date followed
/// by `T` or a space and a time of hours and minutes, seconds, a fraction of
/// up to six digits, then nothing, `Z`, or an offset. The server reads more —
/// `20260901`, `Sep 1 2026`, `yesterday` — and those are an error here.
pub(crate) const POINT_IN_TIME: &str =
    "YYYY-MM-DD, or that with THH:MM[:SS[.ffffff]] and Z or an offset";

/// The form a UUID is read in: the canonical one, in either case. The server
/// takes it without hyphens and in braces as well; here those are an error.
pub(crate) const UUID: &str = "8-4-4-4-12 hexadecimal digits";

/// `text` as a point in time, with its offset applied, in UTC without one;
/// `None` if it is not of the form.
pub(crate) fn point_in_time(text: &str) -> Option<Timestamp> {
    let (moment, offset) = parse(text)?;
    let offset = FixedOffset::east_opt(offset)?;
    let moment = offset.from_local_datetime(&moment).single()?;
    Some(Timestamp::from_micros(moment.timestamp_micros()))
}

/// `text` as a point in time with its offset dropped: what the server makes
/// of it for a `timestamp` without time zone.
pub(crate) fn without_zone(text: &str) -> Option<Timestamp> {
    let (moment, _) = parse(text)?;
    Some(Timestamp::from_micros(moment.and_utc().timestamp_micros()))
}

/// The date `text` starts with, whatever time and offset follow: what the
/// server makes of it for a `date`.
pub(crate) fn calendar_date(text: &str) -> Option<Date> {
    parse(text).map(|(moment, _)| Date::from(moment.date()))
}

/// The date and time of day `text` spells, and its offset in seconds - zero
/// for `Z` and for none.
fn parse(text: &str) -> Option<(NaiveDateTime, i32)> {
    let (date, rest) = text.split_at(text.len().min(10));
    let (year, month, day) = (
        digits(date, 0..4)?,
        digits(date, 5..7)?,
        digits(date, 8..10)?,
    );
    if date.len() != 10 || &date[4..5] != "-" || &date[7..8] != "-" {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(year as i32, month, day)?;
    let (hour, minute, second, micros, offset) = time_of_day(rest)?;
    Some((
        date.and_hms_micro_opt(hour, minute, second, micros)?,
        offset,
    ))
}

/// The time of day after the date: hours, minutes, seconds, microseconds and
/// the offset in seconds; midnight in UTC where there is none.
fn time_of_day(rest: &str) -> Option<(u32, u32, u32, u32, i32)> {
    if rest.is_empty() {
        return Some((0, 0, 0, 0, 0));
    }
    let rest = rest.strip_prefix('T').or_else(|| rest.strip_prefix(' '))?;
    let (offset, rest) = match rest.rfind(['Z', '+', '-']) {
        Some(at) => (Some(&rest[at..]), &rest[..at]),
        None => (None, rest),
    };
    let (hour, minute) = (digits(rest, 0..2)?, digits(rest, 3..5)?);
    if rest.len() < 5 || &rest[2..3] != ":" {
        return None;
    }
    let (second, micros) = match &rest[5..] {
        "" => (0, 0),
        seconds if seconds.starts_with(':') => {
            let second = digits(seconds, 1..3)?;
            let micros = match &seconds[3..] {
                "" => 0,
                fraction if fraction.starts_with('.') && (2..=7).contains(&fraction.len()) => {
                    digits(&format!("{:0<6}", &fraction[1..]), 0..6)?
                }
                _ => return None,
            };
            (second, micros)
        }
        _ => return None,
    };
    let offset = match offset {
        None | Some("Z") => 0,
        Some(offset) if offset.len() == 6 && &offset[3..4] == ":" => {
            let seconds = (digits(offset, 1..3)? * 60 + digits(offset, 4..6)?) as i32 * 60;
            if offset.starts_with('-') {
                -seconds
            } else {
                seconds
            }
        }
        _ => return None,
    };
    Some((hour, minute, second, micros, offset))
}

/// The number the ASCII digits at `at` spell; `None` if they are not digits.
fn digits(text: &str, at: std::ops::Range<usize>) -> Option<u32> {
    let slice = text.get(at)?;
    if slice.is_empty() || !slice.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    slice.parse().ok()
}

/// `text` as a UUID; `None` if it is not the canonical form.
pub(crate) fn uuid(text: &str) -> Option<uuid::Uuid> {
    let canonical = text.len() == 36
        && text.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        });
    if !canonical {
        return None;
    }
    uuid::Uuid::parse_str(text).ok()
}
