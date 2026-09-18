//! Typed terms: what the Python `test_public` and the Go `public_test`
//! check. That a term of one sort does not take the operators of another is
//! checked by the compiler, where the sources check nothing; the doctests of
//! this file's subject say so with `compile_fail`.

use ascetic_ddd_specification::ast::{
    add, and, any, div, equal, field, greater_than, greater_than_equal, is, is_not_null, is_null,
    left_shift, less_than, less_than_equal, modulo, mul, neg, not, not_equal, or, right_shift, sub,
    value,
};
use ascetic_ddd_specification::dsl::{
    Boolean, Datetime, NullBoolean, NullDatetime, NullNumber, NullText, Number, Text, Timespan,
};
use ascetic_ddd_specification::{
    Expr, Interval, Path, Record, Timestamp, Value, is_satisfied_by, pg,
};

type Spec = Expr<Value>;
/// What makes a fresh copy of a tree: a tree is moved into the one built of it.
type Make = fn() -> Spec;

#[test]
fn fields_and_values() {
    assert_eq!(Number::field("age").into_expr(), field("age"));
    assert_eq!(
        Number::field("user.profile.age").into_expr(),
        field("user.profile.age")
    );
    assert_eq!(
        Number::field(Path::item("price")).into_expr(),
        field(Path::item("price"))
    );
    assert_eq!(Number::value(18).into_expr(), value::<Value>(18));
    assert_eq!(Number::value(1.5).into_expr(), value::<Value>(1.5));
    assert_eq!(Text::value("Alice").into_expr(), value::<Value>("Alice"));
    assert_eq!(Boolean::value(true).into_expr(), value::<Value>(true));
    assert_eq!(NullNumber::value(Some(18)).into_expr(), value::<Value>(18));
    assert_eq!(
        NullText::value(None::<&str>).into_expr(),
        Spec::Value(Value::Null)
    );
    assert_eq!(Number::field("age").expr(), &field("age"));
}

#[test]
fn booleans_combine() {
    let (active, deleted) = (
        || Boolean::field("active"),
        || NullBoolean::field("deleted"),
    );
    let (a, d): (Make, Make) = (|| field("active"), || field("deleted"));
    assert_eq!((active() & deleted()).into_expr(), and(a(), d()));
    assert_eq!((active() | deleted()).into_expr(), or(a(), d()));
    assert_eq!((!active()).into_expr(), not(a()));
    assert_eq!(
        active().is(Boolean::value(true)).into_expr(),
        is(a(), value(true))
    );
    assert_eq!(deleted().is_null().into_expr(), is_null(d()));
    assert_eq!(deleted().is_not_null().into_expr(), is_not_null(d()));
    // `&` binds tighter than `|`, in Rust as in SQL.
    assert_eq!(
        (active() | deleted() & active()).into_expr(),
        or(a(), and(d(), a())),
    );
}

#[test]
fn comparables_compare() {
    let (age, n): (fn() -> Number, Make) = (|| Number::field("age"), || field("age"));
    let eighteen = || Number::value(18);
    assert_eq!(age().eq(eighteen()).into_expr(), equal(n(), value(18)));
    assert_eq!(age().ne(eighteen()).into_expr(), not_equal(n(), value(18)));
    assert_eq!(
        age().gt(eighteen()).into_expr(),
        greater_than(n(), value(18))
    );
    assert_eq!(age().lt(eighteen()).into_expr(), less_than(n(), value(18)));
    assert_eq!(
        age().ge(eighteen()).into_expr(),
        greater_than_equal(n(), value(18))
    );
    assert_eq!(
        age().le(eighteen()).into_expr(),
        less_than_equal(n(), value(18))
    );
    assert_eq!(
        Text::field("name").eq(NullText::field("alias")).into_expr(),
        equal::<Value>(field("name"), field("alias")),
    );
}

#[test]
fn a_null_is_tested_not_compared() {
    // `email == maybe`, of an `Option` known when the term is built.
    let email = || NullText::field("email");
    assert_eq!(
        email().eq(NullText::value(None::<&str>)).into_expr(),
        is_null(field("email"))
    );
    assert_eq!(
        email().ne(NullText::value(None::<&str>)).into_expr(),
        is_not_null(field("email"))
    );
    assert_eq!(
        email().eq(NullText::value(Some("a@b"))).into_expr(),
        equal(field("email"), value("a@b")),
    );
}

#[test]
fn numbers_compute() {
    let (a, b) = (|| Number::field("a"), || NullNumber::field("b"));
    let (x, y): (Make, Make) = (|| field("a"), || field("b"));
    assert_eq!((a() + b()).into_expr(), add(x(), y()));
    assert_eq!((a() - b()).into_expr(), sub(x(), y()));
    assert_eq!((a() * b()).into_expr(), mul(x(), y()));
    assert_eq!((a() / b()).into_expr(), div(x(), y()));
    assert_eq!((a() % b()).into_expr(), modulo(x(), y()));
    assert_eq!((a() << b()).into_expr(), left_shift(x(), y()));
    assert_eq!((a() >> b()).into_expr(), right_shift(x(), y()));
    assert_eq!((-a()).into_expr(), neg(x()));
    assert_eq!(
        ((a() - b()) * Number::value(2))
            .gt(Number::value(100))
            .into_expr(),
        greater_than(mul(sub(x(), y()), value(2)), value(100)),
    );
}

#[test]
fn time_computes_to_the_sort_it_makes() {
    let (created, updated) = (
        || Datetime::field("created_at"),
        || NullDatetime::field("updated_at"),
    );
    let day = || Timespan::value(Interval::from_micros(86_400_000_000));
    let age: Timespan = updated() - created();
    let tomorrow: Datetime = created() + day();
    let yesterday: Datetime = created() - day();
    let two_days: Timespan = day() + day();
    let back: Timespan = -day();
    assert_eq!(
        age.gt(day()).into_expr(),
        greater_than(
            sub(field("updated_at"), field("created_at")),
            day().into_expr()
        )
    );
    assert_eq!(
        tomorrow.into_expr(),
        add(field("created_at"), day().into_expr())
    );
    assert_eq!(
        yesterday.into_expr(),
        sub(field("created_at"), day().into_expr())
    );
    assert_eq!(
        two_days.into_expr(),
        add(day().into_expr(), day().into_expr())
    );
    assert_eq!(back.into_expr(), neg(day().into_expr()));
    assert_eq!(
        created()
            .lt(Datetime::value(Timestamp::from_micros(0)))
            .into_expr(),
        less_than::<Value>(field("created_at"), value(Timestamp::from_micros(0))),
    );
}

#[test]
fn a_tree_built_otherwise_comes_in_as_the_sort_it_is_declared() {
    let dear = Boolean::from_expr(any(
        "items",
        Number::field(Path::item("price"))
            .gt(Number::value(500))
            .into_expr(),
    ));
    let specification = (Boolean::field("active") & dear).into_expr();
    assert_eq!(
        specification,
        and(
            field("active"),
            any(
                "items",
                greater_than(field(Path::item("price")), value(500))
            )
        ),
    );
}

#[test]
fn what_is_built_is_a_specification() {
    let specification = ((Number::field("price") - Number::field("discount"))
        .lt(Number::value(100))
        & NullDatetime::field("deleted_at").is_null())
    .into_expr();
    let product = Record::<Value>::object([
        ("price", Record::value(120)),
        ("discount", Record::value(30)),
        ("deleted_at", Record::Value(Value::Null)),
    ]);
    assert_eq!(is_satisfied_by(&specification, &product), Ok(true));
    assert_eq!(
        pg::compile(&specification).expect("compiled").sql,
        "price - discount < $1 AND deleted_at IS NULL",
    );
}
