//! The two readers against each other: whatever the evaluator says of a
//! specification, PostgreSQL must say of its compiled text — the same value
//! of a constant expression, the same error, the same rows selected. This is
//! what the claims of `value.rs`, `evaluate.rs` and `pg/mod.rs` rest on:
//! null logic, integer division, shifts, `IS`, and parentheses that keep the
//! shape of the tree. It needs a live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-specification --features pg --test pg
//! ```

#![cfg(feature = "pg")]

use ascetic_ddd_specification::ast::{
    add, all, and, any, div, equal, field, greater_than, greater_than_equal, is, is_not_null,
    is_null, left_shift, less_than, less_than_equal, modulo, mul, neg, not, not_equal, or,
    right_shift, sub, value,
};
use ascetic_ddd_specification::jsonpath::{Params, Template};
use ascetic_ddd_specification::pg::{Compiler, Relation, Schema, compile};
use ascetic_ddd_specification::{
    EvalError, Expr, Interval, OperandError, Path, Record, Timestamp, Value, evaluate,
    is_satisfied_by,
};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, NoTls};

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

type Spec = Expr<Value>;
/// What makes a fresh copy of a tree: a tree is moved into the one built of it.
type Make = fn() -> Spec;

async fn client() -> Client {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("a live PostgreSQL");
    tokio::spawn(connection);
    client
}

/// The type the server is told a parameter has. A constant expression gives
/// it nothing to infer one from; a null takes the type of the case.
fn type_of(value: &Value, null: &Type) -> Type {
    match value {
        Value::Null => null.clone(),
        Value::Bool(_) => Type::BOOL,
        Value::Int(_) => Type::INT8,
        Value::Float(_) => Type::FLOAT8,
        Value::Text(_) => Type::TEXT,
        Value::Timestamp(_) => Type::TIMESTAMPTZ,
        Value::Interval(_) => Type::INTERVAL,
    }
}

fn null() -> Spec {
    Expr::Value(Value::Null)
}

/// A constant expression; the type of its nulls; and the types of its
/// parameters where they are not what the values say.
type Constant = (Spec, Type, Option<Vec<Type>>);

fn constants() -> Vec<Constant> {
    let (t, f): (Make, Make) = (|| value(true), || value(false));
    let int = |expr| (expr, Type::INT8, None);
    let boolean = |expr| (expr, Type::BOOL, None);
    let time = |expr| (expr, Type::INTERVAL, None);
    // PostgreSQL shifts a `bigint` by an `integer`, and infers that of a
    // parameter; this test names the types itself, so it must say so.
    let shift = |expr| (expr, Type::INT8, Some(vec![Type::INT8, Type::INT4]));
    let shift_in_sum = |expr| {
        (
            expr,
            Type::INT8,
            Some(vec![Type::INT8, Type::INT8, Type::INT4]),
        )
    };
    let (noon, hour) = (
        Timestamp::from_micros(1_700_000_000_000_000),
        Interval::from_micros(3_600_000_000),
    );
    vec![
        // Arithmetic, and the parentheses that keep its shape.
        int(sub(value(10), sub(value(4), value(3)))),
        int(sub(sub(value(10), value(4)), value(3))),
        int(sub(value(10), add(value(4), value(3)))),
        int(div(value(100), div(value(10), value(5)))),
        int(div(mul(value(7), value(3)), value(2))),
        int(mul(add(value(1), value(2)), value(3))),
        int(add(value(1), mul(value(2), value(3)))),
        int(div(value(7), value(2))),
        int(div(value(-7), value(2))),
        int(modulo(value(-7), value(2))),
        int(modulo(value(7), value(-2))),
        int(modulo(value(i64::MIN), value(-1))),
        int(neg(neg(value(5)))),
        int(sub(value(5), neg(value(3)))),
        int(neg(add(value(1), value(2)))),
        shift(left_shift(value(1), value(3))),
        shift(left_shift(value(1), value(64))),
        shift(left_shift(value(1), value(-1))),
        shift(right_shift(value(8), value(65))),
        shift(right_shift(value(-8), value(1))),
        shift_in_sum(left_shift(add(value(1), value(2)), value(3))),
        shift_in_sum(add(value(1), left_shift(value(2), value(3)))),
        int(add(value(1), value(0.5))),
        int(div(value(7.0), value(2))),
        int(mul(value(2.5), value(4))),
        // Where it fails.
        int(div(value(1), value(0))),
        int(modulo(value(1), value(0))),
        int(div(value(1.0), value(0.0))),
        int(add(value(i64::MAX), value(1))),
        int(mul(value(i64::MAX), value(2))),
        int(div(value(i64::MIN), value(-1))),
        int(neg(value(i64::MIN))),
        int(mul(value(f64::MAX), value(2.0))),
        // What is not defined here is not defined there.
        int(add(value("a"), value("b"))),
        int(modulo(value(5.5), value(2))),
        int(neg(value("a"))),
        int(less_than(value(1), value("b"))),
        // Comparisons.
        boolean(greater_than(value(true), value(false))),
        boolean(less_than_equal(value(true), value(true))),
        int(equal(value(1), value(1.0))),
        int(less_than(value(1), value(1.5))),
        int(greater_than_equal(value(2), value(2))),
        int(less_than_equal(value(3), value(2))),
        int(not_equal(value("a"), value("b"))),
        int(less_than(value("a"), value("b"))),
        int(equal(value(f64::NAN), value(f64::NAN))),
        int(greater_than(value(f64::NAN), value(f64::MAX))),
        int(equal(value(-0.0), value(0.0))),
        boolean(equal(equal(value(1), value(1)), value(true))),
        boolean(equal(value(true), equal(value(1), value(2)))),
        boolean(equal(is_null(null()), value(true))),
        // Nulls.
        int(equal(null(), value(1))),
        int(equal(null(), null())),
        int(not_equal(value(1), null())),
        int(add(value(1), null())),
        int(neg(null())),
        int(div(null(), value(0))),
        boolean(not(null())),
        boolean(and(null(), f())),
        boolean(and(f(), null())),
        boolean(and(null(), t())),
        boolean(and(null(), null())),
        boolean(or(null(), t())),
        boolean(or(t(), null())),
        boolean(or(null(), f())),
        boolean(and(or(t(), f()), f())),
        boolean(or(t(), and(f(), f()))),
        boolean(and(t(), and(t(), f()))),
        boolean(not(and(t(), f()))),
        boolean(not(not(t()))),
        boolean(is_null(or(null(), f()))),
        boolean(is_null(is_null(null()))),
        int(is_not_null(equal(value(1), null()))),
        int(is_null(equal(value(1), value(1)))),
        boolean(not(is_null(null()))),
        // `IS`.
        boolean(is(t(), t())),
        boolean(is(t(), f())),
        boolean(is(null(), null())),
        boolean(is(null(), t())),
        int(is(value(1), null())),
        int(is(value(1), value(1))),
        boolean(equal(is(t(), null()), f())),
        boolean(is(equal(value(1), value(1)), t())),
        // Time.
        time(sub(
            value(Timestamp::from_micros(noon.as_micros() + 5)),
            value(noon),
        )),
        time(add(value(noon), value(hour))),
        time(add(value(hour), value(noon))),
        time(sub(value(noon), value(hour))),
        time(add(value(hour), value(hour))),
        time(neg(value(hour))),
        time(less_than(value(noon), add(value(noon), value(hour)))),
        time(greater_than(value(hour), sub(value(hour), value(hour)))),
    ]
}

#[tokio::test]
async fn a_constant_expression_has_one_value_for_both_readers() {
    let client = client().await;
    let nothing = Record::<Value>::object::<&str>([]);
    for (expr, null_type, types) in constants() {
        let query = compile(&expr).expect("compiled");
        let evaluated = evaluate(&expr, &nothing);
        // The value the evaluator found goes in as one more parameter, of a
        // type inferred from what it is compared with.
        let text = format!(
            "SELECT ({}) IS NOT DISTINCT FROM ${}",
            query.sql,
            query.params.len() + 1,
        );
        let types: Vec<Type> = types.unwrap_or_else(|| {
            query
                .params
                .iter()
                .map(|param| type_of(param, &null_type))
                .collect()
        });
        let expected = evaluated.clone().unwrap_or(Value::Null);
        let params: Vec<&(dyn ToSql + Sync)> = query
            .params
            .iter()
            .chain(std::iter::once(&expected))
            .map(|param| param as &(dyn ToSql + Sync))
            .collect();
        let answered = match client.prepare_typed(&text, &types).await {
            Ok(statement) => client.query_one(&statement, &params).await,
            Err(error) => Err(error),
        };
        match (&evaluated, answered) {
            (Ok(value), Ok(row)) => {
                assert!(
                    row.get::<_, bool>(0),
                    "{}: evaluated to {value:?}",
                    query.sql
                );
            }
            // The same failure, not just a failure: by its SQLSTATE.
            (Err(EvalError::Operand(failure)), Err(error)) => {
                let expected = match failure {
                    OperandError::DivisionByZero => SqlState::DIVISION_BY_ZERO,
                    OperandError::OutOfRange => SqlState::NUMERIC_VALUE_OUT_OF_RANGE,
                    OperandError::Unsupported { .. } => SqlState::UNDEFINED_FUNCTION,
                };
                assert_eq!(error.code(), Some(&expected), "{}: {error:?}", query.sql);
            }
            (Ok(value), Err(error)) => {
                panic!(
                    "{}: evaluated to {value:?}, PostgreSQL: {error:?}",
                    query.sql
                )
            }
            (Err(error), answered) => {
                panic!(
                    "{}: evaluated: {error}, PostgreSQL: {answered:?}",
                    query.sql
                )
            }
        }
    }
}

struct Item {
    price: Option<i64>,
    active: Option<bool>,
}

struct Store {
    id: i64,
    a: Option<i64>,
    b: Option<i64>,
    flag: Option<bool>,
    name: Option<&'static str>,
    items: Vec<Item>,
}

fn stores() -> Vec<Store> {
    let item = |price, active| Item { price, active };
    let store = |id, a, b, flag, name, items| Store {
        id,
        a,
        b,
        flag,
        name,
        items,
    };
    vec![
        store(
            1,
            Some(1),
            Some(1),
            Some(true),
            Some("one"),
            vec![item(Some(900), Some(true)), item(Some(10), Some(false))],
        ),
        store(
            2,
            Some(1),
            Some(2),
            Some(false),
            Some("two"),
            vec![item(Some(10), Some(true))],
        ),
        store(
            3,
            None,
            Some(2),
            None,
            None,
            vec![item(None, Some(true)), item(Some(10), None)],
        ),
        store(4, None, None, Some(true), Some("four"), vec![]),
        store(
            5,
            Some(7),
            None,
            Some(false),
            Some("five"),
            vec![item(None, None)],
        ),
        store(
            6,
            Some(-3),
            Some(0),
            None,
            Some(""),
            vec![item(Some(900), None), item(Some(901), Some(true))],
        ),
    ]
}

fn record(store: &Store) -> Record<Value> {
    let items = store.items.iter().map(|item| {
        Record::object([
            ("price", Record::value(item.price)),
            ("active", Record::value(item.active)),
        ])
    });
    Record::object([
        ("id", Record::value(store.id)),
        ("a", Record::value(store.a)),
        ("b", Record::value(store.b)),
        ("flag", Record::value(store.flag)),
        ("name", Record::value(store.name)),
        ("items", Record::collection(items)),
    ])
}

async fn tables(client: &Client) {
    client
        .batch_execute(
            "CREATE TYPE pg_temp.spec_item AS (price int8, active bool);
             CREATE TEMP TABLE spec_stores (
                 id int8 PRIMARY KEY, a int8, b int8, flag bool, name text,
                 items pg_temp.spec_item[] NOT NULL
             );
             CREATE TEMP TABLE spec_items (store_id int8 NOT NULL, price int8, active bool);",
        )
        .await
        .expect("tables");
    for store in stores() {
        client
            .execute(
                "INSERT INTO spec_stores VALUES ($1, $2, $3, $4, $5, '{}')",
                &[&store.id, &store.a, &store.b, &store.flag, &store.name],
            )
            .await
            .expect("a store");
        for item in &store.items {
            client
                .execute(
                    "UPDATE spec_stores SET items = items || ROW($2::int8, $3::bool)::pg_temp.spec_item WHERE id = $1",
                    &[&store.id, &item.price, &item.active],
                )
                .await
                .expect("an embedded item");
            client
                .execute(
                    "INSERT INTO spec_items VALUES ($1, $2, $3)",
                    &[&store.id, &item.price, &item.active],
                )
                .await
                .expect("an item");
        }
    }
}

fn bound(source: &str, params: Params) -> Spec {
    Template::parse(source)
        .unwrap_or_else(|error| panic!("{error}"))
        .bind(&params)
        .unwrap_or_else(|error| panic!("{source}: {error}"))
}

fn specifications() -> Vec<Spec> {
    let item = |name: &str| field(Path::item(name));
    let dear = || greater_than(item("price"), value(500));
    vec![
        equal(field("a"), field("b")),
        not(equal(field("a"), field("b"))),
        not_equal(field("a"), field("b")),
        is(field("a"), field("b")),
        not(is(field("a"), field("b"))),
        is_null(field("a")),
        and(is_not_null(field("a")), is_null(field("b"))),
        or(equal(field("a"), field("b")), field("flag")),
        and(not(field("flag")), greater_than(field("b"), value(1))),
        not(or(field("flag"), is_null(field("name")))),
        greater_than(sub(field("a"), sub(field("b"), value(1))), value(0)),
        less_than(mul(add(field("a"), value(1)), value(2)), value(5)),
        equal(is_null(field("a")), field("flag")),
        equal(field("name"), value("")),
        less_than(field("name"), value("one")),
        is(field("flag"), null()),
        any("items", dear()),
        not(any("items", dear())),
        any("items", or(dear(), item("active"))),
        any("items", and(dear(), item("active"))),
        any("items", not(item("active"))),
        any("items", is_null(item("price"))),
        any("items", greater_than(item("price"), field("a"))),
        all("items", item("active")),
        all("items", greater_than(item("price"), value(5))),
        not(all("items", is_not_null(item("price")))),
        and(field("flag"), any("items", dear())),
        // A null found the way a template finds it, spelled out and bound.
        bound("$[?@.a == null]", Params::none()),
        bound(
            "$[?@.b != null && @.a == %s]",
            Params::positional([Value::Null]),
        ),
        bound(
            "$[?@.a == %d || @.name == %s]",
            Params::positional([Value::Int(1), Value::Null]),
        ),
        bound(
            "$.items[*][?@.price == %s]",
            Params::positional([Value::Null]),
        ),
        bound(
            "$.items[*][?@.active != null && @.price > 500]",
            Params::none(),
        ),
    ]
}

#[tokio::test]
async fn a_specification_selects_the_rows_it_is_satisfied_by() {
    let client = client().await;
    tables(&client).await;
    let relational = Schema::new("spec_stores")
        .relational("items", Relation::new("spec_items", "store_id", "id"));
    let embedded = Schema::new("spec_stores");
    for specification in specifications() {
        let satisfied: Vec<i64> = stores()
            .iter()
            .filter(|store| is_satisfied_by(&specification, &record(store)).expect("evaluated"))
            .map(|store| store.id)
            .collect();
        for (storage, schema) in [("embedded", &embedded), ("relational", &relational)] {
            let query = Compiler::new()
                .schema(schema)
                .compile(&specification)
                .expect("compiled");
            let text = format!("SELECT id FROM spec_stores WHERE {} ORDER BY id", query.sql);
            let params: Vec<&(dyn ToSql + Sync)> = query
                .params
                .iter()
                .map(|param| param as &(dyn ToSql + Sync))
                .collect();
            let selected: Vec<i64> = client
                .query(&text, &params)
                .await
                .unwrap_or_else(|error| panic!("{text}: {error}"))
                .iter()
                .map(|row| row.get(0))
                .collect();
            assert_eq!(selected, satisfied, "{storage}: {text}");
        }
    }
}

#[tokio::test]
async fn a_value_is_written_as_the_type_the_server_asks_for_if_it_fits() {
    let client = client().await;
    let narrow = client
        .prepare_typed(
            "SELECT $1::int4 + $2::int2 + $3::float4",
            &[Type::INT4, Type::INT2, Type::FLOAT4],
        )
        .await
        .expect("prepared");
    let params: [&(dyn ToSql + Sync); 3] = [&Value::Int(40), &Value::Int(2), &Value::Int(1)];
    let row = client
        .query_one(&narrow, &params)
        .await
        .expect("the values fit");
    assert_eq!(row.get::<_, f64>(0), 43.0);
    // What does not fit is refused here, not truncated on the way.
    let wide: [&(dyn ToSql + Sync); 3] = [&Value::Int(i64::MAX), &Value::Int(2), &Value::Int(1)];
    assert!(client.query_one(&narrow, &wide).await.is_err());
    let wrong: [&(dyn ToSql + Sync); 3] = [&Value::from("40"), &Value::Int(2), &Value::Int(1)];
    assert!(client.query_one(&narrow, &wrong).await.is_err());
    // The server infers the type of a parameter from the column it meets.
    let inferred = client
        .query_one(
            "SELECT $1 = 7::int4 AND $2 = 'x'::varchar AND $3 > 0.5::float8",
            &[&Value::Int(7), &Value::from("x"), &Value::Float(0.75)],
        )
        .await
        .expect("inferred");
    assert!(inferred.get::<_, bool>(0));
}
