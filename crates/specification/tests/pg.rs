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
use ascetic_ddd_specification::pg::{Compiler, Schema, compile};
use ascetic_ddd_specification::{
    Arithmetic, EvalError, Expr, Interval, Mapped, Mapping, Operand, OperandError, Path, Record,
    Timestamp, Value, evaluate, is_satisfied_by, transform,
};
use std::cmp::Ordering;
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

fn null() -> Spec {
    Expr::Value(Value::Null)
}

/// A mapping that renames every name of a path by one rule, and leaves the
/// values: the storage's name of a member, whatever leads to it.
struct Renamed(fn(&str) -> &str);

impl Mapping<Value, Value> for Renamed {
    type Error = String;

    fn field(&self, path: &Path) -> Result<Mapped<Value>, String> {
        let mut names = path.names().map(self.0);
        let first = names.next().ok_or("an empty path")?;
        Ok(Mapped::Scalar(Expr::Field(
            names.fold(Path::new(path.root(), first), Path::child),
        )))
    }

    fn value(&self, value: &Value) -> Result<Mapped<Value>, String> {
        Ok(Mapped::Scalar(Expr::Value(value.clone())))
    }
}
fn constants() -> Vec<Spec> {
    let (t, f): (Make, Make) = (|| value(true), || value(false));
    let (noon, hour) = (
        Timestamp::from_micros(1_700_000_000_000_000),
        Interval::from_micros(3_600_000_000),
    );
    let written = vec![
        // Arithmetic, and the parentheses that keep its shape.
        sub(value(10), sub(value(4), value(3))),
        sub(sub(value(10), value(4)), value(3)),
        sub(value(10), add(value(4), value(3))),
        div(value(100), div(value(10), value(5))),
        div(mul(value(7), value(3)), value(2)),
        mul(add(value(1), value(2)), value(3)),
        add(value(1), mul(value(2), value(3))),
        div(value(7), value(2)),
        div(value(-7), value(2)),
        modulo(value(-7), value(2)),
        modulo(value(7), value(-2)),
        modulo(value(i64::MIN), value(-1)),
        neg(neg(value(5))),
        sub(value(5), neg(value(3))),
        neg(add(value(1), value(2))),
        left_shift(value(1), value(3)),
        left_shift(value(1), value(64)),
        left_shift(value(1), value(-1)),
        right_shift(value(8), value(65)),
        right_shift(value(-8), value(1)),
        left_shift(add(value(1), value(2)), value(3)),
        add(value(1), left_shift(value(2), value(3))),
        add(value(1), value(0.5)),
        div(value(7.0), value(2)),
        mul(value(2.5), value(4)),
        // Where it fails.
        div(value(1), value(0)),
        modulo(value(1), value(0)),
        div(value(1.0), value(0.0)),
        add(value(i64::MAX), value(1)),
        mul(value(i64::MAX), value(2)),
        div(value(i64::MIN), value(-1)),
        neg(value(i64::MIN)),
        mul(value(f64::MAX), value(2.0)),
        // What is not defined here is not defined there.
        add(value("a"), value("b")),
        modulo(value(5.5), value(2)),
        neg(value("a")),
        less_than(value(1), value("b")),
        // Comparisons.
        greater_than(value(true), value(false)),
        less_than_equal(value(true), value(true)),
        equal(value(1), value(1.0)),
        less_than(value(1), value(1.5)),
        greater_than_equal(value(2), value(2)),
        less_than_equal(value(3), value(2)),
        not_equal(value("a"), value("b")),
        less_than(value("a"), value("b")),
        equal(value(f64::NAN), value(f64::NAN)),
        greater_than(value(f64::NAN), value(f64::MAX)),
        equal(value(-0.0), value(0.0)),
        equal(equal(value(1), value(1)), value(true)),
        equal(value(true), equal(value(1), value(2))),
        equal(is_null(null()), value(true)),
        // Nulls.
        equal(null(), value(1)),
        equal(null(), null()),
        not_equal(value(1), null()),
        add(value(1), null()),
        neg(null()),
        div(null(), value(0)),
        not(null()),
        and(null(), f()),
        and(f(), null()),
        and(null(), t()),
        and(null(), null()),
        or(null(), t()),
        or(t(), null()),
        or(null(), f()),
        and(or(t(), f()), f()),
        or(t(), and(f(), f())),
        and(t(), and(t(), f())),
        not(and(t(), f())),
        not(not(t())),
        is_null(or(null(), f())),
        is_null(is_null(null())),
        is_not_null(equal(value(1), null())),
        is_null(equal(value(1), value(1))),
        not(is_null(null())),
        // `IS`.
        is(t(), t()),
        is(t(), f()),
        is(null(), null()),
        is(null(), t()),
        is(value(1), null()),
        is(value(1), value(1)),
        equal(is(t(), null()), f()),
        is(equal(value(1), value(1)), t()),
        // Time.
        sub(
            value(Timestamp::from_micros(noon.as_micros() + 5)),
            value(noon),
        ),
        add(value(noon), value(hour)),
        add(value(hour), value(noon)),
        sub(value(noon), value(hour)),
        add(value(hour), value(hour)),
        neg(value(hour)),
        less_than(value(noon), add(value(noon), value(hour))),
        greater_than(value(hour), sub(value(hour), value(hour))),
    ];
    // Floats at their edges, every pair under every operator. What the
    // server makes of each - a value, "out of range" for a result too large
    // or too small to be one, "division by zero" - is the server's to say,
    // and the evaluator's to repeat: a zero from operands that are not zero
    // is an underflow, a NaN divided by zero is a NaN, one divided by
    // infinity is a zero and no underflow.
    let edges = [
        0.0,
        1.0,
        -1.0,
        1e300,
        1e-300,
        f64::MAX,
        f64::MIN_POSITIVE,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
    ];
    let operators: [fn(Spec, Spec) -> Spec; 4] = [add, sub, mul, div];
    let at_the_edges = edges.into_iter().flat_map(|left| {
        edges.into_iter().flat_map(move |right| {
            operators
                .into_iter()
                .map(move |operator| operator(value(left), value(right)))
        })
    });
    written.into_iter().chain(at_the_edges).collect()
}

#[tokio::test]
async fn a_constant_expression_has_one_value_for_both_readers() {
    let client = client().await;
    let nothing = Record::<Value>::object::<&str>([]);
    for expr in constants() {
        let query = compile(&expr).expect("compiled");
        let evaluated = evaluate(&expr, &nothing);
        // The value the evaluator found goes in as one more parameter, of a
        // type inferred from what it is compared with.
        let text = format!(
            "SELECT ({}) IS NOT DISTINCT FROM ${}",
            query.sql,
            query.params.len() + 1,
        );
        let expected = evaluated.clone().unwrap_or(Value::Null);
        let params: Vec<&(dyn ToSql + Sync)> = query
            .params
            .iter()
            .chain(std::iter::once(&expected))
            .map(|param| param as &(dyn ToSql + Sync))
            .collect();
        // Prepared as a user prepares it, the types the server's to find:
        // this test used to name them itself, and so did not see that where
        // every operand is a constant the server has nothing to find them by.
        let answered = match client.prepare(&text).await {
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

impl Item {
    /// The name of the item's maker: a Value Object inside the item, which
    /// the storage keeps as a composite inside the item's row.
    fn maker_name(&self) -> Option<&'static str> {
        self.price
            .map(|price| if price > 500 { "dear" } else { "cheap" })
    }

    /// The item's owner: an object of its own, which the storage keeps in a
    /// table of its own and the item refers to by a key. The third owner has
    /// no name.
    fn owner(&self) -> (i64, Option<&'static str>) {
        match self.active {
            Some(true) => (1, Some("ann")),
            Some(false) => (2, Some("bob")),
            None => (3, None),
        }
    }
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

impl Store {
    /// The store's owner, kept as the owners of items are: in the table of
    /// owners, by a key.
    fn owner(&self) -> (i64, Option<&'static str>) {
        match self.flag {
            Some(true) => (1, Some("ann")),
            Some(false) => (2, Some("bob")),
            None => (3, None),
        }
    }
}

fn record(store: &Store) -> Record<Value> {
    let items = store.items.iter().map(|item| {
        Record::object([
            ("price", Record::value(item.price)),
            ("active", Record::value(item.active)),
            (
                "maker",
                Record::object([("name", Record::value(item.maker_name()))]),
            ),
            (
                "owner",
                Record::object([("name", Record::value(item.owner().1))]),
            ),
        ])
    });
    Record::object([
        ("id", Record::value(store.id)),
        ("a", Record::value(store.a)),
        ("b", Record::value(store.b)),
        ("flag", Record::value(store.flag)),
        ("name", Record::value(store.name)),
        ("items", Record::collection(items)),
        (
            "owner",
            Record::object([("name", Record::value(store.owner().1))]),
        ),
        // Members named as PostgreSQL names other things, under columns of
        // those very names: `user` is the session's user if it is not quoted,
        // `order` does not parse, `createdAt` is folded to `createdat`.
        ("user", Record::value(store.name)),
        ("order", Record::value(store.a)),
        ("createdAt", Record::value(store.b)),
    ])
}

async fn tables(client: &Client) {
    client
        .batch_execute(
            r#"CREATE TYPE pg_temp.spec_maker AS (name text);
             CREATE TYPE pg_temp.spec_item AS (
                 price int8, active bool, maker pg_temp.spec_maker, owner_id int8
             );
             CREATE TEMP TABLE spec_owners (id int8 PRIMARY KEY, name text);
             INSERT INTO spec_owners VALUES (1, 'ann'), (2, 'bob'), (3, NULL);
             CREATE TEMP TABLE spec_stores (
                 id int8 PRIMARY KEY, a int8, b int8, flag bool, name text,
                 items pg_temp.spec_item[] NOT NULL,
                 "user" text, "order" int8, "createdAt" int8, owner_id int8
             );
             CREATE TEMP TABLE spec_items (
                 store_id int8 NOT NULL, price int8, active bool, maker pg_temp.spec_maker,
                 owner_id int8 REFERENCES spec_owners
             );"#,
        )
        .await
        .expect("tables");
    for store in stores() {
        client
            .execute(
                "INSERT INTO spec_stores VALUES ($1, $2, $3, $4, $5, '{}', $5, $2, $3, $6)",
                &[
                    &store.id,
                    &store.a,
                    &store.b,
                    &store.flag,
                    &store.name,
                    &store.owner().0,
                ],
            )
            .await
            .expect("a store");
        for item in &store.items {
            client
                .execute(
                    "UPDATE spec_stores SET items = items || ROW($2::int8, $3::bool, ROW($4::text), $5::int8)::pg_temp.spec_item WHERE id = $1",
                    &[&store.id, &item.price, &item.active, &item.maker_name(), &item.owner().0],
                )
                .await
                .expect("an embedded item");
            client
                .execute(
                    "INSERT INTO spec_items VALUES ($1, $2, $3, ROW($4::text)::pg_temp.spec_maker, $5)",
                    &[&store.id, &item.price, &item.active, &item.maker_name(), &item.owner().0],
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
    let maker_name = || field(Path::item("maker").child("name"));
    let owner_name = || field(Path::item("owner").child("name"));
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
        // A member of a Value Object inside the item: a composite inside the
        // item's row, in the array and in the table alike.
        any("items", equal(maker_name(), value("dear"))),
        any("items", and(is_null(maker_name()), item("active"))),
        all("items", not_equal(maker_name(), value("cheap"))),
        bound(
            "$.items[*][?@.maker.name == %s && @.price > 5]",
            Params::positional([Value::from("cheap")]),
        ),
        // A member of an object the item refers to by a key: the schema says
        // `items.owner` is kept in a table of its own.
        any("items", equal(owner_name(), value("ann"))),
        any("items", and(is_null(owner_name()), dear())),
        all("items", not_equal(owner_name(), value("bob"))),
        any("items", equal(owner_name(), maker_name())),
        // The same of the candidate itself, and both in one predicate.
        equal(field("owner.name"), value("bob")),
        and(is_null(field("owner.name")), is_not_null(field("a"))),
        any("items", equal(owner_name(), field("owner.name"))),
        bound(
            "$.items[*][?@.owner.name == %s && @.price > 5]",
            Params::positional([Value::from("bob")]),
        ),
        // Constants with nothing but constants beside them: their types are
        // said in the text, for the server has nothing to find them by.
        greater_than(field("a"), sub(value(4), value(3))),
        any(
            "items",
            greater_than(item("price"), mul(value(100), value(5))),
        ),
        less_than(field("a"), neg(value(-2))),
        or(is_null(null()), field("flag")),
        // A name is the column's, whatever else PostgreSQL knows by it.
        equal(field("user"), value("one")),
        greater_than(field("order"), value(0)),
        equal(field("createdAt"), value(2)),
        bound("$[?@.user == %s]", Params::positional([Value::from("two")])),
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
    // The owner of an item is in a table of its own in either storage of the items.    // The schema is the storage's keys; the tree reaches the compiler in the
    // storage's names, which a mapping gives it: the items are a table of
    // their own in one storage and an array in the other, and the owner is
    // named by the key's column in both.
    let relational = Schema::new("spec_stores")
        .foreign_key("spec_items", "store_id", "spec_stores", "id")
        .foreign_key("spec_items", "owner_id", "spec_owners", "id")
        .foreign_key("spec_stores", "owner_id", "spec_owners", "id");
    let embedded = Schema::new("spec_stores")
        .foreign_key("spec_stores.items", "owner_id", "spec_owners", "id")
        .foreign_key("spec_stores", "owner_id", "spec_owners", "id");
    let in_a_table = Renamed(|name| match name {
        "items" => "spec_items",
        "owner" => "owner_id",
        name => name,
    });
    let in_the_row = Renamed(|name| match name {
        "owner" => "owner_id",
        name => name,
    });
    for specification in specifications() {
        let satisfied: Vec<i64> = stores()
            .iter()
            .filter(|store| is_satisfied_by(&specification, &record(store)).expect("evaluated"))
            .map(|store| store.id)
            .collect();
        for (storage, schema, mapping) in [
            ("embedded", &embedded, &in_the_row),
            ("relational", &relational, &in_a_table),
        ] {
            let query = Compiler::new()
                .schema(schema)
                .compile(&transform(&specification, mapping).expect("transformed"))
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

/// The values of a domain whose items may have a discount. A discount is a
/// Value Object, and a specification compares it as one: `@.discount >
/// Discount(10)`, not a number found inside it. An item that has no discount
/// has the special case of one - not a null in a discount's place, which a
/// specification would have to step around.
#[derive(Clone, Debug, PartialEq)]
enum Priced {
    Scalar(Value),
    Discount(i64),
    NoDiscount,
}

impl Operand for Priced {
    fn null() -> Self {
        Priced::Scalar(Value::null())
    }

    /// The special case is what is not known, to a comparison: as the column
    /// it is kept in is null.
    fn is_null(&self) -> bool {
        match self {
            Priced::Scalar(value) => value.is_null(),
            Priced::Discount(_) => false,
            Priced::NoDiscount => true,
        }
    }

    fn from_bool(value: bool) -> Self {
        Priced::Scalar(Value::from_bool(value))
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Priced::Scalar(value) => value.as_bool(),
            _ => None,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Priced::Scalar(value) => value.kind(),
            Priced::Discount(_) | Priced::NoDiscount => "discount",
        }
    }

    fn equals(&self, other: &Self) -> Result<bool, OperandError> {
        self.compare(other).map(Ordering::is_eq)
    }

    fn compare(&self, other: &Self) -> Result<Ordering, OperandError> {
        match (self, other) {
            (Priced::Scalar(left), Priced::Scalar(right)) => left.compare(right),
            (Priced::Discount(left), Priced::Discount(right)) => Ok(left.cmp(right)),
            _ => Err(OperandError::unsupported("<", self.kind(), other.kind())),
        }
    }

    fn negate(&self) -> Result<Self, OperandError> {
        match self {
            Priced::Scalar(value) => value.negate().map(Priced::Scalar),
            _ => Err(OperandError::unsupported_unary("-", self.kind())),
        }
    }

    fn compute(&self, op: Arithmetic, other: &Self) -> Result<Self, OperandError> {
        match (self, other) {
            (Priced::Scalar(left), Priced::Scalar(right)) => {
                left.compute(op, right).map(Priced::Scalar)
            }
            _ => Err(OperandError::unsupported(op, self.kind(), other.kind())),
        }
    }
}

/// What the storage has for them: a discount is its percent in a column, and
/// the special case is that column's null.
struct Prices;

impl Mapping<Priced, Value> for Prices {
    type Error = String;

    fn field(&self, path: &Path) -> Result<Mapped<Value>, String> {
        // By the whole path from the candidate: the collection, and a
        // member of its item under it. Where the item is, is the tree's.
        let names: Vec<&str> = path.names().collect();
        match names.as_slice() {
            ["items"] => Ok(Mapped::Scalar(Expr::Field(path.clone()))),
            ["items", "discount"] => Ok(Mapped::Scalar(Expr::Field(
                path.sibling("discount_percent"),
            ))),
            names => Err(format!("no such member: {}", names.join("."))),
        }
    }

    fn value(&self, value: &Priced) -> Result<Mapped<Value>, String> {
        Ok(Mapped::Scalar(Expr::Value(match value {
            Priced::Scalar(value) => value.clone(),
            Priced::Discount(percent) => Value::Int(*percent),
            Priced::NoDiscount => Value::Null,
        })))
    }
}

/// A Value Object is compared as a whole by the evaluator, and what is not
/// there is a special case of it: no path into it, so no member of a null to
/// ask for, and the answer does not hang on the order of the items. The
/// mapping says what it is in the storage, and the two readers agree.
#[tokio::test]
async fn a_value_object_is_compared_as_a_whole_and_its_absence_is_a_special_case() {
    let client = client().await;
    client
        .batch_execute(
            "CREATE TYPE pg_temp.spec_priced AS (price int8, discount_percent int8);
             CREATE TEMP TABLE spec_shops (id int8, items pg_temp.spec_priced[]);
             INSERT INTO spec_shops VALUES
                 (1, ARRAY[ROW(900, 15), ROW(100, NULL)]::pg_temp.spec_priced[]),
                 (2, ARRAY[ROW(100, NULL), ROW(900, 15)]::pg_temp.spec_priced[]),
                 (3, ARRAY[ROW(100, NULL)]::pg_temp.spec_priced[]);",
        )
        .await
        .expect("the shops");
    let item = |price: i64, discount: Priced| {
        Record::object([
            ("price", Record::value(Priced::Scalar(Value::Int(price)))),
            ("discount", Record::value(discount)),
        ])
    };
    let discounted = || item(900, Priced::Discount(15));
    let plain = || item(100, Priced::NoDiscount);
    let shops: [(i64, Record<Priced>); 3] = [
        (1, vec![discounted(), plain()]),
        (2, vec![plain(), discounted()]),
        (3, vec![plain()]),
    ]
    .map(|(id, items)| (id, Record::object([("items", Record::collection(items))])));

    let discount = || field(Path::item("discount"));
    let over = |percent: i64| greater_than(discount(), Expr::Value(Priced::Discount(percent)));
    let specifications: [(Expr<Priced>, Vec<i64>); 5] = [
        (any("items", over(10)), vec![1, 2]),
        (
            any(
                "items",
                equal(discount(), Expr::Value(Priced::Discount(15))),
            ),
            vec![1, 2],
        ),
        (any("items", is_null(discount())), vec![1, 2, 3]),
        (not(any("items", over(10))), vec![3]),
        (any("items", over(20)), vec![]),
    ];
    for (specification, expected) in specifications {
        let satisfied: Vec<i64> = shops
            .iter()
            .filter(|(_, shop)| is_satisfied_by(&specification, shop).expect("evaluated"))
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(satisfied, expected, "{specification:?}");

        let query =
            compile(&transform(&specification, &Prices).expect("transformed")).expect("compiled");
        let text = format!("SELECT id FROM spec_shops WHERE {} ORDER BY id", query.sql);
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
        assert_eq!(selected, satisfied, "{text}");
    }
}

/// The same discount with a special case that answers for itself, as Fowler's
/// Special Case does: it is equal to itself and to no discount, and less than
/// any. Nothing of it is null to the evaluator.
#[derive(Clone, Debug, PartialEq)]
struct Answering(Priced);

impl Operand for Answering {
    fn null() -> Self {
        Answering(Priced::null())
    }

    fn is_null(&self) -> bool {
        matches!(&self.0, Priced::Scalar(value) if value.is_null())
    }

    fn from_bool(value: bool) -> Self {
        Answering(Priced::from_bool(value))
    }

    fn as_bool(&self) -> Option<bool> {
        self.0.as_bool()
    }

    fn kind(&self) -> &'static str {
        self.0.kind()
    }

    fn equals(&self, other: &Self) -> Result<bool, OperandError> {
        self.compare(other).map(Ordering::is_eq)
    }

    fn compare(&self, other: &Self) -> Result<Ordering, OperandError> {
        match (&self.0, &other.0) {
            (Priced::NoDiscount, Priced::NoDiscount) => Ok(Ordering::Equal),
            (Priced::NoDiscount, Priced::Discount(_)) => Ok(Ordering::Less),
            (Priced::Discount(_), Priced::NoDiscount) => Ok(Ordering::Greater),
            (left, right) => left.compare(right),
        }
    }

    fn negate(&self) -> Result<Self, OperandError> {
        self.0.negate().map(Answering)
    }

    fn compute(&self, op: Arithmetic, other: &Self) -> Result<Self, OperandError> {
        self.0.compute(op, &other.0).map(Answering)
    }
}

/// The same storage: the special case is the column's null. That it is one
/// the mapping says, which `transform` reads where the two are compared for
/// equality.
struct AnsweringPrices;

impl Mapping<Answering, Value> for AnsweringPrices {
    type Error = String;

    fn field(&self, path: &Path) -> Result<Mapped<Value>, String> {
        Prices.field(path)
    }

    fn value(&self, value: &Answering) -> Result<Mapped<Value>, String> {
        match &value.0 {
            Priced::NoDiscount => Ok(Mapped::Null(Value::Null)),
            other => Prices.value(other),
        }
    }
}

/// A special case that answers for itself is equal to itself, and the storage
/// has a null for it: `discount = $1` with a null is true of nothing, so the
/// server found no shop where the evaluator found all three. Equality with
/// what the mapping says is the storage's null is the null test.
///
/// What stays the server's own: a null compared with a value is unknown to
/// it, and so is the negation of that, where the special case answers false
/// and true. A special case kept as a value, and not as a null, has none of
/// this.
#[tokio::test]
async fn equality_with_a_special_case_kept_as_a_null_is_the_null_test() {
    let client = client().await;
    client
        .batch_execute(
            "CREATE TYPE pg_temp.spec_answering AS (price int8, discount_percent int8);
             CREATE TEMP TABLE spec_answering_shops (id int8, items pg_temp.spec_answering[]);
             INSERT INTO spec_answering_shops VALUES
                 (1, ARRAY[ROW(900, 15), ROW(100, NULL)]::pg_temp.spec_answering[]),
                 (2, ARRAY[ROW(100, NULL), ROW(900, 15)]::pg_temp.spec_answering[]),
                 (3, ARRAY[ROW(100, NULL)]::pg_temp.spec_answering[]);",
        )
        .await
        .expect("the shops");
    let of = |discount: Priced| Record::object([("discount", Record::value(Answering(discount)))]);
    let shops: [(i64, Record<Answering>); 3] = [
        (1, vec![of(Priced::Discount(15)), of(Priced::NoDiscount)]),
        (2, vec![of(Priced::NoDiscount), of(Priced::Discount(15))]),
        (3, vec![of(Priced::NoDiscount)]),
    ]
    .map(|(id, items)| (id, Record::object([("items", Record::collection(items))])));

    let discount = || field(Path::item("discount"));
    let constant = |discount: Priced| Expr::Value(Answering(discount));
    let none = || constant(Priced::NoDiscount);
    let over = |percent: i64| greater_than(discount(), constant(Priced::Discount(percent)));
    // The specification; the shops it is satisfied by; the shops the server selects.
    let specifications: [(Expr<Answering>, Vec<i64>, Vec<i64>); 6] = [
        (
            any("items", equal(discount(), none())),
            vec![1, 2, 3],
            vec![1, 2, 3],
        ),
        (
            any("items", equal(none(), discount())),
            vec![1, 2, 3],
            vec![1, 2, 3],
        ),
        (
            any("items", not_equal(discount(), none())),
            vec![1, 2],
            vec![1, 2],
        ),
        (any("items", over(10)), vec![1, 2], vec![1, 2]),
        // The server's own logic of a null, which the null test does not reach.
        (any("items", not(over(10))), vec![1, 2, 3], vec![]),
        (
            any(
                "items",
                not_equal(discount(), constant(Priced::Discount(15))),
            ),
            vec![1, 2, 3],
            vec![],
        ),
    ];
    for (specification, in_memory, on_the_server) in specifications {
        let satisfied: Vec<i64> = shops
            .iter()
            .filter(|(_, shop)| is_satisfied_by(&specification, shop).expect("evaluated"))
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(satisfied, in_memory, "{specification:?}");

        let query = compile(&transform(&specification, &AnsweringPrices).expect("transformed"))
            .expect("compiled");
        let text = format!(
            "SELECT id FROM spec_answering_shops WHERE {} ORDER BY id",
            query.sql
        );
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
        assert_eq!(selected, on_the_server, "{text}");
    }
}

/// Why a type is said only where nothing stands beside the constant. A point
/// in time is written as a timestamp with zone or without, whichever the
/// column is. Said to be `timestamptz` beside a column without zone, it would
/// be compared in the session's time zone, and the row would not be found.
#[tokio::test]
async fn a_constant_beside_a_column_takes_the_columns_type() {
    let client = client().await;
    client
        .batch_execute(
            "SET TIME ZONE 'Asia/Tokyo';
             CREATE TEMP TABLE spec_moments (id int8, at timestamp, zoned timestamptz, small int2);
             INSERT INTO spec_moments VALUES
                 (1, '2023-11-14 22:13:20', '2023-11-14 22:13:20+00', 7);",
        )
        .await
        .expect("a table");
    let noon = Timestamp::from_micros(1_700_000_000_000_000);
    let specifications: [Spec; 4] = [
        equal(field("at"), value(noon)),
        equal(field("zoned"), value(noon)),
        equal(field("small"), value(7)),
        // And where nothing stands beside them, the constants say their own.
        equal(field("small"), add(value(3), value(4))),
    ];
    for specification in specifications {
        let query = compile(&specification).expect("compiled");
        let text = format!("SELECT id FROM spec_moments WHERE {}", query.sql);
        let params: Vec<&(dyn ToSql + Sync)> = query
            .params
            .iter()
            .map(|param| param as &(dyn ToSql + Sync))
            .collect();
        let rows = client
            .query(&text, &params)
            .await
            .unwrap_or_else(|error| panic!("{text}: {error}"));
        assert_eq!(rows.len(), 1, "{text}");
    }
}

/// PostgreSQL shifts by an `integer` and by nothing else: a `bigint` column
/// as the count was "operator does not exist: bigint << bigint". Both readers
/// take the count modulo 64, a negative one included.
#[tokio::test]
async fn the_count_of_a_shift_is_an_integer_whatever_its_column_is() {
    let client = client().await;
    client
        .batch_execute(
            "CREATE TEMP TABLE spec_shifts (id int8, n int8, count int8, small int2);
             INSERT INTO spec_shifts VALUES
                 (1, 1, 3, 3), (2, 1, 64, 64), (3, 1, -1, -1), (4, 1, NULL, NULL), (5, 8, 62, NULL);",
        )
        .await
        .expect("a table");
    let row = |n: i64, count: Option<i64>, small: Option<i64>| {
        Record::object([
            ("n", Record::value(n)),
            ("count", Record::value(count)),
            ("small", Record::value(small)),
        ])
    };
    let rows = [
        (1, row(1, Some(3), Some(3))),
        (2, row(1, Some(64), Some(64))),
        (3, row(1, Some(-1), Some(-1))),
        (4, row(1, None, None)),
        (5, row(8, Some(62), None)),
    ];
    let (n, count, small): (Make, Make, Make) =
        (|| field("n"), || field("count"), || field("small"));
    let specifications: [(Spec, Vec<i64>); 8] = [
        (equal(left_shift(n(), count()), value(8)), vec![1]),
        // 64 is no shift at all, and -1 is one by 63.
        (equal(left_shift(n(), count()), value(1)), vec![2]),
        (equal(left_shift(n(), count()), value(i64::MIN)), vec![3]),
        (is_null(left_shift(n(), count())), vec![4]),
        (is_null(left_shift(n(), small())), vec![4, 5]),
        (equal(left_shift(n(), small()), value(8)), vec![1]),
        // An expression as the count, and a constant shifted by a column.
        (
            equal(left_shift(n(), add(count(), value(1))), value(16)),
            vec![1],
        ),
        (equal(right_shift(value(64), count()), value(8)), vec![1]),
    ];
    for (specification, expected) in &specifications {
        let satisfied: Vec<i64> = rows
            .iter()
            .filter(|(_, row)| is_satisfied_by(specification, row).expect("evaluated"))
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(&satisfied, expected);
        let query = compile(specification).expect("compiled");
        let text = format!("SELECT id FROM spec_shifts WHERE {} ORDER BY id", query.sql);
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
        assert_eq!(&selected, expected, "{text}");
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

/// The item of an enclosing collection, named from an inner predicate by how
/// far out it is: the category's limit beside the price of its product. In
/// either storage - arrays nested in a composite, or tables that point at
/// one another - the enclosing item's row is in scope of the inner query.
#[tokio::test]
async fn the_item_of_an_enclosing_collection_is_named_from_an_inner_predicate() {
    let client = client().await;
    client
        .batch_execute(
            r#"CREATE TYPE pg_temp.spec_product AS (price int8);
             CREATE TYPE pg_temp.spec_category AS ("limit" int8, products pg_temp.spec_product[]);
             CREATE TEMP TABLE spec_shops (id int8 PRIMARY KEY, "limit" int8, categories pg_temp.spec_category[]);
             CREATE TEMP TABLE spec_categories (id int8 PRIMARY KEY, shop_id int8, "limit" int8);
             CREATE TEMP TABLE spec_products (category_id int8, price int8);
             INSERT INTO spec_shops VALUES
                 (1, 50, ARRAY[ROW(10, ARRAY[ROW(5), ROW(20)]::pg_temp.spec_product[]),
                               ROW(100, ARRAY[ROW(30)]::pg_temp.spec_product[])]::pg_temp.spec_category[]),
                 (2, 50, ARRAY[ROW(100, ARRAY[ROW(30), ROW(NULL)]::pg_temp.spec_product[])]::pg_temp.spec_category[]),
                 (3, 5, ARRAY[ROW(NULL, ARRAY[ROW(30)]::pg_temp.spec_product[])]::pg_temp.spec_category[]),
                 (4, 50, '{}');
             INSERT INTO spec_categories VALUES (11, 1, 10), (12, 1, 100), (21, 2, 100), (31, 3, NULL);
             INSERT INTO spec_products VALUES (11, 5), (11, 20), (12, 30), (21, 30), (21, NULL), (31, 30);"#,
        )
        .await
        .expect("tables");
    let shop =
        |limit: Option<i64>, categories: Vec<(Option<i64>, Vec<Option<i64>>)>| {
            Record::object([
                ("limit", Record::value(limit)),
                (
                    "categories",
                    Record::collection(categories.into_iter().map(|(limit, prices)| {
                        Record::object([
                            ("limit", Record::value(limit)),
                            (
                                "products",
                                Record::collection(prices.into_iter().map(|price| {
                                    Record::object([("price", Record::value(price))])
                                })),
                            ),
                        ])
                    })),
                ),
            ])
        };
    let shops = [
        (
            1,
            shop(
                Some(50),
                vec![
                    (Some(10), vec![Some(5), Some(20)]),
                    (Some(100), vec![Some(30)]),
                ],
            ),
        ),
        (2, shop(Some(50), vec![(Some(100), vec![Some(30), None])])),
        (3, shop(Some(5), vec![(None, vec![Some(30)])])),
        (4, shop(Some(50), vec![])),
    ];
    let price = || field(Path::item("price"));
    let category_limit = || field(Path::outer(1, "limit"));
    let over_its_category = |predicate| any("categories", any(Path::item("products"), predicate));
    let specifications: [Spec; 6] = [
        over_its_category(greater_than(price(), category_limit())),
        over_its_category(less_than(price(), category_limit())),
        over_its_category(greater_than(price(), field("limit"))),
        over_its_category(and(
            greater_than(price(), category_limit()),
            less_than(category_limit(), field("limit")),
        )),
        over_its_category(is_null(category_limit())),
        not(over_its_category(not(greater_than(
            price(),
            category_limit(),
        )))),
    ];
    let relational = Schema::new("spec_shops")
        .foreign_key("spec_categories", "shop_id", "spec_shops", "id")
        .foreign_key("spec_products", "category_id", "spec_categories", "id");
    let embedded = Schema::new("spec_shops");
    let in_tables = Renamed(|name| match name {
        "categories" => "spec_categories",
        "products" => "spec_products",
        name => name,
    });
    let in_the_row = Renamed(|name| name);
    for specification in &specifications {
        let satisfied: Vec<i64> = shops
            .iter()
            .filter(|(_, shop)| is_satisfied_by(specification, shop).expect("evaluated"))
            .map(|(id, _)| *id)
            .collect();
        for (storage, schema, mapping) in [
            ("embedded", &embedded, &in_the_row),
            ("relational", &relational, &in_tables),
        ] {
            let query = Compiler::new()
                .schema(schema)
                .compile(&transform(specification, mapping).expect("transformed"))
                .expect("compiled");
            let text = format!("SELECT id FROM spec_shops WHERE {} ORDER BY id", query.sql);
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
