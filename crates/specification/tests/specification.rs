//! The tree and its evaluation: what the Python `test_specification` and the
//! Go `specification_test` and `operators_test` check, and the three-valued
//! logic the Go registry has.

use ascetic_ddd_specification::ast::{
    add, all, and, and_all, any, div, equal, field, greater_than, greater_than_equal, is,
    is_not_null, is_null, left_shift, less_than, less_than_equal, modulo, mul, neg, not, not_equal,
    or, or_all, right_shift, sub, value,
};
use ascetic_ddd_specification::{
    ContextError, EvalError, Expr, Infix, Interval, OperandError, Path, Record, Root, Timestamp,
    Value, evaluate, is_satisfied_by, null_test,
};

type Spec = Expr<Value>;
/// What makes a fresh copy of a tree: a tree is moved into the one built of it.
type Make = fn() -> Spec;

fn nothing() -> Record<Value> {
    Record::object::<&str>([])
}

fn constant(expr: &Spec) -> Result<Value, EvalError> {
    evaluate(expr, &nothing())
}

fn null() -> Spec {
    Expr::Value(Value::Null)
}

fn store() -> Record<Value> {
    Record::object([
        ("name", Record::value("MyStore")),
        (
            "items",
            Record::collection([
                Record::object([
                    ("name", Record::value("Laptop")),
                    ("price", Record::value(999)),
                ]),
                Record::object([
                    ("name", Record::value("Mouse")),
                    ("price", Record::value(29)),
                ]),
            ]),
        ),
        ("empty", Record::collection([])),
        (
            "owner",
            Record::object([("profile", Record::object([("age", Record::value(30))]))]),
        ),
    ])
}

#[test]
fn a_path_is_never_empty_and_keeps_its_names_in_order() {
    let path = Path::global("user").child("profile").child("age");
    assert_eq!(path.root(), Root::Global);
    assert_eq!(path.objects(), ["user", "profile"]);
    assert_eq!(path.name(), "age");
    assert_eq!(path.names().collect::<Vec<_>>(), ["user", "profile", "age"]);
    assert_eq!(Path::from("user.profile.age"), path);
    assert_eq!(Path::dotted(Root::Item, "price"), Path::item("price"));
}

#[test]
fn several_operands_nest_to_the_left() {
    let (a, b, c): (Spec, Spec, Spec) = (field("a"), field("b"), field("c"));
    assert_eq!(
        and_all(a.clone(), [b.clone(), c.clone()]),
        and(and(a.clone(), b.clone()), c.clone()),
    );
    assert_eq!(
        or_all(a.clone(), [b.clone(), c.clone()]),
        or(or(a.clone(), b), c)
    );
    assert_eq!(and_all(a.clone(), []), a);
}

#[test]
fn an_operator_is_matched_by_its_constant() {
    let Expr::Infix(_, Infix::AND, right) = and::<Value>(field("a"), equal(field("b"), value(1)))
    else {
        panic!("an AND");
    };
    assert!(matches!(*right, Expr::Infix(_, Infix::EQ, _)));
}

#[test]
fn comparisons() {
    for (expr, expected) in [
        (equal(value(5), value(5)), true),
        (equal(value(5), value(10)), false),
        (not_equal(value(5), value(10)), true),
        (not_equal(value(5), value(5)), false),
        (greater_than(value(10), value(5)), true),
        (greater_than(value(5), value(10)), false),
        (less_than(value(5), value(10)), true),
        (less_than(value(10), value(5)), false),
        (greater_than_equal(value(5), value(5)), true),
        (greater_than_equal(value(4), value(5)), false),
        (less_than_equal(value(5), value(5)), true),
        (less_than_equal(value(6), value(5)), false),
        (equal(value("a"), value("a")), true),
        (less_than(value("a"), value("b")), true),
        (equal(value(true), value(true)), true),
        (not_equal(value(true), value(false)), true),
        // False comes before true, as in PostgreSQL.
        (greater_than(value(true), value(false)), true),
        (less_than_equal(value(true), value(false)), false),
        // An integer meeting a float is promoted, as in PostgreSQL.
        (equal(value(1), value(1.0)), true),
        (less_than(value(1), value(1.5)), true),
        // PostgreSQL's NaN equals itself and is the greatest number.
        (equal(value(f64::NAN), value(f64::NAN)), true),
        (greater_than(value(f64::NAN), value(f64::MAX)), true),
    ] {
        assert_eq!(constant(&expr), Ok(Value::Bool(expected)), "{expr:?}");
    }
}

#[test]
fn arithmetic_is_postgresqls() {
    for (expr, expected) in [
        (add(value(5), value(3)), Value::Int(8)),
        (sub(value(5), value(3)), Value::Int(2)),
        (mul(value(5), value(3)), Value::Int(15)),
        // Integer division truncates, towards zero.
        (div(value(7), value(2)), Value::Int(3)),
        (div(value(-7), value(2)), Value::Int(-3)),
        (modulo(value(-7), value(2)), Value::Int(-1)),
        (modulo(value(i64::MIN), value(-1)), Value::Int(0)),
        (div(value(7.0), value(2)), Value::Float(3.5)),
        (add(value(1), value(0.5)), Value::Float(1.5)),
        (neg(value(5)), Value::Int(-5)),
        (neg(value(2.5)), Value::Float(-2.5)),
        (left_shift(value(1), value(3)), Value::Int(8)),
        (right_shift(value(8), value(2)), Value::Int(2)),
        // The count of a shift is taken modulo 64.
        (left_shift(value(1), value(64)), Value::Int(1)),
        (left_shift(value(1), value(-1)), Value::Int(i64::MIN)),
        (right_shift(value(8), value(65)), Value::Int(4)),
    ] {
        assert_eq!(constant(&expr), Ok(expected), "{expr:?}");
    }
}

#[test]
fn arithmetic_fails_where_postgresql_fails() {
    for (expr, expected) in [
        (div(value(1), value(0)), OperandError::DivisionByZero),
        (modulo(value(1), value(0)), OperandError::DivisionByZero),
        (div(value(1.0), value(0.0)), OperandError::DivisionByZero),
        (add(value(i64::MAX), value(1)), OperandError::OutOfRange),
        (mul(value(i64::MAX), value(2)), OperandError::OutOfRange),
        (div(value(i64::MIN), value(-1)), OperandError::OutOfRange),
        (neg(value(i64::MIN)), OperandError::OutOfRange),
        (mul(value(f64::MAX), value(2.0)), OperandError::OutOfRange),
    ] {
        assert_eq!(
            constant(&expr),
            Err(EvalError::Operand(expected)),
            "{expr:?}"
        );
    }
}

#[test]
fn an_operator_names_itself_and_its_operands_when_it_does_not_apply() {
    let unsupported = |operator: &str, left, right| {
        Err(EvalError::Operand(OperandError::Unsupported {
            operator: operator.to_owned(),
            left,
            right,
        }))
    };
    assert_eq!(
        constant(&greater_than_equal(value(5), value("5"))),
        unsupported(">=", "integer", Some("text")),
    );
    assert_eq!(
        constant(&not_equal(value(true), value(1))),
        unsupported("!=", "boolean", Some("integer")),
    );
    assert_eq!(
        constant(&greater_than(value("a"), value(1.5))),
        unsupported(">", "text", Some("float")),
    );
    assert_eq!(
        constant(&modulo(value(5.5), value(2))),
        unsupported("%", "float", Some("integer")),
    );
    assert_eq!(
        constant(&add(value("a"), value("b"))),
        unsupported("+", "text", Some("text")),
    );
    assert_eq!(constant(&neg(value("a"))), unsupported("-", "text", None));
}

#[test]
fn time() {
    let (noon, hour) = (
        Timestamp::from_micros(43_200_000_000),
        Interval::from_micros(3_600_000_000),
    );
    let later = Timestamp::from_micros(noon.as_micros() + hour.as_micros());
    for (expr, expected) in [
        (sub(value(later), value(noon)), Value::Interval(hour)),
        (add(value(noon), value(hour)), Value::Timestamp(later)),
        (add(value(hour), value(noon)), Value::Timestamp(later)),
        (sub(value(later), value(hour)), Value::Timestamp(noon)),
        (
            add(value(hour), value(hour)),
            Value::Interval(Interval::from_micros(7_200_000_000)),
        ),
        (
            sub(value(hour), value(hour)),
            Value::Interval(Interval::from_micros(0)),
        ),
        (
            neg(value(hour)),
            Value::Interval(Interval::from_micros(-3_600_000_000)),
        ),
        (less_than(value(noon), value(later)), Value::Bool(true)),
        (equal(value(hour), value(hour)), Value::Bool(true)),
    ] {
        assert_eq!(constant(&expr), Ok(expected), "{expr:?}");
    }
    assert!(constant(&add(value(noon), value(noon))).is_err());
    assert!(constant(&mul(value(hour), value(2))).is_err());
}

#[test]
fn a_null_operand_makes_a_null_result() {
    for expr in [
        equal(null(), value(1)),
        equal(null(), null()),
        not_equal(value(1), null()),
        less_than(null(), value(1)),
        add(value(1), null()),
        div(null(), value(0)),
        neg(null()),
        not(null()),
    ] {
        assert_eq!(constant(&expr), Ok(Value::Null), "{expr:?}");
    }
}

#[test]
fn the_connectives_are_three_valued() {
    let (t, f): (Make, Make) = (|| value(true), || value(false));
    for (expr, expected) in [
        (and(t(), t()), Value::Bool(true)),
        (and(t(), f()), Value::Bool(false)),
        (and(null(), f()), Value::Bool(false)),
        (and(f(), null()), Value::Bool(false)),
        (and(null(), t()), Value::Null),
        (and(t(), null()), Value::Null),
        (and(null(), null()), Value::Null),
        (or(f(), f()), Value::Bool(false)),
        (or(f(), t()), Value::Bool(true)),
        (or(null(), t()), Value::Bool(true)),
        (or(t(), null()), Value::Bool(true)),
        (or(null(), f()), Value::Null),
        (or(f(), null()), Value::Null),
        (not(t()), Value::Bool(false)),
        (not(f()), Value::Bool(true)),
    ] {
        assert_eq!(constant(&expr), Ok(expected), "{expr:?}");
    }
    assert_eq!(
        constant(&and(value(1), t())),
        Err(EvalError::NotBoolean("integer"))
    );
    assert_eq!(
        constant(&or(f(), value("x"))),
        Err(EvalError::NotBoolean("text"))
    );
    assert_eq!(
        constant(&not(value(1))),
        Err(EvalError::NotBoolean("integer"))
    );
}

#[test]
fn a_decided_connective_does_not_evaluate_its_right_side() {
    let fails: fn() -> Spec = || equal(div(value(1), value(0)), value(1));
    assert_eq!(
        constant(&and(value(false), fails())),
        Ok(Value::Bool(false))
    );
    assert_eq!(constant(&or(value(true), fails())), Ok(Value::Bool(true)));
    assert!(constant(&and(value(true), fails())).is_err());
    assert!(constant(&and(fails(), value(false))).is_err());
}

#[test]
fn is_and_is_null_are_never_null() {
    for (expr, expected) in [
        (is(value(true), value(true)), true),
        (is(value(true), value(false)), false),
        (is(null(), null()), true),
        (is(null(), value(true)), false),
        (is(value(1), null()), false),
        (is(value(1), value(1)), true),
        (is_null(null()), true),
        (is_null(value(42)), false),
        (is_not_null(value(42)), true),
        (is_not_null(null()), false),
    ] {
        assert_eq!(constant(&expr), Ok(Value::Bool(expected)), "{expr:?}");
    }
}

#[test]
fn members_are_reached_through_objects() {
    let store = store();
    assert_eq!(evaluate(&field("name"), &store), Ok(Value::from("MyStore")));
    assert_eq!(
        evaluate(&field("owner.profile.age"), &store),
        Ok(Value::Int(30))
    );
    assert_eq!(
        is_satisfied_by(&greater_than(field("owner.profile.age"), value(25)), &store),
        Ok(true),
    );
}

#[test]
fn a_member_that_is_not_there_is_an_error() {
    let store = store();
    let missing = |name: &str| Err(EvalError::Context(ContextError::Missing(name.to_owned())));
    assert_eq!(
        evaluate::<Value>(&field("nonexistent"), &store),
        missing("nonexistent")
    );
    assert_eq!(
        evaluate::<Value>(&field("absent.age"), &store),
        missing("absent")
    );
    assert_eq!(
        evaluate::<Value>(&field("owner"), &store),
        Err(EvalError::Context(ContextError::NotAValue(
            "owner".to_owned()
        ))),
    );
    assert_eq!(
        evaluate::<Value>(&field("name.first"), &store),
        Err(EvalError::Context(ContextError::NotAnObject(
            "name".to_owned()
        ))),
    );
    assert_eq!(
        evaluate(&any("name", value(true)), &store),
        Err(EvalError::Context(ContextError::NotACollection(
            "name".to_owned()
        ))),
    );
}

#[test]
fn some_item_satisfies_the_predicate() {
    let store = store();
    let dearer_than = |price: i64| {
        any(
            "items",
            greater_than(field(Path::item("price")), value(price)),
        )
    };
    assert_eq!(is_satisfied_by(&dearer_than(500), &store), Ok(true));
    assert_eq!(is_satisfied_by(&dearer_than(1000), &store), Ok(false));
    assert_eq!(
        is_satisfied_by(
            &any("empty", greater_than(field(Path::item("price")), value(0))),
            &store
        ),
        Ok(false),
    );
    // The candidate is in reach of the predicate too.
    let named_as_the_store = any("items", equal(field(Path::item("name")), field("name")));
    assert_eq!(is_satisfied_by(&named_as_the_store, &store), Ok(false));
}

#[test]
fn any_is_never_null_and_a_null_predicate_is_no_witness() {
    let store = store();
    let unknown = any("items", greater_than(field(Path::item("price")), null()));
    assert_eq!(evaluate(&unknown, &store), Ok(Value::Bool(false)));
    assert_eq!(evaluate(&not(unknown), &store), Ok(Value::Bool(true)));
}

#[test]
fn any_stops_at_its_first_witness() {
    let store = store();
    // The second item would fail: a text compared with a number.
    let first_is_dear = any(
        "items",
        or(
            greater_than(field(Path::item("price")), value(500)),
            greater_than(field(Path::item("name")), value(1)),
        ),
    );
    assert_eq!(is_satisfied_by(&first_is_dear, &store), Ok(true));
}

#[test]
fn all_is_no_item_failing() {
    let store = store();
    let dearer_than = |price: i64| {
        all(
            "items",
            greater_than(field(Path::item("price")), value(price)),
        )
    };
    assert_eq!(is_satisfied_by(&dearer_than(10), &store), Ok(true));
    assert_eq!(is_satisfied_by(&dearer_than(500), &store), Ok(false));
    // Nothing fails in an empty collection.
    assert_eq!(
        is_satisfied_by(
            &all("empty", equal(field(Path::item("price")), value(0))),
            &store
        ),
        Ok(true),
    );
    assert_eq!(
        all::<Value>("items", field(Path::item("active"))),
        not(any("items", not(field(Path::item("active"))))),
    );
}

#[test]
fn the_item_is_only_inside_a_collection() {
    assert_eq!(
        evaluate::<Value>(&field(Path::item("price")), &store()),
        Err(EvalError::NoCurrentItem),
    );
}

#[test]
fn the_predicate_of_any_is_a_boolean() {
    assert_eq!(
        evaluate::<Value>(&any("items", field(Path::item("price"))), &store()),
        Err(EvalError::NotBoolean("integer")),
    );
}

#[test]
fn a_candidate_satisfies_what_is_true_of_it() {
    let store = store();
    assert_eq!(is_satisfied_by(&value(true), &store), Ok(true));
    assert_eq!(is_satisfied_by(&value(false), &store), Ok(false));
    // As a row with a null condition is not selected.
    assert_eq!(is_satisfied_by(&null(), &store), Ok(false));
    assert_eq!(
        is_satisfied_by(&value(1), &store),
        Err(EvalError::NotBoolean("integer"))
    );
}

#[test]
fn equality_with_the_null_constant_is_the_null_test() {
    let a = || field::<Value>("a");
    for (built, expected) in [
        (null_test::equal(a(), null()), is_null(a())),
        (null_test::equal(null(), a()), is_null(a())),
        (null_test::not_equal(a(), null()), is_not_null(a())),
        (null_test::not_equal(null(), a()), is_not_null(a())),
        // Of two nulls one is tested: true, as in the notations this is for.
        (null_test::equal(null(), null()), is_null(null())),
        // Anything else is the comparison it says.
        (null_test::equal(a(), value(1)), equal(a(), value(1))),
        (
            null_test::not_equal(a(), field("b")),
            not_equal(a(), field("b")),
        ),
    ] {
        assert_eq!(built, expected);
    }
}

#[test]
fn the_null_test_is_found_throughout_a_tree() {
    let a = || field::<Value>("a");
    let item = || field::<Value>(Path::item("price"));
    let tree = and(
        not(equal(a(), null())),
        any(
            "items",
            or(not_equal(null(), item()), less_than(item(), null())),
        ),
    );
    assert_eq!(
        null_test::throughout(tree),
        and(
            not(is_null(a())),
            // An order with null is left what it is: null, true of nothing.
            any("items", or(is_not_null(item()), less_than(item(), null()))),
        ),
    );
    // The tree by itself keeps SQL's meaning: `a = NULL` is null.
    let store = store();
    assert_eq!(
        evaluate(&equal(field("name"), null()), &store),
        Ok(Value::Null)
    );
    assert_eq!(
        evaluate(&null_test::equal(field("name"), null()), &store),
        Ok(Value::Bool(false)),
    );
}

#[test]
fn the_values_of_a_tree_can_be_mapped() {
    let spec: Spec = and(
        equal(field("a"), value(1)),
        any("items", equal(field(Path::item("b")), value(2))),
    );
    let doubled = spec.try_map_values(&|value| match value {
        Value::Int(n) => Ok(n * 2),
        other => Err(format!("{other:?}")),
    });
    assert_eq!(
        doubled,
        Ok(and(
            equal(field("a"), Expr::Value(2)),
            any("items", equal(field(Path::item("b")), Expr::Value(4))),
        )),
    );
    let refused: Result<Expr<i64>, _> =
        equal::<Value>(value("x"), value(1)).try_map_values(&|value| match value {
            Value::Int(n) => Ok(*n),
            other => Err(format!("{other:?}")),
        });
    assert_eq!(refused, Err("Text(\"x\")".to_owned()));
}
