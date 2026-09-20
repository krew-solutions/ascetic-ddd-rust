//! Templates: what the Python `test_jsonpath_parser` and the Go
//! `parser_test` check, and what this parser refuses that theirs let through.

use ascetic_ddd_specification::ast::{
    and, any, equal, field, greater_than, is_not_null, is_null, less_than, not, or,
};
use ascetic_ddd_specification::jsonpath::{
    BindError, MatchError, Param, ParamKey, ParamKind, Params, Slot, SyntaxError, Template,
};
use ascetic_ddd_specification::{ContextError, EvalError, Expr, Path, Record, Value};

fn template(source: &str) -> Template {
    Template::parse(source).unwrap_or_else(|error| panic!("{error}"))
}

fn error(source: &str) -> SyntaxError {
    Template::parse(source).expect_err(source)
}

fn literal(value: impl Into<Value>) -> Expr<Slot> {
    Expr::Value(Slot::Literal(value.into()))
}

fn positional(position: usize, kind: ParamKind) -> Expr<Slot> {
    Expr::Value(Slot::Param(Param {
        key: ParamKey::Position(position),
        kind,
    }))
}

fn user(age: i64, name: &str, active: bool) -> Record<Value> {
    Record::object([
        ("age", Record::value(age)),
        ("name", Record::value(name)),
        ("active", Record::value(active)),
    ])
}

fn store() -> Record<Value> {
    let item = |name: &str, price: f64, stock: i64| {
        Record::object([
            ("name", Record::value(name)),
            ("price", Record::value(price)),
            ("stock", Record::value(stock)),
        ])
    };
    Record::object([
        ("limit", Record::value(100.0)),
        (
            "items",
            Record::collection([item("Laptop", 999.0, 5), item("Mouse", 29.0, 100)]),
        ),
        (
            "categories",
            Record::collection([
                Record::object([
                    ("name", Record::value("Electronics")),
                    ("items", Record::collection([item("Laptop", 999.0, 5)])),
                ]),
                Record::object([
                    ("name", Record::value("Stationery")),
                    ("items", Record::collection([item("Pen", 2.0, 500)])),
                ]),
            ]),
        ),
        (
            "warehouse",
            Record::object([("items", Record::collection([item("Widget", 10.0, 5)]))]),
        ),
    ])
}

#[test]
fn comparisons_with_positional_placeholders() {
    let alice = user(30, "Alice", true);
    for (source, param, expected) in [
        ("$[?@.age > %d]", 25, true),
        ("$[?@.age > %d]", 30, false),
        ("$[?@.age < %d]", 35, true),
        ("$[?@.age == %d]", 30, true),
        ("$[?@.age != %d]", 30, false),
        ("$[?@.age >= %d]", 30, true),
        ("$[?@.age <= %d]", 29, false),
    ] {
        let matched = template(source).matches(&alice, &Params::positional([param]));
        assert_eq!(matched, Ok(expected), "{source} with {param}");
    }
}

#[test]
fn placeholders_by_name_and_of_every_kind() {
    let alice = user(30, "Alice", true);
    let by_name = template("$[?@.name == %(name)s && @.age > %(age)d]");
    assert_eq!(
        by_name.matches(
            &alice,
            &Params::named([("name", Value::from("Alice")), ("age", Value::from(25))])
        ),
        Ok(true)
    );
    assert_eq!(
        by_name.matches(
            &alice,
            &Params::named([("name", Value::from("Bob")), ("age", Value::from(25))])
        ),
        Ok(false)
    );
    // `%s` takes a value of any kind, as Python's does.
    assert_eq!(
        template("$[?@.active == %s]").matches(&alice, &Params::positional([true])),
        Ok(true)
    );
    // `%f` takes any number.
    let cheap = template("$.items[*][?@.price < %f]");
    assert_eq!(
        cheap.matches(&store(), &Params::positional([30.5])),
        Ok(true)
    );
    assert_eq!(cheap.matches(&store(), &Params::positional([30])), Ok(true));
    // A name may be used twice, and a name not used is ignored.
    let twice = template("$[?@.age >= %(n)d && @.age <= %(n)d]");
    assert_eq!(
        twice.matches(&alice, &Params::named([("n", 30), ("unused", 1)])),
        Ok(true)
    );
}

#[test]
fn a_template_is_parsed_once_and_bound_many_times() {
    let older_than = template("$[?@.age > %d]");
    let tree = older_than.expr().clone();
    let alice = user(30, "Alice", true);
    assert_eq!(
        older_than.matches(&alice, &Params::positional([25])),
        Ok(true)
    );
    assert_eq!(
        older_than.matches(&alice, &Params::positional([35])),
        Ok(false)
    );
    assert_eq!(older_than.expr(), &tree);
    assert_eq!(older_than.source(), "$[?@.age > %d]");
    assert_eq!(
        older_than.bind(&Params::positional([25])),
        Ok(greater_than(field("age"), Expr::Value(Value::Int(25)))),
    );
    // It can go between threads, as the sources' can.
    fn shared<T: Send + Sync>(_: &T) {}
    shared(&older_than);
}

#[test]
fn parameters_that_do_not_fit_are_refused() {
    let two = template("$[?@.age > %d && @.name == %s]");
    assert_eq!(
        two.bind(&Params::positional([25])),
        Err(BindError::Missing(ParamKey::Position(1)))
    );
    assert_eq!(
        two.bind(&Params::positional([
            Value::from(25),
            Value::from("a"),
            Value::from(1)
        ])),
        Err(BindError::Unused {
            placeholders: 2,
            parameters: 3
        }),
    );
    assert_eq!(
        two.bind(&Params::named([("age", 25)])),
        Err(BindError::WrongStyle)
    );
    assert_eq!(
        two.bind(&Params::positional(["25", "a"])),
        Err(BindError::Mismatch {
            key: ParamKey::Position(0),
            expected: ParamKind::Integer,
            found: "text",
        }),
    );
    let named = template("$[?@.age > %(age)d]");
    assert_eq!(
        named.bind(&Params::named([("years", 25)])),
        Err(BindError::Missing(ParamKey::Name("age".to_owned())))
    );
    assert_eq!(
        named.bind(&Params::positional([25])),
        Err(BindError::WrongStyle)
    );
    assert!(
        template("$[?@.price < %f]")
            .bind(&Params::positional(["1"]))
            .is_err()
    );
    // A null fits a placeholder of any kind.
    assert!(named.bind(&Params::named([("age", Value::Null)])).is_ok());
    assert_eq!(
        named.matches(&user(30, "Alice", true), &Params::none()),
        Err(MatchError::Bind(BindError::WrongStyle)),
    );
}

#[test]
fn literals() {
    let alice = user(30, "Alice", true);
    for (source, expected) in [
        ("$[?@.active == true]", true),
        ("$[?@.active == false]", false),
        ("$[?@.active == TRUE]", true),
        ("$[?@.name == 'Alice']", true),
        ("$[?@.name == \"Alice\"]", true),
        ("$[?@.age == 30]", true),
        ("$[?@.age > -1]", true),
        ("$[?@.age < 30.5]", true),
        ("$[?@.age == 3e1]", true),
        ("$[?@.age == 300E-1]", true),
        ("$[?@.age == null]", false),
        ("$[?@.active]", true),
        ("$[?!@.active]", false),
    ] {
        assert_eq!(
            template(source).matches(&alice, &Params::none()),
            Ok(expected),
            "{source}"
        );
    }
    assert_eq!(
        template(r#"$[?@.a == 'it\'s' && @.b == "\u0041\n\\" && @.c == '\uD83D\uDE00']"#).expr(),
        &and(
            and(
                equal(field("a"), literal("it's")),
                equal(field("b"), literal("A\n\\"))
            ),
            equal(field("c"), literal("\u{1F600}")),
        ),
    );
    assert_eq!(
        template("$[?@.a == 1.5]").expr(),
        &equal(field("a"), literal(1.5))
    );
    assert_eq!(
        template("$[?@.a == 15]").expr(),
        &equal(field("a"), literal(15))
    );
}

#[test]
fn a_null_is_tested_not_compared() {
    // In JSONPath null is a value, and `@.a == null` is how a null is found;
    // in the tree `a = NULL` is null, as in SQL, and true of nothing. What the
    // template means is IS NULL, spelled out or bound to a placeholder.
    let record = |deleted_at: Value| {
        Record::<Value>::object([
            ("deleted_at", Record::Value(deleted_at)),
            ("name", Record::value("x")),
        ])
    };
    let (deleted, alive) = (record(Value::Null), record(Value::Int(5)));
    for (source, params, of_deleted, of_alive) in [
        ("$[?@.deleted_at == null]", Params::none(), true, false),
        ("$[?null == @.deleted_at]", Params::none(), true, false),
        ("$[?@.deleted_at != null]", Params::none(), false, true),
        ("$[?!(@.deleted_at == null)]", Params::none(), false, true),
        (
            "$[?@.deleted_at == %s]",
            Params::positional([Value::Null]),
            true,
            false,
        ),
        (
            "$[?@.deleted_at != %(at)d]",
            Params::named([("at", Value::Null)]),
            false,
            true,
        ),
        (
            "$[?@.deleted_at == %d]",
            Params::positional([5]),
            false,
            true,
        ),
        // An order with null is null, and so is its negation.
        ("$[?@.deleted_at > 1]", Params::none(), false, true),
        ("$[?!(@.deleted_at > 1)]", Params::none(), false, false),
        ("$[?@.deleted_at != 5]", Params::none(), false, false),
        ("$[?@.deleted_at < null]", Params::none(), false, false),
    ] {
        let template = template(source);
        assert_eq!(
            template.matches(&deleted, &params),
            Ok(of_deleted),
            "{source} of a null"
        );
        assert_eq!(
            template.matches(&alive, &params),
            Ok(of_alive),
            "{source} of a value"
        );
    }
    assert_eq!(
        template("$[?@.deleted_at == null]").bind(&Params::none()),
        Ok(is_null(field("deleted_at"))),
    );
    assert_eq!(
        template("$.items[*][?@.price != %s]").bind(&Params::positional([Value::Null])),
        Ok(any("items", is_not_null(field(Path::item("price"))))),
    );
    // The template itself is what was written: the rule is of the values,
    // which a template has when it is bound.
    assert_eq!(
        template("$[?@.deleted_at == null]").expr(),
        &equal(field("deleted_at"), literal(Value::Null)),
    );
}

#[test]
fn logical_operators() {
    let alice = user(30, "Alice", true);
    for (source, params, expected) in [
        (
            "$[?@.age > %d && @.active == %s]",
            Params::positional([Value::from(25), Value::from(true)]),
            true,
        ),
        (
            "$[?@.age > %d && @.active == %s]",
            Params::positional([Value::from(35), Value::from(true)]),
            false,
        ),
        (
            "$[?@.age < %d || @.age > %d]",
            Params::positional([18, 25]),
            true,
        ),
        (
            "$[?@.age < %d || @.age > %d]",
            Params::positional([18, 65]),
            false,
        ),
        ("$[?!(@.active == %s)]", Params::positional([false]), true),
        ("$[?!(@.active == %s)]", Params::positional([true]), false),
        (
            "$[?(@.age >= 18 && @.age <= 65) && @.active == true]",
            Params::none(),
            true,
        ),
        ("$[?!!@.active]", Params::none(), true),
    ] {
        assert_eq!(
            template(source).matches(&alice, &params),
            Ok(expected),
            "{source}"
        );
    }
}

#[test]
fn and_binds_tighter_than_or_and_both_nest_to_the_left() {
    let (a, b, c) = (
        || greater_than(field("a"), literal(1)),
        || greater_than(field("b"), literal(2)),
        || greater_than(field("c"), literal(3)),
    );
    for (source, expected) in [
        ("$[?@.a > 1 && @.b > 2 && @.c > 3]", and(and(a(), b()), c())),
        ("$[?@.a > 1 || @.b > 2 || @.c > 3]", or(or(a(), b()), c())),
        ("$[?@.a > 1 || @.b > 2 && @.c > 3]", or(a(), and(b(), c()))),
        ("$[?@.a > 1 && @.b > 2 || @.c > 3]", or(and(a(), b()), c())),
        (
            "$[?(@.a > 1 || @.b > 2) && @.c > 3]",
            and(or(a(), b()), c()),
        ),
        (
            "$[?@.a > 1 && (@.b > 2 || @.c > 3)]",
            and(a(), or(b(), c())),
        ),
        // `!` takes the comparison after it, as the sources read it.
        ("$[?!@.a > 1 && @.b > 2]", and(not(a()), b())),
        ("$[?((@.a > 1))]", a()),
    ] {
        assert_eq!(template(source).expr(), &expected, "{source}");
    }
}

#[test]
fn paths_and_what_at_means() {
    // In a filter on the candidate, `@` is the candidate.
    assert_eq!(
        template("$[?@.user.profile.age > %d]").expr(),
        &greater_than(field("user.profile.age"), positional(0, ParamKind::Integer)),
    );
    // In a filter on a collection, `@` is the item; `$` is still the candidate.
    assert_eq!(
        template("$.store.items[*][?@.price > $.limit]").expr(),
        &any(
            "store.items",
            greater_than(field(Path::item("price")), field("limit"))
        ),
    );
    // Either side of a comparison is any operand.
    assert_eq!(
        template("$[?%d < @.age]").expr(),
        &less_than(positional(0, ParamKind::Integer), field("age")),
    );
    let store = store();
    assert_eq!(
        template("$.items[*][?@.price > $.limit]").matches(&store, &Params::none()),
        Ok(true)
    );
    assert_eq!(
        template("$.warehouse.items[*][?@.stock < %d]").matches(&store, &Params::positional([10])),
        Ok(true)
    );
    assert_eq!(
        template("$.warehouse.items[*][?@.stock < %d]").matches(&store, &Params::positional([3])),
        Ok(false)
    );
}

#[test]
fn collections_nest() {
    let dear = template("$.categories[*][?@.items[*][?@.price > %f]]");
    assert_eq!(
        dear.expr(),
        &any(
            "categories",
            any(
                Path::item("items"),
                greater_than(field(Path::item("price")), positional(0, ParamKind::Number))
            ),
        ),
    );
    let store = store();
    assert_eq!(dear.matches(&store, &Params::positional([500.0])), Ok(true));
    assert_eq!(
        dear.matches(&store, &Params::positional([1000.0])),
        Ok(false)
    );
    let both = template(
        "$.categories[*][?@.name == %(category)s && @.items[*][?@.price > %(price)f && @.stock > 0]]",
    );
    let of = |category: &str, price: f64| {
        Params::named([
            ("category", Value::from(category)),
            ("price", Value::from(price)),
        ])
    };
    assert_eq!(both.matches(&store, &of("Electronics", 500.0)), Ok(true));
    assert_eq!(both.matches(&store, &of("Stationery", 500.0)), Ok(false));
}

#[test]
fn a_member_that_is_not_there_is_an_error() {
    assert_eq!(
        template("$[?@.nonexistent > %d]")
            .matches(&user(30, "Alice", true), &Params::positional([1])),
        Err(MatchError::Eval(EvalError::Context(ContextError::Missing(
            "nonexistent".to_owned()
        )))),
    );
}

#[test]
fn an_error_says_what_where_and_shows_it() {
    let unexpected = error("$[?@.a # 1]");
    assert_eq!(unexpected.message, "Unexpected character '#'");
    assert_eq!(unexpected.position, 7);
    assert_eq!(unexpected.expected, "valid token");
    assert_eq!(unexpected.expression, "$[?@.a # 1]");
    assert_eq!(
        unexpected.to_string(),
        "Unexpected character '#' at position 7 (expected valid token)\n  $[?@.a # 1]\n         ^",
    );
    let ended = error("$[?@.age >");
    assert_eq!(
        (ended.message.as_str(), ended.position),
        ("Unexpected end of expression", 10)
    );
    // A position counts characters, so the caret stands under the right one.
    assert_eq!(error("$[?@.a == 'é' # 1]").position, 14);
}

#[test]
fn what_the_grammar_does_not_have_is_refused() {
    for (source, message, position) in [
        ("", "Expected '$'", 0),
        ("@.age > 1", "Expected '$'", 0),
        ("$", "Expected filter expression '[?...]'", 1),
        ("$.items", "Expected wildcard '[*]'", 7),
        ("$[?@. > 1]", "Expected field name", 6),
        ("$[?@ > 1]", "Expected field name", 5),
        ("$[?@.age > ]", "Unexpected token ']'", 11),
        ("$[?@.age > foo]", "Unexpected token 'foo'", 11),
        ("$[?@.age > %x]", "Malformed placeholder", 11),
        ("$[?@.age > %(age]", "Malformed placeholder", 11),
        ("$[?@.name == 'open]", "Unterminated string", 13),
        ("$[?@.name == 'a\\qb']", "Invalid escape", 15),
        (
            "$[?@.age > 99999999999999999999]",
            "Number out of range",
            11,
        ),
        (r"$[?@.name == '\u00zz']", "Invalid escape", 14),
        (r"$[?@.name == '\uD83Dx']", "Invalid escape", 14),
        (r"$[?@.name == '\uDE00']", "Invalid escape", 14),
        ("$[?@.age > 1e999]", "Number out of range", 11),
        // What the sources read as something other than what it says.
        ("$[?@.age > 1", "Expected ']'", 12),
        ("$[?(@.age > 1]", "Expected ')'", 13),
        ("$[?@.age > 1)]", "Expected ']'", 12),
        ("$[?@.age > 1]]", "Unexpected token ']'", 13),
        ("$[?@.age > 1] extra", "Unexpected token 'extra'", 14),
        ("$[@.age > 1]", "Expected filter expression '[?...]'", 2),
        ("$[?age > 1]", "Unexpected token 'age'", 3),
        ("$.items[?@.price > 1]", "Expected wildcard '[*]'", 8),
        ("$[?@.items[?@.price > 1]]", "Expected wildcard '[*]'", 11),
        ("$.items[*]", "Expected filter expression '[?...]'", 10),
        ("$[?@.a == 1 == 2]", "Expected ']'", 12),
        (
            "$[?@.a > %d && @.b > %(b)d]",
            "Positional and named placeholders in one template",
            21,
        ),
    ] {
        let error = error(source);
        assert_eq!(
            (error.message.as_str(), error.position),
            (message, position),
            "{source}"
        );
    }
}

/// A text is read in a time that grows as its length: a lexer that counted
/// its placeholders over again for each token took the square of it, and a
/// text as long as this one took minutes to refuse.
#[test]
fn a_long_text_is_refused_in_the_time_it_takes_to_read_it() {
    let long = format!("$[?@.a == %d{}]", " @.a %d".repeat(300_000));
    assert_eq!(error(&long).message, "Expected ']'");
}

#[test]
fn a_tree_has_a_bound_on_its_depth() {
    let nested = format!("$[?{}@.a{}]", "(".repeat(200), ")".repeat(200));
    assert_eq!(error(&nested).message, "Expression is nested too deep");
    let negated = format!("$[?{}@.a]", "!".repeat(200));
    assert_eq!(error(&negated).message, "Expression is nested too deep");
    let chained = format!("$[?@.a{}]", " && @.a".repeat(200));
    assert_eq!(error(&chained).message, "Expression is nested too deep");
    assert!(Template::parse(&format!("$[?@.a{}]", " && @.a".repeat(100))).is_ok());
}
