//! Templates: what the Python `test_jsonpath_parser` and the Go
//! `parser_test` check, and what this parser refuses that theirs let through.

use ascetic_ddd_specification::ast::{
    and, any, equal, field, greater_than, is_not_null, is_null, less_than, not, or,
};
use ascetic_ddd_specification::jsonpath::{
    BindError, MAX_LENGTH, MatchError, Param, ParamKey, ParamKind, Params, Slot, SyntaxError,
    Template,
};
use ascetic_ddd_specification::{
    ContextError, EvalError, Expr, Mapped, Mapping, Path, Record, Value, pg, transform,
};

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

/// An error shows a control character by its escape, not as it is: in the
/// message, and in the line that echoes the template, whose caret moves by
/// what the escape adds.
#[test]
fn a_control_character_is_shown_by_its_escape() {
    assert_eq!(
        error("$[?@.na\u{0}me == 1]").to_string(),
        "Unexpected character '\\x00' at position 7 (expected valid token)\n  $[?@.na\\x00me == 1]\n         ^",
    );
    let in_a_string = error("$[?@.name == 'a\u{0}b' # 1]");
    assert_eq!(
        in_a_string.to_string(),
        "Control character in a string at position 15 (expected its escape, \\n or \\uXXXX)\n  $[?@.name == 'a\\x00b' # 1]\n                 ^",
    );
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
        // RFC 9535, 2.3.5.1: unescaped, a character of a string is %x20 and
        // up. A raw one was taken into the string - a NUL among them, which
        // went as far as the server and failed there.
        (
            "$[?@.name == 'a\u{0}b']",
            "Control character in a string",
            15,
        ),
        ("$[?@.name == 'a\tb']", "Control character in a string", 15),
        ("$[?@.name == 'a\nb']", "Control character in a string", 15),
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

/// The levels of the longest way down a tree: what a reader of it recurses
/// through, and what dropping it does.
fn height<V>(expr: &Expr<V>) -> usize {
    match expr {
        Expr::Value(_) | Expr::Field(_) => 1,
        Expr::Prefix(_, operand) | Expr::Postfix(operand, _) | Expr::Any(_, operand) => {
            1 + height(operand)
        }
        Expr::Infix(left, _, right) => 1 + height(left).max(height(right)),
    }
}

/// Groups nested on the left, `levels` of them, each the first operand of a
/// chain of `&&` that is the first operand of a chain of `||`, `links` long
/// each. How deep the parser is grows by one with a group; how tall the tree
/// is, by two chains.
fn groups_on_the_left(levels: usize, links: impl Fn(usize) -> usize) -> String {
    let inner = (1..=levels).rev().fold("@.a".to_owned(), |inner, level| {
        let n = links(level);
        format!("({inner}{}{})", " && @.a".repeat(n), " || @.a".repeat(n))
    });
    format!("$[?{inner}]")
}

#[test]
fn a_group_on_the_left_counts_towards_the_depth() {
    // As long a chain after each group as a count of how deep the parser is
    // lets through: the count was passed down to the right operands alone, so
    // this was a tree of some sixteen thousand levels, and binding it - or
    // dropping it - overflowed the stack: an abort, not a panic.
    let hostile = groups_on_the_left(127, |level| 128 - level);
    assert_eq!(error(&hostile).message, "Expression is nested too deep");
}

/// The bounds, to the level. A tree of 128 levels is a template and one of
/// 129 is not, whether it grows by a chain or by a comparison above a chain.
/// The parser goes 32 deep and no deeper, by groups - which add no level to
/// the tree - by `!`, or by the filters of collections.
#[test]
fn the_bounds_are_held_to_the_level() {
    let too_deep =
        |source: &str| assert_eq!(error(source).message, "Expression is nested too deep");
    let links = |operands: usize| format!("@.a{}", " && @.a".repeat(operands - 1));

    assert_eq!(height(template(&format!("$[?{}]", links(128))).expr()), 128);
    too_deep(&format!("$[?{}]", links(129)));

    // The chain is the LEFT operand of the comparison: it was read before
    // anything knew of an operator over it.
    assert_eq!(
        height(template(&format!("$[?({}) == true]", links(127))).expr()),
        128
    );
    too_deep(&format!("$[?({}) == true]", links(128)));

    let grouped = |groups: usize| format!("$[?{}@.a{}]", "(".repeat(groups), ")".repeat(groups));
    assert_eq!(height(template(&grouped(32)).expr()), 1);
    too_deep(&grouped(33));

    let negated = |nots: usize| format!("$[?{}@.a]", "!".repeat(nots));
    assert_eq!(height(template(&negated(32)).expr()), 33);
    too_deep(&negated(33));

    let filtered = |filters: usize| {
        format!(
            "$[?{}@.a{}]",
            "@.items[*][?".repeat(filters),
            "]".repeat(filters)
        )
    };
    assert_eq!(height(template(&filtered(32)).expr()), 33);
    too_deep(&filtered(33));
}

/// Whatever the shape, a template is refused or its tree is within the bound:
/// groups at the left of chains and at the right, under `!`, as the
/// predicates of collections, and chains of every length about the bound.
#[test]
fn no_tree_of_a_template_is_taller_than_the_bound() {
    type Wrap = fn(&str, &str) -> String;
    let shapes: [Wrap; 5] = [
        |inner, links| format!("({inner}{links})"),
        |inner, links| format!("(@.a{links} && {inner})"),
        |inner, links| format!("!({inner}{links})"),
        |inner, links| format!("@.items[*][?{inner}{links}]"),
        |inner, links| format!("({inner}{links}) == ({inner})"),
    ];
    let mut accepted = 0;
    for wrap in shapes {
        for levels in [1, 2, 3, 7, 20, 60, 127] {
            for length in [0, 1, 5, 40, 63, 64, 126, 127, 128] {
                let links = " && @.a".repeat(length) + &" || @.a".repeat(length);
                // The last shape doubles the text with each level.
                let levels = if wrap("", "").contains("==") {
                    levels.min(7)
                } else {
                    levels
                };
                let inner = (0..levels).fold("@.a".to_owned(), |inner, _| wrap(&inner, &links));
                match Template::parse(&format!("$[?{inner}]")) {
                    Ok(template) => {
                        accepted += 1;
                        assert!(
                            height(template.expr()) <= 128,
                            "{levels} levels of {length}"
                        );
                    }
                    Err(error) => assert_eq!(error.message, "Expression is nested too deep"),
                }
            }
        }
    }
    // Not all refused: the property is of trees that were made.
    assert!(accepted > 50, "{accepted} accepted");
}

/// What the bounds are for: the deepest templates there can be are parsed,
/// and their trees go through everything that reads a tree - bound,
/// evaluated, compiled, transformed, cloned, compared, shown, dropped - on a
/// megabyte of stack, half of what a thread of Rust is given, in whatever
/// build the tests are: the numbers are in `jsonpath/parser.rs`.
#[test]
fn the_deepest_template_fits_a_megabyte_of_stack() {
    struct Same;
    impl Mapping<Value, Value> for Same {
        type Error = String;
        fn field(&self, path: &Path) -> Result<Mapped<Value>, String> {
            Ok(Mapped::Scalar(Expr::Field(path.clone())))
        }
        fn value(&self, value: &Value) -> Result<Mapped<Value>, String> {
            Ok(Mapped::Scalar(Expr::Value(value.clone())))
        }
    }

    let deep = |open: &str, close: &str, times: usize| {
        format!("$[?{}@.a == %d{}]", open.repeat(times), close.repeat(times))
    };
    // As deep as the parser goes and as tall as a tree gets, at once: 32
    // filters, the inner one the first operand of a chain of three.
    let both = (0..32).fold("@.a == %d".to_owned(), |inner, level| {
        // A comparison is two levels; a filter over a chain of three, four.
        let links = if level >= 30 { 2 } else { 3 };
        format!("@.items[*][?{inner}{}]", " && @.a == %d".repeat(links))
    });
    let sources = [
        (format!("$[?@.a == %d{}]", " && @.a == %d".repeat(126)), 128),
        (deep("(", ")", 32), 2),
        // A `!` and a group are a level of the parser each.
        (deep("!(", ")", 16), 18),
        (deep("@.items[*][?", "]", 32), 34),
        (format!("$[?{both}]"), 128),
    ];
    let readers = move || {
        for (source, tall) in &sources {
            let template = template(source);
            assert_eq!(height(template.expr()), *tall, "{source}");
            let count = source.matches("%d").count();
            let params = Params::positional(vec![1_i64; count]);
            let item = Record::object([("a", Record::value(1_i64))]);
            let record = (0..33).fold(item, |item, _| {
                Record::object([
                    ("a", Record::value(1_i64)),
                    ("items", Record::collection([item])),
                ])
            });
            template.matches(&record, &params).expect("evaluated");
            let bound = template.bind(&params).expect("bound");
            let query = pg::compile(&bound).expect("compiled");
            assert_eq!(query.params.len(), count);
            assert_eq!(transform(&bound, &Same).as_ref(), Ok(&bound));
            assert_eq!(bound.clone(), bound);
            assert!(!format!("{bound:?}").is_empty());
        }
    };
    std::thread::Builder::new()
        .stack_size(1024 * 1024)
        .spawn(readers)
        .expect("a thread")
        .join()
        .expect("the readers returned");
}

/// A text is read in a time that grows as its length: a lexer that counted
/// its placeholders over again for each token took the square of it, and a
/// text as long as this one took minutes to refuse.
#[test]
fn a_long_text_is_refused_in_the_time_it_takes_to_read_it() {
    let long = format!("$[?@.a == %d{}]", " @.a %d".repeat(30_000));
    assert_eq!(error(&long).message, "Expected ']'");
}

/// The bounds on height and nesting bound the shape of a tree and not the
/// size of a text: a text of megabytes was lexed whole before the parser
/// could refuse it, or accepted with a literal of megabytes. The length is
/// the first thing looked at, in bytes of UTF-8, so a template is one in
/// every port or in none.
#[test]
fn a_template_longer_than_the_bound_is_refused_before_it_is_read() {
    let room = "$[?@.a == 1]";
    let at_the_bound = format!("$[?@.a == 1{}]", " ".repeat(MAX_LENGTH - room.len()));
    assert_eq!(at_the_bound.len(), MAX_LENGTH);
    assert!(Template::parse(&at_the_bound).is_ok());
    let over = error(&format!("{at_the_bound} "));
    assert_eq!(
        over.to_string(),
        "Template too long at position 262144 (expected at most 262144 bytes of UTF-8)"
    );
    assert!(Template::parse(&format!("$[?@.a == '{}']", "\u{e9}".repeat(131_072))).is_err());
    let started = std::time::Instant::now();
    let chain = format!("$[?{}]", vec!["@.a == 1"; 400_000].join(" && "));
    assert_eq!(error(&chain).message, "Template too long");
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
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
