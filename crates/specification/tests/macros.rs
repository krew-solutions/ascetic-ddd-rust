//! `#[specification]`: what the Python `test_lambda_parser` and the Go
//! specgen `main_test` check — the tree a predicate function is read as —
//! and that the function and its tree agree on the same candidates.

#![cfg(feature = "macros")]

use ascetic_ddd_specification::ast::{
    add, all, and, any, div, equal, field, greater_than, greater_than_equal, is_not_null, is_null,
    left_shift, less_than, modulo, mul, neg, not, not_equal, or, right_shift, sub, value,
};
use ascetic_ddd_specification::{
    Context, ContextError, Expr, Path, Value, is_satisfied_by, pg, specification,
};

type Spec = Expr<Value>;

struct Profile {
    age: i64,
}

struct Item {
    name: String,
    price: i64,
    active: bool,
    discount: Option<i64>,
}

struct Category {
    items: Vec<Item>,
}

struct Store {
    name: String,
    rating: f64,
    active: bool,
    closed_at: Option<i64>,
    alias: Option<String>,
    owner: Profile,
    items: Vec<Item>,
    categories: Vec<Category>,
}

const ADULT: i64 = 18;

mod limits {
    pub const DEAR: i64 = 500;
}

#[specification]
fn adult_owner(store: &Store) -> bool {
    store.owner.age >= 18
}

#[specification]
#[allow(clippy::comparison_to_empty)]
fn premium(s: &Store) -> bool {
    s.owner.age >= ADULT && s.active && s.name != "" || s.rating > 4.5
}

#[specification]
fn arithmetic(s: &Store) -> bool {
    (s.owner.age + 1) * 2 - 3 > 10 / 2 % 3
        && (s.owner.age << 1 >> 1) == s.owner.age
        && -s.owner.age < -5
}

// A body that is a `return` is a body.
#[specification]
#[allow(clippy::needless_return)]
fn open(s: &Store) -> bool {
    return s.closed_at.is_none() && !s.closed_at.is_some();
}

#[specification]
fn methods(s: &Store) -> bool {
    s.owner.age.ge(&18)
        && s.owner.age.ne(&0).eq(&true)
        && s.rating.lt(&5.0)
        && s.name.as_str() == "MyStore"
}

#[specification]
fn has_dear_items(s: &Store) -> bool {
    s.items
        .iter()
        .any(|item| item.price > limits::DEAR && item.active)
}

#[specification]
fn all_items_active(s: &Store) -> bool {
    s.items.iter().all(|item: &Item| item.active)
}

#[specification]
fn has_dear_item_in_a_category(s: &Store) -> bool {
    s.categories
        .iter()
        .any(|category| category.items.iter().any(|item| item.price > 500))
}

// The item is a plain name under whatever pattern borrows it.
#[specification]
#[allow(clippy::needless_borrowed_reference)]
fn has_an_item_named_as_the_store(s: &Store) -> bool {
    s.items.iter().any(|&ref item| item.name == s.name)
}

#[specification]
pub(crate) fn older_than(s: &Store, age: i64, name: &str) -> bool {
    s.owner.age > age && s.owner.age < age + 100 && s.name == name
}

// `== None` is what `is_none()` is, and what clippy would rather have.
#[specification]
#[allow(clippy::partialeq_to_none)]
fn closed(s: &Store) -> bool {
    s.closed_at != None
}

#[specification]
fn closed_on(s: &Store) -> bool {
    s.closed_at == Some(1_700_000_000)
}

// A parameter that may be none: known only when the tree is asked for.
#[specification]
fn closed_at(s: &Store, at: Option<i64>) -> bool {
    s.closed_at == at
}

#[specification]
fn known_as(s: &Store, alias: Option<&str>) -> bool {
    s.alias.as_deref() == alias
}

// What an `Option` holds is asked under a name. The null test beside the
// predicate makes the whole of two values, so it holds under a `!` too.
#[specification]
fn closed_before(s: &Store, at: i64) -> bool {
    s.closed_at.is_some_and(|closed| closed < at)
}

#[specification]
fn not_closed_before(s: &Store, at: i64) -> bool {
    !s.closed_at.is_some_and(|closed| closed < at)
}

#[specification]
fn open_or_closed_after(s: &Store, at: i64) -> bool {
    s.closed_at.is_none_or(|closed| closed > at)
}

#[specification]
fn known_as_pens(s: &Store) -> bool {
    s.alias.as_deref().is_some_and(|alias| alias == "Pens")
}

// A parameter that is an `Option` is asked the same way, and what it holds
// is compared with what the member holds: an `Option` itself has no order.
#[specification]
fn closed_after(s: &Store, at: Option<i64>) -> bool {
    at.is_some_and(|limit| s.closed_at.is_some_and(|closed| closed > limit))
}

#[specification]
fn not_closed_after(s: &Store, at: Option<i64>) -> bool {
    !at.is_some_and(|limit| s.closed_at.is_some_and(|closed| closed > limit))
}

#[specification]
fn closed_before_if_asked(s: &Store, at: Option<i64>) -> bool {
    at.is_none_or(|at| s.closed_at.is_some_and(|closed| closed < at))
}

// What the item holds, beside the item it is a member of.
#[specification]
fn has_a_well_discounted_item(s: &Store) -> bool {
    s.items.iter().any(|item| {
        item.discount
            .is_some_and(|discount| discount * 10 > item.price - 10)
    })
}

// A specification type: its fields are the constants, `self` is the
// specification, and the candidate comes beside it. `self` alone, as in
// `Store::is_active` below, stays the candidate.
struct DearAndOpen {
    min: i64,
    closed_before: Option<i64>,
}

impl DearAndOpen {
    #[specification]
    fn is_satisfied_by(&self, s: &Store) -> bool {
        s.items.iter().any(|item| item.price > self.min)
            && self
                .closed_before
                .is_none_or(|at| s.closed_at.is_some_and(|closed| closed < at))
    }
}

impl Store {
    #[specification]
    fn is_active(&self) -> bool {
        self.active
    }
}

fn item(name: &str) -> Spec {
    field(Path::item(name))
}

#[test]
fn the_tree_of_a_predicate() {
    let age = || field("owner.age");
    for (tree, expected) in [
        (adult_owner_ast(), greater_than_equal(age(), value(18))),
        (
            premium_ast(),
            or(
                and(
                    and(greater_than_equal(age(), value(18)), field("active")),
                    not_equal(field("name"), value("")),
                ),
                greater_than(field("rating"), value(4.5)),
            ),
        ),
        (
            arithmetic_ast(),
            and(
                and(
                    greater_than(
                        sub(mul(add(age(), value(1)), value(2)), value(3)),
                        modulo(div(value(10), value(2)), value(3)),
                    ),
                    equal(right_shift(left_shift(age(), value(1)), value(1)), age()),
                ),
                less_than(neg(age()), value(-5)),
            ),
        ),
        (
            open_ast(),
            and(
                is_null(field("closed_at")),
                not(is_not_null(field("closed_at"))),
            ),
        ),
        (
            methods_ast(),
            and(
                and(
                    and(
                        greater_than_equal(age(), value(18)),
                        equal(not_equal(age(), value(0)), value(true)),
                    ),
                    less_than(field("rating"), value(5.0)),
                ),
                equal(field("name"), value("MyStore")),
            ),
        ),
        (
            has_dear_items_ast(),
            any(
                "items",
                and(greater_than(item("price"), value(500)), item("active")),
            ),
        ),
        (all_items_active_ast(), all("items", item("active"))),
        (
            has_dear_item_in_a_category_ast(),
            any(
                "categories",
                any(Path::item("items"), greater_than(item("price"), value(500))),
            ),
        ),
        (
            has_an_item_named_as_the_store_ast(),
            any("items", equal(item("name"), field("name"))),
        ),
        (
            older_than_ast(25, "MyStore"),
            and(
                and(
                    greater_than(age(), value(25)),
                    less_than(age(), add(value(25), value(100))),
                ),
                equal(field("name"), value("MyStore")),
            ),
        ),
        (Store::is_active_ast(), field("active")),
        (
            DearAndOpen {
                min: 500,
                closed_before: Some(5),
            }
            .is_satisfied_by_ast(),
            and(
                any("items", greater_than(item("price"), value(500))),
                or(
                    is_null(value(5)),
                    and(
                        is_not_null(field("closed_at")),
                        less_than(field("closed_at"), value(5)),
                    ),
                ),
            ),
        ),
        // A comparison with none is the null test, in the tree as in Rust.
        (closed_ast(), is_not_null(field("closed_at"))),
        (
            closed_on_ast(),
            equal(field("closed_at"), value(1_700_000_000)),
        ),
        (closed_at_ast(None), is_null(field("closed_at"))),
        (closed_at_ast(Some(7)), equal(field("closed_at"), value(7))),
        (known_as_ast(None), is_null(field("alias"))),
        (
            known_as_ast(Some("Pens")),
            equal(field("alias"), value("Pens")),
        ),
        // What an `Option` holds: the null test, and the predicate of the
        // same member.
        (
            closed_before_ast(5),
            and(
                is_not_null(field("closed_at")),
                less_than(field("closed_at"), value(5)),
            ),
        ),
        (
            not_closed_before_ast(5),
            not(and(
                is_not_null(field("closed_at")),
                less_than(field("closed_at"), value(5)),
            )),
        ),
        (
            open_or_closed_after_ast(5),
            or(
                is_null(field("closed_at")),
                greater_than(field("closed_at"), value(5)),
            ),
        ),
        (
            known_as_pens_ast(),
            and(
                is_not_null(field("alias")),
                equal(field("alias"), value("Pens")),
            ),
        ),
        (
            closed_after_ast(Some(5)),
            and(
                is_not_null(value(5)),
                and(
                    is_not_null(field("closed_at")),
                    greater_than(field("closed_at"), value(5)),
                ),
            ),
        ),
        (
            closed_before_if_asked_ast(None),
            or(
                is_null(value(Value::Null)),
                and(
                    is_not_null(field("closed_at")),
                    less_than(field("closed_at"), value(Value::Null)),
                ),
            ),
        ),
        (
            has_a_well_discounted_item_ast(),
            any(
                "items",
                and(
                    is_not_null(item("discount")),
                    greater_than(
                        mul(item("discount"), value(10)),
                        sub(item("price"), value(10)),
                    ),
                ),
            ),
        ),
    ] {
        assert_eq!(tree, expected);
    }
}

// What a domain object does to be a candidate: say which of its members
// are values, which objects, which collections.

fn missing<T>(name: &str) -> Result<T, ContextError> {
    Err(ContextError::Missing(name.to_owned()))
}

impl Context<Value> for Profile {
    fn field(&self, name: &str) -> Result<Value, ContextError> {
        match name {
            "age" => Ok(self.age.into()),
            _ => missing(name),
        }
    }
    fn object(&self, name: &str) -> Result<&dyn Context<Value>, ContextError> {
        missing(name)
    }
    fn collection(&self, name: &str) -> Result<Vec<&dyn Context<Value>>, ContextError> {
        missing(name)
    }
}

impl Context<Value> for Item {
    fn field(&self, name: &str) -> Result<Value, ContextError> {
        match name {
            "name" => Ok((&self.name).into()),
            "price" => Ok(self.price.into()),
            "active" => Ok(self.active.into()),
            "discount" => Ok(self.discount.into()),
            _ => missing(name),
        }
    }
    fn object(&self, name: &str) -> Result<&dyn Context<Value>, ContextError> {
        missing(name)
    }
    fn collection(&self, name: &str) -> Result<Vec<&dyn Context<Value>>, ContextError> {
        missing(name)
    }
}

impl Context<Value> for Category {
    fn field(&self, name: &str) -> Result<Value, ContextError> {
        missing(name)
    }
    fn object(&self, name: &str) -> Result<&dyn Context<Value>, ContextError> {
        missing(name)
    }
    fn collection(&self, name: &str) -> Result<Vec<&dyn Context<Value>>, ContextError> {
        match name {
            "items" => Ok(self
                .items
                .iter()
                .map(|item| item as &dyn Context<Value>)
                .collect()),
            _ => missing(name),
        }
    }
}

impl Context<Value> for Store {
    fn field(&self, name: &str) -> Result<Value, ContextError> {
        match name {
            "name" => Ok((&self.name).into()),
            "rating" => Ok(self.rating.into()),
            "active" => Ok(self.active.into()),
            "closed_at" => Ok(self.closed_at.into()),
            "alias" => Ok(self.alias.as_deref().into()),
            _ => missing(name),
        }
    }
    fn object(&self, name: &str) -> Result<&dyn Context<Value>, ContextError> {
        match name {
            "owner" => Ok(&self.owner),
            _ => missing(name),
        }
    }
    fn collection(&self, name: &str) -> Result<Vec<&dyn Context<Value>>, ContextError> {
        match name {
            "items" => Ok(self
                .items
                .iter()
                .map(|item| item as &dyn Context<Value>)
                .collect()),
            "categories" => Ok(self
                .categories
                .iter()
                .map(|c| c as &dyn Context<Value>)
                .collect()),
            _ => missing(name),
        }
    }
}

fn stores() -> Vec<Store> {
    // A dear item has a discount, of a tenth of its price.
    let item = |name: &str, price: i64, active| Item {
        name: name.to_owned(),
        price,
        active,
        discount: (price > 100).then_some(price / 10),
    };
    vec![
        Store {
            name: "MyStore".to_owned(),
            rating: 4.0,
            active: true,
            closed_at: None,
            alias: None,
            owner: Profile { age: 30 },
            items: vec![item("Laptop", 999, true), item("Mouse", 29, true)],
            categories: vec![Category {
                items: vec![item("Laptop", 999, true)],
            }],
        },
        Store {
            name: "Pen".to_owned(),
            rating: 4.9,
            active: false,
            closed_at: Some(1_700_000_000),
            alias: Some("Pens".to_owned()),
            owner: Profile { age: 17 },
            items: vec![item("Pen", 2, false), item("Ink", 900, false)],
            categories: vec![
                Category { items: vec![] },
                Category {
                    items: vec![item("Pen", 2, true)],
                },
            ],
        },
        Store {
            name: String::new(),
            rating: 0.0,
            active: true,
            closed_at: None,
            alias: Some("Inks".to_owned()),
            owner: Profile { age: 125 },
            items: vec![],
            categories: vec![],
        },
    ]
}

#[test]
fn the_function_and_its_tree_agree() {
    type Predicate = fn(&Store) -> bool;
    let predicates: [(&str, Predicate, Spec); 14] = [
        ("closed", closed, closed_ast()),
        ("closed_on", closed_on, closed_on_ast()),
        ("adult_owner", adult_owner, adult_owner_ast()),
        ("premium", premium, premium_ast()),
        ("arithmetic", arithmetic, arithmetic_ast()),
        ("open", open, open_ast()),
        ("methods", methods, methods_ast()),
        ("has_dear_items", has_dear_items, has_dear_items_ast()),
        ("all_items_active", all_items_active, all_items_active_ast()),
        (
            "has_dear_item_in_a_category",
            has_dear_item_in_a_category,
            has_dear_item_in_a_category_ast(),
        ),
        (
            "has_an_item_named_as_the_store",
            has_an_item_named_as_the_store,
            has_an_item_named_as_the_store_ast(),
        ),
        ("is_active", Store::is_active, Store::is_active_ast()),
        ("known_as_pens", known_as_pens, known_as_pens_ast()),
        (
            "has_a_well_discounted_item",
            has_a_well_discounted_item,
            has_a_well_discounted_item_ast(),
        ),
    ];
    for store in stores() {
        for (name, function, tree) in &predicates {
            assert_eq!(
                is_satisfied_by(tree, &store),
                Ok(function(&store)),
                "{name} of {:?}",
                store.name
            );
        }
        for age in [10, 25, 30, 200] {
            assert_eq!(
                is_satisfied_by(&older_than_ast(age, "MyStore"), &store),
                Ok(older_than(&store, age, "MyStore")),
            );
        }
        for at in [None, Some(1_700_000_000), Some(5)] {
            assert_eq!(
                is_satisfied_by(&closed_at_ast(at), &store),
                Ok(closed_at(&store, at)),
                "closed_at({at:?}) of {:?}",
                store.name,
            );
        }
        for at in [0, 1_700_000_000, 1_700_000_001] {
            for (name, function, tree) in [
                (
                    "closed_before",
                    closed_before as fn(&Store, i64) -> bool,
                    closed_before_ast(at),
                ),
                (
                    "not_closed_before",
                    not_closed_before,
                    not_closed_before_ast(at),
                ),
                (
                    "open_or_closed_after",
                    open_or_closed_after,
                    open_or_closed_after_ast(at),
                ),
            ] {
                assert_eq!(
                    is_satisfied_by(&tree, &store),
                    Ok(function(&store, at)),
                    "{name}({at}) of {:?}",
                    store.name,
                );
            }
        }
        for at in [None, Some(0), Some(1_700_000_000), Some(1_700_000_001)] {
            for (name, function, tree) in [
                (
                    "closed_after",
                    closed_after as fn(&Store, Option<i64>) -> bool,
                    closed_after_ast(at),
                ),
                (
                    "not_closed_after",
                    not_closed_after,
                    not_closed_after_ast(at),
                ),
                (
                    "closed_before_if_asked",
                    closed_before_if_asked,
                    closed_before_if_asked_ast(at),
                ),
            ] {
                assert_eq!(
                    is_satisfied_by(&tree, &store),
                    Ok(function(&store, at)),
                    "{name}({at:?}) of {:?}",
                    store.name,
                );
            }
        }
        for min in [2, 500] {
            for closed_before in [None, Some(5), Some(1_700_000_001)] {
                let specification = DearAndOpen { min, closed_before };
                assert_eq!(
                    is_satisfied_by(&specification.is_satisfied_by_ast(), &store),
                    Ok(specification.is_satisfied_by(&store)),
                    "DearAndOpen({min}, {closed_before:?}) of {:?}",
                    store.name,
                );
            }
        }
        for alias in [None, Some("Pens"), Some("Inks")] {
            assert_eq!(
                is_satisfied_by(&known_as_ast(alias), &store),
                Ok(known_as(&store, alias)),
                "known_as({alias:?}) of {:?}",
                store.name,
            );
        }
    }
}

#[test]
fn the_tree_compiles_to_a_query() {
    let query = pg::compile(&has_dear_items_ast()).expect("compiled");
    assert_eq!(
        query.sql,
        r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "item_1"."price" > $1 AND "item_1"."active")"#,
    );
    assert_eq!(query.params, [Value::Int(500)]);
    assert_eq!(
        pg::compile(&all_items_active_ast()).expect("compiled").sql,
        r#"NOT EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE NOT "item_1"."active")"#,
    );
    // What an `Option` holds, of a parameter and of a member: a none is a
    // null constant, which has no neighbour to take its type from.
    let query = pg::compile(&not_closed_after_ast(None)).expect("compiled");
    assert_eq!(
        query.sql,
        r#"NOT ($1::text IS NOT NULL AND "closed_at" IS NOT NULL AND "closed_at" > $2)"#,
    );
    assert_eq!(query.params, [Value::Null, Value::Null]);
    assert_eq!(
        pg::compile(&closed_before_if_asked_ast(Some(5)))
            .expect("compiled")
            .sql,
        r#"$1::bigint IS NULL OR "closed_at" IS NOT NULL AND "closed_at" < $2"#,
    );
    // The tree of a specification type, with its fields as the constants.
    let query = pg::compile(
        &DearAndOpen {
            min: 500,
            closed_before: None,
        }
        .is_satisfied_by_ast(),
    )
    .expect("compiled");
    assert_eq!(
        query.sql,
        r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "item_1"."price" > $1) AND ($2::text IS NULL OR "closed_at" IS NOT NULL AND "closed_at" < $3)"#,
    );
    assert_eq!(query.params, [Value::Int(500), Value::Null, Value::Null]);
}
