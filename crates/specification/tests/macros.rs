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
}

struct Category {
    items: Vec<Item>,
}

struct Store {
    name: String,
    rating: f64,
    active: bool,
    closed_at: Option<i64>,
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
    let item = |name: &str, price, active| Item {
        name: name.to_owned(),
        price,
        active,
    };
    vec![
        Store {
            name: "MyStore".to_owned(),
            rating: 4.0,
            active: true,
            closed_at: None,
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
            owner: Profile { age: 125 },
            items: vec![],
            categories: vec![],
        },
    ]
}

#[test]
fn the_function_and_its_tree_agree() {
    type Predicate = fn(&Store) -> bool;
    let predicates: [(&str, Predicate, Spec); 10] = [
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
    }
}

#[test]
fn the_tree_compiles_to_a_query() {
    let query = pg::compile(&has_dear_items_ast()).expect("compiled");
    assert_eq!(
        query.sql,
        "EXISTS (SELECT 1 FROM unnest(items) AS item_1 WHERE item_1.price > $1 AND item_1.active)",
    );
    assert_eq!(query.params, [Value::Int(500)]);
    assert_eq!(
        pg::compile(&all_items_active_ast()).expect("compiled").sql,
        "NOT EXISTS (SELECT 1 FROM unnest(items) AS item_1 WHERE NOT item_1.active)",
    );
}
