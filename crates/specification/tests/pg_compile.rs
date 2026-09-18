//! The text of compiled queries. The expected strings of the first tests
//! are those of the Go `postgresql_visitor_test`, `postgresql_wildcard_test`,
//! `schema_test` and `compile_test`, names in Go's case as they are there;
//! the later tests are where this compiler differs from the sources.

use ascetic_ddd_specification::ast::{
    add, all, and, and_all, any, div, equal, field, greater_than, greater_than_equal, is,
    is_not_null, is_null, left_shift, less_than, mul, neg, not, not_equal, or, sub, value,
};
use ascetic_ddd_specification::pg::{CompileError, Compiler, Query, Relation, Schema, compile};
use ascetic_ddd_specification::{Expr, Path, Value};

type Spec = Expr<Value>;
/// What makes a fresh copy of a tree: a tree is moved into the one built of it.
type Make = fn() -> Spec;

fn sql(specification: &Spec) -> String {
    compile(specification)
        .unwrap_or_else(|error| panic!("{error}"))
        .sql
}

fn sql_with(schema: &Schema, specification: &Spec) -> String {
    Compiler::new()
        .schema(schema)
        .compile(specification)
        .unwrap_or_else(|error| panic!("{error}"))
        .sql
}

fn item(name: &str) -> Spec {
    field(Path::item(name))
}

#[test]
fn fields_values_and_operators() {
    assert_eq!(
        compile::<Value>(&greater_than_equal(field("age"), value(18))),
        Ok(Query {
            sql: "age >= $1".to_owned(),
            params: vec![Value::Int(18)],
        }),
    );
    for (specification, expected) in [
        (field("users.name"), "users.name"),
        (value(1), "$1"),
        (
            greater_than_equal(field("user.profile.age"), value(18)),
            "user.profile.age >= $1",
        ),
        (is_null(field("deleted_at")), "deleted_at IS NULL"),
        (is_not_null(field("created_at")), "created_at IS NOT NULL"),
        (not(less_than(field("age"), value(18))), "NOT age < $1"),
        (
            and(
                equal(field("active"), value(true)),
                greater_than(field("age"), value(18)),
            ),
            "active = $1 AND age > $2",
        ),
        (not_equal(field("a"), value(1)), "a != $1"),
        (
            greater_than(sub(field("price"), field("discount")), value(100)),
            "price - discount > $1",
        ),
        (
            or(
                and(
                    equal(field("active"), value(true)),
                    greater_than_equal(field("age"), value(18)),
                ),
                equal(field("premium"), value(true)),
            ),
            "active = $1 AND age >= $2 OR premium = $3",
        ),
    ] {
        assert_eq!(sql(&specification), expected);
    }
}

#[test]
fn parameters_are_numbered_in_order_and_from_an_offset() {
    let specification: Spec = and(equal(field("a"), value("x")), equal(field("b"), value(2)));
    let query = Compiler::new().offset(2).compile(&specification);
    assert_eq!(
        query,
        Ok(Query {
            sql: "a = $3 AND b = $4".to_owned(),
            params: vec![Value::from("x"), Value::Int(2)],
        }),
    );
}

#[test]
fn an_embedded_collection_is_unnested() {
    let dear = |price: i64| greater_than(item("Price"), value(price));
    for (specification, expected) in [
        (
            any("Items", dear(500)),
            "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)",
        ),
        (
            any("Items", item("Active")),
            "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Active)",
        ),
        (
            any(
                "Items",
                and_all(
                    dear(500),
                    [item("Active"), greater_than(item("Stock"), value(0))],
                ),
            ),
            "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1 AND item_1.Active AND item_1.Stock > $2)",
        ),
        (
            and(field("Active"), any("Items", dear(500))),
            "Active AND EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)",
        ),
        (
            not(any("Items", dear(500))),
            "NOT EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)",
        ),
        (
            any(
                "Items",
                greater_than(sub(item("Price"), value(100)), value(400)),
            ),
            "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price - $1 > $2)",
        ),
        (
            and_all(
                field("Active"),
                [
                    any("Items", dear(500)),
                    any("Items", less_than(item("Price"), value(100))),
                ],
            ),
            "Active AND EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1) AND EXISTS (SELECT 1 FROM unnest(Items) AS item_2 WHERE item_2.Price < $2)",
        ),
        (
            any("Categories", any(Path::item("Items"), dear(500))),
            "EXISTS (SELECT 1 FROM unnest(Categories) AS category_1 WHERE EXISTS (SELECT 1 FROM unnest(category_1.Items) AS item_2 WHERE item_2.Price > $1))",
        ),
        (
            any(
                "Categories",
                and(item("Active"), any(Path::item("Items"), dear(500))),
            ),
            "EXISTS (SELECT 1 FROM unnest(Categories) AS category_1 WHERE category_1.Active AND EXISTS (SELECT 1 FROM unnest(category_1.Items) AS item_2 WHERE item_2.Price > $1))",
        ),
        (
            any(
                "Regions",
                any(
                    Path::item("Categories"),
                    any(Path::item("Items"), dear(500)),
                ),
            ),
            "EXISTS (SELECT 1 FROM unnest(Regions) AS region_1 WHERE EXISTS (SELECT 1 FROM unnest(region_1.Categories) AS category_2 WHERE EXISTS (SELECT 1 FROM unnest(category_2.Items) AS item_3 WHERE item_3.Price > $1)))",
        ),
        (
            any("Store.Items", dear(500)),
            "EXISTS (SELECT 1 FROM unnest(Store.Items) AS item_1 WHERE item_1.Price > $1)",
        ),
    ] {
        assert_eq!(sql(&specification), expected);
    }
}

#[test]
fn a_relational_collection_is_joined_by_its_keys() {
    let stores = || Schema::new("stores").alias("s");
    let dear: Spec = any("Items", greater_than(item("Price"), value(500)));
    for (schema, specification, expected) in [
        (
            stores().relational("Items", Relation::new("items", "store_id", "id")),
            dear.clone(),
            "EXISTS (SELECT 1 FROM items AS item_1 WHERE item_1.store_id = s.id AND item_1.Price > $1)",
        ),
        (
            stores().relational(
                "Items",
                Relation::new("items", "tenant_id", "tenant_id").and("store_id", "id"),
            ),
            dear.clone(),
            "EXISTS (SELECT 1 FROM items AS item_1 WHERE item_1.tenant_id = s.tenant_id AND item_1.store_id = s.id AND item_1.Price > $1)",
        ),
        (
            stores().relational(
                "Items",
                Relation::new("store_items", "store_id", "id").alias("si"),
            ),
            dear.clone(),
            "EXISTS (SELECT 1 FROM store_items AS si_1 WHERE si_1.store_id = s.id AND si_1.Price > $1)",
        ),
        // Without an alias the root row goes by its table.
        (
            Schema::new("stores")
                .relational("Items", Relation::new("public.items", "store_id", "id")),
            dear.clone(),
            "EXISTS (SELECT 1 FROM public.items AS item_1 WHERE item_1.store_id = stores.id AND item_1.Price > $1)",
        ),
        // What the schema says embedded, and what it does not mention, is.
        (
            stores().embedded("Items"),
            dear.clone(),
            "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)",
        ),
        (
            stores().relational("Orders", Relation::new("orders", "store_id", "id")),
            dear.clone(),
            "EXISTS (SELECT 1 FROM unnest(Items) AS item_1 WHERE item_1.Price > $1)",
        ),
        // A collection inside a collection joins to the row it is inside of,
        // and is looked up by the whole of its path.
        (
            stores()
                .relational("Categories", Relation::new("categories", "store_id", "id"))
                .relational(
                    "Categories.Items",
                    Relation::new("items", "category_id", "id"),
                ),
            any(
                "Categories",
                any(Path::item("Items"), greater_than(item("Price"), value(500))),
            ),
            "EXISTS (SELECT 1 FROM categories AS category_1 WHERE category_1.store_id = s.id AND EXISTS (SELECT 1 FROM items AS item_2 WHERE item_2.category_id = category_1.id AND item_2.Price > $1))",
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
}

#[test]
fn two_collections_of_one_name_are_two_collections() {
    let schema = Schema::new("stores")
        .alias("s")
        .relational("Items", Relation::new("store_items", "store_id", "id"))
        .relational("Categories", Relation::new("categories", "store_id", "id"));
    let of_store: Spec = any("Items", item("Active"));
    let of_category: Spec = any("Categories", any(Path::item("Items"), item("Active")));
    assert_eq!(
        sql_with(&schema, &of_store),
        "EXISTS (SELECT 1 FROM store_items AS item_1 WHERE item_1.store_id = s.id AND item_1.Active)",
    );
    // `Categories.Items` is not mentioned: embedded in the category's row.
    assert_eq!(
        sql_with(&schema, &of_category),
        "EXISTS (SELECT 1 FROM categories AS category_1 WHERE category_1.store_id = s.id AND EXISTS (SELECT 1 FROM unnest(category_1.Items) AS item_2 WHERE item_2.Active))",
    );
}

#[test]
fn a_collection_of_the_candidate_inside_another_joins_to_the_root() {
    let schema = Schema::new("stores")
        .alias("s")
        .relational("Items", Relation::new("items", "store_id", "id"))
        .relational("Tags", Relation::new("tags", "store_id", "id"));
    let specification: Spec = any("Items", any("Tags", equal(item("Name"), value("sale"))));
    assert_eq!(
        sql_with(&schema, &specification),
        "EXISTS (SELECT 1 FROM items AS item_1 WHERE item_1.store_id = s.id AND EXISTS (SELECT 1 FROM tags AS tag_2 WHERE tag_2.store_id = s.id AND tag_2.Name = $1))",
    );
}

#[test]
fn the_predicate_of_a_relational_collection_stays_inside_its_keys() {
    let schema = Schema::new("stores")
        .alias("s")
        .relational("Items", Relation::new("items", "store_id", "id"));
    let specification: Spec = any(
        "Items",
        or(item("Active"), greater_than(item("Price"), value(500))),
    );
    assert_eq!(
        sql_with(&schema, &specification),
        "EXISTS (SELECT 1 FROM items AS item_1 WHERE item_1.store_id = s.id AND (item_1.Active OR item_1.Price > $1))",
    );
}

#[test]
fn parentheses_keep_the_shape_of_the_tree() {
    let (a, b, c): (Make, Make, Make) = (|| field("a"), || field("b"), || field("c"));
    for (specification, expected) in [
        // Looser inside tighter.
        (and(or(a(), b()), c()), "(a OR b) AND c"),
        (or(and(a(), b()), c()), "a AND b OR c"),
        (mul(add(a(), b()), c()), "(a + b) * c"),
        (add(mul(a(), b()), c()), "a * b + c"),
        (not(and(a(), b())), "NOT (a AND b)"),
        (not(equal(a(), b())), "NOT a = b"),
        (is_null(or(a(), b())), "(a OR b) IS NULL"),
        (neg(add(a(), b())), "-(a + b)"),
        (left_shift(add(a(), b()), c()), "a + b << c"),
        (add(a(), left_shift(b(), c())), "a + (b << c)"),
        // As tight: by the side the operator groups to.
        (sub(sub(a(), b()), c()), "a - b - c"),
        (sub(a(), sub(b(), c())), "a - (b - c)"),
        (sub(a(), add(b(), c())), "a - (b + c)"),
        (div(a(), div(b(), c())), "a / (b / c)"),
        (div(mul(a(), b()), c()), "a * b / c"),
        // A comparison groups to neither side.
        (equal(equal(a(), b()), c()), "(a = b) = c"),
        (equal(a(), equal(b(), c())), "a = (b = c)"),
        (equal(is_null(a()), c()), "(a IS NULL) = c"),
        (is_null(is_null(a())), "(a IS NULL) IS NULL"),
        (is_null(equal(a(), b())), "a = b IS NULL"),
        // The connectives regroup freely.
        (and(a(), and(b(), c())), "a AND b AND c"),
        (or(a(), or(b(), c())), "a OR b OR c"),
        // Two minus signs are a comment.
        (neg(neg(a())), "-(-a)"),
        (sub(a(), neg(b())), "a - -b"),
        (not(not(a())), "NOT NOT a"),
        (
            all("items", item("active")),
            "NOT EXISTS (SELECT 1 FROM unnest(items) AS item_1 WHERE NOT item_1.active)",
        ),
    ] {
        assert_eq!(sql(&specification), expected);
    }
}

#[test]
fn is_takes_a_parameter() {
    assert_eq!(
        sql(&is(field("active"), value(true))),
        "active IS NOT DISTINCT FROM $1"
    );
    assert_eq!(
        sql(&equal(is(field("a"), field("b")), value(true))),
        "(a IS NOT DISTINCT FROM b) = $1",
    );
}

#[test]
fn a_name_that_is_not_an_identifier_is_refused() {
    let invalid = |name: &str| Err(CompileError::InvalidIdentifier(name.to_owned()));
    assert_eq!(
        compile::<Value>(&field("age; DROP TABLE users")),
        invalid("age; DROP TABLE users")
    );
    assert_eq!(compile::<Value>(&field("a..b")), invalid(""));
    assert_eq!(compile::<Value>(&field("1st")), invalid("1st"));
    assert_eq!(
        compile::<Value>(&any("items x", value(true))),
        invalid("items x")
    );
    let schema =
        Schema::new("stores").relational("items", Relation::new("items; --", "store_id", "id"));
    assert_eq!(
        Compiler::new()
            .schema(&schema)
            .compile::<Value>(&any("items", value(true))),
        invalid("items; --"),
    );
}

#[test]
fn the_item_is_only_inside_a_collection() {
    assert_eq!(
        compile::<Value>(&item("price")),
        Err(CompileError::NoCurrentItem)
    );
    assert_eq!(
        compile::<Value>(&any(Path::item("items"), value(true))),
        Err(CompileError::NoCurrentItem),
    );
}
