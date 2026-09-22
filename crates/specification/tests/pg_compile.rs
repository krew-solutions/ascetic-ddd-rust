//! The text of compiled queries. The expected strings of the first tests
//! are those of the Go `postgresql_visitor_test`, `postgresql_wildcard_test`,
//! `schema_test` and `compile_test`, names in Go's case as they are there
//! and, as they are not there, between quotes;
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
            sql: r#""age" >= $1"#.to_owned(),
            params: vec![Value::Int(18)],
        }),
    );
    for (specification, expected) in [
        (field("users.name"), r#""users"."name""#),
        (value(1), "$1"),
        (
            greater_than_equal(field("user.profile.age"), value(18)),
            r#""user"."profile"."age" >= $1"#,
        ),
        (is_null(field("deleted_at")), r#""deleted_at" IS NULL"#),
        (
            is_not_null(field("created_at")),
            r#""created_at" IS NOT NULL"#,
        ),
        (not(less_than(field("age"), value(18))), r#"NOT "age" < $1"#),
        (
            and(
                equal(field("active"), value(true)),
                greater_than(field("age"), value(18)),
            ),
            r#""active" = $1 AND "age" > $2"#,
        ),
        (not_equal(field("a"), value(1)), r#""a" != $1"#),
        (
            greater_than(sub(field("price"), field("discount")), value(100)),
            r#""price" - "discount" > $1"#,
        ),
        (
            or(
                and(
                    equal(field("active"), value(true)),
                    greater_than_equal(field("age"), value(18)),
                ),
                equal(field("premium"), value(true)),
            ),
            r#""active" = $1 AND "age" >= $2 OR "premium" = $3"#,
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
            sql: r#""a" = $3 AND "b" = $4"#.to_owned(),
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
            r#"EXISTS (SELECT 1 FROM unnest("Items") AS "item_1" WHERE "item_1"."Price" > $1)"#,
        ),
        (
            any("Items", item("Active")),
            r#"EXISTS (SELECT 1 FROM unnest("Items") AS "item_1" WHERE "item_1"."Active")"#,
        ),
        (
            any(
                "Items",
                and_all(
                    dear(500),
                    [item("Active"), greater_than(item("Stock"), value(0))],
                ),
            ),
            r#"EXISTS (SELECT 1 FROM unnest("Items") AS "item_1" WHERE "item_1"."Price" > $1 AND "item_1"."Active" AND "item_1"."Stock" > $2)"#,
        ),
        (
            and(field("Active"), any("Items", dear(500))),
            r#""Active" AND EXISTS (SELECT 1 FROM unnest("Items") AS "item_1" WHERE "item_1"."Price" > $1)"#,
        ),
        (
            not(any("Items", dear(500))),
            r#"NOT EXISTS (SELECT 1 FROM unnest("Items") AS "item_1" WHERE "item_1"."Price" > $1)"#,
        ),
        (
            any(
                "Items",
                greater_than(sub(item("Price"), value(100)), value(400)),
            ),
            r#"EXISTS (SELECT 1 FROM unnest("Items") AS "item_1" WHERE "item_1"."Price" - $1 > $2)"#,
        ),
        (
            and_all(
                field("Active"),
                [
                    any("Items", dear(500)),
                    any("Items", less_than(item("Price"), value(100))),
                ],
            ),
            r#""Active" AND EXISTS (SELECT 1 FROM unnest("Items") AS "item_1" WHERE "item_1"."Price" > $1) AND EXISTS (SELECT 1 FROM unnest("Items") AS "item_2" WHERE "item_2"."Price" < $2)"#,
        ),
        (
            any("Categories", any(Path::item("Items"), dear(500))),
            r#"EXISTS (SELECT 1 FROM unnest("Categories") AS "category_1" WHERE EXISTS (SELECT 1 FROM unnest("category_1"."Items") AS "item_2" WHERE "item_2"."Price" > $1))"#,
        ),
        (
            any(
                "Categories",
                and(item("Active"), any(Path::item("Items"), dear(500))),
            ),
            r#"EXISTS (SELECT 1 FROM unnest("Categories") AS "category_1" WHERE "category_1"."Active" AND EXISTS (SELECT 1 FROM unnest("category_1"."Items") AS "item_2" WHERE "item_2"."Price" > $1))"#,
        ),
        (
            any(
                "Regions",
                any(
                    Path::item("Categories"),
                    any(Path::item("Items"), dear(500)),
                ),
            ),
            r#"EXISTS (SELECT 1 FROM unnest("Regions") AS "region_1" WHERE EXISTS (SELECT 1 FROM unnest("region_1"."Categories") AS "category_2" WHERE EXISTS (SELECT 1 FROM unnest("category_2"."Items") AS "item_3" WHERE "item_3"."Price" > $1)))"#,
        ),
        (
            any("Store.Items", dear(500)),
            r#"EXISTS (SELECT 1 FROM unnest("Store"."Items") AS "item_1" WHERE "item_1"."Price" > $1)"#,
        ),
    ] {
        assert_eq!(sql(&specification), expected);
    }
}

/// A member of a Value Object inside an item is a member of a composite kept
/// in the item's row. With dots alone PostgreSQL reads a schema, a table and
/// a column, and says there is no such table; the sources, besides, drop the
/// item's alias from such a path and write `"maker"."name"`, which is the
/// column of another table if the query has one of that name.
#[test]
fn a_member_of_an_object_inside_an_item_is_a_member_of_a_composite() {
    let maker = |names: &[&str]| {
        let path = names
            .iter()
            .fold(Path::item("maker"), |path, name| path.child(*name));
        equal(field(path), value("x"))
    };
    let schema = Schema::new("stores")
        .alias("s")
        .relational("items", Relation::new("store_items", "store_id", "id"));
    for (specification, expected) in [
        (
            any("items", maker(&["name"])),
            r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE ("item_1"."maker")."name" = $1)"#,
        ),
        (
            any("items", maker(&["country", "code"])),
            r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE (("item_1"."maker")."country")."code" = $1)"#,
        ),
        // The item of an inner collection, which is itself a member of the outer item.
        (
            any("items", any(Path::item("parts"), maker(&["name"]))),
            r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE EXISTS (SELECT 1 FROM unnest("item_1"."parts") AS "part_2" WHERE ("part_2"."maker")."name" = $1))"#,
        ),
    ] {
        assert_eq!(sql(&specification), expected);
    }
    // In a table of its own an item is a row as well, and its column a composite.
    assert_eq!(
        sql_with(&schema, &any("items", maker(&["name"]))),
        r#"EXISTS (SELECT 1 FROM "store_items" AS "item_1" WHERE "item_1"."store_id" = "s"."id" AND ("item_1"."maker")."name" = $1)"#,
    );
    // From the candidate the dots stay: a qualified name, `alias.column`.
    assert_eq!(
        sql(&equal(field("s.maker"), value("x"))),
        r#""s"."maker" = $1"#
    );
}

/// An object on the way to a member is looked up in the schema, as a
/// collection is. Kept in a table of its own it is read through its key, by a
/// subquery in the column's place: at most the one row the key names, and
/// null if there is none.
#[test]
fn a_member_of_an_object_kept_in_a_table_of_its_own_is_read_through_the_key() {
    let owner = || Relation::new("owners", "id", "owner_id");
    let owner_name = || field(Path::item("owner").child("name"));
    let named = |name: &str| equal(owner_name(), value(name));
    // Whether the items are an array or a table, their owner is a table.
    let embedded = Schema::new("stores")
        .alias("s")
        .relational("items.owner", owner());
    assert_eq!(
        sql_with(&embedded, &any("items", named("ann"))),
        concat!(
            r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "#,
            r#"(SELECT "owner_2"."name" FROM "owners" AS "owner_2" "#,
            r#"WHERE "owner_2"."id" = "item_1"."owner_id") = $1)"#,
        ),
    );
    let relational = Schema::new("stores")
        .alias("s")
        .relational("items", Relation::new("store_items", "store_id", "id"))
        .relational("items.owner", owner().alias("o"));
    assert_eq!(
        sql_with(&relational, &any("items", named("ann"))),
        concat!(
            r#"EXISTS (SELECT 1 FROM "store_items" AS "item_1" "#,
            r#"WHERE "item_1"."store_id" = "s"."id" AND "#,
            r#"(SELECT "o_2"."name" FROM "owners" AS "o_2" "#,
            r#"WHERE "o_2"."id" = "item_1"."owner_id") = $1)"#,
        ),
    );
    // A key of two columns; and what is inside the owner's row is a composite.
    let composite_key = Schema::new("stores").alias("s").relational(
        "items.owner",
        Relation::new("public.owners", "tenant_id", "tenant_id").and("id", "owner_id"),
    );
    let city = field(Path::item("owner").child("address").child("city"));
    assert_eq!(
        sql_with(&composite_key, &any("items", is_null(city))),
        concat!(
            r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "#,
            r#"(SELECT ("owner_2"."address")."city" FROM "public"."owners" AS "owner_2" "#,
            r#"WHERE "owner_2"."tenant_id" = "item_1"."tenant_id" "#,
            r#"AND "owner_2"."id" = "item_1"."owner_id") IS NULL)"#,
        ),
    );
    // Of the candidate itself, the key is the root row's; and each object
    // read so has an alias of its own.
    let of_both = Schema::new("stores")
        .alias("s")
        .relational("owner", owner())
        .relational("items.owner", owner());
    assert_eq!(
        sql_with(
            &of_both,
            &any("items", equal(owner_name(), field("owner.name")))
        ),
        concat!(
            r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "#,
            r#"(SELECT "owner_2"."name" FROM "owners" AS "owner_2" "#,
            r#"WHERE "owner_2"."id" = "item_1"."owner_id") = "#,
            r#"(SELECT "owner_3"."name" FROM "owners" AS "owner_3" "#,
            r#"WHERE "owner_3"."id" = "s"."owner_id"))"#,
        ),
    );
    // The root row by its table, which carries its schema.
    let qualified = Schema::new("public.stores").relational("owner", owner());
    assert_eq!(
        sql_with(&qualified, &equal(field("owner.name"), value("x"))),
        concat!(
            r#"(SELECT "owner_1"."name" FROM "owners" AS "owner_1" "#,
            r#"WHERE "owner_1"."id" = "public"."stores"."owner_id") = $1"#,
        ),
    );
    // What the schema does not mention stays what the dots have meant.
    assert_eq!(
        sql_with(&of_both, &equal(field("s.name"), value("x"))),
        r#""s"."name" = $1"#
    );
    assert_eq!(
        sql_with(
            &of_both,
            &any(
                "items",
                equal(field(Path::item("maker").child("name")), value("x"))
            )
        ),
        r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE ("item_1"."maker")."name" = $1)"#,
    );
}

#[test]
fn a_relational_collection_is_joined_by_its_keys() {
    let stores = || Schema::new("stores").alias("s");
    let dear: Spec = any("Items", greater_than(item("Price"), value(500)));
    for (schema, specification, expected) in [
        (
            stores().relational("Items", Relation::new("items", "store_id", "id")),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."store_id" = "s"."id" AND "item_1"."Price" > $1)"#,
        ),
        (
            stores().relational(
                "Items",
                Relation::new("items", "tenant_id", "tenant_id").and("store_id", "id"),
            ),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."tenant_id" = "s"."tenant_id" AND "item_1"."store_id" = "s"."id" AND "item_1"."Price" > $1)"#,
        ),
        (
            stores().relational(
                "Items",
                Relation::new("store_items", "store_id", "id").alias("si"),
            ),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM "store_items" AS "si_1" WHERE "si_1"."store_id" = "s"."id" AND "si_1"."Price" > $1)"#,
        ),
        // Without an alias the root row goes by its table.
        (
            Schema::new("stores")
                .relational("Items", Relation::new("public.items", "store_id", "id")),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM "public"."items" AS "item_1" WHERE "item_1"."store_id" = "stores"."id" AND "item_1"."Price" > $1)"#,
        ),
        // And the table may carry its schema: `identifier` refused the dot,
        // where the table of a collection went through `qualified`.
        (
            Schema::new("public.stores")
                .relational("Items", Relation::new("public.items", "store_id", "id")),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM "public"."items" AS "item_1" WHERE "item_1"."store_id" = "public"."stores"."id" AND "item_1"."Price" > $1)"#,
        ),
        // What the schema says embedded, and what it does not mention, is.
        (
            stores().embedded("Items"),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM unnest("Items") AS "item_1" WHERE "item_1"."Price" > $1)"#,
        ),
        (
            stores().relational("Orders", Relation::new("orders", "store_id", "id")),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM unnest("Items") AS "item_1" WHERE "item_1"."Price" > $1)"#,
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
            r#"EXISTS (SELECT 1 FROM "categories" AS "category_1" WHERE "category_1"."store_id" = "s"."id" AND EXISTS (SELECT 1 FROM "items" AS "item_2" WHERE "item_2"."category_id" = "category_1"."id" AND "item_2"."Price" > $1))"#,
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
        r#"EXISTS (SELECT 1 FROM "store_items" AS "item_1" WHERE "item_1"."store_id" = "s"."id" AND "item_1"."Active")"#,
    );
    // `Categories.Items` is not mentioned: embedded in the category's row.
    assert_eq!(
        sql_with(&schema, &of_category),
        r#"EXISTS (SELECT 1 FROM "categories" AS "category_1" WHERE "category_1"."store_id" = "s"."id" AND EXISTS (SELECT 1 FROM unnest("category_1"."Items") AS "item_2" WHERE "item_2"."Active"))"#,
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
        r#"EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."store_id" = "s"."id" AND EXISTS (SELECT 1 FROM "tags" AS "tag_2" WHERE "tag_2"."store_id" = "s"."id" AND "tag_2"."Name" = $1))"#,
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
        r#"EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."store_id" = "s"."id" AND ("item_1"."Active" OR "item_1"."Price" > $1))"#,
    );
}

#[test]
fn parentheses_keep_the_shape_of_the_tree() {
    let (a, b, c): (Make, Make, Make) = (|| field("a"), || field("b"), || field("c"));
    for (specification, expected) in [
        // Looser inside tighter.
        (and(or(a(), b()), c()), r#"("a" OR "b") AND "c""#),
        (or(and(a(), b()), c()), r#""a" AND "b" OR "c""#),
        (mul(add(a(), b()), c()), r#"("a" + "b") * "c""#),
        (add(mul(a(), b()), c()), r#""a" * "b" + "c""#),
        (not(and(a(), b())), r#"NOT ("a" AND "b")"#),
        (not(equal(a(), b())), r#"NOT "a" = "b""#),
        (is_null(or(a(), b())), r#"("a" OR "b") IS NULL"#),
        (neg(add(a(), b())), r#"-("a" + "b")"#),
        (left_shift(add(a(), b()), c()), r#""a" + "b" << "c""#),
        (add(a(), left_shift(b(), c())), r#""a" + ("b" << "c")"#),
        // As tight: by the side the operator groups to.
        (sub(sub(a(), b()), c()), r#""a" - "b" - "c""#),
        (sub(a(), sub(b(), c())), r#""a" - ("b" - "c")"#),
        (sub(a(), add(b(), c())), r#""a" - ("b" + "c")"#),
        (div(a(), div(b(), c())), r#""a" / ("b" / "c")"#),
        (div(mul(a(), b()), c()), r#""a" * "b" / "c""#),
        // A comparison groups to neither side.
        (equal(equal(a(), b()), c()), r#"("a" = "b") = "c""#),
        (equal(a(), equal(b(), c())), r#""a" = ("b" = "c")"#),
        (equal(is_null(a()), c()), r#"("a" IS NULL) = "c""#),
        (is_null(is_null(a())), r#"("a" IS NULL) IS NULL"#),
        (is_null(equal(a(), b())), r#""a" = "b" IS NULL"#),
        // The connectives regroup freely.
        (and(a(), and(b(), c())), r#""a" AND "b" AND "c""#),
        (or(a(), or(b(), c())), r#""a" OR "b" OR "c""#),
        // Two minus signs are a comment.
        (neg(neg(a())), r#"-(-"a")"#),
        (sub(a(), neg(b())), r#""a" - -"b""#),
        (not(not(a())), r#"NOT NOT "a""#),
        (
            all("items", item("active")),
            r#"NOT EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE NOT "item_1"."active")"#,
        ),
    ] {
        assert_eq!(sql(&specification), expected);
    }
}

#[test]
fn is_takes_a_parameter() {
    assert_eq!(
        sql(&is(field("active"), value(true))),
        r#""active" IS NOT DISTINCT FROM $1"#
    );
    assert_eq!(
        sql(&equal(is(field("a"), field("b")), value(true))),
        r#"("a" IS NOT DISTINCT FROM "b") = $1"#,
    );
}

/// A constant is a parameter, and the server finds its type from what stands
/// beside it. Where every operand of an operator is a constant there is
/// nothing beside it - "operator is not unique: unknown + unknown" - so there
/// the text says the type, by the kind of the value. Beside a column it does
/// not: the value adapts to the column, which a type said would take away.
#[test]
fn a_constant_with_nothing_beside_it_has_its_type_said() {
    let null = || Expr::Value(Value::Null);
    for (specification, expected) in [
        // Beside a column, or beside what has a type already: as it was.
        (greater_than(field("price"), value(1)), r#""price" > $1"#),
        (
            greater_than(add(field("price"), value(1)), value(2)),
            r#""price" + $1 > $2"#,
        ),
        // Both operands constants.
        (
            greater_than(field("price"), add(value(1), value(2))),
            r#""price" > $1::bigint + $2::bigint"#,
        ),
        (
            less_than(value(1), value(2.5)),
            "$1::bigint < $2::double precision",
        ),
        (equal(value("a"), value("b")), "$1::text = $2::text"),
        // What was typed so is a type for what stands beside it.
        (
            mul(add(value(1), value(2)), value(3)),
            "($1::bigint + $2::bigint) * $3",
        ),
        // PostgreSQL shifts a bigint by an integer.
        (left_shift(value(1), value(4)), "$1::bigint << $2::integer"),
        // Alone under its operator.
        (neg(value(5)), "-$1::bigint"),
        (not(value(true)), "NOT $1::boolean"),
        (is_null(value(7)), "$1::bigint IS NULL"),
        // A null has no kind. Beside a constant it takes that one's type from
        // the server; alone, what its operator is of.
        (add(null(), value(1)), "$1 + $2::bigint"),
        (add(null(), null()), "$1::bigint + $2::bigint"),
        (equal(null(), null()), "$1 = $2"),
        (is_null(null()), "$1::text IS NULL"),
        (neg(null()), "-$1::bigint"),
        (not(null()), "NOT $1"),
    ] {
        assert_eq!(sql(&specification), expected);
    }
}

#[test]
fn a_name_that_is_not_an_identifier_is_refused() {
    let invalid = |name: &str| Err(CompileError::InvalidIdentifier(name.to_owned()));
    assert_eq!(
        compile::<Value>(&field("age; DROP TABLE users")),
        invalid("age; DROP TABLE users")
    );
    assert_eq!(
        compile::<Value>(&field(r#"a" OR "b"#)),
        invalid(r#"a" OR "b"#)
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
    assert_eq!(
        compile::<Value>(&any(
            "items",
            greater_than(item("price"), field(Path::outer(1, "limit")))
        )),
        Err(CompileError::NoCurrentItem),
    );
}

// The item of an enclosing collection has an alias of its own, which the
// inner query names as SQL lets it: `Root::Item(1)` is that alias.
#[test]
fn the_item_of_an_enclosing_collection_is_its_alias() {
    let over_its_category = any(
        "categories",
        any(
            Path::item("products"),
            greater_than(item("price"), field(Path::outer(1, "limit"))),
        ),
    );
    assert_eq!(
        sql(&over_its_category),
        r#"EXISTS (SELECT 1 FROM unnest("categories") AS "category_1" WHERE EXISTS (SELECT 1 FROM unnest("category_1"."products") AS "product_2" WHERE "product_2"."price" > "category_1"."limit"))"#,
    );
    // In tables of their own, the enclosing row is the one the keys point at,
    // and its columns are named the same way.
    let schema = Schema::new("shops")
        .relational("categories", Relation::new("categories", "shop_id", "id"))
        .relational(
            "categories.products",
            Relation::new("products", "category_id", "id"),
        );
    assert_eq!(
        sql_with(&schema, &over_its_category),
        r#"EXISTS (SELECT 1 FROM "categories" AS "category_1" WHERE "category_1"."shop_id" = "shops"."id" AND EXISTS (SELECT 1 FROM "products" AS "product_2" WHERE "product_2"."category_id" = "category_1"."id" AND "product_2"."price" > "category_1"."limit"))"#,
    );
    // Two collections out, the candidate's own row beside.
    let three_deep = any(
        "categories",
        any(
            Path::item("products"),
            any(
                Path::item("tags"),
                and(
                    greater_than(item("weight"), field(Path::outer(2, "limit"))),
                    less_than(field(Path::outer(1, "price")), field("limit")),
                ),
            ),
        ),
    );
    assert_eq!(
        sql_with(&Schema::new("shops"), &three_deep),
        r#"EXISTS (SELECT 1 FROM unnest("categories") AS "category_1" WHERE EXISTS (SELECT 1 FROM unnest("category_1"."products") AS "product_2" WHERE EXISTS (SELECT 1 FROM unnest("product_2"."tags") AS "tag_3" WHERE "tag_3"."weight" > "category_1"."limit" AND "product_2"."price" < "shops"."limit")))"#,
    );
}

// Inside a collection's predicate the candidate's column is qualified with
// its row. Unqualified, PostgreSQL read it from the innermost row that has a
// column of that name: a category with a `limit` of its own hid the shop's,
// and the query selected other rows than the evaluator was satisfied by. The
// row is what the schema calls it, so without a schema there is no query.
#[test]
fn the_candidates_column_inside_a_predicate_is_qualified_with_its_row() {
    let over_the_shops_limit = any("categories", greater_than(item("limit"), field("limit")));
    assert_eq!(
        sql_with(&Schema::new("shops"), &over_the_shops_limit),
        r#"EXISTS (SELECT 1 FROM unnest("categories") AS "category_1" WHERE "category_1"."limit" > "shops"."limit")"#,
    );
    assert_eq!(
        sql_with(
            &Schema::new("public.shops").alias("s"),
            &over_the_shops_limit
        ),
        r#"EXISTS (SELECT 1 FROM unnest("categories") AS "category_1" WHERE "category_1"."limit" > "s"."limit")"#,
    );
    assert_eq!(
        compile::<Value>(&over_the_shops_limit),
        Err(CompileError::NoTable),
    );
    // A name of several parts the author qualified, and it stays as written;
    // outside a collection's predicate a name is unqualified, as it was.
    assert_eq!(
        sql(&any(
            "categories",
            greater_than(item("limit"), field("s.limit"))
        )),
        r#"EXISTS (SELECT 1 FROM unnest("categories") AS "category_1" WHERE "category_1"."limit" > "s"."limit")"#,
    );
    assert_eq!(
        sql(&greater_than(field("limit"), value(1))),
        r#""limit" > $1"#
    );
}
