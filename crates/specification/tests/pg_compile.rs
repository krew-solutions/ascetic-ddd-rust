//! The text of compiled queries. The expected strings of the first tests
//! are those of the Go `postgresql_visitor_test`, `postgresql_wildcard_test`,
//! `schema_test` and `compile_test`, names in Go's case as they are there
//! and, as they are not there, between quotes;
//! the later tests are where this compiler differs from the sources.

use ascetic_ddd_specification::ast::{
    add, all, and, and_all, any, div, equal, field, greater_than, greater_than_equal, is,
    is_not_null, is_null, left_shift, less_than, mul, neg, not, not_equal, or, right_shift, sub,
    value,
};
use ascetic_ddd_specification::pg::{CompileError, Compiler, ForeignKey, Query, Schema, compile};
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
    let schema =
        Schema::new("stores")
            .alias("s")
            .foreign_key("store_items", "store_id", "stores", "id");
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
    } // In a table of its own an item is a row as well, and its column a composite.
    assert_eq!(
        sql_with(&schema, &any("store_items", maker(&["name"]))),
        r#"EXISTS (SELECT 1 FROM "store_items" AS "store_item_1" WHERE "store_item_1"."store_id" = "s"."id" AND ("store_item_1"."maker")."name" = $1)"#,
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
    let owner_name = || field(Path::item("owner_id").child("name"));
    let named = |name: &str| equal(owner_name(), value(name));
    // Whether the items are an array or a table, their owner is a table. A
    // row of an array has no table: it is named by the array's column.
    let embedded =
        Schema::new("stores")
            .alias("s")
            .foreign_key("stores.items", "owner_id", "owners", "id");
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
        .foreign_key("store_items", "store_id", "stores", "id")
        .foreign_key("store_items", "owner_id", "owners", "id");
    assert_eq!(
        sql_with(&relational, &any("store_items", named("ann"))),
        concat!(
            r#"EXISTS (SELECT 1 FROM "store_items" AS "store_item_1" "#,
            r#"WHERE "store_item_1"."store_id" = "s"."id" AND "#,
            r#"(SELECT "owner_2"."name" FROM "owners" AS "owner_2" "#,
            r#"WHERE "owner_2"."id" = "store_item_1"."owner_id") = $1)"#,
        ),
    );
    // A key of two columns; and what is inside the owner's row is a composite.    // A key of two columns is named by either of them, unless another key
    // has it too.
    let composite_key = Schema::new("stores").alias("s").key(
        ForeignKey::new("stores.items", "tenant_id", "public.owners", "tenant_id")
            .and("owner_id", "id"),
    );
    let city = field(Path::item("owner_id").child("address").child("city"));
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
        .foreign_key("stores", "owner_id", "owners", "id")
        .foreign_key("stores.items", "owner_id", "owners", "id");
    assert_eq!(
        sql_with(
            &of_both,
            &any("items", equal(owner_name(), field("owner_id.name")))
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
    let qualified =
        Schema::new("public.stores").foreign_key("public.stores", "owner_id", "owners", "id");
    assert_eq!(
        sql_with(&qualified, &equal(field("owner_id.name"), value("x"))),
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

/// From the candidate a path of two names is a qualified name, `"s"."price"`:
/// an object under the root is a table's alias. So a Value Object kept in
/// the candidate's row as a composite column could not be reached:
/// `"address"."city"` is a table PostgreSQL does not have. The schema says
/// which columns are composites, as it says which are keys, and a path
/// through one is a member of it; an undeclared name stays a qualifier. The
/// rows are in `tests/pg.rs`.
#[test]
fn a_composite_column_of_the_candidate_is_declared() {
    let city = || field("address.city");
    let schema = Schema::new("stores")
        .alias("s")
        .composite("stores", "address");
    assert_eq!(
        sql_with(&schema, &equal(city(), value("x"))),
        r#"("s"."address")."city" = $1"#
    );
    assert_eq!(
        sql_with(&schema, &is_null(field("address.country.code"))),
        r#"(("s"."address")."country")."code" IS NULL"#
    );
    // Inside a collection's predicate the candidate's, beside the item's.
    assert_eq!(
        sql_with(&schema, &any("items", equal(item("city"), city()))),
        r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "item_1"."city" = ("s"."address")."city")"#
    );
    // An undeclared name stays a qualifier; without an alias, the table's.
    assert_eq!(
        sql_with(&schema, &equal(field("owner.name"), value("x"))),
        r#""owner"."name" = $1"#
    );
    assert_eq!(
        sql_with(
            &Schema::new("stores").composite("stores", "address"),
            &equal(city(), value("x"))
        ),
        r#"("stores"."address")."city" = $1"#
    );
    // Without a schema there is no row to read a composite of; and a
    // composite column of another table is not the candidate's.
    assert_eq!(sql(&equal(city(), value("x"))), r#""address"."city" = $1"#);
    assert_eq!(
        sql_with(
            &Schema::new("stores").composite("items", "address"),
            &equal(city(), value("x"))
        ),
        r#""address"."city" = $1"#
    );
}

/// Of a composite `IS NULL` is true when all its members are null and
/// `IS NOT NULL` when none is, so a row with a null member is neither: the
/// SQL standard's null predicate over a row value, which PostgreSQL follows.
/// An `Option` of a Value Object is `Some` or `None` whatever its members
/// hold; `IS NOT NULL` of the column said otherwise of a `Some` with a null
/// inside. A null test of a column the schema declares a composite is of the
/// value as a whole, `IS DISTINCT FROM NULL`, as the manual advises; a `None`
/// is a null column, not a row of nulls. The rows are in `tests/pg.rs`.
#[test]
fn a_null_test_of_a_declared_composite_is_of_the_value_as_a_whole() {
    let schema = Schema::new("stores")
        .alias("s")
        .composite("stores", "discount");
    let discount = || field("discount");
    let percent = || field("discount.percent");
    for (specification, expected) in [
        (
            is_not_null(discount()),
            r#""discount" IS DISTINCT FROM NULL"#,
        ),
        (
            is_null(discount()),
            r#""discount" IS NOT DISTINCT FROM NULL"#,
        ),
        // The guard a macro writes, and its negation.
        (
            and(is_not_null(discount()), greater_than(percent(), value(10))),
            r#""discount" IS DISTINCT FROM NULL AND ("s"."discount")."percent" > $1"#,
        ),
        (
            not(is_null(discount())),
            r#"NOT "discount" IS NOT DISTINCT FROM NULL"#,
        ),
        // Inside a collection's predicate, qualified as the candidate's columns are.
        (
            any("items", is_null(discount())),
            r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "s"."discount" IS NOT DISTINCT FROM NULL)"#,
        ),
        // What is not declared is tested as it was: another column, a
        // scalar member of the composite.
        (is_null(field("price")), r#""price" IS NULL"#),
        (is_null(percent()), r#"("s"."discount")."percent" IS NULL"#),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
    // A row of the items array is named by the array's column, as it is to
    // a key; a row of a table by the table; a composite inside a composite
    // by the column.
    assert_eq!(
        sql_with(
            &Schema::new("stores").composite("stores.items", "maker"),
            &any("items", is_not_null(item("maker")))
        ),
        r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "item_1"."maker" IS DISTINCT FROM NULL)"#
    );
    assert_eq!(
        sql_with(
            &Schema::new("stores")
                .alias("s")
                .foreign_key("store_items", "store_id", "stores", "id")
                .composite("store_items", "maker"),
            &any("store_items", is_not_null(item("maker")))
        ),
        r#"EXISTS (SELECT 1 FROM "store_items" AS "store_item_1" WHERE "store_item_1"."store_id" = "s"."id" AND "store_item_1"."maker" IS DISTINCT FROM NULL)"#
    );
    assert_eq!(
        sql_with(
            &Schema::new("stores")
                .alias("s")
                .composite("stores", "discount")
                .composite("stores.discount", "country"),
            &is_null(field("discount.country"))
        ),
        r#"("s"."discount")."country" IS NOT DISTINCT FROM NULL"#
    );
    // Without a schema, and with the composite declared on another table.
    assert_eq!(sql(&is_null(discount())), r#""discount" IS NULL"#);
    assert_eq!(
        sql_with(
            &Schema::new("stores").composite("items", "discount"),
            &is_null(discount())
        ),
        r#""discount" IS NULL"#
    );
}

#[test]
fn a_relational_collection_is_joined_by_its_keys() {
    let stores = || Schema::new("stores").alias("s");
    // The tree names a collection by its table.
    let dear: Spec = any("items", greater_than(item("Price"), value(500)));
    for (schema, specification, expected) in [
        (
            stores().foreign_key("items", "store_id", "stores", "id"),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."store_id" = "s"."id" AND "item_1"."Price" > $1)"#,
        ),
        (
            stores().key(
                ForeignKey::new("items", "tenant_id", "stores", "tenant_id").and("store_id", "id"),
            ),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."tenant_id" = "s"."tenant_id" AND "item_1"."store_id" = "s"."id" AND "item_1"."Price" > $1)"#,
        ),
        // Without an alias the root row goes by its table; and a table may
        // carry its schema, in the tree as in the key.
        (
            Schema::new("stores").foreign_key("public.items", "store_id", "stores", "id"),
            any("public.items", greater_than(item("Price"), value(500))),
            r#"EXISTS (SELECT 1 FROM "public"."items" AS "item_1" WHERE "item_1"."store_id" = "stores"."id" AND "item_1"."Price" > $1)"#,
        ),
        (
            Schema::new("public.stores").foreign_key(
                "public.items",
                "store_id",
                "public.stores",
                "id",
            ),
            any("public.items", greater_than(item("Price"), value(500))),
            r#"EXISTS (SELECT 1 FROM "public"."items" AS "item_1" WHERE "item_1"."store_id" = "public"."stores"."id" AND "item_1"."Price" > $1)"#,
        ),
        // A name that is no key's, and no table's with a key to the row, is
        // an array in the row.
        (
            stores().foreign_key("orders", "store_id", "stores", "id"),
            dear.clone(),
            r#"EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "item_1"."Price" > $1)"#,
        ),
        // A collection inside a collection joins to the row it is inside of:
        // the key of its table that references that row's table.
        (
            stores()
                .foreign_key("categories", "store_id", "stores", "id")
                .foreign_key("items", "category_id", "categories", "id"),
            any(
                "categories",
                any(Path::item("items"), greater_than(item("Price"), value(500))),
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
        .foreign_key("store_items", "store_id", "stores", "id")
        .foreign_key("categories", "store_id", "stores", "id");
    let of_store: Spec = any("store_items", item("Active"));
    let of_category: Spec = any("categories", any(Path::item("items"), item("Active")));
    assert_eq!(
        sql_with(&schema, &of_store),
        r#"EXISTS (SELECT 1 FROM "store_items" AS "store_item_1" WHERE "store_item_1"."store_id" = "s"."id" AND "store_item_1"."Active")"#,
    );
    // No key of a table `items` references categories: an array in the row.
    assert_eq!(
        sql_with(&schema, &of_category),
        r#"EXISTS (SELECT 1 FROM "categories" AS "category_1" WHERE "category_1"."store_id" = "s"."id" AND EXISTS (SELECT 1 FROM unnest("category_1"."items") AS "item_2" WHERE "item_2"."Active"))"#,
    );
}

#[test]
fn a_collection_of_the_candidate_inside_another_joins_to_the_root() {
    let schema = Schema::new("stores")
        .alias("s")
        .foreign_key("items", "store_id", "stores", "id")
        .foreign_key("tags", "store_id", "stores", "id");
    let specification: Spec = any("items", any("tags", equal(item("Name"), value("sale"))));
    assert_eq!(
        sql_with(&schema, &specification),
        r#"EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."store_id" = "s"."id" AND EXISTS (SELECT 1 FROM "tags" AS "tag_2" WHERE "tag_2"."store_id" = "s"."id" AND "tag_2"."Name" = $1))"#,
    );
}

#[test]
fn the_predicate_of_a_relational_collection_stays_inside_its_keys() {
    let schema = Schema::new("stores")
        .alias("s")
        .foreign_key("items", "store_id", "stores", "id");
    let specification: Spec = any(
        "items",
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
        (
            left_shift(add(a(), b()), c()),
            r#""a" + "b" << "c"::integer"#,
        ),
        (
            add(a(), left_shift(b(), c())),
            r#""a" + ("b" << "c"::integer)"#,
        ),
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
/// PostgreSQL shifts by an `integer` and by nothing else: `bigint << bigint`
/// is "operator does not exist", and a column is a `bigint` more often than
/// not. A constant as the count is inferred by the server from the operator,
/// and where nothing stands beside it was said an integer already; a column
/// or an expression as the count has a type of its own, which the server will
/// not convert, so it is cast. A cast binds tighter than any operator, so
/// what is not an atom is parenthesised. The rows are in `tests/pg.rs`.
#[test]
fn the_count_of_a_shift_is_an_integer() {
    let (a, b, c): (Make, Make, Make) = (|| field("a"), || field("b"), || field("c"));
    for (specification, expected) in [
        // A column or an expression is cast.
        (left_shift(a(), b()), r#""a" << "b"::integer"#),
        (
            right_shift(a(), add(b(), value(1))),
            r#""a" >> ("b" + $1)::integer"#,
        ),
        (
            left_shift(a(), left_shift(b(), c())),
            r#""a" << ("b" << "c"::integer)::integer"#,
        ),
        (left_shift(a(), neg(b())), r#""a" << (-"b")::integer"#),
        (
            left_shift(add(a(), b()), c()),
            r#""a" + "b" << "c"::integer"#,
        ),
        // A constant is inferred, as it was.
        (left_shift(a(), value(3)), r#""a" << $1"#),
        (left_shift(value(1), value(4)), "$1::bigint << $2::integer"),
        (right_shift(value(64), b()), r#"$1 >> "b"::integer"#),
    ] {
        assert_eq!(sql(&specification), expected);
    }
}

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
    let schema = Schema::new("stores").foreign_key("items; --", "store_id", "stores", "id");
    assert_eq!(
        Compiler::new()
            .schema(&schema)
            .compile::<Value>(&any("items; --", value(true))),
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
        .foreign_key("categories", "shop_id", "shops", "id")
        .foreign_key("products", "category_id", "categories", "id");
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

/// A schema is the foreign keys of a storage and nothing of any query. A
/// tree names a collection by its table, and where two keys of that table
/// reference the row it is named from - the transfers from an account and
/// the transfers to it - by the key's name, which is what PostgreSQL calls
/// it. An object is named by the key's column. A row of an array, which has
/// no table, is named by the array's column; and what the compiler calls a
/// row in a query is its own.
#[test]
fn a_schema_is_the_foreign_keys_of_the_storage() {
    let schema = Schema::new("accounts")
        .alias("a")
        .foreign_key("transfers", "from_account_id", "accounts", "id")
        .foreign_key("transfers", "to_account_id", "accounts", "id")
        .foreign_key("accounts", "owner_id", "owners", "id")
        .foreign_key("accounts.cards", "issuer_id", "banks", "id");
    let over = |what: &str| any(what, greater_than(item("amount"), value(100)));
    assert_eq!(
        sql_with(&schema, &over("transfers_from_account_id_fkey")),
        r#"EXISTS (SELECT 1 FROM "transfers" AS "transfer_1" WHERE "transfer_1"."from_account_id" = "a"."id" AND "transfer_1"."amount" > $1)"#,
    );
    assert_eq!(
        sql_with(&schema, &over("transfers_to_account_id_fkey")),
        r#"EXISTS (SELECT 1 FROM "transfers" AS "transfer_1" WHERE "transfer_1"."to_account_id" = "a"."id" AND "transfer_1"."amount" > $1)"#,
    );
    // By the table alone, the name fits two keys.
    assert_eq!(
        Compiler::new().schema(&schema).compile(&over("transfers")),
        Err(CompileError::AmbiguousKey(
            "transfers has 2 keys to accounts: transfers_from_account_id_fkey, \
             transfers_to_account_id_fkey; name the key"
                .to_owned()
        )),
    );
    // A key given a name goes by it.
    let named = Schema::new("accounts")
        .alias("a")
        .key(ForeignKey::new("transfers", "from_account_id", "accounts", "id").named("outgoing"));
    assert_eq!(
        sql_with(&named, &over("outgoing")),
        r#"EXISTS (SELECT 1 FROM "transfers" AS "transfer_1" WHERE "transfer_1"."from_account_id" = "a"."id" AND "transfer_1"."amount" > $1)"#,
    );
    // A key named where it does not go: the tree stands in the account's row.
    assert_eq!(
        Compiler::new()
            .schema(&schema)
            .compile(&any("accounts_owner_id_fkey", item("x"))),
        Err(CompileError::WrongKey(
            "the key accounts_owner_id_fkey references owners, not accounts".to_owned()
        )),
    );
    // A key on a row of an array.
    assert_eq!(
        sql_with(
            &schema,
            &any(
                "cards",
                equal(field(Path::item("issuer_id").child("name")), value("x"))
            )
        ),
        r#"EXISTS (SELECT 1 FROM unnest("cards") AS "card_1" WHERE (SELECT "bank_2"."name" FROM "banks" AS "bank_2" WHERE "bank_2"."id" = "card_1"."issuer_id") = $1)"#,
    );
    // A column of two keys.
    let shared = Schema::new("stores")
        .key(ForeignKey::new("stores", "tenant_id", "tenants", "id"))
        .key(ForeignKey::new("stores", "tenant_id", "owners", "tenant_id").and("owner_id", "id"));
    assert_eq!(
        Compiler::new()
            .schema(&shared)
            .compile::<Value>(&equal(field("tenant_id.name"), value("x"))),
        Err(CompileError::AmbiguousKey(
            "tenant_id is a column of 2 keys of stores: stores_tenant_id_fkey, \
             stores_tenant_id_owner_id_fkey; name the key"
                .to_owned()
        )),
    );
    assert_eq!(
        sql_with(&shared, &equal(field("owner_id.name"), value("x"))),
        r#"(SELECT "owner_1"."name" FROM "owners" AS "owner_1" WHERE "owner_1"."tenant_id" = "stores"."tenant_id" AND "owner_1"."id" = "stores"."owner_id") = $1"#,
    );
}
