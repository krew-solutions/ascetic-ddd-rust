//! The text of compiled queries. The expected strings of the first tests
//! are those of the Go `postgresql_visitor_test`, `postgresql_wildcard_test`,
//! `schema_test` and `compile_test`, names in Go's case as they are there
//! and, as they are not there, between quotes;
//! the later tests are where this compiler differs from the sources.

use ascetic_ddd_specification::ast::{
    add, all, and, and_all, any, div, equal, field, greater_than, greater_than_equal, is,
    is_not_null, is_null, left_shift, less_than, mul, neg, not, not_equal, or, outer_item,
    right_shift, sub, value,
};
use ascetic_ddd_specification::pg::{
    CompileError, Compiler, ForeignKey, Kind, Layout, Query, Schema, compile,
};
use ascetic_ddd_specification::{Date, Expr, Path, Value};

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
        // A UUID, which a template writes as a string and a constant of the
        // domain has as itself.
        (
            equal(
                value(uuid::Uuid::from_u128(1)),
                value(uuid::Uuid::from_u128(2)),
            ),
            "$1::uuid = $2::uuid",
        ),
        (
            equal(value(Date::from_days(0)), value(Date::from_days(1))),
            "$1::date = $2::date",
        ),
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

/// PostgreSQL's `text` holds no NUL: a parameter with one in it is "invalid
/// byte sequence for encoding UTF8: 0x00" from the server, at execution - a
/// failure of the query where the application expects one of the data. A
/// text with a NUL is refused where every value meets the server, by the
/// compiler; in memory it is a string like any other. The server's refusal
/// is in `tests/pg.rs`.
#[test]
fn a_text_with_a_nul_is_no_text_postgresql_has() {
    let nul: Spec = equal(field("name"), value("a\u{0}b"));
    assert_eq!(compile(&nul).unwrap_err(), CompileError::NulInText);
    assert_eq!(
        compile(&nul).unwrap_err().to_string(),
        "a text with a NUL (U+0000) in it is no text PostgreSQL has"
    );
    assert_eq!(
        compile(&any("items", equal(item("name"), value("\u{0}")))).unwrap_err(),
        CompileError::NulInText
    );
    let plain: Spec = equal(field("name"), value("ab"));
    assert!(compile(&plain).is_ok());
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

/// What the encoding of a policy writes into its document, in the tests of
/// documents below.
fn policy() -> Layout {
    Layout::new()
        .value("status", Kind::Text)
        .value("priority", Kind::Int)
        .value("weight", Kind::Float)
        .value("published", Kind::Bool)
        .value("created_at", Kind::Timestamp)
        .value("due", Kind::Date)
        .value("owner_id", Kind::Uuid)
        .value("a'b", Kind::Text)
        .object(
            "targeting",
            Layout::new()
                .value("mode", Kind::Text)
                .object("window", Layout::new().value("hours", Kind::Int)),
        )
        .collection(
            "versions",
            Layout::new()
                .value("number", Kind::Int)
                .value("published_at", Kind::Timestamp)
                .collection(
                    "approvals",
                    Layout::new()
                        .value("by", Kind::Text)
                        .value("limit", Kind::Int),
                ),
        )
}

fn policies() -> Schema {
    Schema::new("policies")
        .alias("p")
        .document("policies", "document", policy())
}

/// A member of a document is read by `->>` as text, and the text is read as
/// the kind the layout gives the member: a value of a type, as a column is,
/// so that a constant beside it takes that type. A text is ordered by code
/// point, as the evaluator orders it. A column beside the document is a
/// column.
#[test]
fn a_member_of_a_document_is_read_as_the_kind_its_layout_gives_it() {
    let schema = policies();
    for (specification, expected) in [
        (
            greater_than(field("document.priority"), value(5)),
            r#"("p"."document" ->> 'priority')::bigint > $1"#,
        ),
        (
            equal(field("document.status"), value("active")),
            r#"(("p"."document" ->> 'status') COLLATE "C") = $1"#,
        ),
        (
            field("document.published"),
            r#"("p"."document" ->> 'published')::boolean"#,
        ),
        (
            less_than(field("document.weight"), value(0.5)),
            r#"("p"."document" ->> 'weight')::double precision < $1"#,
        ),
        (
            greater_than(field("document.created_at"), value("2026-01-10")),
            r#"("p"."document" ->> 'created_at')::timestamptz > $1"#,
        ),
        (
            equal(field("document.due"), value(Date::from_days(0))),
            r#"("p"."document" ->> 'due')::date = $1"#,
        ),
        (
            equal(field("document.owner_id"), value("x")),
            r#"("p"."document" ->> 'owner_id')::uuid = $1"#,
        ),
        (
            equal(field("document.targeting.window.hours"), value(8)),
            r#"("p"."document" -> 'targeting' -> 'window' ->> 'hours')::bigint = $1"#,
        ),
        (
            and(
                field("archived"),
                equal(field("document.targeting.mode"), value("x")),
            ),
            r#""archived" AND (("p"."document" -> 'targeting' ->> 'mode') COLLATE "C") = $1"#,
        ),
        // A cast binds tighter than unary minus, and than the cast of the
        // count of a shift.
        (
            less_than(neg(field("document.priority")), value(0)),
            r#"-("p"."document" ->> 'priority')::bigint < $1"#,
        ),
        (
            equal(
                left_shift(field("mask"), field("document.priority")),
                value(4),
            ),
            r#""mask" << ("p"."document" ->> 'priority')::bigint::integer = $1"#,
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
}

/// A null test reads whether the member is null, which an object and a
/// collection of a document can be said to be: `->>` of a JSON null, and of
/// a member that is not there, is the null of SQL. An object is no value to
/// compare.
#[test]
fn a_null_test_of_a_member_of_a_document_reads_whether_it_is_null() {
    let schema = policies();
    for (specification, expected) in [
        (
            is_null(field("document.priority")),
            r#"("p"."document" ->> 'priority')::bigint IS NULL"#,
        ),
        (
            is_not_null(field("document.targeting")),
            r#"("p"."document" ->> 'targeting') IS NOT NULL"#,
        ),
        (
            is_null(field("document.versions")),
            r#"("p"."document" ->> 'versions') IS NULL"#,
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
    assert_eq!(
        Compiler::new()
            .schema(&schema)
            .compile::<Value>(&equal(field("document.targeting"), value("x"))),
        Err(CompileError::UndeclaredMember(
            "policies.document.targeting is an object or a collection of its document, not a value"
                .to_owned()
        )),
    );
}

/// An array of objects in a document is read with `jsonb_array_elements`,
/// whose rows are the objects; a member of one is read as a member of the
/// document is. The aliases of the enclosing queries are in scope of the
/// inner one, the candidate's row is named by the schema, as for the other
/// storages of a collection.
#[test]
fn a_collection_in_a_document_is_read_with_jsonb_array_elements() {
    let schema = policies();
    let approved_by = |name: &str| any(Path::item("approvals"), equal(item("by"), value(name)));
    for (specification, expected) in [
        (
            any("document.versions", greater_than(item("number"), value(2))),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'number')::bigint > $1)"#,
        ),
        (
            any("document.versions", approved_by("ann")),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE EXISTS (SELECT 1 FROM jsonb_array_elements("version_1" -> 'approvals') AS "approval_2" WHERE (("approval_2" ->> 'by') COLLATE "C") = $1))"#,
        ),
        (
            any(
                "document.versions",
                any(
                    Path::item("approvals"),
                    greater_than(item("limit"), field(Path::outer(1, "number"))),
                ),
            ),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE EXISTS (SELECT 1 FROM jsonb_array_elements("version_1" -> 'approvals') AS "approval_2" WHERE ("approval_2" ->> 'limit')::bigint > ("version_1" ->> 'number')::bigint))"#,
        ),
        (
            any(
                "document.versions",
                greater_than(item("published_at"), field("document.created_at")),
            ),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'published_at')::timestamptz > ("p"."document" ->> 'created_at')::timestamptz)"#,
        ),
        (
            any(
                "document.versions",
                greater_than(item("number"), field("floor")),
            ),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'number')::bigint > "p"."floor")"#,
        ),
        (
            all("document.versions", is_not_null(item("published_at"))),
            r#"NOT EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE NOT ("version_1" ->> 'published_at')::timestamptz IS NOT NULL)"#,
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
}

/// A path into a document is what the layout declares, or the query is
/// refused: the layout is what the encoding writes, and a member it does not
/// write would be a null in every row, a query that selects nothing in
/// silence. A key is written as a literal, of the alphabet of names.
#[test]
fn a_path_into_a_document_is_what_its_layout_declares_or_refused() {
    let schema = policies();
    let refused = |specification: Spec, message: &str| {
        assert_eq!(
            Compiler::new().schema(&schema).compile(&specification),
            Err(CompileError::UndeclaredMember(message.to_owned())),
        );
    };
    refused(
        equal(field("document.title"), value("x")),
        "policies.document.title is not a member of its document's layout",
    );
    refused(
        equal(field("document.status.code"), value("x")),
        "policies.document.status is not an object of its document's layout",
    );
    refused(
        any("document.status", item("x")),
        "policies.document.status is not a collection of its document's layout",
    );
    refused(
        any("document.targeting", item("x")),
        "policies.document.targeting is not a collection of its document's layout",
    );
    refused(
        any("document.versions", item("title")),
        "policies.document.versions.title is not a member of its document's layout",
    );
    assert_eq!(
        Compiler::new().schema(&schema).compile::<Value>(&equal(
            field(Path::global("document").child("a'b")),
            value("x")
        )),
        Err(CompileError::InvalidIdentifier("a'b".to_owned())),
    );
    // Not declared a document, the column is what the dots have meant.
    assert_eq!(
        sql(&equal(field("document.status"), value("x"))),
        r#""document"."status" = $1"#,
    );
}

/// A document is a column of a table, any table of the schema: the table
/// of a collection, the table of an object kept by a key.
#[test]
fn a_document_is_a_column_of_any_table_of_the_schema() {
    let schema = Schema::new("stores")
        .alias("s")
        .foreign_key("store_items", "store_id", "stores", "id")
        .foreign_key("stores", "owner_id", "owners", "id")
        .document(
            "store_items",
            "document",
            Layout::new()
                .value("price", Kind::Int)
                .collection("tags", Layout::new().value("name", Kind::Text)),
        )
        .document(
            "owners",
            "document",
            Layout::new().value("name", Kind::Text),
        );
    let price = || field(Path::item("document").child("price"));
    for (specification, expected) in [
        (
            any("store_items", greater_than(price(), value(5))),
            r#"EXISTS (SELECT 1 FROM "store_items" AS "store_item_1" WHERE "store_item_1"."store_id" = "s"."id" AND ("store_item_1"."document" ->> 'price')::bigint > $1)"#,
        ),
        (
            any(
                "store_items",
                any(
                    Path::item("document").child("tags"),
                    equal(item("name"), value("x")),
                ),
            ),
            r#"EXISTS (SELECT 1 FROM "store_items" AS "store_item_1" WHERE "store_item_1"."store_id" = "s"."id" AND EXISTS (SELECT 1 FROM jsonb_array_elements("store_item_1"."document" -> 'tags') AS "tag_2" WHERE (("tag_2" ->> 'name') COLLATE "C") = $1))"#,
        ),
        (
            equal(field("owner_id.document.name"), value("x")),
            r#"(SELECT (("owner_1"."document" ->> 'name') COLLATE "C") FROM "owners" AS "owner_1" WHERE "owner_1"."id" = "s"."owner_id") = $1"#,
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
}

/// Where the schema says a document has a GIN index, an equality of a text,
/// an integer or a boolean member with a constant is what the document
/// contains, where only truth counts: the condition, `AND` and `OR` there.
/// Containment is false where the equality is null, so under `NOT` the
/// equality stays; and a null constant, a kind of several spellings, two
/// members compared, stay as they are.
#[test]
fn an_equality_where_only_truth_counts_is_containment_with_a_gin_index() {
    let schema = policies().gin("policies", "document");
    let status = |text: &str| equal(field("document.status"), value(text));
    for (specification, expected) in [
        (
            status("active"),
            r#""p"."document" @> jsonb_object(ARRAY['status'], ARRAY[$1::text])"#,
        ),
        (
            equal(value(5), field("document.priority")),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['priority'], $1::bigint::text::jsonb)"#,
        ),
        (
            equal(field("document.published"), value(true)),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['published'], $1::boolean::text::jsonb)"#,
        ),
        (
            equal(field("document.targeting.mode"), value("x")),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['targeting'], jsonb_object(ARRAY['mode'], ARRAY[$1::text]))"#,
        ),
        (
            and(
                status("active"),
                greater_than(field("document.priority"), value(5)),
            ),
            r#""p"."document" @> jsonb_object(ARRAY['status'], ARRAY[$1::text]) AND ("p"."document" ->> 'priority')::bigint > $2"#,
        ),
        (
            or(status("a"), status("b")),
            r#""p"."document" @> jsonb_object(ARRAY['status'], ARRAY[$1::text]) OR "p"."document" @> jsonb_object(ARRAY['status'], ARRAY[$2::text])"#,
        ),
        (
            not(status("a")),
            r#"NOT (("p"."document" ->> 'status') COLLATE "C") = $1"#,
        ),
        (
            not_equal(field("document.status"), value("a")),
            r#"(("p"."document" ->> 'status') COLLATE "C") != $1"#,
        ),
        (
            equal(field("document.status"), Expr::Value(Value::Null)),
            r#"(("p"."document" ->> 'status') COLLATE "C") = $1"#,
        ),
        (
            equal(field("document.weight"), value(0.5)),
            r#"("p"."document" ->> 'weight')::double precision = $1"#,
        ),
        (
            equal(field("document.created_at"), value("2026-01-01")),
            r#"("p"."document" ->> 'created_at')::timestamptz = $1"#,
        ),
        (
            equal(field("document.status"), field("document.targeting.mode")),
            r#"(("p"."document" ->> 'status') COLLATE "C") = (("p"."document" -> 'targeting' ->> 'mode') COLLATE "C")"#,
        ),
        (
            equal(status("a"), field("archived")),
            r#"((("p"."document" ->> 'status') COLLATE "C") = $1) = "archived""#,
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
    // Without the index the equality is what it was.
    assert_eq!(
        sql_with(&policies(), &status("active")),
        r#"(("p"."document" ->> 'status') COLLATE "C") = $1"#,
    );
}

/// `any` over a collection of a document with a GIN index, where only its
/// truth counts, has beside it what the document contains if it is true:
/// the equalities its predicate joins by `AND`, of the item and of the items
/// of the item's collections, in one object of the array. They follow from
/// the predicate, which stays as it was: an `OR`, a range, a member of the
/// candidate are left out of the containment and kept in the `EXISTS`.
#[test]
fn any_over_a_collection_with_a_gin_index_has_its_equalities_beside_it() {
    let schema = policies().gin("policies", "document");
    let number = |n: i64| equal(item("number"), value(n));
    let approved_by = |name: &str| any(Path::item("approvals"), equal(item("by"), value(name)));
    for (specification, expected) in [
        (
            any("document.versions", number(3)),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['versions'], '[]'::jsonb || jsonb_set('{}'::jsonb, ARRAY['number'], $1::bigint::text::jsonb)) AND EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'number')::bigint = $2)"#,
        ),
        (
            any(
                "document.versions",
                and_all(
                    number(3),
                    [
                        greater_than(item("published_at"), value("2026-01-01")),
                        approved_by("ann"),
                    ],
                ),
            ),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['versions'], '[]'::jsonb || (jsonb_set('{}'::jsonb, ARRAY['number'], $1::bigint::text::jsonb) || jsonb_set('{}'::jsonb, ARRAY['approvals'], '[]'::jsonb || jsonb_object(ARRAY['by'], ARRAY[$2::text])))) AND EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'number')::bigint = $3 AND ("version_1" ->> 'published_at')::timestamptz > $4 AND EXISTS (SELECT 1 FROM jsonb_array_elements("version_1" -> 'approvals') AS "approval_2" WHERE (("approval_2" ->> 'by') COLLATE "C") = $5))"#,
        ),
        // Two items of one collection: the array contains both.
        (
            any(
                "document.versions",
                and(approved_by("ann"), approved_by("bob")),
            ),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['versions'], '[]'::jsonb || jsonb_set('{}'::jsonb, ARRAY['approvals'], '[]'::jsonb || jsonb_object(ARRAY['by'], ARRAY[$1::text]) || jsonb_object(ARRAY['by'], ARRAY[$2::text]))) AND EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE EXISTS (SELECT 1 FROM jsonb_array_elements("version_1" -> 'approvals') AS "approval_2" WHERE (("approval_2" ->> 'by') COLLATE "C") = $3) AND EXISTS (SELECT 1 FROM jsonb_array_elements("version_1" -> 'approvals') AS "approval_3" WHERE (("approval_3" ->> 'by') COLLATE "C") = $4))"#,
        ),
        // Two constants at one member: the first is asked for, which still
        // follows from the predicate.
        (
            any("document.versions", and(number(3), number(4))),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['versions'], '[]'::jsonb || jsonb_set('{}'::jsonb, ARRAY['number'], $1::bigint::text::jsonb)) AND EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'number')::bigint = $2 AND ("version_1" ->> 'number')::bigint = $3)"#,
        ),
        // Nothing that follows: the `EXISTS` alone.
        (
            any("document.versions", or(number(3), number(4))),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'number')::bigint = $1 OR ("version_1" ->> 'number')::bigint = $2)"#,
        ),
        // Where the falsity counts as well, nothing is beside it.
        (
            not(any("document.versions", number(3))),
            r#"NOT EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'number')::bigint = $1)"#,
        ),
        (
            all("document.versions", number(3)),
            r#"NOT EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE NOT ("version_1" ->> 'number')::bigint = $1)"#,
        ),
        // Beside another condition, under `OR`.
        (
            or(field("archived"), any("document.versions", number(3))),
            r#""archived" OR "p"."document" @> jsonb_set('{}'::jsonb, ARRAY['versions'], '[]'::jsonb || jsonb_set('{}'::jsonb, ARRAY['number'], $1::bigint::text::jsonb)) AND EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'number')::bigint = $2)"#,
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
}

/// A document with a GIN index is a column of any table: of a collection's
/// table, whose rows the predicate of `any` stands on.
#[test]
fn a_document_with_a_gin_index_of_a_table_of_a_collection() {
    let schema = Schema::new("stores")
        .alias("s")
        .foreign_key("store_items", "store_id", "stores", "id")
        .document(
            "store_items",
            "document",
            Layout::new()
                .value("price", Kind::Int)
                .collection("tags", Layout::new().value("name", Kind::Text)),
        )
        .gin("store_items", "document");
    for (specification, expected) in [
        (
            any(
                "store_items",
                equal(field(Path::item("document").child("price")), value(5)),
            ),
            r#"EXISTS (SELECT 1 FROM "store_items" AS "store_item_1" WHERE "store_item_1"."store_id" = "s"."id" AND "store_item_1"."document" @> jsonb_set('{}'::jsonb, ARRAY['price'], $1::bigint::text::jsonb))"#,
        ),
        (
            any(
                "store_items",
                any(
                    Path::item("document").child("tags"),
                    equal(item("name"), value("x")),
                ),
            ),
            r#"EXISTS (SELECT 1 FROM "store_items" AS "store_item_1" WHERE "store_item_1"."store_id" = "s"."id" AND "store_item_1"."document" @> jsonb_set('{}'::jsonb, ARRAY['tags'], '[]'::jsonb || jsonb_object(ARRAY['name'], ARRAY[$1::text])) AND EXISTS (SELECT 1 FROM jsonb_array_elements("store_item_1"."document" -> 'tags') AS "tag_2" WHERE (("tag_2" ->> 'name') COLLATE "C") = $2))"#,
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
}

fn element() -> Spec {
    outer_item(0)
}

/// What a policy's document holds, with arrays of values in it.
fn targeted() -> Schema {
    Schema::new("policies").alias("p").document(
        "policies",
        "document",
        Layout::new()
            .object(
                "targeting",
                Layout::new().values("device_groups", Kind::Text),
            )
            .values("ranks", Kind::Int)
            .collection(
                "versions",
                Layout::new()
                    .value("number", Kind::Int)
                    .values("labels", Kind::Text),
            ),
    )
}

/// An element of a collection of values is the item itself: of an array of
/// a scalar type the alias `unnest` gives it, of a document's array of
/// values the text `jsonb_array_elements_text` gives it, read as its kind.
#[test]
fn an_element_of_a_collection_of_values_is_the_item_itself() {
    assert_eq!(
        sql(&any("tags", equal(element(), value("sale")))),
        r#"EXISTS (SELECT 1 FROM unnest("tags") AS "tag_1" WHERE "tag_1" = $1)"#,
    );
    let schema = targeted();
    for (specification, expected) in [
        (
            any(
                "document.targeting.device_groups",
                equal(element(), value("g1")),
            ),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'targeting' -> 'device_groups') AS "device_group_1" WHERE ("device_group_1" COLLATE "C") = $1)"#,
        ),
        (
            any("document.ranks", greater_than(element(), value(3))),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'ranks') AS "rank_1" WHERE "rank_1"::bigint > $1)"#,
        ),
        (
            any("document.ranks", is_null(element())),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'ranks') AS "rank_1" WHERE "rank_1"::bigint IS NULL)"#,
        ),
        (
            any(
                "document.versions",
                any(
                    Path::item("labels"),
                    equal(element(), field(Path::outer(1, "number"))),
                ),
            ),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text("version_1" -> 'labels') AS "label_2" WHERE ("label_2" COLLATE "C") = ("version_1" ->> 'number')::bigint))"#,
        ),
        (
            any(
                "document.ranks",
                any("document.ranks", greater_than(element(), outer_item(1))),
            ),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'ranks') AS "rank_1" WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'ranks') AS "rank_2" WHERE "rank_2"::bigint > "rank_1"::bigint))"#,
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
}

/// Where the document has a GIN index, the equalities of an element with
/// constants are what its array contains: an array of values contains them.
#[test]
fn a_collection_of_values_with_a_gin_index_has_its_equalities_beside_it() {
    let schema = targeted().gin("policies", "document");
    for (specification, expected) in [
        (
            any(
                "document.targeting.device_groups",
                equal(element(), value("g1")),
            ),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['targeting'], jsonb_set('{}'::jsonb, ARRAY['device_groups'], '[]'::jsonb || (jsonb_object(ARRAY['v'], ARRAY[$1::text]) -> 'v'))) AND EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'targeting' -> 'device_groups') AS "device_group_1" WHERE ("device_group_1" COLLATE "C") = $2)"#,
        ),
        (
            any(
                "document.ranks",
                and(equal(element(), value(3)), equal(value(4), element())),
            ),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['ranks'], '[]'::jsonb || $1::bigint::text::jsonb || $2::bigint::text::jsonb) AND EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'ranks') AS "rank_1" WHERE "rank_1"::bigint = $3 AND $4 = "rank_1"::bigint)"#,
        ),
        (
            any(
                "document.versions",
                and(
                    equal(field(Path::item("number")), value(2)),
                    any(Path::item("labels"), equal(element(), value("x"))),
                ),
            ),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['versions'], '[]'::jsonb || (jsonb_set('{}'::jsonb, ARRAY['number'], $1::bigint::text::jsonb) || jsonb_set('{}'::jsonb, ARRAY['labels'], '[]'::jsonb || (jsonb_object(ARRAY['v'], ARRAY[$2::text]) -> 'v')))) AND EXISTS (SELECT 1 FROM jsonb_array_elements("p"."document" -> 'versions') AS "version_1" WHERE ("version_1" ->> 'number')::bigint = $3 AND EXISTS (SELECT 1 FROM jsonb_array_elements_text("version_1" -> 'labels') AS "label_2" WHERE ("label_2" COLLATE "C") = $4))"#,
        ),
        // A null constant, a range: nothing that follows.
        (
            any(
                "document.ranks",
                or(
                    equal(element(), Expr::Value(Value::Null)),
                    greater_than(element(), value(3)),
                ),
            ),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'ranks') AS "rank_1" WHERE "rank_1"::bigint = $1 OR "rank_1"::bigint > $2)"#,
        ),
    ] {
        assert_eq!(sql_with(&schema, &specification), expected);
    }
}

/// The item itself where the item has members is refused, and so is a
/// member of an item that is a value.
#[test]
fn the_item_itself_is_a_value_or_refused() {
    let schema = targeted()
        .document(
            "policies",
            "document",
            Layout::new()
                .collection("versions", Layout::new().value("number", Kind::Int))
                .values("ranks", Kind::Int),
        )
        .foreign_key("policy_rules", "policy_id", "policies", "id");
    for (specification, error) in [
        (
            any("policy_rules", equal(element(), value(1))),
            CompileError::NotAValue(
                "an item of policy_rules is a row of a table: name its column".to_owned(),
            ),
        ),
        (
            any("document.versions", equal(element(), value(1))),
            CompileError::NotAValue(
                "an item of policies.document.versions is an object of a document: name its member"
                    .to_owned(),
            ),
        ),
        (
            any("document.ranks", equal(item("code"), value(1))),
            CompileError::UndeclaredMember(
                "policies.document.ranks.code: the items of policies.document.ranks are values, \
                 which have no members"
                    .to_owned(),
            ),
        ),
        (element(), CompileError::NoCurrentItem),
    ] {
        assert_eq!(
            Compiler::new().schema(&schema).compile(&specification),
            Err(error)
        );
    }
}

/// A GIN index on a path of a document - `CREATE INDEX … USING gin
/// ((document -> 'subgroups'))` - serves containment of the value at the
/// path, written of the expression the index is on; containment of the
/// whole document it does not serve. What is not under the path is as it
/// was; of several paths on the way, the deepest is written.
#[test]
fn a_gin_index_on_a_path_serves_containment_of_the_value_there() {
    let groups = |name: &str| {
        any(
            "document.targeting.device_groups",
            equal(element(), value(name)),
        )
    };
    let at_groups = targeted().gin_at("policies", "document", "targeting.device_groups");
    for (specification, expected) in [
        (
            groups("g1"),
            r#"("p"."document" -> 'targeting' -> 'device_groups') @> ('[]'::jsonb || (jsonb_object(ARRAY['v'], ARRAY[$1::text]) -> 'v')) AND EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'targeting' -> 'device_groups') AS "device_group_1" WHERE ("device_group_1" COLLATE "C") = $2)"#,
        ),
        (
            any("document.ranks", equal(element(), value(3))),
            r#"EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'ranks') AS "rank_1" WHERE "rank_1"::bigint = $1)"#,
        ),
    ] {
        assert_eq!(sql_with(&at_groups, &specification), expected);
    }
    let at_targeting = policies().gin_at("policies", "document", "targeting");
    for (specification, expected) in [
        (
            equal(field("document.targeting.mode"), value("x")),
            r#"("p"."document" -> 'targeting') @> (jsonb_object(ARRAY['mode'], ARRAY[$1::text]))"#,
        ),
        (
            equal(field("document.targeting.window.hours"), value(8)),
            r#"("p"."document" -> 'targeting') @> (jsonb_set('{}'::jsonb, ARRAY['window'], jsonb_set('{}'::jsonb, ARRAY['hours'], $1::bigint::text::jsonb)))"#,
        ),
        (
            equal(field("document.status"), value("x")),
            r#"(("p"."document" ->> 'status') COLLATE "C") = $1"#,
        ),
    ] {
        assert_eq!(sql_with(&at_targeting, &specification), expected);
    }
    let both = targeted()
        .gin("policies", "document")
        .gin_at("policies", "document", "targeting");
    for (specification, expected) in [
        (
            groups("g1"),
            r#"("p"."document" -> 'targeting') @> (jsonb_set('{}'::jsonb, ARRAY['device_groups'], '[]'::jsonb || (jsonb_object(ARRAY['v'], ARRAY[$1::text]) -> 'v'))) AND EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'targeting' -> 'device_groups') AS "device_group_1" WHERE ("device_group_1" COLLATE "C") = $2)"#,
        ),
        (
            any("document.ranks", equal(element(), value(3))),
            r#""p"."document" @> jsonb_set('{}'::jsonb, ARRAY['ranks'], '[]'::jsonb || $1::bigint::text::jsonb) AND EXISTS (SELECT 1 FROM jsonb_array_elements_text("p"."document" -> 'ranks') AS "rank_1" WHERE "rank_1"::bigint = $2)"#,
        ),
    ] {
        assert_eq!(sql_with(&both, &specification), expected);
    }
}
