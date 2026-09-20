//! From the domain's terms to the storage's: the composite identity of the
//! Python `test_infrastructure` and the Go `transform_visitor_test` — a
//! `something.id` that is a `MemberSomethingId` in the domain and three
//! columns in the table.

use ascetic_ddd_specification::ast::{
    and, any, equal, field, greater_than, is_null, less_than, not, not_equal, value,
};
use ascetic_ddd_specification::{
    Expr, Infix, Mapped, Mapping, Path, Record, Root, TransformError, Value, is_satisfied_by, pg,
    transform,
};

#[derive(Clone, Debug, PartialEq)]
struct MemberId {
    tenant_id: i64,
    member_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
struct MemberSomethingId {
    member_id: MemberId,
    something_id: i64,
}

/// The values of the domain: scalars, and the Value Objects it compares by.
#[derive(Clone, Debug, PartialEq)]
enum Domain {
    Scalar(Value),
    MemberId(MemberId),
    MemberSomethingId(MemberSomethingId),
}

impl From<i64> for Domain {
    fn from(value: i64) -> Self {
        Domain::Scalar(Value::Int(value))
    }
}

impl From<MemberSomethingId> for Domain {
    fn from(value: MemberSomethingId) -> Self {
        Domain::MemberSomethingId(value)
    }
}

fn something_id(tenant_id: i64, member_id: i64, something_id: i64) -> MemberSomethingId {
    MemberSomethingId {
        member_id: MemberId {
            tenant_id,
            member_id,
        },
        something_id,
    }
}

fn column(path: &Path, name: &str) -> Mapped<Value> {
    Mapped::Scalar(Expr::Field(path.sibling(name)))
}

struct Something;

impl Mapping<Domain, Value> for Something {
    type Error = String;

    fn field(&self, path: &Path) -> Result<Mapped<Value>, String> {
        match path.names().collect::<Vec<_>>().as_slice() {
            ["something", "id"] => Ok(Mapped::Composite(vec![
                Mapped::Composite(vec![column(path, "tenant_id"), column(path, "member_id")]),
                column(path, "something_id"),
            ])),
            ["something", "member_id"] => Ok(Mapped::Composite(vec![
                column(path, "tenant_id"),
                column(path, "member_id"),
            ])),
            ["something", name @ ("rank" | "deleted_at")] => Ok(column(path, name)),
            // In a collection's predicate the names are the item's.
            ["weight"] if path.root() == Root::Item => Ok(column(path, "weight_grams")),
            names => Err(format!("unknown field: {}", names.join("."))),
        }
    }

    fn value(&self, value: &Domain) -> Result<Mapped<Value>, String> {
        let scalar = |value: i64| Mapped::Scalar(Expr::Value(Value::Int(value)));
        let member =
            |id: &MemberId| Mapped::Composite(vec![scalar(id.tenant_id), scalar(id.member_id)]);
        Ok(match value {
            Domain::Scalar(value) => Mapped::Scalar(Expr::Value(value.clone())),
            Domain::MemberId(id) => member(id),
            Domain::MemberSomethingId(id) => {
                Mapped::Composite(vec![member(&id.member_id), scalar(id.something_id)])
            }
        })
    }

    fn collection(&self, path: &Path) -> Result<Path, String> {
        match path.names().collect::<Vec<_>>().as_slice() {
            ["something", "parts"] => Ok(Path::global("something_parts")),
            names => Err(format!("unknown collection: {}", names.join("."))),
        }
    }
}

type Transformed = Result<Expr<Value>, TransformError<String>>;

fn int(value: i64) -> Expr<Value> {
    Expr::Value(Value::Int(value))
}

#[test]
fn an_equality_of_composites_is_the_conjunction_of_the_equalities_of_their_parts() {
    let specification: Expr<Domain> = equal(field("something.id"), value(something_id(10, 3, 5)));
    let transformed = transform(&specification, &Something);
    assert_eq!(
        transformed,
        Ok(and(
            and(
                equal(field("something.tenant_id"), int(10)),
                equal(field("something.member_id"), int(3)),
            ),
            equal(field("something.something_id"), int(5)),
        )),
    );
    let query = pg::compile(&transformed.expect("transformed")).expect("compiled");
    assert_eq!(
        query.sql,
        r#""something"."tenant_id" = $1 AND "something"."member_id" = $2 AND "something"."something_id" = $3"#,
    );
    assert_eq!(query.params, [Value::Int(10), Value::Int(3), Value::Int(5)]);
}

#[test]
fn composites_are_unequal_when_not_equal_in_every_part() {
    let specification: Expr<Domain> =
        not_equal(field("something.id"), value(something_id(10, 3, 5)));
    let transformed = transform(&specification, &Something).expect("transformed");
    assert_eq!(
        pg::compile(&transformed).expect("compiled").sql,
        r#"NOT ("something"."tenant_id" = $1 AND "something"."member_id" = $2 AND "something"."something_id" = $3)"#,
    );
    // The sources have NOT (t != $1 AND m != $2 AND s != $3), by which an
    // identity is unequal to itself and equal to one that shares no part.
    let row = |tenant_id, member_id, something_id| {
        Record::<Value>::object([(
            "something",
            Record::object([
                ("tenant_id", Record::value(tenant_id)),
                ("member_id", Record::value(member_id)),
                ("something_id", Record::value(something_id)),
            ]),
        )])
    };
    assert_eq!(is_satisfied_by(&transformed, &row(10, 3, 5)), Ok(false));
    assert_eq!(is_satisfied_by(&transformed, &row(10, 3, 6)), Ok(true));
    assert_eq!(is_satisfied_by(&transformed, &row(11, 4, 6)), Ok(true));
}

#[test]
fn what_is_not_composite_passes_through_with_its_names_and_values_mapped() {
    let specification: Expr<Domain> = and(
        not(less_than(field("something.rank"), value(3))),
        is_null(field("something.deleted_at")),
    );
    assert_eq!(
        transform(&specification, &Something),
        Ok(and(
            not(less_than(field("something.rank"), int(3))),
            is_null(field("something.deleted_at")),
        )),
    );
}

#[test]
fn the_predicate_of_a_collection_is_transformed_too() {
    let specification: Expr<Domain> = any(
        "something.parts",
        greater_than(field(Path::item("weight")), value(100)),
    );
    assert_eq!(
        transform(&specification, &Something),
        Ok(any(
            "something_parts",
            greater_than(field(Path::item("weight_grams")), int(100)),
        )),
    );
}

/// What a repository does with a specification: the mapping says what the
/// members are in the storage, the schema how the storage is laid out, and
/// the schema names a collection as the mapping left it.
#[test]
fn a_mapping_and_a_schema_are_given_together() {
    let specification: Expr<Domain> = any(
        "something.parts",
        greater_than(field(Path::item("weight")), value(100)),
    );
    let schema = pg::Schema::new("things").alias("t").relational(
        "something_parts",
        pg::Relation::new("parts", "thing_id", "id"),
    );
    let query = pg::Compiler::new()
        .schema(&schema)
        .compile(&transform(&specification, &Something).expect("transformed"))
        .expect("compiled");
    assert_eq!(
        query.sql,
        concat!(
            r#"EXISTS (SELECT 1 FROM "parts" AS "something_part_1" "#,
            r#"WHERE "something_part_1"."thing_id" = "t"."id" "#,
            r#"AND "something_part_1"."weight_grams" > $1)"#,
        ),
    );
    assert_eq!(query.params, [Value::Int(100)]);
}

#[test]
fn composites_of_different_shapes_do_not_compare() {
    let member_id = Domain::MemberId(MemberId {
        tenant_id: 10,
        member_id: 3,
    });
    // Of one length, but the first part of one is itself a composite.
    let shorter: Expr<Domain> = equal(field("something.id"), Expr::Value(member_id.clone()));
    assert_eq!(
        transform(&shorter, &Something),
        Transformed::Err(TransformError::ShapeMismatch)
    );
    // A composite against a scalar.
    let scalar: Expr<Domain> = equal(field("something.id"), value(5));
    assert_eq!(
        transform(&scalar, &Something),
        Transformed::Err(TransformError::NotComposite)
    );
    let reversed: Expr<Domain> = equal(value(5), field("something.id"));
    assert_eq!(
        transform(&reversed, &Something),
        Transformed::Err(TransformError::NotComposite)
    );
}

#[test]
fn a_composite_takes_only_equality() {
    let id = || value::<Domain>(something_id(10, 3, 5));
    assert_eq!(
        transform(&greater_than(field("something.id"), id()), &Something),
        Transformed::Err(TransformError::UnsupportedOperator(Infix::GT)),
    );
    assert_eq!(
        transform(&and(field("something.id"), id()), &Something),
        Transformed::Err(TransformError::UnsupportedOperator(Infix::AND)),
    );
    // Nor can one stand where a single expression is needed.
    for specification in [
        field("something.id"),
        is_null(field("something.id")),
        not(id()),
    ] {
        assert_eq!(
            transform(&specification, &Something),
            Transformed::Err(TransformError::UnexpectedComposite),
        );
    }
}

#[test]
fn composites_of_different_lengths_do_not_compare() {
    struct Uneven;
    impl Mapping<Domain, Value> for Uneven {
        type Error = String;
        fn field(&self, path: &Path) -> Result<Mapped<Value>, String> {
            Ok(Mapped::Composite(vec![
                column(path, "a"),
                column(path, "b"),
            ]))
        }
        fn value(&self, _: &Domain) -> Result<Mapped<Value>, String> {
            Ok(Mapped::Composite(vec![
                Mapped::Scalar(int(1)),
                Mapped::Scalar(int(2)),
                Mapped::Scalar(int(3)),
            ]))
        }
    }
    let specification: Expr<Domain> = equal(field("id"), value(1));
    assert_eq!(
        transform(&specification, &Uneven),
        Transformed::Err(TransformError::ShapeMismatch)
    );
}

#[test]
fn a_composite_has_parts() {
    struct Hollow;
    impl Mapping<Domain, Value> for Hollow {
        type Error = String;
        fn field(&self, _: &Path) -> Result<Mapped<Value>, String> {
            Ok(Mapped::Composite(Vec::new()))
        }
        fn value(&self, _: &Domain) -> Result<Mapped<Value>, String> {
            Ok(Mapped::Composite(Vec::new()))
        }
    }
    let specification: Expr<Domain> = equal(field("id"), value(1));
    assert_eq!(
        transform(&specification, &Hollow),
        Transformed::Err(TransformError::EmptyComposite)
    );
}

#[test]
fn the_mappings_refusal_is_the_transformations() {
    let specification: Expr<Domain> = equal(field("something.colour"), value(1));
    assert_eq!(
        transform(&specification, &Something),
        Transformed::Err(TransformError::Mapping(
            "unknown field: something.colour".to_owned()
        )),
    );
}
