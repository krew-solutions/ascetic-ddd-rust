# ascetic-ddd-specification-macros

The `#[specification]` attribute of
[`ascetic-ddd-specification`](../specification), which re-exports it; depend
on that crate, not on this one. A procedural macro has to live in a crate of
its own kind, which is the only reason there are two.

Put on a predicate function, the attribute leaves the function as it is and
writes beside it `<name>_ast`, which returns the same predicate as a tree —
to compile to SQL, or to evaluate against something that is not the Rust
type. It is the port of the Python `lambda_filter`, which reads a lambda's
source at run time, and of the Go `cmd/specgen`, which generates a file
before compilation.

```rust,ignore
use ascetic_ddd_specification::specification;

#[specification]
fn adult(user: &User, age: i64) -> bool {
    user.profile.age >= age && user.deleted_at.is_none()
}

// fn adult_ast(age: i64) -> Expr<Value>
```

The first parameter, or `self`, is the candidate; the others are constants
of the specification, and `<name>_ast` takes the same. What the body may
consist of is listed in the documentation of `ascetic-ddd-specification`.

An `Option` is its value or the null: `x == None` is `IS NULL`, as
`x.is_none()` is; `Some(v)` is `v`; and a parameter of an `Option` type that
is none when the tree is asked for makes the null test too, so that
`closed_at == at` means the same in the function and in its tree.

What an `Option` holds is asked under a name:
`s.closed_at.is_some_and(|at| at < 5)` is `closed_at IS NOT NULL AND
closed_at < 5`, and `is_none_or` is `closed_at IS NULL OR closed_at < 5`. The
null test makes the whole of two values, as it is in Rust, so the function and
its tree agree under a `!` too. A parameter that is an `Option` is asked the
same way, `at.is_some_and(|at| …)`.

An `Option` itself is not ordered. Rust has a none below every `Some`, the
storage has a null: of a none `s.closed_at < Some(5)` is true to the function
and null to the tree, and a guard on one side, `s.closed_at.is_some() &&
s.closed_at > at`, does not make the other side a value. So `< <= > >=` with
`Some(..)`, `None` or a parameter declared as an `Option` is a compile error,
which names the two forms above. Two members that are `Option`s are not seen
to be: the macro has no types.

What has no meaning in a specification — a call, a cast, an `if`, a member
by number — is a compile error at the place it stands. The Go generator writes `spec.Value(nil)` and a `TODO` comment there
and goes on.
