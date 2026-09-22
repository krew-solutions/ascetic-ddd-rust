//! From the expression a predicate returns to the code that builds its
//! tree: what the Python source does at run time with `inspect` and `ast`,
//! and the Go source before compilation with `go/ast` and a generator.
//!
//! Anything the tree has no node for is an error at the place it stands.
//! The Go generator writes `spec.Value(nil)` and a `TODO` comment there, and
//! the specification compiles, to a query that compares with null.

use proc_macro2::TokenStream as Tokens;
use quote::quote;
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{BinOp, Error, Expr, ExprClosure, ExprMethodCall, Ident, Lit, Member, Pat, Stmt, UnOp};

use crate::signature::Predicate;

/// What the names in the expression stand for at this point of it.
pub(crate) struct Scope<'a> {
    /// The candidate: paths from it are from the root.
    candidate: &'a Ident,
    /// The items of the enclosing `any`s and `all`s, the nearest last: the
    /// tree names each by how far out it is, `Path::outer(up, ..)`.
    items: Vec<&'a Ident>,
    /// What an `Option` holds, under the name the closure of `is_some_and` or
    /// of `is_none_or` gives it. The latest is the nearest.
    held: Vec<(&'a Ident, Held)>,
    /// The parameters declared as an `Option`.
    options: Vec<&'a Ident>,
    /// Whether `self` is the specification, and not the candidate: a path
    /// from it is a value, a constant of the specification.
    receiver: bool,
}

/// What stands behind the name given to what an `Option` holds: the `Option`,
/// which is its value or the null.
#[derive(Clone)]
enum Held {
    /// A member of the candidate or of the item.
    Member(Place),
    /// A value from outside - a parameter - as the tree function writes it.
    Outside(Tokens),
}

/// A member of the candidate or of the item: where its path starts, and the
/// names along it.
#[derive(Clone)]
struct Place {
    root: Root,
    names: Vec<String>,
}

#[derive(Clone, Copy)]
enum Root {
    Candidate,
    /// The item `up` collections out: 0 the nearest.
    Item(usize),
}

impl Place {
    /// The code of the `Path`; none of the candidate or the item itself.
    fn path(&self) -> Option<Tokens> {
        let ast = ast();
        let (first, names) = self.names.split_first()?;
        let root = match self.root {
            Root::Candidate => quote!(global(#first)),
            Root::Item(0) => quote!(item(#first)),
            Root::Item(up) => quote!(outer(#up, #first)),
        };
        Some(quote!(#ast::Path::#root #(.child(#names))*))
    }

    /// The same place, seen from one collection further in.
    fn further_in(self) -> Self {
        Place {
            root: match self.root {
                Root::Candidate => Root::Candidate,
                Root::Item(up) => Root::Item(up + 1),
            },
            names: self.names,
        }
    }
}

impl<'a> Scope<'a> {
    pub(crate) fn of(predicate: &'a Predicate<'_>) -> Self {
        Scope {
            candidate: &predicate.candidate,
            items: Vec::new(),
            held: Vec::new(),
            options: predicate.options(),
            receiver: predicate.receiver.is_some(),
        }
    }

    /// The scope of the predicate of a collection whose items are `item`.
    /// What is held so far is one collection further out from here.
    fn inside(&self, item: &'a Ident) -> Self {
        Scope {
            candidate: self.candidate,
            items: self.items.iter().copied().chain([item]).collect(),
            held: self
                .held
                .iter()
                .filter(|(name, _)| *name != item)
                .cloned()
                .map(|(name, held)| match held {
                    Held::Member(place) => (name, Held::Member(place.further_in())),
                    outside => (name, outside),
                })
                .collect(),
            options: self.options.clone(),
            receiver: self.receiver,
        }
    }

    /// The scope of a predicate of what an `Option` holds, which it calls
    /// `name`.
    fn holding(&self, name: &'a Ident, held: Held) -> Self {
        Scope {
            candidate: self.candidate,
            items: self.items.clone(),
            held: self.held.iter().cloned().chain([(name, held)]).collect(),
            options: self.options.clone(),
            receiver: self.receiver,
        }
    }
}

/// What a name at the start of a path is.
enum Base<'s> {
    Candidate,
    /// The item `up` collections out.
    Item(usize),
    /// What an `Option` holds: the member that is the `Option`.
    Held(&'s Place),
    /// What an `Option` from outside holds: that value, as it is written.
    Outside(&'s Tokens),
    /// Not a part of the candidate: a parameter, a constant. A value.
    Other,
}

impl Scope<'_> {
    /// The nearest of that name, as Rust reads it: what is held is named
    /// inside everything else, the items inside the candidate, the nearest
    /// item inside the ones further out.
    fn base(&self, name: &Ident) -> Result<Base<'_>, Error> {
        if let Some((_, held)) = self.held.iter().rev().find(|(held, _)| *held == name) {
            Ok(match held {
                Held::Member(place) => Base::Held(place),
                Held::Outside(option) => Base::Outside(option),
            })
        } else if let Some(up) = self.items.iter().rev().position(|item| *item == name) {
            Ok(Base::Item(up))
        } else if name == self.candidate {
            Ok(Base::Candidate)
        } else {
            Ok(Base::Other)
        }
    }
}

fn ast() -> Tokens {
    quote!(::ascetic_ddd_specification::ast)
}

/// Equality is built by `null_test`: `x == None` is what Rust writes for
/// "x is none", and a parameter of an `Option` type may be none when the
/// tree is asked for. In the tree a comparison with null is true of nothing.
fn null_test() -> Tokens {
    quote!(::ascetic_ddd_specification::null_test)
}

fn inexpressible(expr: &Expr) -> Error {
    Error::new_spanned(expr, "not expressible in a specification")
}

/// The code that builds the tree of `expr`.
pub(crate) fn expr(expr: &Expr, scope: &Scope<'_>) -> Result<Tokens, Error> {
    let ast = ast();
    match expr {
        Expr::Paren(inner) => self::expr(&inner.expr, scope),
        Expr::Group(inner) => self::expr(&inner.expr, scope),
        Expr::Reference(inner) => self::expr(&inner.expr, scope),
        Expr::Block(block) if block.label.is_none() => match block.block.stmts.as_slice() {
            [Stmt::Expr(inner, None)] => self::expr(inner, scope),
            _ => Err(inexpressible(expr)),
        },
        Expr::Lit(literal) => match &literal.lit {
            Lit::Int(_) | Lit::Float(_) | Lit::Str(_) | Lit::Bool(_) => {
                Ok(quote!(#ast::value(#literal)))
            }
            _ => Err(inexpressible(expr)),
        },
        Expr::Unary(unary) => match (&unary.op, unary.expr.as_ref()) {
            (UnOp::Deref(_), inner) => self::expr(inner, scope),
            (UnOp::Not(_), inner) => {
                let operand = self::expr(inner, scope)?;
                Ok(quote!(#ast::not(#operand)))
            }
            // `-5` is the constant it looks like, not a negation of `5`.
            (UnOp::Neg(_), Expr::Lit(literal))
                if matches!(literal.lit, Lit::Int(_) | Lit::Float(_)) =>
            {
                Ok(quote!(#ast::value(-#literal)))
            }
            (UnOp::Neg(_), inner) => {
                let operand = self::expr(inner, scope)?;
                Ok(quote!(#ast::neg(#operand)))
            }
            _ => Err(inexpressible(expr)),
        },
        Expr::Binary(binary) => {
            if is_order(&binary.op) {
                ordered(expr, [&binary.left, &binary.right], scope)?;
            }
            let make = infix(&binary.op).ok_or_else(|| inexpressible(expr))?;
            let left = self::expr(&binary.left, scope)?;
            let right = self::expr(&binary.right, scope)?;
            Ok(quote!(#make(#left, #right)))
        }
        // An `Option` is its value or the null: `None` is the null constant,
        // and `Some(x)` is `x`.
        Expr::Path(path) if path.path.is_ident("None") => {
            Ok(quote!(#ast::value(::ascetic_ddd_specification::Value::Null)))
        }
        Expr::Call(call) => match (call.func.as_ref(), call.args.first()) {
            (Expr::Path(some), Some(value))
                if some.path.is_ident("Some") && call.args.len() == 1 =>
            {
                self::expr(value, scope)
            }
            _ => Err(inexpressible(expr)),
        },
        Expr::Path(_) | Expr::Field(_) => match member(expr, scope)? {
            Some(path) => Ok(quote!(#ast::field(#path))),
            None => {
                if scope.receiver
                    && matches!(chain(expr)?, Some((base, names)) if base == "self" && names.is_empty())
                {
                    return Err(Error::new_spanned(
                        expr,
                        "the specification itself is not a value: name a field of it",
                    ));
                }
                let outside = outside(expr, scope)?;
                Ok(quote!(#ast::value(::core::clone::Clone::clone(&#outside))))
            }
        },
        Expr::MethodCall(call) => method(call, scope),
        _ => Err(inexpressible(expr)),
    }
}

fn is_order(op: &BinOp) -> bool {
    matches!(
        op,
        BinOp::Lt(_) | BinOp::Le(_) | BinOp::Gt(_) | BinOp::Ge(_)
    )
}

/// An `Option` is not ordered in a specification. Rust has a none below every
/// `Some`, the storage has a null that is neither below nor above: of a none
/// `closed_at < Some(5)` is true to the function and null to the tree, and a
/// guard on one side does not make the other side a value. What is ordered is
/// what an `Option` holds, where no none is left to compare.
///
/// Refused where an `Option` is seen: `Some(..)`, `None`, a parameter declared
/// as one. A member that is an `Option` is not seen: the macro has no types.
fn ordered(
    comparison: &impl quote::ToTokens,
    operands: [&Expr; 2],
    scope: &Scope<'_>,
) -> Result<(), Error> {
    if operands.iter().any(|operand| is_option(operand, scope)) {
        return Err(Error::new_spanned(
            comparison,
            "an `Option` has no order in a specification: order what it holds, \
             `x.is_some_and(|x| x < 5)` or `x.is_none_or(|x| x < 5)`",
        ));
    }
    Ok(())
}

/// Whether `expr` is seen to be an `Option`.
fn is_option(expr: &Expr, scope: &Scope<'_>) -> bool {
    match expr {
        Expr::Paren(inner) => is_option(&inner.expr, scope),
        Expr::Group(inner) => is_option(&inner.expr, scope),
        Expr::Reference(inner) => is_option(&inner.expr, scope),
        Expr::Unary(unary) if matches!(unary.op, UnOp::Deref(_)) => is_option(&unary.expr, scope),
        Expr::MethodCall(call) if keeps_the_value(call) => is_option(&call.receiver, scope),
        Expr::Call(call) => {
            matches!(call.func.as_ref(), Expr::Path(some) if some.path.is_ident("Some"))
        }
        Expr::Path(path) => match path.path.get_ident() {
            Some(name) if name == "None" => true,
            Some(name) => {
                matches!(scope.base(name), Ok(Base::Other)) && scope.options.contains(&name)
            }
            None => false,
        },
        _ => false,
    }
}

/// A value from outside as the tree function can write it. The name a closure
/// gives to what a parameter holds is not the tree function's: it stands for
/// the parameter, which is its value or the null.
fn outside(expr: &Expr, scope: &Scope<'_>) -> Result<Tokens, Error> {
    let Some((base, names)) = chain(expr)? else {
        return Ok(quote!(#expr));
    };
    match (scope.base(base)?, names.as_slice()) {
        (Base::Outside(option), []) => Ok(quote!(#option)),
        (Base::Outside(_), _) => Err(Error::new_spanned(
            expr,
            "a member of what a parameter holds is not a value the tree can name",
        )),
        _ => Ok(quote!(#expr)),
    }
}

/// The function that makes the node of a binary operator.
fn infix(op: &BinOp) -> Option<Tokens> {
    let (ast, null_test) = (ast(), null_test());
    Some(match op {
        BinOp::Eq(_) => quote!(#null_test::equal),
        BinOp::Ne(_) => quote!(#null_test::not_equal),
        BinOp::Lt(_) => quote!(#ast::less_than),
        BinOp::Le(_) => quote!(#ast::less_than_equal),
        BinOp::Gt(_) => quote!(#ast::greater_than),
        BinOp::Ge(_) => quote!(#ast::greater_than_equal),
        BinOp::And(_) => quote!(#ast::and),
        BinOp::Or(_) => quote!(#ast::or),
        BinOp::Add(_) => quote!(#ast::add),
        BinOp::Sub(_) => quote!(#ast::sub),
        BinOp::Mul(_) => quote!(#ast::mul),
        BinOp::Div(_) => quote!(#ast::div),
        BinOp::Rem(_) => quote!(#ast::modulo),
        BinOp::Shl(_) => quote!(#ast::left_shift),
        BinOp::Shr(_) => quote!(#ast::right_shift),
        _ => return None,
    })
}

/// The code of the `Path` that `expr` is, if it is a member of the
/// candidate or of the item; `None` if it is a value from elsewhere.
fn member(expr: &Expr, scope: &Scope<'_>) -> Result<Option<Tokens>, Error> {
    let Some(place) = place(expr, scope)? else {
        return Ok(None);
    };
    let path = place.path().ok_or_else(|| {
        Error::new_spanned(
            expr,
            "the candidate itself is not a value: name one of its members",
        )
    })?;
    Ok(Some(path))
}

/// The member that `expr` is, if it is one of the candidate or of the item;
/// `None` if it is a value from elsewhere.
fn place(expr: &Expr, scope: &Scope<'_>) -> Result<Option<Place>, Error> {
    let Some((base, names)) = chain(expr)? else {
        return Ok(None);
    };
    let (root, held) = match scope.base(base)? {
        Base::Candidate => (Root::Candidate, Vec::new()),
        Base::Item(up) => (Root::Item(up), Vec::new()),
        Base::Held(place) => (place.root, place.names.clone()),
        Base::Outside(_) | Base::Other => return Ok(None),
    };
    Ok(Some(Place {
        root,
        names: held.into_iter().chain(names).collect(),
    }))
}

/// `a.b.c` as its first name and the names after it; `None` for a path of
/// several segments, `limits::MAX`, which is a constant whatever it names.
fn chain(expr: &Expr) -> Result<Option<(&Ident, Vec<String>)>, Error> {
    match expr {
        Expr::Paren(inner) => chain(&inner.expr),
        Expr::Group(inner) => chain(&inner.expr),
        Expr::Reference(inner) => chain(&inner.expr),
        Expr::Unary(unary) if matches!(unary.op, UnOp::Deref(_)) => chain(&unary.expr),
        Expr::Path(path) if path.qself.is_none() => match path.path.get_ident() {
            Some(name) => Ok(Some((name, Vec::new()))),
            None => Ok(None),
        },
        Expr::Field(field) => match &field.member {
            Member::Named(name) => Ok(chain(&field.base)?.map(|(base, mut names)| {
                names.push(name.unraw().to_string());
                (base, names)
            })),
            Member::Unnamed(_) => Err(Error::new_spanned(
                &field.member,
                "a specification reaches a named member",
            )),
        },
        _ => Err(inexpressible(expr)),
    }
}

fn method(call: &ExprMethodCall, scope: &Scope<'_>) -> Result<Tokens, Error> {
    let (ast, null_test) = (ast(), null_test());
    let arguments: Vec<&Expr> = call.args.iter().collect();
    let unary = |make: Tokens| {
        let operand = expr(&call.receiver, scope)?;
        Ok(quote!(#make(#operand)))
    };
    let binary = |make: Tokens, right: &Expr| {
        let left = expr(&call.receiver, scope)?;
        let right = expr(right, scope)?;
        Ok(quote!(#make(#left, #right)))
    };
    let order = |make: Tokens, right: &Expr| {
        ordered(call, [&call.receiver, right], scope)?;
        binary(make, right)
    };
    match (call.method.to_string().as_str(), arguments.as_slice()) {
        ("is_none", []) => unary(quote!(#ast::is_null)),
        ("is_some", []) => unary(quote!(#ast::is_not_null)),
        // What a Value Object compares by, where Go's has `Equal`, `LessThan`.
        ("eq", [right]) => binary(quote!(#null_test::equal), right),
        ("ne", [right]) => binary(quote!(#null_test::not_equal), right),
        ("lt", [right]) => order(quote!(#ast::less_than), right),
        ("le", [right]) => order(quote!(#ast::less_than_equal), right),
        ("gt", [right]) => order(quote!(#ast::greater_than), right),
        ("ge", [right]) => order(quote!(#ast::greater_than_equal), right),
        ("any", [Expr::Closure(predicate)]) => quantifier(quote!(any), call, predicate, scope),
        ("all", [Expr::Closure(predicate)]) => quantifier(quote!(all), call, predicate, scope),
        ("is_some_and", [Expr::Closure(predicate)]) => {
            held(quote!(and), quote!(is_not_null), call, predicate, scope)
        }
        ("is_none_or", [Expr::Closure(predicate)]) => {
            held(quote!(or), quote!(is_null), call, predicate, scope)
        }
        _ if keeps_the_value(call) => expr(&call.receiver, scope),
        _ => Err(Error::new_spanned(
            &call.method,
            "this method has no meaning in a specification",
        )),
    }
}

/// A change of how the value is held, not of the value.
fn keeps_the_value(call: &ExprMethodCall) -> bool {
    call.args.is_empty()
        && ["clone", "as_str", "as_ref", "as_deref"]
            .iter()
            .any(|method| call.method == method)
}

/// `option.is_some_and(|held| predicate)`: the `Option` is not null and the
/// predicate is true of it; `option.is_none_or(..)`: it is null, or the
/// predicate is. The name the closure gives to what is held stands for the
/// `Option` - a member, or a parameter - which is its value or the null.
///
/// The null test beside the predicate is what Rust means, and it makes the
/// whole of two values as Rust has it: of a none the predicate is null, and
/// `false AND null` is false, `true OR null` true. So the function and its
/// tree agree under a `!` too, which `member < Some(5)` does not.
fn held(
    join: Tokens,
    test: Tokens,
    call: &ExprMethodCall,
    predicate: &ExprClosure,
    scope: &Scope<'_>,
) -> Result<Tokens, Error> {
    let ast = ast();
    let mut option = call.receiver.as_ref();
    while let Expr::MethodCall(inner) = option {
        if !keeps_the_value(inner) {
            break;
        }
        option = inner.receiver.as_ref();
    }
    let nameless = || {
        Error::new_spanned(
            &call.receiver,
            "an `Option` asked for what it holds is a member of the candidate or of the item, \
             or a parameter",
        )
    };
    let (held, option) = match place(option, scope)? {
        Some(place) => {
            let path = place.path().ok_or_else(nameless)?;
            (Held::Member(place), quote!(#ast::field(#path)))
        }
        None => {
            chain(option)?.ok_or_else(nameless)?;
            let outside = outside(option, scope)?;
            let value = quote!(#ast::value(::core::clone::Clone::clone(&#outside)));
            (Held::Outside(outside), value)
        }
    };
    let name = match predicate.inputs.iter().collect::<Vec<_>>().as_slice() {
        [pattern] => name(pattern)?,
        _ => {
            return Err(Error::new_spanned(
                &predicate.inputs,
                "the predicate of an `Option` takes what it holds",
            ));
        }
    };
    let predicate = expr(&predicate.body, &scope.holding(name, held))?;
    Ok(quote!(#ast::#join(#ast::#test(#option), #predicate)))
}

/// `collection.iter().any(|item| predicate)`, and the same with `all`.
fn quantifier(
    make: Tokens,
    call: &ExprMethodCall,
    predicate: &ExprClosure,
    scope: &Scope<'_>,
) -> Result<Tokens, Error> {
    let ast = ast();
    let collection = match call.receiver.as_ref() {
        Expr::MethodCall(iter)
            if iter.args.is_empty() && (iter.method == "iter" || iter.method == "into_iter") =>
        {
            iter.receiver.as_ref()
        }
        other => other,
    };
    let source = member(collection, scope)?.ok_or_else(|| {
        Error::new_spanned(
            collection,
            "a collection of a specification is a member of the candidate or of the item",
        )
    })?;
    let item = match predicate.inputs.iter().collect::<Vec<_>>().as_slice() {
        [pattern] => name(pattern)?,
        _ => {
            return Err(Error::new_spanned(
                &predicate.inputs,
                "the predicate of a collection takes the item",
            ));
        }
    };
    let predicate = expr(&predicate.body, &scope.inside(item))?;
    Ok(quote!(#ast::#make(#source, #predicate)))
}

/// The name a closure gives its item: `|item|`, `|&item|`, `|item: &Item|`.
fn name(pattern: &Pat) -> Result<&Ident, Error> {
    match pattern {
        Pat::Ident(pattern) if pattern.subpat.is_none() => Ok(&pattern.ident),
        Pat::Reference(pattern) => name(&pattern.pat),
        Pat::Type(pattern) => name(&pattern.pat),
        Pat::Paren(pattern) => name(&pattern.pat),
        _ => Err(Error::new(pattern.span(), "the item is a plain name")),
    }
}
