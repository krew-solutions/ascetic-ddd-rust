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
    /// The item of the nearest enclosing `any` or `all`.
    item: Option<&'a Ident>,
    /// The items of those further out. The tree has one `@`, the nearest;
    /// these can be named in Rust and not in the tree.
    outer: Vec<&'a Ident>,
}

impl<'a> Scope<'a> {
    pub(crate) fn of(predicate: &'a Predicate<'_>) -> Self {
        Scope {
            candidate: &predicate.candidate,
            item: None,
            outer: Vec::new(),
        }
    }

    /// The scope of the predicate of a collection whose items are `item`.
    fn inside(&self, item: &'a Ident) -> Self {
        Scope {
            candidate: self.candidate,
            item: Some(item),
            outer: self.outer.iter().copied().chain(self.item).collect(),
        }
    }
}

/// What a name at the start of a path is.
enum Base {
    Candidate,
    Item,
    /// Not a part of the candidate: a parameter, a constant. A value.
    Other,
}

impl Scope<'_> {
    fn base(&self, name: &Ident) -> Result<Base, Error> {
        if name == self.candidate {
            Ok(Base::Candidate)
        } else if self.item == Some(name) {
            Ok(Base::Item)
        } else if self.outer.contains(&name) {
            Err(Error::new_spanned(
                name,
                "the item of an enclosing collection cannot be referred to from the predicate \
                 of an inner one: a specification has only the nearest item",
            ))
        } else {
            Ok(Base::Other)
        }
    }
}

fn ast() -> Tokens {
    quote!(::ascetic_ddd_specification::ast)
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
            let make = infix(&binary.op).ok_or_else(|| inexpressible(expr))?;
            let left = self::expr(&binary.left, scope)?;
            let right = self::expr(&binary.right, scope)?;
            Ok(quote!(#ast::#make(#left, #right)))
        }
        Expr::Path(_) | Expr::Field(_) => match member(expr, scope)? {
            Some(path) => Ok(quote!(#ast::field(#path))),
            None => Ok(quote!(#ast::value(::core::clone::Clone::clone(&#expr)))),
        },
        Expr::MethodCall(call) => method(call, scope),
        _ => Err(inexpressible(expr)),
    }
}

/// The function of `ast` that makes the node of a binary operator.
fn infix(op: &BinOp) -> Option<Tokens> {
    Some(match op {
        BinOp::Eq(_) => quote!(equal),
        BinOp::Ne(_) => quote!(not_equal),
        BinOp::Lt(_) => quote!(less_than),
        BinOp::Le(_) => quote!(less_than_equal),
        BinOp::Gt(_) => quote!(greater_than),
        BinOp::Ge(_) => quote!(greater_than_equal),
        BinOp::And(_) => quote!(and),
        BinOp::Or(_) => quote!(or),
        BinOp::Add(_) => quote!(add),
        BinOp::Sub(_) => quote!(sub),
        BinOp::Mul(_) => quote!(mul),
        BinOp::Div(_) => quote!(div),
        BinOp::Rem(_) => quote!(modulo),
        BinOp::Shl(_) => quote!(left_shift),
        BinOp::Shr(_) => quote!(right_shift),
        _ => return None,
    })
}

/// The code of the `Path` that `expr` is, if it is a member of the
/// candidate or of the item; `None` if it is a value from elsewhere.
fn member(expr: &Expr, scope: &Scope<'_>) -> Result<Option<Tokens>, Error> {
    let Some((base, names)) = chain(expr)? else {
        return Ok(None);
    };
    let ast = ast();
    let root = match scope.base(base)? {
        Base::Candidate => quote!(global),
        Base::Item => quote!(item),
        Base::Other => return Ok(None),
    };
    let mut names = names.into_iter();
    let first = names.next().ok_or_else(|| {
        Error::new_spanned(
            expr,
            "the candidate itself is not a value: name one of its members",
        )
    })?;
    Ok(Some(quote!(#ast::Path::#root(#first) #(.child(#names))*)))
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
            Some(name) if name == "None" => Err(Error::new_spanned(
                expr,
                "a specification has no `None` to compare with: use `is_none()`",
            )),
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
    let ast = ast();
    let arguments: Vec<&Expr> = call.args.iter().collect();
    let unary = |make: Tokens| {
        let operand = expr(&call.receiver, scope)?;
        Ok(quote!(#ast::#make(#operand)))
    };
    let binary = |make: Tokens, right: &Expr| {
        let left = expr(&call.receiver, scope)?;
        let right = expr(right, scope)?;
        Ok(quote!(#ast::#make(#left, #right)))
    };
    match (call.method.to_string().as_str(), arguments.as_slice()) {
        ("is_none", []) => unary(quote!(is_null)),
        ("is_some", []) => unary(quote!(is_not_null)),
        // What a Value Object compares by, where Go's has `Equal`, `LessThan`.
        ("eq", [right]) => binary(quote!(equal), right),
        ("ne", [right]) => binary(quote!(not_equal), right),
        ("lt", [right]) => binary(quote!(less_than), right),
        ("le", [right]) => binary(quote!(less_than_equal), right),
        ("gt", [right]) => binary(quote!(greater_than), right),
        ("ge", [right]) => binary(quote!(greater_than_equal), right),
        ("any", [Expr::Closure(predicate)]) => quantifier(quote!(any), call, predicate, scope),
        ("all", [Expr::Closure(predicate)]) => quantifier(quote!(all), call, predicate, scope),
        // A change of how the value is held, not of the value.
        ("clone" | "as_str" | "as_ref", []) => expr(&call.receiver, scope),
        _ => Err(Error::new_spanned(
            &call.method,
            "this method has no meaning in a specification",
        )),
    }
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
