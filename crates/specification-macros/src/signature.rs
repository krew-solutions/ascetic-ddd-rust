//! What a function must look like to be read as a specification: its
//! candidate, its parameters, and the one expression it returns.

use syn::spanned::Spanned;
use syn::{Error, Expr, FnArg, Ident, ItemFn, Pat, PatType, ReturnType, Stmt, Type};

/// A predicate function, taken apart.
pub(crate) struct Predicate<'f> {
    /// What the function calls the thing it is a predicate of: its first
    /// parameter, or `self`.
    pub candidate: Ident,
    /// The other parameters: the constants of the specification. The tree
    /// function takes the same.
    pub parameters: Vec<&'f PatType>,
    /// The expression returned.
    pub body: &'f Expr,
}

impl<'f> Predicate<'f> {
    pub(crate) fn of(function: &'f ItemFn) -> Result<Self, Error> {
        let signature = &function.sig;
        let qualifier = signature
            .constness
            .map(|token| token.span())
            .or(signature.asyncness.map(|token| token.span()))
            .or(signature.unsafety.map(|token| token.span()))
            .or(signature.abi.as_ref().map(Spanned::span))
            .or(signature.variadic.as_ref().map(Spanned::span));
        if let Some(span) = qualifier {
            return Err(Error::new(span, "a specification is a plain function"));
        }
        if !signature.generics.params.is_empty() || signature.generics.where_clause.is_some() {
            return Err(Error::new_spanned(
                &signature.generics,
                "a specification is not generic",
            ));
        }
        match &signature.output {
            ReturnType::Type(_, ty) if is_bool(ty) => {}
            output => {
                return Err(Error::new_spanned(
                    output,
                    "a specification must return `bool`",
                ));
            }
        }
        let mut inputs = signature.inputs.iter();
        let candidate = match inputs.next() {
            Some(FnArg::Receiver(receiver)) => Ident::new("self", receiver.self_token.span),
            Some(FnArg::Typed(parameter)) => name(parameter)?.clone(),
            None => {
                return Err(Error::new_spanned(
                    &signature.ident,
                    "a specification takes its candidate as the first parameter",
                ));
            }
        };
        let parameters = inputs
            .map(|input| match input {
                // The tree function takes the parameter as it is declared
                // and names it as the predicate does: it must have a name.
                FnArg::Typed(parameter) => name(parameter).map(|_| parameter),
                FnArg::Receiver(receiver) => {
                    Err(Error::new_spanned(receiver, "`self` comes first"))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Predicate {
            candidate,
            parameters,
            body: body(function)?,
        })
    }
}

fn is_bool(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.is_ident("bool"))
}

fn name(parameter: &PatType) -> Result<&Ident, Error> {
    match parameter.pat.as_ref() {
        Pat::Ident(pattern) if pattern.subpat.is_none() => Ok(&pattern.ident),
        pattern => Err(Error::new_spanned(
            pattern,
            "a parameter of a specification is a plain name",
        )),
    }
}

/// The expression the function returns, which must be all it does.
fn body(function: &ItemFn) -> Result<&Expr, Error> {
    match function.block.stmts.as_slice() {
        [Stmt::Expr(Expr::Return(returned), _)] => returned
            .expr
            .as_deref()
            .ok_or_else(|| Error::new_spanned(returned, "a specification must return `bool`")),
        [Stmt::Expr(expr, None)] => Ok(expr),
        _ => Err(Error::new_spanned(
            &function.block,
            "the body of a specification is a single expression",
        )),
    }
}
