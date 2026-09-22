//! What a function must look like to be read as a specification: its
//! candidate, its parameters, and the one expression it returns.

use syn::spanned::Spanned;
use syn::{Error, Expr, FnArg, Ident, ItemFn, Pat, PatType, Receiver, ReturnType, Stmt, Type};

/// A predicate function, taken apart.
pub(crate) struct Predicate<'f> {
    /// What the function calls the thing it is a predicate of: its first
    /// parameter, or `self` when there is no other.
    pub candidate: Ident,
    /// `self` of a method that takes a candidate beside it: the specification
    /// itself, whose fields are the constants. The tree function takes it
    /// as the method does.
    pub receiver: Option<&'f Receiver>,
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
        let mut inputs = signature.inputs.iter().peekable();
        // A method of a specification type takes the candidate beside
        // `self`, and nothing else: its constants are the fields of `self`.
        // `self` used to be the candidate whatever followed, and the tree
        // of such a method had the candidate's members for constants.
        let receiver = match inputs.peek() {
            Some(FnArg::Receiver(receiver)) if signature.inputs.len() > 1 => {
                inputs.next();
                Some(receiver)
            }
            _ => None,
        };
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
        if receiver.is_some() {
            if let Some(extra) = inputs.next() {
                return Err(Error::new_spanned(
                    extra,
                    "a method of a specification takes the candidate alone: its constants are its fields",
                ));
            }
        }
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
            receiver,
            parameters,
            body: body(function)?,
        })
    }
}

impl Predicate<'_> {
    /// The parameters declared as an `Option`, by name: what the body is
    /// seen to compare when it compares one of them.
    pub(crate) fn options(&self) -> Vec<&Ident> {
        self.parameters
            .iter()
            .filter(|parameter| is_option(&parameter.ty))
            .filter_map(|parameter| name(parameter).ok())
            .collect()
    }
}

/// Whether the type is written as an `Option`, borrowed or not, under
/// whatever path: `Option<T>`, `&Option<T>`, `std::option::Option<T>`.
fn is_option(ty: &Type) -> bool {
    match ty {
        Type::Reference(reference) => is_option(&reference.elem),
        Type::Paren(inner) => is_option(&inner.elem),
        Type::Group(inner) => is_option(&inner.elem),
        Type::Path(path) => path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "Option"),
        _ => false,
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
