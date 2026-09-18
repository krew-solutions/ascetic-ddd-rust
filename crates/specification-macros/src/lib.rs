#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

mod signature;
mod translate;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as Tokens;
use quote::{format_ident, quote};
use syn::{Error, ItemFn};

use crate::signature::Predicate;
use crate::translate::Scope;

/// Keeps the predicate function it is put on, and beside it writes
/// `<name>_ast`, the function that returns the same predicate as a tree.
/// See the crate's documentation for what the predicate may consist of.
#[proc_macro_attribute]
pub fn specification(arguments: TokenStream, item: TokenStream) -> TokenStream {
    let item = Tokens::from(item);
    // The function stays whatever happens to its tree, so that an error
    // here is the only one reported, not followed by "cannot find function".
    let tree = expand(arguments.into(), item.clone()).unwrap_or_else(Error::into_compile_error);
    quote!(#item #tree).into()
}

fn expand(arguments: Tokens, item: Tokens) -> Result<Tokens, Error> {
    if !arguments.is_empty() {
        return Err(Error::new_spanned(
            arguments,
            "`specification` takes no arguments",
        ));
    }
    let function: ItemFn = syn::parse2(item)?;
    let predicate = Predicate::of(&function)?;
    let tree = translate::expr(predicate.body, &Scope::of(&predicate))?;
    let visibility = &function.vis;
    let name = format_ident!("{}_ast", function.sig.ident);
    let parameters = &predicate.parameters;
    let doc = format!(
        "The predicate [`{}`] as a specification: a tree to evaluate or to compile.",
        function.sig.ident,
    );
    Ok(quote! {
        #[doc = #doc]
        #[allow(unused_variables)]
        #visibility fn #name(#(#parameters),*)
            -> ::ascetic_ddd_specification::Expr<::ascetic_ddd_specification::Value>
        {
            #tree
        }
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::expand;

    fn error_of(function: proc_macro2::TokenStream) -> String {
        expand(quote!(), function)
            .expect_err("the function is not a specification")
            .to_string()
    }

    #[test]
    fn a_comparison_of_a_member_with_a_literal() {
        let tree = expand(
            quote!(),
            quote!(
                fn adult(u: &User) -> bool {
                    u.age >= 18
                }
            ),
        )
        .expect("a specification")
        .to_string();
        assert!(tree.contains("fn adult_ast ()"), "{tree}");
        assert!(tree.contains("greater_than_equal"), "{tree}");
        assert!(tree.contains("Path :: global (\"age\")"), "{tree}");
    }

    #[test]
    fn an_option_is_its_value_or_the_null() {
        let tree = expand(
            quote!(),
            quote!(
                fn f(u: &U, at: Option<i64>) -> bool {
                    u.a == None && u.b != Some(5) && u.c.as_deref() == at
                }
            ),
        )
        .expect("a specification")
        .to_string();
        // Equality goes through the rule, so that a none is tested.
        assert_eq!(tree.matches("null_test :: equal").count(), 2, "{tree}");
        assert_eq!(tree.matches("null_test :: not_equal").count(), 1, "{tree}");
        assert!(!tree.contains("ast :: equal"), "{tree}");
        assert!(tree.contains("Value :: Null"), "{tree}");
        assert!(tree.contains("value (5)"), "{tree}");
        assert!(tree.contains("Path :: global (\"c\")"), "{tree}");
    }

    #[test]
    fn what_is_not_a_predicate_is_refused_where_it_stands() {
        for (function, message) in [
            (
                quote!(
                    fn f(u: &U) -> bool {
                        if u.a { true } else { false }
                    }
                ),
                "not expressible",
            ),
            (
                quote!(
                    fn f(u: &U) -> bool {
                        u.a as i64 > 1
                    }
                ),
                "not expressible",
            ),
            (
                quote!(
                    fn f(u: &U) -> bool {
                        u.0 > 1
                    }
                ),
                "named member",
            ),
            (
                quote!(
                    fn f(u: &U) -> bool {
                        u.a.starts_with("x")
                    }
                ),
                "method",
            ),
            (
                quote!(
                    fn f(u: &U) -> bool {
                        u.a == Some(1, 2)
                    }
                ),
                "not expressible",
            ),
            (
                quote!(
                    fn f(u: &U) -> bool {
                        check(u)
                    }
                ),
                "not expressible",
            ),
            (
                quote!(
                    fn f(u: &U, o: &U) -> bool {
                        u == o
                    }
                ),
                "candidate itself",
            ),
            (
                quote!(
                    fn f(u: &U) -> bool {
                        let a = u.a;
                        a > 1
                    }
                ),
                "single expression",
            ),
            (
                quote!(
                    fn f(u: &U) -> i64 {
                        u.a
                    }
                ),
                "return `bool`",
            ),
            (
                quote!(
                    fn f() -> bool {
                        true
                    }
                ),
                "candidate",
            ),
            (
                quote!(
                    fn f<T>(u: &T) -> bool {
                        true
                    }
                ),
                "generic",
            ),
            (
                quote!(
                    async fn f(u: &U) -> bool {
                        u.a
                    }
                ),
                "plain function",
            ),
        ] {
            let error = error_of(function.clone());
            assert!(error.contains(message), "{function}: {error}");
        }
    }

    #[test]
    fn the_item_of_an_outer_collection_is_out_of_reach_of_an_inner_predicate() {
        let error = error_of(quote! {
            fn f(s: &Store) -> bool {
                s.categories.iter().any(|c| c.items.iter().any(|i| i.price > c.limit))
            }
        });
        assert!(error.contains("enclosing"), "{error}");
    }

    #[test]
    fn arguments_are_refused() {
        let error = expand(
            quote!(sql),
            quote!(
                fn f(u: &U) -> bool {
                    u.a
                }
            ),
        )
        .expect_err("no arguments are taken")
        .to_string();
        assert!(error.contains("no arguments"), "{error}");
    }
}
