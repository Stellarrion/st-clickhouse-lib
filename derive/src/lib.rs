//! Derive macro for `st_clickhouse::Row`.
//!
//! Generates `COLUMN_NAMES`, `COLUMN_COUNT`, and row materialization glue for
//! structs with named fields.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[derive(Default)]
struct ClickhouseAttr {
    rename: Option<String>,
    default: bool,
    skip: bool,
}

/// Extract #[clickhouse(...)] field options.
fn clickhouse_attr(attrs: &[syn::Attribute]) -> syn::Result<ClickhouseAttr> {
    let mut out = ClickhouseAttr::default();
    for attr in attrs {
        if !attr.path().is_ident("clickhouse") {
            continue;
        }
        if let syn::Meta::List(meta_list) = &attr.meta {
            let tokens = &meta_list.tokens;
            let parsed = syn::parse2::<ClickhouseAttr>(tokens.clone())?;
            if parsed.rename.is_some() {
                out.rename = parsed.rename;
            }
            out.default |= parsed.default;
            out.skip |= parsed.skip;
        }
    }
    Ok(out)
}

impl syn::parse::Parse for ClickhouseAttr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut out = ClickhouseAttr::default();
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            if ident == "name" || ident == "rename" {
                input.parse::<syn::Token![=]>()?;
                let lit: syn::LitStr = input.parse()?;
                out.rename = Some(lit.value());
            } else if ident == "default" {
                out.default = true;
            } else if ident == "skip" {
                out.skip = true;
            } else {
                return Err(syn::Error::new_spanned(
                    ident,
                    "unsupported clickhouse attribute; expected rename, name, default, or skip",
                ));
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(out)
    }
}

#[proc_macro_derive(Row, attributes(clickhouse))]
/// Derive `st_clickhouse::Row` for a struct with named fields.
pub fn derive_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_derive(&input).unwrap_or_else(|e| e.to_compile_error().into())
}

fn expand_derive(input: &DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    name,
                    "Row can only be derived for structs with named fields",
                ));
            },
        },
        _ => {
            return Err(syn::Error::new_spanned(
                name,
                "Row can only be derived for structs",
            ));
        },
    };

    struct FieldSpec<'a> {
        ident: &'a syn::Ident,
        ty: &'a syn::Type,
        col_name: String,
        default: bool,
        skip: bool,
    }

    let mut specs = Vec::<FieldSpec<'_>>::new();

    for field in fields {
        let Some(ident) = field.ident.as_ref() else {
            return Err(syn::Error::new_spanned(
                field,
                "Row can only be derived for named fields",
            ));
        };
        let attr = clickhouse_attr(&field.attrs)?;
        let col_name = attr.rename.unwrap_or_else(|| ident.to_string());
        specs.push(FieldSpec {
            ident,
            ty: &field.ty,
            col_name,
            default: attr.default,
            skip: attr.skip,
        });
    }

    let active_specs = specs.iter().filter(|spec| !spec.skip).collect::<Vec<_>>();
    let col_names = active_specs
        .iter()
        .map(|spec| spec.col_name.as_str())
        .collect::<Vec<_>>();
    let col_count = col_names.len();
    let has_default_or_skip = specs.iter().any(|spec| spec.default || spec.skip);
    let from_row_fields = specs.iter().map(|spec| {
        let ident = spec.ident;
        let ty = spec.ty;
        let col_name = &spec.col_name;
        if spec.skip {
            quote! {
                #ident: <#ty as ::core::default::Default>::default()
            }
        } else if spec.default {
            quote! {
                #ident: {
                    if block.columns.iter().any(|column| column.name == #col_name) {
                        let col = block.column::<#ty>(#col_name).map_err(|e| {
                            st_clickhouse::Error::Protocol(format!(
                                "decode field '{}' from column '{}' at row {row_index}: {e}",
                                stringify!(#ident),
                                #col_name,
                            ))
                        })?;
                        st_clickhouse::ClickHouseColumnData::get(&col, row_index).map_err(|e| {
                            st_clickhouse::Error::Protocol(format!(
                                "decode field '{}' from column '{}' at row {row_index}: {e}",
                                stringify!(#ident),
                                #col_name,
                            ))
                        })?
                    } else {
                        <#ty as ::core::default::Default>::default()
                    }
                }
            }
        } else {
            quote! {
                #ident: {
                    let col = block.column::<#ty>(#col_name).map_err(|e| {
                        st_clickhouse::Error::Protocol(format!(
                            "decode field '{}' from column '{}' at row {row_index}: {e}",
                            stringify!(#ident),
                            #col_name,
                        ))
                    })?;
                    st_clickhouse::ClickHouseColumnData::get(&col, row_index).map_err(|e| {
                        st_clickhouse::Error::Protocol(format!(
                            "decode field '{}' from column '{}' at row {row_index}: {e}",
                            stringify!(#ident),
                            #col_name,
                        ))
                    })?
                }
            }
        }
    });

    let from_columns_body = if has_default_or_skip {
        quote! {
            let _ = columns;
            let _ = row_index;
            Err(st_clickhouse::Error::Protocol(
                "name-based row decoding required for default or skipped fields".into()
            ))
        }
    } else {
        let column_fields = active_specs.iter().map(|spec| {
            let ident = spec.ident;
            let ty = spec.ty;
            let col_name = &spec.col_name;
            quote! {
                #ident: {
                    let col = columns.get(idx).ok_or_else(|| {
                        st_clickhouse::Error::Protocol(format!(
                            "decode field '{}' from column '{}' at row {row_index}: column index {idx} not found",
                            stringify!(#ident),
                            #col_name,
                        ))
                    })?;
                    idx += 1;
                    // SAFETY: the derive maps each struct field to the
                    // concrete Rust type declared on that field.
                    unsafe { col.to_typed::<#ty>(row_index) }.map_err(|e| {
                        st_clickhouse::Error::Protocol(format!(
                            "decode field '{}' from column '{}' at row {row_index}: {e}",
                            stringify!(#ident),
                            #col_name,
                        ))
                    })?
                }
            }
        });
        quote! {
            let mut idx = 0usize;
            Ok(#name {
                #(#column_fields,)*
            })
        }
    };

    let expanded = quote! {
        impl st_clickhouse::Row for #name {
            const COLUMN_NAMES: &'static [&'static str] = &[#(#col_names),*];
            const COLUMN_COUNT: usize = #col_count;

            fn from_row(block: &st_clickhouse::Block, row_index: usize) -> st_clickhouse::Result<Self> {
                Ok(#name {
                    #(#from_row_fields,)*
                })
            }

            fn from_columns(
                columns: &[&st_clickhouse::column::AnyColumnData<'_>],
                row_index: usize,
            ) -> st_clickhouse::Result<Self> {
                #from_columns_body
            }
        }
    };

    Ok(expanded.into())
}
