//! Attribute precedence and validated Kafka schema roles.
use proc_macro2::Span;
use syn::{ext::IdentExt, spanned::Spanned, *};

#[derive(Default)]
struct Options {
    rename: Option<String>,
    rename_all: Option<String>,
    crate_path: Option<String>,
    codec: Option<String>,
    bytes: Option<bool>,
    role: Option<Role>,
}

#[derive(Clone)]
pub enum Role {
    Payload,
    Key,
    Timestamp,
    Partition,
    Header(Option<String>),
}

pub struct Member {
    pub ident: Ident,
    pub ty: Type,
    pub column: String,
    pub role: Role,
    pub bytes: bool,
}

pub struct Model {
    pub ident: Ident,
    pub vis: Visibility,
    pub generics: Generics,
    pub runtime: Path,
    pub zerocopy: bool,
    pub members: Vec<Member>,
}

impl Model {
    pub fn parse(input: DeriveInput) -> Result<Self> {
        let options = merged(&input.attrs, false)?;
        let default_path = match proc_macro_crate::crate_name("rdkafka") {
            Ok(proc_macro_crate::FoundCrate::Name(name)) => format!("::{name}"),
            _ => "::rdkafka".to_owned(),
        };
        let runtime = syn::parse_str(options.crate_path.as_deref().unwrap_or(&default_path))?;
        if input.generics.lifetimes().next().is_some() {
            return Err(Error::new(
                input.generics.span(),
                "Record schemas own their fields; use String, Vec, or Cow<'static, _> and borrow \
                 through RecordRef",
            ));
        }
        let zerocopy = match options.codec.as_deref() {
            None | Some("compact") => false,
            Some("zerocopy") => true,
            Some(_) => {
                return Err(Error::new(
                    input.span(),
                    "codec must be compact or zerocopy",
                ));
            }
        };
        if let Some(style) = options.rename_all.as_deref() {
            rename("field_name", Some(style))?;
        }
        let Data::Struct(data) = &input.data else {
            return Err(Error::new(
                input.span(),
                "Record requires a named-field struct",
            ));
        };
        let Fields::Named(fields) = &data.fields else {
            return Err(Error::new(
                data.fields.span(),
                "Record requires named fields",
            ));
        };
        if fields.named.is_empty() {
            return Err(Error::new(
                fields.span(),
                "Record requires at least one field",
            ));
        }
        let structure = synstructure::Structure::new(&input);
        let mut members = Vec::with_capacity(structure.variants()[0].bindings().len());
        let mut columns = std::collections::BTreeSet::new();
        let mut singletons = std::collections::BTreeSet::new();
        let mut headers = std::collections::BTreeSet::new();
        for field in &fields.named {
            let attrs = merged(&field.attrs, true)?;
            let ident = field
                .ident
                .clone()
                .ok_or_else(|| Error::new(field.span(), "missing field name"))?;
            let column = attrs.rename.unwrap_or(rename(
                &ident.unraw().to_string(),
                options.rename_all.as_deref(),
            )?);
            if column.is_empty() || column.contains('\0') || !columns.insert(column.clone()) {
                return Err(Error::new(
                    field.span(),
                    "field names must be nonempty, unique, and contain no NUL",
                ));
            }
            let role = match attrs.role.unwrap_or(Role::Payload) {
                Role::Header(name) => {
                    let name = name.unwrap_or_else(|| column.clone());
                    if name.is_empty() || name.contains('\0') || !headers.insert(name.clone()) {
                        return Err(Error::new(
                            field.span(),
                            "header names must be nonempty, unique, and contain no NUL",
                        ));
                    }
                    Role::Header(Some(name))
                }
                role => role,
            };
            let singleton = match role {
                Role::Key => Some("key"),
                Role::Partition => Some("partition"),
                Role::Timestamp => Some("timestamp"),
                _ => None,
            };
            if let Some(name) = singleton
                && !singletons.insert(name)
            {
                return Err(Error::new(
                    field.span(),
                    format!("only one {name} field is allowed"),
                ));
            }
            if zerocopy && !matches!(role, Role::Payload) {
                return Err(Error::new(
                    field.span(),
                    "zerocopy records cannot contain metadata fields",
                ));
            }
            members.push(Member {
                ident,
                ty: field.ty.clone(),
                column,
                role,
                bytes: attrs.bytes.unwrap_or(false),
            });
        }
        Ok(Self {
            ident: input.ident,
            vis: input.vis,
            generics: input.generics,
            runtime,
            zerocopy,
            members,
        })
    }
}

fn merged(attrs: &[Attribute], field: bool) -> Result<Options> {
    let mut result = Options::default();
    for namespace in ["serde", "record", "kafka"] {
        let current = parse(attrs, namespace, field)?;
        result.rename = current.rename.or(result.rename);
        result.rename_all = current.rename_all.or(result.rename_all);
        result.crate_path = current.crate_path.or(result.crate_path);
        result.codec = current.codec.or(result.codec);
        result.bytes = current.bytes.or(result.bytes);
        result.role = current.role.or(result.role);
    }
    Ok(result)
}

fn parse(attrs: &[Attribute], namespace: &str, field: bool) -> Result<Options> {
    let mut options = Options::default();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident(namespace)) {
        let Meta::List(list) = &attr.meta else {
            return Err(Error::new(
                attr.span(),
                "expected a parenthesized attribute",
            ));
        };
        let metas = darling::ast::NestedMeta::parse_meta_list(list.tokens.clone())?;
        for nested in metas {
            let darling::ast::NestedMeta::Meta(meta) = nested else {
                return Err(Error::new(attr.span(), "expected a named attribute"));
            };
            let name = meta
                .path()
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            let serde = namespace == "serde";
            match name.as_str() {
                "rename" if field => {
                    if serde && matches!(meta, Meta::List(_)) {
                        continue;
                    }
                    set(&mut options.rename, string(&meta)?, meta.span())?;
                }
                "rename_all" if !field => {
                    if serde && matches!(meta, Meta::List(_)) {
                        continue;
                    }
                    set(&mut options.rename_all, string(&meta)?, meta.span())?;
                }
                "crate_path" if !field && !serde => {
                    set(&mut options.crate_path, string(&meta)?, meta.span())?
                }
                "codec" if !field && !serde => {
                    set(&mut options.codec, string(&meta)?, meta.span())?
                }
                "with" if field => {
                    let path = string(&meta)?;
                    if path.trim_start_matches("::") == "serde_bytes" {
                        set(&mut options.bytes, true, meta.span())?;
                    } else if !serde {
                        return Err(Error::new(
                            meta.span(),
                            "only with = \"serde_bytes\" is supported",
                        ));
                    }
                }
                "bytes" if field && !serde => {
                    set(&mut options.bytes, boolean(&meta)?, meta.span())?
                }
                "key" | "partition" | "timestamp" | "header" if field && !serde => {
                    let role = match name.as_str() {
                        "key" => Role::Key,
                        "partition" => Role::Partition,
                        "timestamp" => Role::Timestamp,
                        _ => Role::Header(if matches!(meta, Meta::Path(_)) {
                            None
                        } else {
                            Some(string(&meta)?)
                        }),
                    };
                    if name != "header" && !matches!(meta, Meta::Path(_)) {
                        return Err(Error::new(
                            meta.span(),
                            "metadata markers do not accept a value",
                        ));
                    }
                    set(&mut options.role, role, meta.span())?;
                }
                _ if serde => {}
                _ => {
                    return Err(Error::new(
                        meta.span(),
                        format!("unknown {namespace} attribute `{name}` for this location"),
                    ));
                }
            }
        }
    }
    Ok(options)
}

fn set<T>(slot: &mut Option<T>, value: T, span: Span) -> Result<()> {
    if slot.replace(value).is_some() {
        return Err(Error::new(span, "duplicate or conflicting attribute"));
    }
    Ok(())
}
fn string(meta: &Meta) -> Result<String> {
    if let Meta::NameValue(value) = meta
        && let Expr::Lit(value) = &value.value
        && let Lit::Str(value) = &value.lit
    {
        return Ok(value.value());
    }
    Err(Error::new(meta.span(), "expected a string literal"))
}
fn boolean(meta: &Meta) -> Result<bool> {
    match meta {
        Meta::Path(_) => Ok(true),
        Meta::NameValue(value) => match &value.value {
            Expr::Lit(ExprLit {
                lit: Lit::Bool(value),
                ..
            }) => Ok(value.value),
            _ => Err(Error::new(meta.span(), "expected a boolean literal")),
        },
        _ => Err(Error::new(meta.span(), "expected a boolean literal")),
    }
}
fn rename(value: &str, style: Option<&str>) -> Result<String> {
    use convert_case::{Case, Casing};
    use heck::{ToLowerCamelCase, ToShoutySnakeCase, ToSnakeCase, ToUpperCamelCase};
    Ok(match style {
        None => value.to_owned(),
        Some("snake_case") => value.to_snake_case(),
        Some("camelCase") => value.to_lower_camel_case(),
        Some("PascalCase") => value.to_upper_camel_case(),
        Some("SCREAMING_SNAKE_CASE") => value.to_shouty_snake_case(),
        Some("kebab-case") => value.to_case(Case::Kebab),
        Some("SCREAMING-KEBAB-CASE") => value.to_case(Case::Cobol),
        Some("lowercase") => value.to_lowercase(),
        Some("UPPERCASE") => value.to_uppercase(),
        Some(_) => return Err(Error::new(Span::call_site(), "unsupported rename_all case")),
    })
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::Model;

    #[test]
    fn unit_scenario_rejects_ambiguous_schemas() {
        let invalid = [
            parse_quote!(
                enum Event {
                    One,
                }
            ),
            parse_quote!(
                struct Event(u64);
            ),
            parse_quote!(
                struct Event {}
            ),
            parse_quote!(
                struct Event {
                    #[record(key)]
                    a: u64,
                    #[record(key)]
                    b: u64,
                }
            ),
            parse_quote!(
                struct Event {
                    #[record(key, timestamp)]
                    a: u64,
                }
            ),
            parse_quote!(
                struct Event {
                    #[record(rename = "same")]
                    a: u64,
                    #[record(rename = "same")]
                    b: u64,
                }
            ),
            parse_quote!(
                #[record(codec = "json")]
                struct Event {
                    a: u64,
                }
            ),
            parse_quote!(
                #[record(codec = "zerocopy")]
                struct Event {
                    #[record(key)]
                    a: u64,
                }
            ),
            parse_quote!(
                #[record(rename_all = "unknown")]
                struct Event {
                    a: u64,
                }
            ),
            parse_quote!(
                struct Event {
                    #[kafka(unknown)]
                    a: u64,
                }
            ),
            parse_quote!(
                struct Event {
                    #[record(header = "a")]
                    a: u64,
                    #[record(header = "a")]
                    b: u64,
                }
            ),
        ];
        for input in invalid {
            assert!(Model::parse(input).is_err());
        }
    }

    #[test]
    fn unit_scenario_higher_namespace_overrides_metadata_role() {
        let model = Model::parse(parse_quote!(
            #[serde(rename_all = "snake_case")]
            #[record(rename_all = "camelCase")]
            #[kafka(rename_all = "PascalCase")]
            struct Event {
                #[serde(rename = "serde")]
                #[record(rename = "record", key)]
                #[kafka(rename = "kafka", header = "trace")]
                event_id: String,
                value_name: u64,
            }
        ))
        .unwrap();
        assert_eq!(model.members[0].column, "kafka");
        assert!(matches!(&model.members[0].role,super::Role::Header(Some(name)) if name=="trace"));
        assert_eq!(model.members[1].column, "ValueName");
    }
}
