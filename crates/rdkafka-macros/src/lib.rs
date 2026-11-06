//! Generate typed Kafka projections and reusable column batches.
mod generate;
mod model;

use proc_macro::TokenStream;

/// Derive a Kafka record, borrowed projection, and Structure-of-Arrays batch.
///
/// `kafka` attributes override `record` attributes, which override serde defaults.
/// Named fields support `key`, `timestamp`, `partition`, `header`, `rename`, and
/// `with = "serde_bytes"`. Containers support `rename_all`, `crate_path`, and
/// `codec = "zerocopy"`.
#[proc_macro_derive(Record, attributes(record, kafka, serde))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    model::Model::parse(input)
        .map(generate::expand)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
