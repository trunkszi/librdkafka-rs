//! Emit typed wire projections and independent contiguous field columns.
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{GenericArgument, PathArguments, Type, ext::IdentExt, parse_quote};

use crate::model::{Model, Role};

pub fn expand(model: Model) -> TokenStream {
    let Model {
        ident,
        vis,
        mut generics,
        runtime: rt,
        zerocopy,
        members,
    } = model;
    let batch = format_ident!("{}Batch", ident.unraw());
    let view = format_ident!("{}RecordRef", ident.unraw());
    for param in generics.type_params_mut() {
        param.bounds.push(parse_quote!(::core::clone::Clone));
        param.bounds.push(parse_quote!(#rt::column::ColumnValue));
        param
            .bounds
            .push(parse_quote!(for<'__decode> #rt::serde_bridge::BorrowDecode<'__decode>));
        param
            .bounds
            .push(parse_quote!(#rt::__private::serde::Serialize));
        param
            .bounds
            .push(parse_quote!(#rt::__private::serde::de::DeserializeOwned));
        param.bounds.push(parse_quote!('static));
    }
    let (ig, tg, wc) = generics.split_for_impl();
    let mut vg = generics.clone();
    vg.params.insert(0, parse_quote!('__record));
    let (vig, vtg, vwc) = vg.split_for_impl();
    let names: Vec<_> = members.iter().map(|m| &m.ident).collect();
    let mut marker_name = "__rdkafka_marker".to_owned();
    while names
        .iter()
        .any(|name| name.unraw() == marker_name.as_str())
    {
        marker_name.push('_');
    }
    let marker = format_ident!("{marker_name}", span = proc_macro2::Span::mixed_site());
    let tys: Vec<_> = members.iter().map(|m| &m.ty).collect();
    let columns: Vec<_> = members.iter().map(|m| &m.column).collect();
    let accessors: Vec<_> = names
        .iter()
        .map(|n| format_ident!("column_{}", n.unraw()))
        .collect();
    let projected: Vec<_> = tys.iter().map(|t| project(t, &rt)).collect();
    let borrowed_values: Vec<_> = members
        .iter()
        .map(|m| {
            let n = &m.ident;
            borrow(&m.ty, quote!(&record.#n))
        })
        .collect();
    let own: Vec<_> = members
        .iter()
        .map(|m| {
            let n = &m.ident;
            owned(&m.ty, quote!(&self.#n), &rt)
        })
        .collect();
    let updates: Vec<_> = members
        .iter()
        .map(|m| {
            let n = &m.ident;
            update(&m.ty, quote!(&record.#n), quote!(__slot), &rt)
        })
        .collect();
    let push_own: Vec<_> = members
        .iter()
        .map(|m| {
            let n = &m.ident;
            owned(&m.ty, quote!(&record.#n), &rt)
        })
        .collect();
    let mut prepare = Vec::new();
    let mut push_fields = Vec::new();
    for (index, m) in members.iter().enumerate() {
        let n = &m.ident;
        let value = &push_own[index];
        let update = &updates[index];
        if has_sequence(&m.ty) {
            let temp = format_ident!("__prepared_{index}", span = proc_macro2::Span::mixed_site());
            prepare.push(quote!(let #temp=#value;));
            push_fields.push(quote!(self.#n.push_owned(#temp);));
        } else {
            push_fields.push(quote!(self.#n.push_with(||#value,|__slot|{#update});));
        }
    }
    let mut enc = Vec::new();
    let mut dec = Vec::new();
    let mut header_encode = Vec::new();

    let mut timestamp = quote!(::core::option::Option::None);
    let mut partition = quote!(-1i32);
    for (index, m) in members.iter().enumerate() {
        let ty = &projected[index];
        let decode = decode_expr(&m.ty, &rt);
        let temp = format_ident!("__field_{index}", span = proc_macro2::Span::mixed_site());
        match &m.role {
            Role::Payload => {
                enc.push(if m.bytes {
                    quote!(#rt::__private::serde_bytes::serialize(#temp, &mut __serializer)?;)
                } else {
                    quote!(#rt::__private::serde::Serialize::serialize(#temp, &mut __serializer)?;)
                });
                dec.push(quote!(let #temp:#ty=#decode;));
            }
            Role::Key => {
                if let Some(bytes) = metadata_bytes(&m.ty, quote!(#temp)) {
                    header_encode.push(quote!(__writer.key(#bytes)?;));
                } else {
                    header_encode.push(compact_metadata_write(
                        &m.ty,
                        quote!(#temp),
                        m.bytes,
                        None,
                        &rt,
                    ));
                }
                if let Some(value) = metadata_decode(&m.ty, quote!(wire.key), &rt) {
                    dec.push(quote!(let #temp:#ty=#value;));
                } else {
                    let value = compact_metadata_decode(&m.ty, quote!(wire.key), &rt);
                    dec.push(quote!(let #temp:#ty=#value;));
                }
            }
            Role::Header(Some(name)) => {
                if let Some(bytes) = metadata_bytes(&m.ty, quote!(#temp)) {
                    header_encode.push(quote!(__writer.header(#name,#bytes)?;));
                } else {
                    header_encode.push(compact_metadata_write(
                        &m.ty,
                        quote!(#temp),
                        m.bytes,
                        Some(name),
                        &rt,
                    ));
                }
                let input = quote!(wire.headers.iter().find(|header|header.name==#name).and_then(|header|header.value));
                if let Some(value) = metadata_decode(&m.ty, input.clone(), &rt) {
                    dec.push(quote!(let #temp:#ty=#value;));
                } else {
                    let value = compact_metadata_decode(&m.ty, input, &rt);
                    dec.push(quote!(let #temp:#ty=#value;));
                }
            }
            Role::Timestamp => {
                if argument(&m.ty, "Option").is_some() {
                    timestamp = quote!(*#temp);
                    dec.push(quote!(let #temp=wire.timestamp;));
                } else {
                    timestamp = quote!(::core::option::Option::Some(*#temp));
                    dec.push(quote!(let #temp=wire.timestamp.ok_or_else(||#rt::Error::Codec("missing record timestamp".into()))?;));
                }
            }
            Role::Partition => {
                partition = quote!(*#temp);
                dec.push(quote!(let #temp=wire.partition;));
            }
            Role::Header(None) => unreachable!(),
        }
    }
    let compact_encode = quote! {destination.push_with("",#partition,-1,#timestamp,|__writer|{__writer.encode_payload(|__out|{let mut __serializer=#rt::serde_bridge::Serializer::new(__out);#(#enc)* ::core::result::Result::Ok(())})?;#(#header_encode)* ::core::result::Result::Ok(())})};
    let encode_body = if zerocopy {
        quote!(destination.push(#rt::WireRecord::new("",::core::option::Option::Some(#rt::record::zerocopy_bytes(self)))))
    } else {
        quote! {<#batch #tg>::__encode_fields((#(&self.#names,)*),destination)}
    };
    let batch_encode = if zerocopy {
        quote! {let record=#ident{#(#names: ::core::clone::Clone::clone(&self.#names.as_slice()[__index])),*};#rt::Record::encode_into(&record,&mut wire)?;}
    } else {
        quote! {Self::__encode_fields((#(&self.#names.as_slice()[__index],)*),&mut wire)?;}
    };
    let decoded: Vec<_> = (0..members.len())
        .map(|index| format_ident!("__field_{index}", span = proc_macro2::Span::mixed_site()))
        .collect();
    let compact_method = if zerocopy {
        quote! {}
    } else {
        quote! {
            fn __encode_fields(__fields:(#(&#tys,)*),destination:&mut #rt::WireBatch)->#rt::Result<()>{let (#(#decoded,)*)=__fields;#compact_encode}
        }
    };
    let view_body = if zerocopy {
        quote! {let record=#rt::record::zerocopy_ref::<Self>(wire.payload.ok_or_else(||#rt::Error::Layout("missing record payload".into()))?)?;::core::result::Result::Ok(#view{#(#names:#borrowed_values,)*#marker: ::core::marker::PhantomData})}
    } else {
        quote! {let mut __deserializer=#rt::serde_bridge::Deserializer::new(wire.payload.ok_or_else(||#rt::Error::Codec("missing record payload".into()))?);#(#dec)* __deserializer.finish()?;::core::result::Result::Ok(#view{#(#names:#decoded,)*#marker: ::core::marker::PhantomData})}
    };
    let decode_body = if zerocopy {
        quote!(#rt::record::zerocopy_read::<Self>(wire.payload.ok_or_else(||#rt::Error::Layout("missing record payload".into()))?))
    } else {
        quote!(#rt::RecordView::to_owned(&<Self as #rt::Record>::view(wire)?))
    };
    let first = &names[0];
    quote! {
        impl #ig #rt::column::ColumnValue for #ident #tg #wc {
            fn extra_bytes(&self)->usize{0usize #(.saturating_add(#rt::column::ColumnValue::extra_bytes(&self.#names)))*}
            fn clear_value(&mut self,retention:#rt::buffer_policy::Retention){#(#rt::column::ColumnValue::clear_value(&mut self.#names,retention);)*}
            fn shrink_value(&mut self){#(#rt::column::ColumnValue::shrink_value(&mut self.#names);)*}
        }
        impl #ig #rt::Record for #ident #tg #wc {
            type View<'__record>=#view #vtg where Self:'__record;
            type Batch=#batch #tg;
            fn encode_into(&self,destination:&mut #rt::WireBatch)->#rt::Result<()>{#encode_body}
            fn decode(wire:#rt::WireRecord<'_>)->#rt::Result<Self>{#decode_body}
            fn view<'__record>(wire:#rt::WireRecord<'__record>)->#rt::Result<Self::View<'__record>>{#view_body}
        }
        #[doc="A borrowed typed projection over validated Kafka wire bytes."]
        #[derive(Clone)]
        #vis struct #view #vg #vwc {
            #(pub #names:#projected,)*
            #marker: ::core::marker::PhantomData<(&'__record (),::std::rc::Rc<()>,fn()->#ident #tg)>,
        }
        impl #vig #rt::RecordView<'__record> for #view #vtg #vwc {
            type Owned=#ident #tg;
            fn to_owned(&self)->#rt::Result<Self::Owned>{::core::result::Result::Ok(#ident{#(#names:#own),*})}
        }
        impl #vig #view #vtg #vwc {
            #[doc="Materialize this projection as its owned schema.\n\n# Errors\nRejects encoded sequence elements that cannot be decoded."]
            pub fn to_owned(&self)->#rt::Result<#ident #tg>{<Self as #rt::RecordView>::to_owned(self)}
            #[doc="Decode a borrowed projection.\n\n# Errors\nRejects invalid metadata and payload encoding."]
            pub fn from_wire(wire:#rt::WireRecord<'__record>)->#rt::Result<Self>{<#ident #tg as #rt::Record>::view(wire)}
        }
        #[doc="A reusable Structure-of-Arrays batch with one vector per schema field."]
        #vis struct #batch #generics #wc {#(#names: #rt::column::ReusableColumn<#tys>,)*#marker: ::core::marker::PhantomData<::std::rc::Rc<()>>}
        impl #ig ::core::default::Default for #batch #tg #wc {fn default()->Self{Self{#(#names: #rt::column::ReusableColumn::default(),)*#marker: ::core::marker::PhantomData}}}
        impl #ig #rt::Batch for #batch #tg #wc {
            type Record=#ident #tg;
            fn with_capacity(records:usize)->Self{Self{#(#names: #rt::column::ReusableColumn::with_capacity(records),)*#marker: ::core::marker::PhantomData}}
            fn len(&self)->usize{self.#first.len()}
            fn allocated_bytes(&self)->usize{0usize #(+ #rt::buffer_policy::MemoryBuffer::allocated_bytes(&self.#names))*}
            fn clear_with_policy(&mut self,policy:#rt::BufferPolicy){let capacity=[#(#rt::buffer_policy::MemoryBuffer::record_capacity(&self.#names)),*].into_iter().max().unwrap_or(0);let inline=0usize #(+ #rt::buffer_policy::MemoryBuffer::inline_bytes(&self.#names))*;let retention=policy.retention(capacity,<Self as #rt::Batch>::allocated_bytes(self),inline);#(#rt::buffer_policy::MemoryBuffer::clear_retained(&mut self.#names,retention);)*}
            fn shrink_to_fit(&mut self){#(#rt::buffer_policy::MemoryBuffer::shrink_to_fit(&mut self.#names);)*}
            fn push(&mut self,record:&Self::Record){#(self.#names.push(&record.#names);)*}
            fn push_ref<'__record>(&mut self,record:<Self::Record as #rt::Record>::View<'__record>)->#rt::Result<()>{#(#prepare)* #(#push_fields)* ::core::result::Result::Ok(())}
            fn from_wire_batch(wire:&#rt::WireBatch)->#rt::Result<Self>{let mut batch=Self::with_capacity(wire.len());for record in wire.iter(){batch.push_ref(<#ident #tg as #rt::Record>::view(record)?)?;}::core::result::Result::Ok(batch)}
            fn to_wire_batch(&self)->#rt::Result<#rt::WireBatch>{let mut wire=#rt::WireBatch::with_capacity(self.len());for __index in 0..self.len(){#batch_encode}::core::result::Result::Ok(wire)}
        }
        impl #ig #batch #tg #wc {
            #compact_method
            #[doc="Names of the schema columns after attribute precedence and renaming."]
            pub const COLUMN_NAMES:&'static [&'static str]=&[#(#columns),*];
            #(#[doc="Borrow the contiguous values in this column."]pub fn #accessors(&self)->&[#tys]{self.#names.as_slice()})*
            #[doc="Create empty preallocated columns.\n\n# Panics\nPanics if the requested allocation exceeds the vector size limit."]pub fn with_capacity(records:usize)->Self{<Self as #rt::Batch>::with_capacity(records)}
            #[doc="Number of records in this batch."]pub fn len(&self)->usize{<Self as #rt::Batch>::len(self)}
            #[doc="Whether the batch contains no records."]pub fn is_empty(&self)->bool{self.len()==0}
            #[doc="Aggregate retained column allocation bytes."]pub fn allocated_bytes(&self)->usize{<Self as #rt::Batch>::allocated_bytes(self)}
            #[doc="Append an owned record's fields.\n\n# Panics\nPanics if growing a column exceeds the vector size limit."]pub fn push(&mut self,record:&#ident #tg){<Self as #rt::Batch>::push(self,record)}
            #[doc="Append a borrowed projection.\n\n# Errors\nRejects invalid encoded sequence elements before updating columns.\n\n# Panics\nPanics if growing a column exceeds the vector size limit."]pub fn push_ref<'__record>(&mut self,record:#view #vtg)->#rt::Result<()>{<Self as #rt::Batch>::push_ref(self,record)}
            #[doc="Clear using the default retention budget."]pub fn clear(&mut self){<Self as #rt::Batch>::clear(self)}
            #[doc="Clear and enforce an aggregate retention budget."]pub fn clear_with_policy(&mut self,policy:#rt::BufferPolicy){<Self as #rt::Batch>::clear_with_policy(self,policy)}
            #[doc="Clear without trimming capacity."]pub fn clear_unbounded(&mut self){<Self as #rt::Batch>::clear_unbounded(self)}
            #[doc="Release unused column capacity."]pub fn shrink_to_fit(&mut self){<Self as #rt::Batch>::shrink_to_fit(self)}
            #[doc="Encode columns into wire arenas.\n\n# Errors\nReturns a field encoding error."]pub fn to_wire_batch(&self)->#rt::Result<#rt::WireBatch>{<Self as #rt::Batch>::to_wire_batch(self)}
            #[doc="Decode all wire records into columns.\n\n# Errors\nRejects invalid metadata or payload encodings."]pub fn from_wire_batch(wire:&#rt::WireBatch)->#rt::Result<Self>{<Self as #rt::Batch>::from_wire_batch(wire)}
        }
    }
}
fn argument(ty: &Type, name: &str) -> Option<Type> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    if segment.ident != name {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|a| {
        if let GenericArgument::Type(t) = a {
            Some(t.clone())
        } else {
            None
        }
    })
}
fn cow_kind(ty: &Type) -> Option<&'static str> {
    match argument(ty, "Cow")? {
        Type::Path(p) if p.path.is_ident("str") => Some("str"),
        Type::Slice(s) if matches!(s.elem.as_ref(),Type::Path(p) if p.path.is_ident("u8")) => {
            Some("bytes")
        }
        _ => None,
    }
}
fn is_string(ty: &Type) -> bool {
    cow_kind(ty) == Some("str")
        || matches!(ty,Type::Path(p) if p.path.segments.last().is_some_and(|s|s.ident=="String"))
}
fn is_bytes(ty: &Type) -> bool {
    cow_kind(ty) == Some("bytes")
        || argument(ty, "Vec").is_some_and(|t| matches!(t,Type::Path(p) if p.path.is_ident("u8")))
}
fn project(ty: &Type, rt: &syn::Path) -> TokenStream {
    if is_string(ty) {
        quote!(&'__record str)
    } else if is_bytes(ty) {
        quote!(&'__record [u8])
    } else if let Some(inner) = argument(ty, "Vec") {
        let t = project(&inner, rt);
        quote!(#rt::serde_bridge::EncodedSequence<'__record,#t>)
    } else if let Some(inner) = argument(ty, "Option") {
        let t = project(&inner, rt);
        quote!(::core::option::Option<#t>)
    } else {
        quote!(#ty)
    }
}
fn borrow(ty: &Type, value: TokenStream) -> TokenStream {
    if is_string(ty) || is_bytes(ty) {
        quote!((#value).as_ref())
    } else if let Some(inner) = argument(ty, "Option") {
        let v = borrow(&inner, quote!(__value));
        quote!((#value).as_ref().map(|__value|#v))
    } else {
        quote!(::core::clone::Clone::clone(#value))
    }
}
fn owned(ty: &Type, value: TokenStream, rt: &syn::Path) -> TokenStream {
    if let Some(kind) = cow_kind(ty) {
        if kind == "str" {
            quote!(::std::borrow::Cow::Owned(::std::string::String::from(*#value)))
        } else {
            quote!(::std::borrow::Cow::Owned((#value).to_vec()))
        }
    } else if is_string(ty) {
        quote!(::std::string::String::from(*#value))
    } else if is_bytes(ty) {
        quote!((#value).to_vec())
    } else if let Some(inner) = argument(ty, "Vec") {
        let v = owned(&inner, quote!(&__value), rt);
        quote!((#value).iter().map(|__item|{let __value=__item?;let __owned=#v;::core::result::Result::Ok(__owned)}).collect::<::core::result::Result<::std::vec::Vec<_>,#rt::Error>>()?)
    } else if let Some(inner) = argument(ty, "Option") {
        let v = owned(&inner, quote!(__value), rt);
        if has_sequence(&inner) {
            quote!(match (#value).as_ref(){::core::option::Option::Some(__value)=>::core::option::Option::Some(#v),::core::option::Option::None=>::core::option::Option::None})
        } else {
            quote!((#value).as_ref().map(|__value|#v))
        }
    } else {
        quote!(::core::clone::Clone::clone(#value))
    }
}

fn metadata_bytes(ty: &Type, value: TokenStream) -> Option<TokenStream> {
    if is_string(ty) {
        Some(quote!(::core::option::Option::Some((#value).as_bytes())))
    } else if is_bytes(ty) {
        Some(quote!(::core::option::Option::Some((#value).as_ref())))
    } else if let Some(inner) = argument(ty, "Option") {
        let bytes = metadata_bytes(&inner, quote!(__value))?;
        Some(quote!((#value).as_ref().and_then(|__value|#bytes)))
    } else {
        None
    }
}
fn metadata_decode(ty: &Type, value: TokenStream, rt: &syn::Path) -> Option<TokenStream> {
    if is_string(ty) {
        Some(
            quote!(::core::str::from_utf8((#value).ok_or_else(||#rt::Error::Codec("missing string metadata".into()))?).map_err(|error|#rt::Error::Codec(error.to_string()))?),
        )
    } else if is_bytes(ty) {
        Some(quote!((#value).ok_or_else(||#rt::Error::Codec("missing byte metadata".into()))?))
    } else if let Some(inner) = argument(ty, "Option") {
        let decode = metadata_decode(&inner, quote!(::core::option::Option::Some(__value)), rt)?;
        Some(
            quote!(match #value {::core::option::Option::Some(__value)=>::core::option::Option::Some(#decode),::core::option::Option::None=>::core::option::Option::None}),
        )
    } else {
        None
    }
}

fn update(ty: &Type, value: TokenStream, slot: TokenStream, rt: &syn::Path) -> TokenStream {
    if let Some(kind) = cow_kind(ty) {
        if kind == "str" {
            quote! {let __owned=(#slot).to_mut();__owned.clear();__owned.push_str(*#value);}
        } else {
            quote! {let __owned=(#slot).to_mut();__owned.clear();__owned.extend_from_slice(*#value);}
        }
    } else if is_string(ty) {
        quote! {(#slot).clear();(#slot).push_str(*#value);}
    } else if is_bytes(ty) {
        quote! {(#slot).clear();(#slot).extend_from_slice(*#value);}
    } else if let Some(inner) = argument(ty, "Option") {
        let update = update(&inner, quote!(__value), quote!(__inner), rt);
        let owned = owned(&inner, quote!(__value), rt);
        quote! {match (#slot,(#value).as_ref()) {(__target,::core::option::Option::None)=>*__target=::core::option::Option::None,(::core::option::Option::Some(__inner),::core::option::Option::Some(__value))=>{#update},(__target,::core::option::Option::Some(__value))=>*__target=::core::option::Option::Some(#owned)}}
    } else {
        quote! {::core::clone::Clone::clone_from(#slot,#value);}
    }
}

fn has_sequence(ty: &Type) -> bool {
    if is_bytes(ty) {
        false
    } else if argument(ty, "Vec").is_some() {
        true
    } else if let Some(inner) = argument(ty, "Option") {
        has_sequence(&inner)
    } else {
        false
    }
}
fn decode_expr(ty: &Type, rt: &syn::Path) -> TokenStream {
    if has_sequence(ty) {
        let projected = project(ty, rt);
        quote!(<#projected as #rt::serde_bridge::BorrowDecode>::borrow_decode(&mut __deserializer)?)
    } else {
        quote!(#rt::__private::serde::Deserialize::deserialize(&mut __deserializer)?)
    }
}

fn compact_metadata_write(
    ty: &Type,
    value: TokenStream,
    bytes: bool,
    header: Option<&str>,
    rt: &syn::Path,
) -> TokenStream {
    let optional = argument(ty, "Option").is_some();
    let field = if optional {
        quote!(__metadata_value)
    } else {
        value.clone()
    };
    let serialize = if bytes {
        quote!(#rt::__private::serde_bytes::serialize(#field,&mut #rt::serde_bridge::Serializer::new(__out))?;)
    } else {
        quote!(#rt::__private::serde::Serialize::serialize(#field,&mut #rt::serde_bridge::Serializer::new(__out))?;)
    };
    let present = if let Some(name) = header {
        quote!(__writer.encode_header(#name,|__out|{#serialize ::core::result::Result::Ok(())})?;)
    } else {
        quote!(__writer.encode_key(|__out|{#serialize ::core::result::Result::Ok(())})?;)
    };
    if optional {
        let absent = if let Some(name) = header {
            quote!(__writer.header(#name,::core::option::Option::None)?;)
        } else {
            quote!(__writer.key(::core::option::Option::None)?;)
        };
        quote!(match (#value).as_ref(){::core::option::Option::Some(__metadata_value)=>{#present},::core::option::Option::None=>{#absent}})
    } else {
        present
    }
}
fn compact_metadata_decode(ty: &Type, input: TokenStream, rt: &syn::Path) -> TokenStream {
    let inner = argument(ty, "Option");
    let actual = inner.as_ref().unwrap_or(ty);
    let projected = project(actual, rt);
    let decode = if has_sequence(actual) {
        quote!(<#projected as #rt::serde_bridge::BorrowDecode>::borrow_decode(&mut __metadata)?)
    } else {
        quote!(#rt::__private::serde::Deserialize::deserialize(&mut __metadata)?)
    };
    let present = quote!({let mut __metadata=#rt::serde_bridge::Deserializer::new(__metadata_bytes);let __metadata_value:#projected=#decode;__metadata.finish()?;__metadata_value});
    if inner.is_some() {
        quote!(match #input {::core::option::Option::Some(__metadata_bytes)=>::core::option::Option::Some(#present),::core::option::Option::None=>::core::option::Option::None})
    } else {
        quote!({let __metadata_bytes=(#input).ok_or_else(||#rt::Error::Codec("missing encoded metadata".into()))?;#present})
    }
}
