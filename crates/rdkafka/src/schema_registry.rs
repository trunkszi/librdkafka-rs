//! Schema Registry REST operations, caller-thread caches, and Confluent framing.
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    Error, Result,
    http::{HttpRequest, HttpTransport},
    oauth::percent_encode,
};

/// Schema language understood by the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}
impl SchemaType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
        }
    }
    fn parse(value: Option<&serde_json::Value>) -> Result<Self> {
        match value.and_then(serde_json::Value::as_str) {
            None if value.is_none() => Ok(Self::Avro),
            Some("AVRO") => Ok(Self::Avro),
            Some("JSON") => Ok(Self::Json),
            Some("PROTOBUF") => Ok(Self::Protobuf),
            _ => Err(registry_error("unsupported schema type")),
        }
    }
}
/// Schema definition, including language to prevent cross-language cache collisions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Schema {
    pub schema: String,
    pub schema_type: SchemaType,
}
impl Schema {
    pub fn new(schema: impl Into<String>, schema_type: SchemaType) -> Self {
        Self {
            schema: schema.into(),
            schema_type,
        }
    }
    fn json(&self) -> serde_json::Value {
        serde_json::json!({"schema": self.schema, "schemaType": self.schema_type.as_str()})
    }
}
/// Registry identity and definition for a subject version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    pub id: u32,
    pub subject: String,
    pub version: u32,
    pub schema: Schema,
}
#[derive(Default)]
struct Cache {
    ids: HashMap<u32, Schema>,
    subjects: HashMap<(String, Schema), u32>,
}
/// Synchronous, thread-affine Schema Registry client with a private shared cache.
///
/// Clones of internal cache handles remain on the creating thread. Latest-version
/// queries always reach the registry because subject versions may change remotely.
pub struct SchemaRegistryClient {
    transport: HttpTransport,
    base_url: String,
    headers: Vec<(String, String)>,
    cache: Rc<RefCell<Cache>>,
}
impl SchemaRegistryClient {
    pub fn new(transport: HttpTransport, base_url: impl Into<String>) -> Self {
        Self {
            transport,
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            headers: Vec::new(),
            cache: Rc::default(),
        }
    }
    /// Set authentication or other request headers. Header values are never logged.
    pub fn set_headers(&mut self, headers: Vec<(String, String)>) {
        self.headers = headers;
    }
    pub fn clear_cache(&mut self) {
        *self.cache.borrow_mut() = Cache::default();
    }
    /// Register a schema under a subject, reusing a previously resolved identity.
    ///
    /// # Errors
    /// Rejects invalid subjects, HTTP failures, and malformed registry responses.
    pub fn register_schema(&mut self, subject: &str, schema: &Schema) -> Result<u32> {
        let encoded = encode_subject(subject)?;
        let key = (subject.to_owned(), schema.clone());
        if let Some(id) = self.cache.borrow().subjects.get(&key) {
            return Ok(*id);
        }
        let path = format!("/subjects/{encoded}/versions");
        let value = self.request("POST", &path, Some(schema.json()))?;
        let id = schema_id(&value)?;
        let mut cache = self.cache.borrow_mut();
        cache.subjects.insert(key, id);
        cache.ids.insert(id, schema.clone());
        Ok(id)
    }
    /// Fetch the definition for a global schema ID.
    ///
    /// # Errors
    /// Rejects unknown IDs, invalid schema types, and malformed responses.
    pub fn get_schema_by_id(&mut self, id: u32) -> Result<Schema> {
        if let Some(schema) = self.cache.borrow().ids.get(&id) {
            return Ok(schema.clone());
        }
        let value = self.request("GET", &format!("/schemas/ids/{id}"), None)?;
        let schema = parse_schema(&value)?;
        self.cache.borrow_mut().ids.insert(id, schema.clone());
        Ok(schema)
    }
    /// Fetch the current latest version for a subject without caching its freshness.
    ///
    /// # Errors
    /// Rejects invalid or unknown subjects and inconsistent or malformed identities.
    pub fn get_latest_subject_schema(&mut self, subject: &str) -> Result<RegisteredSchema> {
        let encoded = encode_subject(subject)?;
        let value = self.request("GET", &format!("/subjects/{encoded}/versions/latest"), None)?;
        self.registered(subject, &value)
    }
    /// Resolve an existing subject/schema pair without registering it.
    ///
    /// # Errors
    /// Rejects invalid subjects, failed lookups, or mismatched subjects and schemas.
    pub fn lookup_schema(&mut self, subject: &str, schema: &Schema) -> Result<RegisteredSchema> {
        let encoded = encode_subject(subject)?;
        let value = self.request("POST", &format!("/subjects/{encoded}"), Some(schema.json()))?;
        let registered = parse_registered(subject, &value)?;
        if registered.schema != *schema {
            return Err(registry_error("lookup returned a different schema"));
        }
        self.cache_registered(&registered);
        Ok(registered)
    }
    /// Resolve the ID of an already registered subject/schema pair.
    ///
    /// # Errors
    /// Rejects invalid subjects, unknown schemas, and invalid registry responses.
    pub fn lookup_schema_id(&mut self, subject: &str, schema: &Schema) -> Result<u32> {
        encode_subject(subject)?;
        let key = (subject.to_owned(), schema.clone());
        if let Some(id) = self.cache.borrow().subjects.get(&key) {
            return Ok(*id);
        }
        Ok(self.lookup_schema(subject, schema)?.id)
    }
    fn registered(&self, subject: &str, value: &serde_json::Value) -> Result<RegisteredSchema> {
        let schema = parse_registered(subject, value)?;
        self.cache_registered(&schema);
        Ok(schema)
    }
    fn cache_registered(&self, value: &RegisteredSchema) {
        let mut cache = self.cache.borrow_mut();
        cache
            .subjects
            .insert((value.subject.clone(), value.schema.clone()), value.id);
        cache.ids.insert(value.id, value.schema.clone());
    }
    fn request(
        &mut self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let mut request = HttpRequest::new(method, format!("{}{path}", self.base_url));
        request.headers = self.headers.clone();
        request.headers.push((
            "Accept".into(),
            "application/vnd.schemaregistry.v1+json".into(),
        ));
        if let Some(body) = body {
            request.headers.push((
                "Content-Type".into(),
                "application/vnd.schemaregistry.v1+json".into(),
            ));
            request.body = serde_json::to_vec(&body)
                .map_err(|_| registry_error("schema serialization failed"))?;
        }
        let response = self
            .transport
            .execute(request)
            .map_err(|_| registry_error("registry request failed"))?;
        if !(200..300).contains(&response.status) {
            return Err(registry_error(&format!(
                "registry returned HTTP {}",
                response.status
            )));
        }
        serde_json::from_slice(&response.body)
            .map_err(|_| registry_error("invalid registry response JSON"))
    }
}
fn encode_subject(subject: &str) -> Result<String> {
    if subject.trim().is_empty() || matches!(subject, "." | "..") {
        return Err(registry_error("invalid registry subject"));
    }
    Ok(percent_encode(subject))
}
fn registry_error(message: &str) -> Error {
    Error::SchemaRegistry(message.into())
}
fn schema_id(value: &serde_json::Value) -> Result<u32> {
    value["id"]
        .as_u64()
        .and_then(|id| u32::try_from(id).ok())
        .ok_or_else(|| registry_error("invalid schema ID"))
}
fn parse_schema(value: &serde_json::Value) -> Result<Schema> {
    let schema = value["schema"]
        .as_str()
        .ok_or_else(|| registry_error("missing schema definition"))?;
    Ok(Schema::new(
        schema,
        SchemaType::parse(value.get("schemaType"))?,
    ))
}
fn parse_registered(subject: &str, value: &serde_json::Value) -> Result<RegisteredSchema> {
    if value["subject"].as_str() != Some(subject) {
        return Err(registry_error("registry returned a different subject"));
    }
    let version = value["version"]
        .as_u64()
        .and_then(|v| u32::try_from(v).ok())
        .filter(|v| *v > 0)
        .ok_or_else(|| registry_error("invalid schema version"))?;
    Ok(RegisteredSchema {
        id: schema_id(value)?,
        subject: subject.to_owned(),
        version,
        schema: parse_schema(value)?,
    })
}
/// Encode a Confluent prefix and payload into a reusable caller-owned buffer.
/// The destination is replaced only after validating its required length.
///
/// # Errors
/// Returns an error on size overflow or buffer allocation failure.
pub fn encode_confluent(schema_id: u32, payload: &[u8], destination: &mut Vec<u8>) -> Result<()> {
    let length = payload
        .len()
        .checked_add(5)
        .ok_or_else(|| registry_error("frame length overflow"))?;
    destination
        .try_reserve(length.saturating_sub(destination.len()))
        .map_err(|_| registry_error("frame allocation failed"))?;
    destination.clear();
    destination.push(0);
    destination.extend_from_slice(&schema_id.to_be_bytes());
    destination.extend_from_slice(payload);
    Ok(())
}
/// Decode the five-byte Confluent prefix without copying the payload.
/// Schema ID zero is a valid unsigned wire value.
///
/// # Errors
/// Rejects truncated frames and unknown magic bytes.
pub fn decode_confluent(frame: &[u8]) -> Result<(u32, &[u8])> {
    if frame.len() < 5 {
        return Err(registry_error("truncated Confluent frame"));
    }
    if frame[0] != 0 {
        return Err(registry_error("invalid Confluent magic byte"));
    }
    Ok((
        u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]),
        &frame[5..],
    ))
}
