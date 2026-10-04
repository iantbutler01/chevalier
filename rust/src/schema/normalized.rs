use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::parsers::FieldDescription;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaType {
    Object,
    Array,
    String,
    Integer,
    Number,
    Boolean,
    Null,
}

impl SchemaType {
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "object" => Ok(Self::Object),
            "array" => Ok(Self::Array),
            "string" => Ok(Self::String),
            "integer" => Ok(Self::Integer),
            "number" => Ok(Self::Number),
            "boolean" => Ok(Self::Boolean),
            "null" => Ok(Self::Null),
            other => Err(Error::NonRetryable(format!(
                "Unsupported JSON schema type: {}",
                other
            ))),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Array => "array",
            Self::String => "string",
            Self::Integer => "integer",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Null => "null",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AdditionalProperties {
    Bool(bool),
    Schema(Box<ToolParametersSchema>),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ToolParametersSchema {
    pub schema_type: Option<SchemaType>,
    pub description: Option<String>,
    pub properties: BTreeMap<String, ToolParametersSchema>,
    pub required: Vec<String>,
    pub items: Option<Box<ToolParametersSchema>>,
    pub any_of: Vec<ToolParametersSchema>,
    pub one_of: Vec<ToolParametersSchema>,
    pub all_of: Vec<ToolParametersSchema>,
    pub enum_values: Vec<Value>,
    pub additional_properties: Option<AdditionalProperties>,
}

impl ToolParametersSchema {
    pub fn from_json_schema(schema: &Value) -> Result<Self> {
        let mut expanded = schema.clone();
        super::inline_schema_refs(&mut expanded);
        Self::parse_schema_value(&expanded)
    }

    pub fn from_field_descriptions(fields: &[FieldDescription]) -> Self {
        let mut properties = BTreeMap::new();
        let mut required = Vec::new();

        for field in fields {
            let mut schema = Self::from_field_type(&field.field_type);
            if !field.description.is_empty() {
                schema.description = Some(field.description.clone());
            }
            if field.required {
                required.push(field.name.clone());
            }
            properties.insert(field.name.clone(), schema);
        }

        Self {
            schema_type: Some(SchemaType::Object),
            properties,
            required,
            ..Self::default()
        }
    }

    pub fn to_json_schema(&self) -> Value {
        let mut map = Map::new();

        if let Some(description) = &self.description {
            map.insert(
                "description".to_string(),
                Value::String(description.clone()),
            );
        }

        if let Some(schema_type) = &self.schema_type {
            map.insert(
                "type".to_string(),
                Value::String(schema_type.as_str().to_string()),
            );
        }

        if !self.properties.is_empty() {
            let properties = self
                .properties
                .iter()
                .map(|(name, schema)| (name.clone(), schema.to_json_schema()))
                .collect::<Map<String, Value>>();
            map.insert("properties".to_string(), Value::Object(properties));
            map.entry("type".to_string())
                .or_insert_with(|| Value::String("object".to_string()));
            map.insert(
                "required".to_string(),
                Value::Array(
                    self.required
                        .iter()
                        .map(|name| Value::String(name.clone()))
                        .collect(),
                ),
            );
        } else if !self.required.is_empty() || matches!(self.schema_type, Some(SchemaType::Object))
        {
            map.insert(
                "required".to_string(),
                Value::Array(
                    self.required
                        .iter()
                        .map(|name| Value::String(name.clone()))
                        .collect(),
                ),
            );
        }

        if let Some(items) = &self.items {
            map.insert("items".to_string(), items.to_json_schema());
            map.entry("type".to_string())
                .or_insert_with(|| Value::String("array".to_string()));
        }

        if !self.any_of.is_empty() {
            map.insert(
                "anyOf".to_string(),
                Value::Array(self.any_of.iter().map(Self::to_json_schema).collect()),
            );
        }

        if !self.one_of.is_empty() {
            map.insert(
                "oneOf".to_string(),
                Value::Array(self.one_of.iter().map(Self::to_json_schema).collect()),
            );
        }

        if !self.all_of.is_empty() {
            map.insert(
                "allOf".to_string(),
                Value::Array(self.all_of.iter().map(Self::to_json_schema).collect()),
            );
        }

        if !self.enum_values.is_empty() {
            map.insert("enum".to_string(), Value::Array(self.enum_values.clone()));
        }

        if let Some(additional_properties) = &self.additional_properties {
            map.insert(
                "additionalProperties".to_string(),
                match additional_properties {
                    AdditionalProperties::Bool(value) => Value::Bool(*value),
                    AdditionalProperties::Schema(schema) => schema.to_json_schema(),
                },
            );
        }

        Value::Object(map)
    }

    pub fn top_level_field_descriptions(&self) -> Vec<FieldDescription> {
        self.properties
            .iter()
            .map(|(name, schema)| FieldDescription {
                name: name.clone(),
                field_type: schema.field_type_label(),
                description: schema.description.clone().unwrap_or_default(),
                required: self.required.iter().any(|required| required == name),
            })
            .collect()
    }

    fn parse_schema_value(value: &Value) -> Result<Self> {
        let object = value.as_object().ok_or_else(|| {
            Error::NonRetryable(format!(
                "JSON schema must be an object, received: {}",
                value
            ))
        })?;

        let mut schema = Self {
            description: object
                .get("description")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string()),
            ..Self::default()
        };

        let (schema_type, implied_any_of) = parse_schema_type(object.get("type"))?;
        schema.schema_type = schema_type;
        schema.any_of.extend(implied_any_of);

        if let Some(properties) = object.get("properties").and_then(|value| value.as_object()) {
            schema.schema_type.get_or_insert(SchemaType::Object);
            for (name, property_schema) in properties {
                schema
                    .properties
                    .insert(name.clone(), Self::parse_schema_value(property_schema)?);
            }
        }

        if let Some(required) = object.get("required").and_then(|value| value.as_array()) {
            schema.required = required
                .iter()
                .filter_map(|value| value.as_str().map(|value| value.to_string()))
                .collect();
        }

        if let Some(items) = object.get("items") {
            schema.schema_type.get_or_insert(SchemaType::Array);
            schema.items = Some(Box::new(Self::parse_schema_value(items)?));
        }

        schema.any_of.extend(parse_variants(object.get("anyOf"))?);
        schema.one_of.extend(parse_variants(object.get("oneOf"))?);
        schema.all_of.extend(parse_variants(object.get("allOf"))?);

        if let Some(enum_values) = object.get("enum").and_then(|value| value.as_array()) {
            schema.enum_values = enum_values.clone();
        }

        if let Some(additional_properties) = object.get("additionalProperties") {
            schema.additional_properties = Some(match additional_properties {
                Value::Bool(value) => AdditionalProperties::Bool(*value),
                Value::Object(_) => AdditionalProperties::Schema(Box::new(
                    Self::parse_schema_value(additional_properties)?,
                )),
                other => {
                    return Err(Error::NonRetryable(format!(
                        "Unsupported additionalProperties schema: {}",
                        other
                    )));
                }
            });
        }

        Ok(schema)
    }

    fn from_field_type(field_type: &str) -> Self {
        let ty = strip_type_modifiers(field_type);
        if ty.is_empty() {
            return Self::primitive(SchemaType::String);
        }

        if ty.eq_ignore_ascii_case("array") {
            return Self {
                schema_type: Some(SchemaType::Array),
                items: Some(Box::new(Self::primitive(SchemaType::String))),
                ..Self::default()
            };
        }

        if ty.starts_with('[') {
            return Self {
                schema_type: Some(SchemaType::Array),
                items: Some(Box::new(Self::from_field_type(
                    extract_slice_inner_type(ty).unwrap_or("String"),
                ))),
                ..Self::default()
            };
        }

        let (base, generics) = split_type_and_generics(ty);
        let ident = base.rsplit("::").next().unwrap_or(base);
        let ident_lower = ident.to_ascii_lowercase();

        match ident_lower.as_str() {
            "option" => generics
                .map(Self::from_field_type)
                .unwrap_or_else(|| Self::primitive(SchemaType::String)),
            "vec" | "vecdeque" | "linkedlist" => Self {
                schema_type: Some(SchemaType::Array),
                items: Some(Box::new(Self::from_field_type(
                    generics.unwrap_or("String"),
                ))),
                ..Self::default()
            },
            "hashmap" | "btreemap" | "indexmap" => Self {
                schema_type: Some(SchemaType::Object),
                additional_properties: Some(AdditionalProperties::Bool(true)),
                ..Self::default()
            },
            "string" | "str" => Self::primitive(SchemaType::String),
            "number" | "f32" | "f64" | "float" => Self::primitive(SchemaType::Number),
            "integer" | "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" | "usize"
            | "isize" => Self::primitive(SchemaType::Integer),
            "boolean" | "bool" => Self::primitive(SchemaType::Boolean),
            "object" => Self::primitive(SchemaType::Object),
            "value" if base.contains("serde_json") => Self::primitive(SchemaType::Object),
            _ => Self::primitive(SchemaType::Object),
        }
    }

    fn primitive(schema_type: SchemaType) -> Self {
        Self {
            schema_type: Some(schema_type),
            ..Self::default()
        }
    }

    fn field_type_label(&self) -> String {
        match &self.schema_type {
            Some(SchemaType::Array) => self
                .items
                .as_deref()
                .map(|items| format!("Vec<{}>", items.field_type_label()))
                .unwrap_or_else(|| "array".to_string()),
            Some(schema_type) => schema_type.as_str().to_string(),
            None if !self.properties.is_empty() => "object".to_string(),
            None if self.items.is_some() => "array".to_string(),
            _ => "string".to_string(),
        }
    }
}

fn parse_schema_type(
    value: Option<&Value>,
) -> Result<(Option<SchemaType>, Vec<ToolParametersSchema>)> {
    let Some(value) = value else {
        return Ok((None, Vec::new()));
    };

    match value {
        Value::String(schema_type) => Ok((Some(SchemaType::from_str(schema_type)?), Vec::new())),
        Value::Array(types) => {
            let mut variants = Vec::new();
            for schema_type in types.iter().filter_map(Value::as_str) {
                let parsed = SchemaType::from_str(schema_type)?;
                if parsed != SchemaType::Null {
                    variants.push(ToolParametersSchema::primitive(parsed));
                }
            }

            match variants.len() {
                0 => Ok((Some(SchemaType::Null), Vec::new())),
                1 => Ok((
                    variants.pop().and_then(|schema| schema.schema_type),
                    Vec::new(),
                )),
                _ => Ok((None, variants)),
            }
        }
        other => Err(Error::NonRetryable(format!(
            "Unsupported JSON schema type declaration: {}",
            other
        ))),
    }
}

fn parse_variants(value: Option<&Value>) -> Result<Vec<ToolParametersSchema>> {
    let Some(Value::Array(variants)) = value else {
        return Ok(Vec::new());
    };

    variants
        .iter()
        .map(ToolParametersSchema::parse_schema_value)
        .collect()
}

fn strip_type_modifiers(field_type: &str) -> &str {
    let mut ty = field_type.trim();
    loop {
        if let Some(stripped) = ty.strip_prefix('&') {
            ty = stripped.trim_start();
            if let Some(stripped_mut) = ty.strip_prefix("mut") {
                ty = stripped_mut.trim_start();
            }
            continue;
        }
        break;
    }
    ty
}

fn split_type_and_generics(ty: &str) -> (&str, Option<&str>) {
    if let Some(start) = ty.find('<') {
        let base = &ty[..start];
        let mut depth = 0usize;
        for (offset, ch) in ty[start..].char_indices() {
            match ch {
                '<' => depth += 1,
                '>' => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                    if depth == 0 {
                        let inner = &ty[start + 1..start + offset];
                        return (base, Some(inner));
                    }
                }
                _ => {}
            }
        }
        (base, None)
    } else {
        (ty, None)
    }
}

fn extract_slice_inner_type(ty: &str) -> Option<&str> {
    if let Some(end) = ty.find(';') {
        return Some(ty[1..end].trim());
    }

    ty.rfind(']').map(|end| ty[1..end].trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_nested_schema_with_local_refs() {
        let raw = json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": { "$ref": "#/$defs/ThreadItem" }
                }
            },
            "required": ["items"],
            "$defs": {
                "ThreadItem": {
                    "type": "object",
                    "properties": {
                        "text": { "type": "string" },
                        "image_ids": {
                            "type": "array",
                            "items": { "type": "integer" }
                        }
                    },
                    "required": ["text"]
                }
            }
        });

        let parsed = ToolParametersSchema::from_json_schema(&raw).unwrap();
        let round_tripped = parsed.to_json_schema();
        let nested = &round_tripped["properties"]["items"]["items"]["properties"];
        assert_eq!(nested["text"]["type"], "string");
        assert_eq!(nested["image_ids"]["items"]["type"], "integer");
        assert!(round_tripped.get("$defs").is_none());
    }

    #[test]
    fn field_descriptions_round_trip_into_recursive_schema() {
        let fields = vec![
            FieldDescription {
                name: "title".to_string(),
                field_type: "string".to_string(),
                description: "Thread title".to_string(),
                required: true,
            },
            FieldDescription {
                name: "items".to_string(),
                field_type: "Vec<object>".to_string(),
                description: "Thread items".to_string(),
                required: false,
            },
        ];

        let parsed = ToolParametersSchema::from_field_descriptions(&fields);
        let round_tripped = parsed.to_json_schema();
        assert_eq!(round_tripped["properties"]["title"]["type"], "string");
        assert_eq!(round_tripped["properties"]["items"]["type"], "array");
    }

    #[test]
    fn strips_nullable_type_arrays_to_primary_type() {
        let raw = json!({
            "type": ["string", "null"]
        });

        let parsed = ToolParametersSchema::from_json_schema(&raw).unwrap();
        assert_eq!(parsed.schema_type, Some(SchemaType::String));
        assert!(parsed.any_of.is_empty());
    }

    #[test]
    fn preserves_required_in_untyped_alternatives() {
        let raw = json!({
            "type": "object",
            "properties": {"instruction": {"type": "string"}, "script": {"type": "string"}},
            "anyOf": [{"required": ["instruction"]}, {"required": ["script"]}]
        });
        let round_tripped = ToolParametersSchema::from_json_schema(&raw)
            .unwrap()
            .to_json_schema();
        assert_eq!(round_tripped["anyOf"], raw["anyOf"]);
    }
}
