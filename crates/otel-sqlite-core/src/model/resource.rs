use super::attribute::{Attribute, AttributeValue};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Resource {
    pub id: String,
    pub attributes: Vec<Attribute>,
    pub schema_url: String,
}

impl Resource {
    pub fn new(attributes: Vec<Attribute>) -> Self {
        Self {
            attributes,
            ..Self::default()
        }
    }

    pub fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes
            .iter()
            .find(|attribute| attribute.key == key)
            .map(|attribute| &attribute.value)
    }

    pub fn service_name(&self) -> &str {
        self.get("service.name")
            .and_then(AttributeValue::as_str)
            .unwrap_or("")
    }

    pub fn host_name(&self) -> &str {
        self.get("host.name")
            .and_then(AttributeValue::as_str)
            .unwrap_or("")
    }
}
