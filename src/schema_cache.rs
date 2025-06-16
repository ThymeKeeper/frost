// src/schema_cache.rs
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct SchemaCache {
    pub version: u32,
    pub last_refreshed: i64,
    pub databases: HashMap<String, Database>,
    pub available_roles: Vec<String>,  // List of all available roles
    pub current_role: Option<String>,  // Currently selected role in IDE
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Database {
    pub name: String,
    pub comment: Option<String>,
    pub owner: Option<String>,
    pub last_refreshed: i64,
    pub schemas: HashMap<String, Schema>,
    pub accessible_by_roles: HashSet<String>,  // Which roles can see this database
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Schema {
    pub name: String,
    pub database: String,
    pub comment: Option<String>,
    pub owner: Option<String>,
    pub last_refreshed: i64,
    pub objects: HashMap<String, SchemaObject>,
    pub accessible_by_roles: HashSet<String>,  // Which roles can see this schema
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SchemaObject {
    pub name: String,
    pub object_type: ObjectType,
    pub comment: Option<String>,
    pub owner: Option<String>,
    pub last_refreshed: i64,
    
    // Table/View specific
    pub row_count: Option<i64>,
    pub bytes: Option<i64>,
    pub columns: Vec<Column>,
    
    // Procedure/Function specific
    pub arguments: Option<String>,
    pub return_type: Option<String>,
    pub language: Option<String>,
    
    // Task specific
    pub schedule: Option<String>,
    pub state: Option<String>,
    
    // Role access tracking
    pub accessible_by_roles: HashSet<String>,  // Which roles can see this object
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ObjectType {
    Table,
    View,
    Procedure,
    Function,
    Task,
    Stage,
    Stream,
    Sequence,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Column {
    pub name: String,
    pub position: i32,
    pub data_type: String,
    pub type_details: DataType,
    pub is_nullable: bool,
    pub is_identity: bool,
    pub default_value: Option<String>,
    pub comment: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "base_type")]
pub enum DataType {
    // Text types
    Varchar { length: Option<u32> },
    Char { length: Option<u32> },
    String { length: Option<u32> },
    Text,
    
    // Numeric types
    Number { precision: Option<u32>, scale: Option<u32> },
    Decimal { precision: Option<u32>, scale: Option<u32> },
    Numeric { precision: Option<u32>, scale: Option<u32> },
    Int,
    Integer,
    BigInt,
    SmallInt,
    TinyInt,
    Float,
    Float4,
    Float8,
    Double,
    DoublePrecision,
    Real,
    
    // Date/Time types
    Date,
    Time { precision: Option<u32> },
    Timestamp { precision: Option<u32> },
    TimestampLtz { precision: Option<u32> },
    TimestampNtz { precision: Option<u32> },
    TimestampTz { precision: Option<u32> },
    
    // Boolean
    Boolean,
    
    // Semi-structured
    Variant,
    Object,
    Array,
    
    // Binary
    Binary { length: Option<u32> },
    Varbinary { length: Option<u32> },
    
    // Geospatial
    Geography,
    Geometry,
    
    // Other
    Unknown { raw_type: String },
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            version: 2,  // Bump version for role support
            last_refreshed: current_timestamp(),
            databases: HashMap::new(),
            available_roles: Vec::new(),
            current_role: None,
        }
    }
    
    /// Check if an object is accessible by the current role (if set) or any role
    pub fn is_object_accessible(&self, db: &str, schema: &str, object: &str) -> bool {
        if let Some(database) = self.databases.get(db) {
            if let Some(schema_obj) = database.schemas.get(schema) {
                if let Some(obj) = schema_obj.objects.get(object) {
                    if let Some(ref current_role) = self.current_role {
                        return obj.accessible_by_roles.contains(current_role);
                    }
                    // If no role selected, object is accessible if any role can see it
                    return !obj.accessible_by_roles.is_empty();
                }
            }
        }
        false
    }
}

impl Database {
    pub fn new(name: String, owner: Option<String>, comment: Option<String>) -> Self {
        Self {
            name,
            comment,
            owner,
            last_refreshed: current_timestamp(),
            schemas: HashMap::new(),
            accessible_by_roles: HashSet::new(),
        }
    }
    
    pub fn add_role_access(&mut self, role: &str) {
        self.accessible_by_roles.insert(role.to_string());
    }
}

impl Schema {
    pub fn new(name: String, database: String, owner: Option<String>, comment: Option<String>) -> Self {
        Self {
            name,
            database,
            comment,
            owner,
            last_refreshed: current_timestamp(),
            objects: HashMap::new(),
            accessible_by_roles: HashSet::new(),
        }
    }
    
    pub fn add_role_access(&mut self, role: &str) {
        self.accessible_by_roles.insert(role.to_string());
    }
}

impl SchemaObject {
    pub fn add_role_access(&mut self, role: &str) {
        self.accessible_by_roles.insert(role.to_string());
    }
    
    pub fn is_accessible_by_role(&self, role: Option<&str>) -> bool {
        if let Some(r) = role {
            self.accessible_by_roles.contains(r)
        } else {
            // If no role specified, accessible if any role can see it
            !self.accessible_by_roles.is_empty()
        }
    }
}

pub fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}