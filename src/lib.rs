
// src/lib.rs
// Re-export modules that both binaries need
pub mod config;
pub mod db_navigator;
pub mod schema_cache;

pub use schema_cache::{
    SchemaCache, Database, Schema, SchemaObject, Column, ObjectType, DataType,
    current_timestamp
};