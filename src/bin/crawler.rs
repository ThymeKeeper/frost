// src/bin/crawler.rs
use clap::Parser;
use std::path::PathBuf;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader};
use std::collections::{HashSet, HashMap};
use odbc::{create_environment_v3, Data, Statement, odbc_safe};
use anyhow::Result;
use Frost::{
    config::Config,
    schema_cache::{SchemaCache, Database, Schema, SchemaObject, Column, ObjectType, DataType, current_timestamp},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    let data_dir = if let Some(dir) = args.data_dir {
        dir
    } else {
        let exe_path = std::env::current_exe()
            .expect("Failed to get current exe path");
        let exe_dir = exe_path.parent()
            .expect("Failed to get parent directory")
            .to_path_buf();
        exe_dir
    };
    
    fs::create_dir_all(&data_dir)?;
    
    println!("Crawler using data_dir: {}", data_dir.display());
    
    // Try to acquire lock
    let lock_path = data_dir.join("crawler.lock");
    let _lock_file = match OpenOptions::new()
        .write(true)
        .create(true)
        .open(&lock_path)
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            println!("Crawler already running");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    
    #[cfg(unix)]
    {
        use fs2::FileExt;
        if _lock_file.try_lock_exclusive().is_err() {
            println!("Crawler already running");
            return Ok(());
        }
    }
    
    println!("Snowflake crawler started");
    
    // Load configuration
    let config = Config::load()?;
    
    let env = create_environment_v3()
        .map_err(|e| anyhow::anyhow!("Failed to create ODBC environment: {:?}", e))?;
    let conn = env.connect_with_connection_string(&config.connection_string)
        .map_err(|e| anyhow::anyhow!("Failed to connect: {:?}", e))?;
    println!("Connected successfully!");
    
    // Load or create cache
    let cache_path = data_dir.join("schema_cache.json");
    let mut cache = load_or_create_cache(&cache_path)?;
    
    // Get available roles for the user
    let available_roles = get_available_roles(&conn)?;
    println!("Found {} available roles: {:?}", available_roles.len(), available_roles);
    cache.available_roles = available_roles.clone();
    
    // Get current role to restore later
    let original_role = get_current_role(&conn)?;
    println!("Current role: {:?}", original_role);
    
    // Process queue with role iteration
    let queue_path = data_dir.join("crawler_queue.txt");
    match process_queue(&queue_path, &mut cache, &conn, &cache_path, &available_roles) {
        Ok(_) => println!("Queue processed successfully"),
        Err(e) => {
            println!("Error processing queue: {}", e);
            // Restore original role
            if let Some(role) = original_role {
                let _ = set_role(&conn, &role);
            }
            save_cache(&cache_path, &cache)?;
            return Err(e);
        }
    }
    
    // Restore original role
    if let Some(role) = original_role {
        let _ = set_role(&conn, &role);
    }
    
    // Final save
    save_cache(&cache_path, &cache)?;
    println!("Cache saved to: {}", cache_path.display());
    println!("Total databases in cache: {}", cache.databases.len());
    
    // Clean up lock file
    #[cfg(unix)]
    {
        use fs2::FileExt;
        let _ = _lock_file.unlock();
    }
    drop(_lock_file);
    let _ = fs::remove_file(&lock_path);
    
    println!("Crawler finished");
    Ok(())
}

fn get_available_roles<'env>(conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>) -> Result<Vec<String>> {
    let stmt = Statement::with_parent(conn)?;
    let mut roles = Vec::new();
    
    // First get the current user
    let current_user = match stmt.exec_direct("SELECT CURRENT_USER()")? {
        Data(mut stmt) => {
            if let Some(mut cursor) = stmt.fetch()? {
                cursor.get_data::<String>(1)?.unwrap_or_default()
            } else {
                return Ok(roles);
            }
        }
        _ => return Ok(roles),
    };
    
    // Now get grants for this user
    let stmt = Statement::with_parent(conn)?;
    let query = format!("SHOW GRANTS TO USER \"{}\"", current_user);
    
    match stmt.exec_direct(&query)? {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch()? {
                // The structure is typically:
                // Column 1: created_on
                // Column 2: privilege (e.g., "USAGE", "OWNERSHIP")
                // Column 3: granted_on (e.g., "ROLE", "DATABASE", "SCHEMA")
                // Column 4: name (the actual object name)
                
                if let Ok(Some(granted_on)) = cursor.get_data::<String>(3) {
                    if granted_on == "ROLE" {
                        // Column 4 has the role name when granted_on is "ROLE"
                        if let Ok(Some(role)) = cursor.get_data::<String>(4) {
                            roles.push(role);
                        }
                    }
                }
            }
        }
        _ => {}
    }
    
    // Always include the current role if it's not already in the list
    let current_role = get_current_role(conn)?;
    if let Some(role) = current_role {
        if !roles.contains(&role) {
            roles.push(role);
        }
    }
    
    println!("Found {} available roles: {:?}", roles.len(), roles);
    
    Ok(roles)
}

fn get_current_role<'env>(conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>) -> Result<Option<String>> {
    let stmt = Statement::with_parent(conn)?;
    
    match stmt.exec_direct("SELECT CURRENT_ROLE()")? {
        Data(mut stmt) => {
            if let Some(mut cursor) = stmt.fetch()? {
                return Ok(cursor.get_data::<String>(1)?);  // Add Ok() and ?
            }
        }
        _ => {}
    }
    
    Ok(None)
}

fn set_role<'env>(conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>, role: &str) -> Result<()> {
    let stmt = Statement::with_parent(conn)?;
    stmt.exec_direct(&format!("USE ROLE \"{}\"", role))?;
    Ok(())
}

fn load_or_create_cache(path: &PathBuf) -> Result<SchemaCache> {
    if path.exists() {
        let content = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    } else {
        Ok(SchemaCache::new())
    }
}

fn save_cache(path: &PathBuf, cache: &SchemaCache) -> Result<()> {
    let start = std::time::Instant::now();
    
    println!("Saving cache with {} databases", cache.databases.len());
    let total_schemas: usize = cache.databases.values().map(|db| db.schemas.len()).sum();
    let total_objects: usize = cache.databases.values()
        .flat_map(|db| db.schemas.values())
        .map(|schema| schema.objects.len())
        .sum();
    println!("  Total schemas: {}, Total objects: {}", total_schemas, total_objects);
    
    let json = serde_json::to_string_pretty(cache)?;
    println!("  JSON size: {:.2} MB", json.len() as f64 / 1_048_576.0);
    
    // Write to temp file first for atomic update
    let temp_path = path.with_extension("tmp");
    fs::write(&temp_path, json)?;
    fs::rename(&temp_path, path)?;
    
    println!("  Cache saved successfully in {:.2?}", start.elapsed());
    Ok(())
}

fn process_queue<'env>(
    queue_path: &PathBuf, 
    cache: &mut SchemaCache, 
    conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>,
    cache_path: &PathBuf,
    available_roles: &[String],
) -> Result<()> {
    if !queue_path.exists() {
        return Ok(());
    }
    
    // Read and clear queue
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(queue_path)?;
    
    #[cfg(unix)]
    {
        use fs2::FileExt;
        file.lock_exclusive()?;
    }
    
    let reader = BufReader::new(&file);
    let commands: Vec<String> = reader
        .lines()
        .filter_map(|line| line.ok())
        .map(|line| {
            if line.starts_with('\u{FEFF}') {
                line.trim_start_matches('\u{FEFF}').to_string()
            } else {
                line
            }
        })
        .filter(|line| !line.trim().is_empty())
        .collect();
    
    file.set_len(0)?;
    
    #[cfg(unix)]
    {
        use fs2::FileExt;
        file.unlock()?;
    }
    
    // Deduplicate
    let mut seen = HashSet::new();
    let unique_commands: Vec<String> = commands
        .into_iter()
        .filter(|cmd| seen.insert(cmd.clone()))
        .collect();
    
    if unique_commands.is_empty() {
        return Ok(());
    }
    
    // Process commands for each role
    for cmd in unique_commands {
        println!("\n=== Processing command: {} ===", cmd);
        let parts: Vec<&str> = cmd.split_whitespace().collect();
        
        match parts.as_slice() {
            ["REFRESH", "DATABASE", db] => {
                refresh_database_optimized(conn, cache, db)?;
                save_cache(cache_path, cache)?;
            }
            ["REFRESH", "SCHEMA", schema] => {
                if let Some((db, sch)) = schema.split_once('.') {
                    refresh_schema_optimized(conn, cache, db, sch)?;
                    save_cache(cache_path, cache)?;
                }
            }
            ["REFRESH", "TABLE", table] => {
                let parts: Vec<&str> = table.split('.').collect();
                if parts.len() == 3 {
                    // Refreshing a table means refreshing its schema
                    refresh_schema_optimized(conn, cache, parts[0], parts[1])?;
                    save_cache(cache_path, cache)?;
                }
            }
            ["REFRESH", "ALL"] => {
                // Use optimized version that doesn't switch roles
                refresh_all_optimized(conn, cache, cache_path, available_roles)?;
                save_cache(cache_path, cache)?;
            }
            _ => eprintln!("Unknown command: {}", cmd),
        }
    }
    
    Ok(())
}

// ============ OPTIMIZED IMPLEMENTATIONS ============

struct DatabaseMetadata {
    schemas: Vec<SchemaMetadata>,
}

struct SchemaMetadata {
    name: String,
    owner: Option<String>,
    comment: Option<String>,
    objects: Vec<ObjectMetadata>,
}

struct ObjectMetadata {
    schema_name: String,
    object_name: String,
    object_type: ObjectType,
    owner: Option<String>,
    comment: Option<String>,
    row_count: Option<i64>,
    bytes: Option<i64>,
    columns: Vec<ColumnMetadata>,
    // For procedures/functions
    arguments: Option<String>,
    return_type: Option<String>,
    language: Option<String>,
}

struct ColumnMetadata {
    name: String,
    position: i32,
    data_type: String,
    is_nullable: bool,
    default_value: Option<String>,
    comment: Option<String>,
}

fn refresh_all_optimized<'env>(
    conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>, 
    cache: &mut SchemaCache,
    cache_path: &PathBuf,
    available_roles: &[String],
) -> Result<()> {
    println!("Starting optimized refresh (no role switching)...");
    let start_time = std::time::Instant::now();
    
    // Step 1: Get all accessible databases in one query
    let databases = get_all_accessible_databases(conn)?;
    println!("Found {} accessible databases", databases.len());
    
    // Step 2: For each database, get ALL metadata in one efficient query
    for (i, (db_name, db_owner, db_comment)) in databases.iter().enumerate() {
        println!("[{}/{}] Processing database: {}", i + 1, databases.len(), db_name);
        
        // Get or create database entry
        let db = cache.databases.entry(db_name.clone()).or_insert_with(|| {
            Database::new(db_name.clone(), db_owner.clone(), db_comment.clone())
        });
        db.last_refreshed = current_timestamp();
        
        // Get all schemas and objects for this database in ONE query
        match get_database_metadata_batch(conn, db_name) {
            Ok(metadata) => {
                process_database_metadata(cache, db_name, metadata);
            }
            Err(e) => {
                eprintln!("  Skipping database {} due to error: {}", db_name, e);
                continue;
            }
        }
        
        // Save periodically to avoid losing progress
        if i % 10 == 0 {
            save_cache(cache_path, cache)?;
        }
    }
    
    // Step 3: Determine role access more efficiently
    println!("Determining role access...");
    update_role_access_efficient(conn, cache, available_roles)?;
    
    cache.last_refreshed = current_timestamp();
    let elapsed = start_time.elapsed();
    println!("Optimized refresh completed in {:.2?}!", elapsed);
    Ok(())
}

fn refresh_database_optimized<'env>(
    conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>, 
    cache: &mut SchemaCache,
    database: &str,
) -> Result<()> {
    println!("Refreshing database {} (optimized)...", database);
    let start_time = std::time::Instant::now();
    
    // Get or create database entry
    let db_entry = cache.databases.entry(database.to_string()).or_insert_with(|| {
        Database::new(database.to_string(), None, None)
    });
    db_entry.last_refreshed = current_timestamp();
    
    // Get all metadata for this database in one batch
    match get_database_metadata_batch(conn, database) {
        Ok(metadata) => {
            process_database_metadata(cache, database, metadata);
            
            // Mark as accessible by current role
            if let Some(role) = get_current_role(conn)? {
                if let Some(db) = cache.databases.get_mut(database) {
                    db.add_role_access(&role);
                    for schema in db.schemas.values_mut() {
                        schema.add_role_access(&role);
                        for obj in schema.objects.values_mut() {
                            obj.add_role_access(&role);
                        }
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error refreshing database {}: {}", database, e);
            return Err(e);
        }
    }
    
    let elapsed = start_time.elapsed();
    println!("Database {} refreshed in {:.2?}", database, elapsed);
    Ok(())
}

fn refresh_schema_optimized<'env>(
    conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>, 
    cache: &mut SchemaCache,
    database: &str,
    schema: &str,
) -> Result<()> {
    println!("Refreshing schema {}.{} (optimized)...", database, schema);
    let start_time = std::time::Instant::now();
    
    // Ensure database exists in cache
    let db = cache.databases.entry(database.to_string()).or_insert_with(|| {
        Database::new(database.to_string(), None, None)
    });
    
    // Use the database
    let stmt = Statement::with_parent(conn)?;
    stmt.exec_direct(&format!("USE DATABASE \"{}\"", database))?;
    
    // Get schema metadata
    let stmt = Statement::with_parent(conn)?;
    let mut schema_meta = None;
    
    match stmt.exec_direct(&format!("SHOW SCHEMAS LIKE '{}'", schema))? {
        Data(mut stmt) => {
            if let Some(mut cursor) = stmt.fetch()? {
                let schema_name: String = cursor.get_data(2)?.unwrap_or_default();
                let owner: Option<String> = cursor.get_data(6)?;
                let comment: Option<String> = cursor.get_data(7)?;
                
                schema_meta = Some(SchemaMetadata {
                    name: schema_name,
                    owner,
                    comment,
                    objects: Vec::new(),
                });
            }
        }
        _ => {}
    }
    
    if schema_meta.is_none() {
        return Err(anyhow::anyhow!("Schema {}.{} not found", database, schema));
    }
    
    let mut schema_metadata = schema_meta.unwrap();
    
    // Get all objects and columns for this specific schema in ONE query
    let stmt = Statement::with_parent(conn)?;
    let query = format!(r#"
        SELECT 
            t.table_name,
            t.table_type,
            t.row_count,
            t.bytes,
            t.comment,
            t.table_owner,
            c.column_name,
            c.ordinal_position,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            c.comment as column_comment
        FROM {}.information_schema.tables t
        LEFT JOIN {}.information_schema.columns c 
            ON t.table_schema = c.table_schema 
            AND t.table_name = c.table_name
        WHERE t.table_schema = '{}'
        AND t.table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY t.table_name, c.ordinal_position
    "#, database, database, schema);
    
    let mut current_object: Option<ObjectMetadata> = None;
    let mut current_table = String::new();
    
    match stmt.exec_direct(&query)? {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch()? {
                let table_name: String = cursor.get_data(1)?.unwrap_or_default();
                
                // If we've moved to a new table, save the previous one
                if table_name != current_table && !current_table.is_empty() {
                    if let Some(obj) = current_object.take() {
                        schema_metadata.objects.push(obj);
                    }
                }
                
                // Start new object if needed
                if table_name != current_table {
                    current_table = table_name.clone();
                    
                    let table_type: String = cursor.get_data(2)?.unwrap_or_default();
                    let object_type = match table_type.as_str() {
                        "VIEW" => ObjectType::View,
                        _ => ObjectType::Table,
                    };
                    
                    current_object = Some(ObjectMetadata {
                        schema_name: schema.to_string(),
                        object_name: table_name.clone(),
                        object_type,
                        owner: cursor.get_data(6)?,
                        comment: cursor.get_data(5)?,
                        row_count: cursor.get_data(3)?,
                        bytes: cursor.get_data(4)?,
                        columns: Vec::new(),
                        arguments: None,
                        return_type: None,
                        language: None,
                    });
                }
                
                // Add column if present
                if let Some(ref mut obj) = current_object {
                    if let Ok(Some(column_name)) = cursor.get_data::<String>(7) {
                        let char_length: Option<i32> = cursor.get_data(10)?;
                        let precision: Option<i32> = cursor.get_data(11)?;
                        let scale: Option<i32> = cursor.get_data(12)?;
                        let is_nullable: String = cursor.get_data(13)?.unwrap_or_default();
                        let column_default: Option<String> = cursor.get_data(14)?;
                        let column_comment: Option<String> = cursor.get_data(15)?;
                        
                        obj.columns.push(ColumnMetadata {
                            name: column_name,
                            position: cursor.get_data(8)?.unwrap_or(0),
                            data_type: build_full_type(
                                &cursor.get_data::<String>(9)?.unwrap_or_default(),
                                char_length,
                                precision,
                                scale
                            ),
                            is_nullable: is_nullable == "YES",
                            default_value: column_default,
                            comment: column_comment,
                        });
                    }
                }
            }
            
            // Don't forget the last object
            if let Some(obj) = current_object {
                schema_metadata.objects.push(obj);
            }
        }
        _ => {}
    }
    
    // Get procedures and functions for this schema
    let stmt = Statement::with_parent(conn)?;
    let proc_query = format!(
        "SELECT procedure_name, procedure_owner, comment, 
                argument_signature, data_type, procedure_language
         FROM {}.information_schema.procedures 
         WHERE procedure_schema = '{}'",
        database, schema
    );
    
    match stmt.exec_direct(&proc_query)? {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch()? {
                let proc_name: String = cursor.get_data(1)?.unwrap_or_default();
                let owner: Option<String> = cursor.get_data(2)?;
                let comment: Option<String> = cursor.get_data(3)?;
                let arguments: Option<String> = cursor.get_data(4)?;
                let return_type: Option<String> = cursor.get_data(5)?;
                let language: Option<String> = cursor.get_data(6)?;
                
                let object_type = if return_type.is_some() && return_type.as_ref().unwrap() != "NULL" {
                    ObjectType::Function
                } else {
                    ObjectType::Procedure
                };
                
                schema_metadata.objects.push(ObjectMetadata {
                    schema_name: schema.to_string(),
                    object_name: proc_name,
                    object_type,
                    owner,
                    comment,
                    row_count: None,
                    bytes: None,
                    columns: Vec::new(),
                    arguments,
                    return_type,
                    language,
                });
            }
        }
        _ => {}
    }
    
    // Process the collected metadata
    let schema_obj = db.schemas.entry(schema.to_string()).or_insert_with(|| {
        Schema::new(
            schema_metadata.name.clone(),
            database.to_string(),
            schema_metadata.owner,
            schema_metadata.comment,
        )
    });
    
    schema_obj.last_refreshed = current_timestamp();
    
    // Clear and repopulate objects
    schema_obj.objects.clear();
    
    for obj_meta in schema_metadata.objects {
        let obj = SchemaObject {
            name: obj_meta.object_name.clone(),
            object_type: obj_meta.object_type,
            comment: obj_meta.comment,
            owner: obj_meta.owner,
            last_refreshed: current_timestamp(),
            row_count: obj_meta.row_count,
            bytes: obj_meta.bytes,
            columns: obj_meta.columns.into_iter().map(|col| Column {
                name: col.name,
                position: col.position,
                data_type: col.data_type.clone(),
                type_details: parse_snowflake_type(&col.data_type, None, None, None),
                is_nullable: col.is_nullable,
                is_identity: false,
                default_value: col.default_value,
                comment: col.comment,
            }).collect(),
            arguments: obj_meta.arguments,
            return_type: obj_meta.return_type,
            language: obj_meta.language,
            schedule: None,
            state: None,
            accessible_by_roles: HashSet::new(),
        };
        
        // Mark as accessible by current role
        if let Ok(Some(role)) = get_current_role(conn) {
            let mut obj = obj;
            obj.add_role_access(&role);
            schema_obj.objects.insert(obj_meta.object_name, obj);
        } else {
            schema_obj.objects.insert(obj_meta.object_name, obj);
        }
    }
    
    // Mark schema as accessible by current role
    if let Ok(Some(role)) = get_current_role(conn) {
        schema_obj.add_role_access(&role);
    }
    
    let elapsed = start_time.elapsed();
    println!("Schema {}.{} refreshed in {:.2?}", database, schema, elapsed);
    Ok(())
}

fn get_all_accessible_databases<'env>(
    conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>
) -> Result<Vec<(String, Option<String>, Option<String>)>> {
    let stmt = Statement::with_parent(conn)?;
    let mut databases = Vec::new();
    
    // This query runs at the account level, no database context needed
    match stmt.exec_direct("SHOW DATABASES")? {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch()? {
                let db_name: String = cursor.get_data(2)?.unwrap_or_default();
                let owner: Option<String> = cursor.get_data(6)?;
                let comment: Option<String> = cursor.get_data(7)?;
                databases.push((db_name, owner, comment));
            }
        }
        _ => {}
    }
    
    Ok(databases)
}

fn get_database_metadata_batch<'env>(
    conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>,
    database: &str,
) -> Result<DatabaseMetadata> {
    let stmt = Statement::with_parent(conn)?;
    
    // First, use the database
    stmt.exec_direct(&format!("USE DATABASE \"{}\"", database))?;
    
    // Get all schemas
    let stmt = Statement::with_parent(conn)?;
    let mut schemas = HashMap::new();
    
    match stmt.exec_direct("SHOW SCHEMAS")? {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch()? {
                let schema_name: String = cursor.get_data(2)?.unwrap_or_default();
                let owner: Option<String> = cursor.get_data(6)?;
                let comment: Option<String> = cursor.get_data(7)?;
                
                schemas.insert(schema_name.clone(), SchemaMetadata {
                    name: schema_name,
                    owner,
                    comment,
                    objects: Vec::new(),
                });
            }
        }
        _ => {}
    }
    
    // Now get ALL objects and columns across ALL schemas in ONE query
    let stmt = Statement::with_parent(conn)?;
    let query = format!(r#"
        SELECT 
            t.table_schema,
            t.table_name,
            t.table_type,
            t.row_count,
            t.bytes,
            t.comment,
            t.table_owner,
            c.column_name,
            c.ordinal_position,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            c.comment as column_comment
        FROM {}.information_schema.tables t
        LEFT JOIN {}.information_schema.columns c 
            ON t.table_schema = c.table_schema 
            AND t.table_name = c.table_name
        WHERE t.table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
    "#, database, database);
    
    let mut current_object: Option<ObjectMetadata> = None;
    let mut current_schema = String::new();
    let mut current_table = String::new();
    
    match stmt.exec_direct(&query)? {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch()? {
                let schema_name: String = cursor.get_data(1)?.unwrap_or_default();
                let table_name: String = cursor.get_data(2)?.unwrap_or_default();
                
                // If we've moved to a new table, save the previous one
                if table_name != current_table && !current_table.is_empty() {
                    if let Some(obj) = current_object.take() {
                        if let Some(schema) = schemas.get_mut(&current_schema) {
                            schema.objects.push(obj);
                        }
                    }
                }
                
                // Start new object if needed
                if table_name != current_table {
                    current_table = table_name.clone();
                    current_schema = schema_name.clone();
                    
                    let table_type: String = cursor.get_data(3)?.unwrap_or_default();
                    let object_type = match table_type.as_str() {
                        "VIEW" => ObjectType::View,
                        _ => ObjectType::Table,
                    };
                    
                    current_object = Some(ObjectMetadata {
                        schema_name: schema_name.clone(),
                        object_name: table_name.clone(),
                        object_type,
                        owner: cursor.get_data(7)?,
                        comment: cursor.get_data(6)?,
                        row_count: cursor.get_data(4)?,
                        bytes: cursor.get_data(5)?,
                        columns: Vec::new(),
                        arguments: None,
                        return_type: None,
                        language: None,
                    });
                }
                
                // Add column if present
                if let Some(ref mut obj) = current_object {
                    if let Ok(Some(column_name)) = cursor.get_data::<String>(8) {
                        let char_length: Option<i32> = cursor.get_data(11)?;
                        let precision: Option<i32> = cursor.get_data(12)?;
                        let scale: Option<i32> = cursor.get_data(13)?;
                        let is_nullable: String = cursor.get_data(14)?.unwrap_or_default();
                        let column_default: Option<String> = cursor.get_data(15)?;
                        let column_comment: Option<String> = cursor.get_data(16)?;
                        
                        obj.columns.push(ColumnMetadata {
                            name: column_name,
                            position: cursor.get_data(9)?.unwrap_or(0),
                            data_type: build_full_type(
                                &cursor.get_data::<String>(10)?.unwrap_or_default(),
                                char_length,
                                precision,
                                scale
                            ),
                            is_nullable: is_nullable == "YES",
                            default_value: column_default,
                            comment: column_comment,
                        });
                    }
                }
            }
            
            // Don't forget the last object
            if let Some(obj) = current_object {
                if let Some(schema) = schemas.get_mut(&current_schema) {
                    schema.objects.push(obj);
                }
            }
        }
        _ => {}
    }
    
    // Get procedures and functions in batch
    let stmt = Statement::with_parent(conn)?;
    let proc_query = format!(
        "SELECT procedure_schema, procedure_name, procedure_owner, comment, 
                argument_signature, data_type, procedure_language
         FROM {}.information_schema.procedures 
         ORDER BY procedure_schema, procedure_name",
        database
    );
    
    match stmt.exec_direct(&proc_query)? {
        Data(mut stmt) => {
            while let Some(mut cursor) = stmt.fetch()? {
                let schema_name: String = cursor.get_data(1)?.unwrap_or_default();
                let proc_name: String = cursor.get_data(2)?.unwrap_or_default();
                let owner: Option<String> = cursor.get_data(3)?;
                let comment: Option<String> = cursor.get_data(4)?;
                let arguments: Option<String> = cursor.get_data(5)?;
                let return_type: Option<String> = cursor.get_data(6)?;
                let language: Option<String> = cursor.get_data(7)?;
                
                let object_type = if return_type.is_some() && return_type.as_ref().unwrap() != "NULL" {
                    ObjectType::Function
                } else {
                    ObjectType::Procedure
                };
                
                if let Some(schema) = schemas.get_mut(&schema_name) {
                    schema.objects.push(ObjectMetadata {
                        schema_name: schema_name.clone(),
                        object_name: proc_name,
                        object_type,
                        owner,
                        comment,
                        row_count: None,
                        bytes: None,
                        columns: Vec::new(),
                        arguments,
                        return_type,
                        language,
                    });
                }
            }
        }
        _ => {}
    }
    
    Ok(DatabaseMetadata {
        schemas: schemas.into_values().collect(),
    })
}

fn process_database_metadata(
    cache: &mut SchemaCache,
    database_name: &str,
    metadata: DatabaseMetadata,
) {
    let db = cache.databases.get_mut(database_name).unwrap();
    
    for schema_meta in metadata.schemas {
        let schema = db.schemas.entry(schema_meta.name.clone()).or_insert_with(|| {
            Schema::new(
                schema_meta.name.clone(),
                database_name.to_string(),
                schema_meta.owner,
                schema_meta.comment,
            )
        });
        
        schema.last_refreshed = current_timestamp();
        
        // Process objects
        for obj_meta in schema_meta.objects {
            let obj = schema.objects.entry(obj_meta.object_name.clone()).or_insert_with(|| {
                SchemaObject {
                    name: obj_meta.object_name.clone(),
                    object_type: obj_meta.object_type,
                    comment: obj_meta.comment,
                    owner: obj_meta.owner,
                    last_refreshed: current_timestamp(),
                    row_count: obj_meta.row_count,
                    bytes: obj_meta.bytes,
                    columns: Vec::new(),
                    arguments: obj_meta.arguments,
                    return_type: obj_meta.return_type,
                    language: obj_meta.language,
                    schedule: None,
                    state: None,
                    accessible_by_roles: HashSet::new(),
                }
            });
            
            // Update columns
            obj.columns = obj_meta.columns.into_iter().map(|col| Column {
                name: col.name,
                position: col.position,
                data_type: col.data_type.clone(),
                type_details: parse_snowflake_type(&col.data_type, None, None, None),
                is_nullable: col.is_nullable,
                is_identity: false,
                default_value: col.default_value,
                comment: col.comment,
            }).collect();
            
            obj.last_refreshed = current_timestamp();
        }
    }
}

fn update_role_access_efficient<'env>(
    conn: &odbc::Connection<'env, odbc_safe::AutocommitOn>,
    cache: &mut SchemaCache,
    available_roles: &[String],
) -> Result<()> {
    // For now, mark everything as accessible by current role
    // In a more sophisticated implementation, you could query grants more efficiently
    let current_role = get_current_role(conn)?;
    if let Some(role) = &current_role {
        for db in cache.databases.values_mut() {
            db.add_role_access(role);
            for schema in db.schemas.values_mut() {
                schema.add_role_access(role);
                for obj in schema.objects.values_mut() {
                    obj.add_role_access(role);
                }
            }
        }
    }
    
    // Mark all available roles as having access (temporary simplification)
    for role in available_roles {
        for db in cache.databases.values_mut() {
            db.add_role_access(role);
            for schema in db.schemas.values_mut() {
                schema.add_role_access(role);
                for obj in schema.objects.values_mut() {
                    obj.add_role_access(role);
                }
            }
        }
    }
    
    Ok(())
}


// Keep existing helper functions
fn parse_snowflake_type(base_type: &str, char_length: Option<i32>, 
                       precision: Option<i32>, scale: Option<i32>) -> DataType {
    use DataType::*;
    
    match base_type.to_uppercase().as_str() {
        "VARCHAR" | "CHARACTER VARYING" => Varchar { length: char_length.map(|l| l as u32) },
        "CHAR" | "CHARACTER" => Char { length: char_length.map(|l| l as u32) },
        "STRING" | "TEXT" => Text,
        
        "NUMBER" | "NUMERIC" => Number { 
            precision: precision.map(|p| p as u32), 
            scale: scale.map(|s| s as u32) 
        },
        "DECIMAL" => Decimal { 
            precision: precision.map(|p| p as u32), 
            scale: scale.map(|s| s as u32) 
        },
        "INT" | "INTEGER" => Integer,
        "BIGINT" => BigInt,
        "SMALLINT" => SmallInt,
        "TINYINT" => TinyInt,
        "FLOAT" | "FLOAT4" => Float4,
        "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "REAL" => Double,
        
        "DATE" => Date,
        "TIME" => Time { precision: precision.map(|p| p as u32) },
        "TIMESTAMP" | "DATETIME" => Timestamp { precision: precision.map(|p| p as u32) },
        "TIMESTAMP_LTZ" | "TIMESTAMPLTZ" => TimestampLtz { precision: precision.map(|p| p as u32) },
        "TIMESTAMP_NTZ" | "TIMESTAMPNTZ" => TimestampNtz { precision: precision.map(|p| p as u32) },
        "TIMESTAMP_TZ" | "TIMESTAMPTZ" => TimestampTz { precision: precision.map(|p| p as u32) },
        
        "BOOLEAN" => Boolean,
        "VARIANT" => Variant,
        "OBJECT" => Object,
        "ARRAY" => Array,
        
        "BINARY" => Binary { length: char_length.map(|l| l as u32) },
        "VARBINARY" => Varbinary { length: char_length.map(|l| l as u32) },
        
        "GEOGRAPHY" => Geography,
        "GEOMETRY" => Geometry,
        
        _ => Unknown { raw_type: base_type.to_string() },
    }
}

fn build_full_type(base_type: &str, char_length: Option<i32>, 
                   precision: Option<i32>, scale: Option<i32>) -> String {
    match base_type.to_uppercase().as_str() {
        "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" | "STRING" => {
            if let Some(len) = char_length {
                format!("{}({})", base_type, len)
            } else {
                base_type.to_string()
            }
        }
        "NUMBER" | "NUMERIC" | "DECIMAL" => {
            match (precision, scale) {
                (Some(p), Some(s)) if s > 0 => format!("{}({},{})", base_type, p, s),
                (Some(p), _) => format!("{}({})", base_type, p),
                _ => base_type.to_string(),
            }
        }
        "TIME" | "TIMESTAMP" | "TIMESTAMP_LTZ" | "TIMESTAMP_NTZ" | "TIMESTAMP_TZ" => {
            if let Some(p) = precision {
                format!("{}({})", base_type, p)
            } else {
                base_type.to_string()
            }
        }
        _ => base_type.to_string(),
    }
}