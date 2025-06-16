// src/autocomplete.rs
use std::collections::{HashSet, HashMap};
use regex::Regex;
use once_cell::sync::Lazy;
use crate::schema_cache::{SchemaCache, ObjectType};

// Compile regex once at startup
static TABLE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b([a-zA-Z_][a-zA-Z0-9_]*\.){2}[a-zA-Z_][a-zA-Z0-9_]*\b").unwrap()
});


#[derive(Debug, Clone, PartialEq)]
pub struct Suggestion {
    pub text: String,
    pub display_text: String,
    pub kind: SuggestionKind,
    pub detail: Option<String>, // Additional info like data type for columns
}

#[derive(Debug, Clone, PartialEq)]
pub enum SuggestionKind {
    Keyword,
    Database,
    Schema,
    Table,
    View,
    Column,
    Function,
    Procedure,
    Variable,
}

pub struct Autocomplete {
    pub active: bool,
    pub suggestions: Vec<Suggestion>,
    pub selected: usize,
    pub prefix: String,
    pub word_start: usize,
    pub word_end: usize,
    pub view_offset: usize,
    // Cache for table references found in buffer
    table_refs_cache: (String, HashSet<String>), // (buffer_hash, table_refs)
}

// Hardcoded SQL keywords and functions
static SQL_KEYWORDS: Lazy<Vec<&'static str>> = Lazy::new(|| {
    vec![
        // Keywords
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
        "CREATE", "TABLE", "VIEW", "DATABASE", "SCHEMA", "AS", "DROP", "ALTER", "ADD",
        "COLUMN", "CONSTRAINT", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE",
        "NOT", "NULL", "DEFAULT", "INDEX", "ON", "USING", "GRANT", "REVOKE", "TO",
        "WITH", "RECURSIVE", "CTE", "TEMP", "TEMPORARY", "IF", "EXISTS", "CASCADE",
        "RESTRICT", "TRUNCATE", "DISTINCT", "ORDER", "BY", "GROUP", "HAVING", "LIMIT",
        "OFFSET", "UNION", "ALL", "EXCEPT", "INTERSECT", "JOIN", "INNER", "LEFT",
        "RIGHT", "FULL", "OUTER", "CROSS", "NATURAL", "USING", "ON", "AND", "OR",
        "IN", "NOT", "EXISTS", "BETWEEN", "LIKE", "ILIKE", "IS", "NULL", "TRUE",
        "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "AS", "TRY_CAST",
        "COALESCE", "NULLIF", "DECODE", "NVL", "NVL2", "PARTITION", "OVER", "WINDOW",
        "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
        "QUALIFY", "FETCH", "FIRST", "NEXT", "ROWS", "ONLY", "PERCENT", "TIES",
        "LATERAL", "FLATTEN", "GENERATOR", "ROWCOUNT", "BERNOULLI", "SYSTEM",
        "WAREHOUSE", "USE", "ROLE", "SECONDARY", "ROLES", "SHOW", "DESCRIBE", "DESC",
        "EXPLAIN", "CALL", "EXECUTE", "IMMEDIATE", "BEGIN", "DECLARE", "EXCEPTION",
        "RETURN", "RETURNS", "LANGUAGE", "SQL", "JAVASCRIPT", "PYTHON", "JAVA", "SCALA",
        "HANDLER", "PROCEDURE", "FUNCTION", "TASK", "STREAM", "STAGE", "PIPE", "SEQUENCE",
        "FILE", "FORMAT", "COPY", "UNLOAD", "PUT", "GET", "REMOVE", "LIST",
        
        // Common functions
        "COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE", "MEDIAN", "MODE",
        "PERCENTILE_CONT", "PERCENTILE_DISC", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE",
        "LISTAGG", "ARRAY_AGG", "OBJECT_AGG", "STRING_AGG", "XMLAGG", "CORR", "COVAR_POP",
        "COVAR_SAMP", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT",
        "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY",
        
        // String functions
        "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "OCTET_LENGTH", "BIT_LENGTH",
        "CONCAT", "CONCAT_WS", "||", "SUBSTRING", "SUBSTR", "LEFT", "RIGHT", "REVERSE",
        "UPPER", "LOWER", "INITCAP", "TRIM", "LTRIM", "RTRIM", "LPAD", "RPAD",
        "REPEAT", "REPLACE", "TRANSLATE", "ASCII", "CHR", "CHARINDEX", "POSITION",
        "CONTAINS", "ENDSWITH", "STARTSWITH", "SPLIT", "SPLIT_PART", "SPLIT_TO_TABLE",
        "STRTOK", "STRTOK_TO_ARRAY", "STRTOK_SPLIT_TO_TABLE", "REGEXP", "REGEXP_COUNT",
        "REGEXP_INSTR", "REGEXP_LIKE", "REGEXP_REPLACE", "REGEXP_SUBSTR", "RLIKE",
        "PARSE_JSON", "PARSE_XML", "TRY_PARSE_JSON", "CHECK_JSON", "CHECK_XML",
        "JSON_EXTRACT_PATH_TEXT", "GET_PATH", "STRIP_NULL_VALUE", "OBJECT_CONSTRUCT",
        "OBJECT_INSERT", "OBJECT_DELETE", "OBJECT_KEYS", "ARRAY_CONSTRUCT", "ARRAY_APPEND",
        "ARRAY_CAT", "ARRAY_COMPACT", "ARRAY_CONTAINS", "ARRAY_INSERT", "ARRAY_POSITION",
        "ARRAY_PREPEND", "ARRAY_SIZE", "ARRAY_SLICE", "ARRAY_TO_STRING", "ARRAYS_OVERLAP",
        
        // Date/time functions
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "GETDATE", "SYSTIMESTAMP",
        "SYSDATE", "LOCALTIMESTAMP", "LOCALTIME", "DATE", "TIME", "TIMESTAMP",
        "TO_DATE", "TO_TIME", "TO_TIMESTAMP", "TO_TIMESTAMP_LTZ", "TO_TIMESTAMP_NTZ",
        "TO_TIMESTAMP_TZ", "TRY_TO_DATE", "TRY_TO_TIME", "TRY_TO_TIMESTAMP",
        "DATE_PART", "EXTRACT", "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "DAYOFWEEK",
        "DAYOFYEAR", "HOUR", "MINUTE", "SECOND", "NANOSECOND", "EPOCHSECOND",
        "EPOCHMILLISECONDS", "EPOCHMICROSECONDS", "EPOCHNANOSECONDS", "TIMEZONE",
        "DATE_TRUNC", "TRUNC", "ROUND", "DATEADD", "DATEDIFF", "TIMEDIFF", "TIMESTAMPADD",
        "TIMESTAMPDIFF", "ADD_MONTHS", "NEXT_DAY", "PREVIOUS_DAY", "LAST_DAY",
        "MONTHNAME", "DAYNAME", "WEEKOFYEAR", "YEAROFWEEK", "YEAROFWEEKISO",
        
        // Numeric functions
        "ABS", "SIGN", "MOD", "CEIL", "CEILING", "FLOOR", "ROUND", "TRUNCATE", "TRUNC",
        "EXP", "LN", "LOG", "LOG10", "POWER", "POW", "SQRT", "SQUARE", "CBRT",
        "FACTORIAL", "RANDOM", "UNIFORM", "NORMAL", "ZIPF", "SEQ1", "SEQ2", "SEQ4", "SEQ8",
        "UUID_STRING", "UUID_STRING", "HASH", "HASH_AGG", "MD5", "MD5_HEX", "SHA1",
        "SHA1_HEX", "SHA2", "SHA2_HEX", "BITAND", "BITOR", "BITXOR", "BITNOT",
        "BITSHIFTLEFT", "BITSHIFTRIGHT", "BOOLAND", "BOOLOR", "BOOLXOR", "BOOLNOT",
        
        // Conversion functions
        "CAST", "TRY_CAST", "TO_BOOLEAN", "TO_BINARY", "TO_CHAR", "TO_VARCHAR",
        "TO_NUMBER", "TO_NUMERIC", "TO_DECIMAL", "TO_DOUBLE", "TO_VARIANT", "TO_OBJECT",
        "TO_ARRAY", "TO_GEOGRAPHY", "TO_GEOMETRY", "TRY_TO_BOOLEAN", "TRY_TO_BINARY",
        "TRY_TO_NUMBER", "TRY_TO_NUMERIC", "TRY_TO_DECIMAL", "TRY_TO_DOUBLE",
        "TRY_TO_GEOGRAPHY", "TRY_TO_GEOMETRY", "AS_BINARY", "AS_CHAR", "AS_VARCHAR",
        "AS_NUMBER", "AS_DECIMAL", "AS_DOUBLE", "AS_DATE", "AS_TIME", "AS_TIMESTAMP_LTZ",
        "AS_TIMESTAMP_NTZ", "AS_TIMESTAMP_TZ", "AS_OBJECT", "AS_ARRAY",
        
        // System functions
        "SYSTEM$ABORT_SESSION", "SYSTEM$ABORT_TRANSACTION", "SYSTEM$CANCEL_ALL_QUERIES",
        "SYSTEM$CANCEL_QUERY", "SYSTEM$CLUSTERING_DEPTH", "SYSTEM$CLUSTERING_INFORMATION",
        "SYSTEM$CLUSTERING_RATIO", "SYSTEM$CURRENT_USER_TASK_NAME", "SYSTEM$DATABASE_REFRESH_PROGRESS",
        "SYSTEM$DATABASE_REFRESH_PROGRESS_BY_JOB", "SYSTEM$GET_AWS_SNS_IAM_POLICY",
        "SYSTEM$GET_PREDECESSOR_RETURN_VALUE", "SYSTEM$LAST_CHANGE_COMMIT_TIME",
        "SYSTEM$PIPE_FORCE_RESUME", "SYSTEM$PIPE_STATUS", "SYSTEM$STREAM_GET_TABLE_TIMESTAMP",
        "SYSTEM$STREAM_HAS_DATA", "SYSTEM$TASK_DEPENDENTS_ENABLE", "SYSTEM$TYPEOF",
        "SYSTEM$USER_TASK_CANCEL_ONGOING_EXECUTIONS", "SYSTEM$WAIT", "SYSTEM$WHITELIST",
        "SYSTEM$WHITELIST_PRIVATELINK",
        
        // Information schema
        "INFORMATION_SCHEMA", "ACCOUNT_USAGE", "ORGANIZATION_USAGE", "SNOWFLAKE",
        
        // Common Snowflake objects
        "ACCOUNTADMIN", "SYSADMIN", "SECURITYADMIN", "USERADMIN", "PUBLIC",
        "COMPUTE_WH", "LOAD_WH", "DEV_WH", "PROD_WH", "ANALYTICS_WH",
    ]
});

impl Autocomplete {
    pub fn new() -> Self {
        Self {
            active: false,
            suggestions: Vec::new(),
            selected: 0,
            prefix: String::new(),
            word_start: 0,
            word_end: 0,
            view_offset: 0,
            table_refs_cache: (String::new(), HashSet::new()),
        }
    }

    fn extract_table_references(buffer: &str) -> HashSet<String> {
        // Performance optimization: only scan recent part of buffer
        const MAX_SCAN_SIZE: usize = 10_000; // 10KB
        let scan_buffer = if buffer.len() > MAX_SCAN_SIZE {
            &buffer[buffer.len() - MAX_SCAN_SIZE..]
        } else {
            buffer
        };
        
        let mut referenced_tables = HashSet::new();
        let mut match_count = 0;
        const MAX_TABLES_TO_SCAN: usize = 20; // Limit table scanning

        for cap in TABLE_PATTERN.captures_iter(scan_buffer) {
            if match_count >= MAX_TABLES_TO_SCAN {
                break;
            }
            if let Some(m) = cap.get(0) {
                referenced_tables.insert(m.as_str().to_uppercase());
                match_count += 1;
            }
        }
        
        referenced_tables
    }
    
    pub fn update_suggestions(
        &mut self,
        buffer: &str,
        caret: usize,
        cache: Option<&SchemaCache>,
    ) {

        // Quick hash of buffer for cache invalidation (use length + first/last 100 chars)
        let buffer_hash = if buffer.len() < 200 {
            buffer.to_string()
        } else {
            format!("{}:{}:{}", 
                buffer.len(),
                &buffer[..100],
                &buffer[buffer.len()-100..]
            )
        };
        
        // Only scan buffer if it changed significantly
        let table_refs = if self.table_refs_cache.0 != buffer_hash {
            let refs = Self::extract_table_references(buffer);
            self.table_refs_cache = (buffer_hash, refs.clone());
            refs
        } else {
            self.table_refs_cache.1.clone()
        };

        // Find the word at caret position
        let (word, start, end) = get_word_at_position(buffer, caret);
        
        if word.is_empty() {
            self.suggestions.clear();
            self.active = false;
            return;
        }
        
        self.prefix = word.to_string();
        self.word_start = start;
        self.word_end = end;
        
        let mut suggestions = Vec::new();
        
        // Analyze the context - what comes before the current word?
        let prefix_parts: Vec<&str> = word.split('.').collect();
        let context_depth = prefix_parts.len();
        
        // Check if we have a dot-qualified name and if the qualifier is valid
        let treat_as_simple_word = if context_depth > 1 {
            // Check if the first part is a recognized database
            if let Some(cache) = cache {
                !cache.databases.contains_key(prefix_parts[0])
            } else {
                true
            }
        } else {
            false
        };
        
        // Determine what types of suggestions are appropriate based on context
        if context_depth == 1 || treat_as_simple_word {
            // No dots OR unrecognized qualifier - we're at the top level
            // Suggest: keywords, databases, and columns from referenced tables
            
            // Extract just the last part if we're treating a qualified name as simple
            let search_word = if treat_as_simple_word {
                prefix_parts.last().unwrap_or(&word)
            } else {
                &word
            };
            
            // 1. Add keyword suggestions
            suggestions.extend(get_keyword_suggestions(search_word));
            
            // 2. Add database suggestions
            if let Some(cache) = cache {
                suggestions.extend(get_database_suggestions(search_word, cache));
            }
            
            // 3. Add column suggestions from referenced tables
            if let Some(cache) = cache {
                suggestions.extend(get_column_suggestions(buffer, search_word, cache));
            }
            
        } else if context_depth == 2 {
            // One dot - format is "database.?"
            // Only suggest schemas for that database
            
            if let Some(cache) = cache {
                let db_name = prefix_parts[0];
                suggestions.extend(get_schema_suggestions_for_db(db_name, prefix_parts[1], cache));
            }
            
        } else if context_depth == 3 {
            // Two dots - format is "database.schema.?"
            // Only suggest tables/views/functions/procedures for that schema
            
            if let Some(cache) = cache {
                let db_name = prefix_parts[0];
                let schema_name = prefix_parts[1];
                suggestions.extend(get_object_suggestions_for_schema(
                    db_name, 
                    schema_name, 
                    prefix_parts[2], 
                    cache
                ));
            }
            
        } else if context_depth == 4 {
            // Three dots - format is "database.schema.table.?"
            // Only suggest columns for that specific table
            
            if let Some(cache) = cache {
                let db_name = prefix_parts[0];
                let schema_name = prefix_parts[1];
                let table_name = prefix_parts[2];
                suggestions.extend(get_columns_for_table(
                    db_name,
                    schema_name,
                    table_name,
                    prefix_parts[3],
                    cache
                ));
            }
        }
        
        // Remove duplicates and sort
        suggestions.sort_by(|a, b| {
            // Sort by kind priority first, then alphabetically within each kind
            let kind_priority = |k: &SuggestionKind| match k {
                SuggestionKind::Database => 0,
                SuggestionKind::Schema => 1,
                SuggestionKind::Table => 2,
                SuggestionKind::View => 3,
                SuggestionKind::Function => 4,
                SuggestionKind::Procedure => 5,
                SuggestionKind::Column => 6,
                SuggestionKind::Keyword => 7,
                SuggestionKind::Variable => 8,
            };
            
            kind_priority(&a.kind).cmp(&kind_priority(&b.kind))
                .then(a.text.to_lowercase().cmp(&b.text.to_lowercase()))
        });
        
        // For columns, keep duplicates if they're from different tables
        // For other types, remove exact duplicates
        suggestions.dedup_by(|a, b| {
            match (&a.kind, &b.kind) {
                (SuggestionKind::Column, SuggestionKind::Column) => {
                    // Keep columns with same name but different details (different tables)
                    a.text == b.text && a.detail == b.detail
                }
                _ => a.text == b.text
            }
        });
        
        // Limit suggestions to 20
        suggestions.truncate(80);
        
        self.suggestions = suggestions;
        self.selected = 0;
        self.view_offset = 0;
        self.active = !self.suggestions.is_empty();
    }
    
    pub fn accept_suggestion(&self) -> Option<(usize, usize, String)> {
        if !self.active || self.suggestions.is_empty() {
            return None;
        }
        
        let suggestion = &self.suggestions[self.selected];
        
        // For qualified names, only complete the current part
        let completion = if self.prefix.contains('.') {
            let prefix_parts: Vec<&str> = self.prefix.split('.').collect();
            let suggestion_parts: Vec<&str> = suggestion.text.split('.').collect();
            
            // If suggestion has more parts than prefix, we're completing a partial qualified name
            if suggestion_parts.len() > prefix_parts.len() {
                // Return only the parts after what's already typed
                suggestion_parts[prefix_parts.len() - 1..].join(".")
            } else if suggestion_parts.len() == prefix_parts.len() {
                // Same level - replace just the last part
                suggestion_parts.last().unwrap_or(&suggestion.text.as_str()).to_string()
            } else {
                // Suggestion has fewer parts - just use the last part
                suggestion_parts.last().unwrap_or(&suggestion.text.as_str()).to_string()
            }
        } else {
            suggestion.text.clone()
        };
        
        // Calculate the replacement range
        let (start, end) = if self.prefix.contains('.') {
            // For qualified names, only replace from the last dot
            if let Some(last_dot_pos) = self.prefix.rfind('.') {
                (self.word_start + last_dot_pos + 1, self.word_end)
            } else {
                (self.word_start, self.word_end)
            }
        } else {
            (self.word_start, self.word_end)
        };
        
        Some((start, end, completion))
    }
    
    pub fn move_up(&mut self) {
        if !self.suggestions.is_empty() && self.selected > 0 {
            self.selected -= 1;
            // Scroll up if needed
            if self.selected < self.view_offset {
                self.view_offset = self.selected;
            }
        }
    }

    pub fn move_down(&mut self) {
        if !self.suggestions.is_empty() && self.selected + 1 < self.suggestions.len() {
            self.selected += 1;
            // Scroll down if needed (assuming max 8 visible items)
            let max_visible = 8;
            if self.selected >= self.view_offset + max_visible {
                self.view_offset = self.selected - max_visible + 1;
            }
        }
    }
}

// Get only database suggestions
fn get_database_suggestions(prefix: &str, cache: &SchemaCache) -> Vec<Suggestion> {
    let prefix_upper = prefix.to_uppercase();
    let current_role = cache.current_role.as_deref();
    
    // Pre-sort database names for binary search if there are many
    let mut db_names: Vec<_> = cache.databases.keys().collect();
    if db_names.len() > 100 {
        db_names.sort_by(|a, b| a.to_uppercase().cmp(&b.to_uppercase()));
    }

    cache.databases
        .iter()
        .filter(|(db_name, db)| {
            // Check if accessible by current role
            let accessible = if let Some(role) = current_role {
                db.accessible_by_roles.contains(role)
            } else {
                true
            };
            accessible && db_name.to_uppercase().starts_with(&prefix_upper)
        })
        .map(|(db_name, db)| Suggestion {
            text: db_name.clone(),
            display_text: db_name.clone(),
            kind: SuggestionKind::Database,
            detail: db.comment.clone(),
        })
        .collect()
}

// Get schemas for a specific database
fn get_schema_suggestions_for_db(db_name: &str, prefix: &str, cache: &SchemaCache) -> Vec<Suggestion> {
    let prefix_upper = prefix.to_uppercase();
    let current_role = cache.current_role.as_deref();
    
    if let Some(db) = cache.databases.get(db_name) {
        db.schemas
            .iter()
            .filter(|(schema_name, schema)| {
                let accessible = if let Some(role) = current_role {
                    schema.accessible_by_roles.contains(role)
                } else {
                    true
                };
                accessible && schema_name.to_uppercase().starts_with(&prefix_upper)
            })
            .map(|(schema_name, schema)| Suggestion {
                text: format!("{}.{}", db_name, schema_name),  // Full name for completion
                display_text: schema_name.clone(),  // Just schema name for display
                kind: SuggestionKind::Schema,
                detail: schema.comment.clone(),
            })
            .collect()
    } else {
        Vec::new()
    }
}

// Get objects for a specific schema
fn get_object_suggestions_for_schema(
    db_name: &str,
    schema_name: &str,
    prefix: &str,
    cache: &SchemaCache
) -> Vec<Suggestion> {
    let prefix_upper = prefix.to_uppercase();
    let current_role = cache.current_role.as_deref();
    
    if let Some(db) = cache.databases.get(db_name) {
        if let Some(schema) = db.schemas.get(schema_name) {
            return schema.objects
                .iter()
                .filter(|(obj_name, obj)| {
                    let accessible = if let Some(role) = current_role {
                        obj.accessible_by_roles.contains(role)
                    } else {
                        true
                    };
                    accessible && obj_name.to_uppercase().starts_with(&prefix_upper)
                })
                .map(|(obj_name, obj)| {
                    let kind = match obj.object_type {
                        ObjectType::Table => SuggestionKind::Table,
                        ObjectType::View => SuggestionKind::View,
                        ObjectType::Function => SuggestionKind::Function,
                        ObjectType::Procedure => SuggestionKind::Procedure,
                        _ => return None,
                    };
                    
                    Some(Suggestion {
                        text: format!("{}.{}.{}", db_name, schema_name, obj_name),  // Full name for completion
                        display_text: obj_name.clone(),  // Just object name for display
                        kind,
                        detail: obj.comment.clone(),
                    })
                })
                .filter_map(|x| x)
                .collect();
        }
    }
    Vec::new()
}

// Get columns for a specific table
fn get_columns_for_table(
    db_name: &str,
    schema_name: &str,
    table_name: &str,
    prefix: &str,
    cache: &SchemaCache
) -> Vec<Suggestion> {
    let prefix_upper = prefix.to_uppercase();
    
    if let Some(db) = cache.databases.get(db_name) {
        if let Some(schema) = db.schemas.get(schema_name) {
            if let Some(obj) = schema.objects.get(table_name) {
                return obj.columns
                    .iter()
                    .filter(|col| col.name.to_uppercase().starts_with(&prefix_upper))
                    .map(|col| Suggestion {
                        text: format!("{}.{}.{}.{}", db_name, schema_name, table_name, col.name),  // Full for completion
                        display_text: col.name.clone(),  // Just column name for display
                        kind: SuggestionKind::Column,
                        detail: Some(format!("{} - {}", table_name, col.data_type)),
                    })
                    .collect();
            }
        }
    }
    Vec::new()
}

// Helper function to extract word at position (including dots for qualified names)
fn get_word_at_position(buffer: &str, pos: usize) -> (&str, usize, usize) {
    if pos > buffer.len() {
        return ("", pos, pos);
    }
    
    let bytes = buffer.as_bytes();
    
    // Find start of word (including dots and underscores)
    let mut start = pos;
    while start > 0 {
        let prev = start - 1;
        if bytes[prev].is_ascii_alphanumeric() || bytes[prev] == b'_' || bytes[prev] == b'.' {
            start = prev;
        } else {
            break;
        }
    }
    
    // Find end of word (including dots and underscores)
    let mut end = pos;
    while end < bytes.len() {
        if bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_' || bytes[end] == b'.' {
            end += 1;
        } else {
            break;
        }
    }
    
    (&buffer[start..end], start, end)
}

// Get keyword suggestions
fn get_keyword_suggestions(prefix: &str) -> Vec<Suggestion> {
    let prefix_upper = prefix.to_uppercase();
    
    SQL_KEYWORDS.iter()
        .filter(|&&kw| kw.starts_with(&prefix_upper))
        .map(|&kw| Suggestion {
            text: kw.to_string(),
            display_text: kw.to_string(),
            kind: if kw.contains('$') || kw.ends_with("(") {
                SuggestionKind::Function
            } else {
                SuggestionKind::Keyword
            },
            detail: None,
        })
        .collect()
}

// Get schema object suggestions
fn get_schema_suggestions(prefix: &str, cache: &SchemaCache) -> Vec<Suggestion> {
    let mut suggestions = Vec::new();
    let prefix_upper = prefix.to_uppercase();
    let prefix_parts: Vec<&str> = prefix.split('.').collect();
    
    // If we're filtering by role, only show accessible objects
    let current_role = cache.current_role.as_deref();

    // Add databases
    for db_name in cache.databases.keys() {
        let db = &cache.databases[db_name];
        // Check database accessibility if role is set
        let db_accessible = if let Some(role) = current_role {
            db.accessible_by_roles.contains(role)
        } else {
            true
        };
        
        if !db_accessible {
            continue;
        }
        
        // Always include database names if they match
        if db_name.to_uppercase().starts_with(&prefix_upper) {
            suggestions.push(Suggestion {
                text: db_name.clone(),
                display_text: db_name.clone(),
                kind: SuggestionKind::Database,
                detail: db.comment.clone(),
            });
        }
        
        // Add schemas
        for schema_name in db.schemas.keys() {
            let schema = &db.schemas[schema_name];
            
            // Check schema accessibility
            let schema_accessible = if let Some(role) = current_role {
                schema.accessible_by_roles.contains(role)
            } else {
                true
            };
            
            if !schema_accessible {
                continue;
            }
            // Add database.schema combinations
            let qualified_schema = format!("{}.{}", db_name, schema_name);
            
            if qualified_schema.to_uppercase().starts_with(&prefix_upper) {
                suggestions.push(Suggestion {
                    text: qualified_schema,
                    display_text: schema_name.clone(),
                    kind: SuggestionKind::Schema,
                    detail: schema.comment.clone(),
                });
            }
            
            // Add objects (tables, views, functions, etc.)
            for (obj_name, obj) in &schema.objects {
                // Check object accessibility
                if let Some(role) = current_role {
                    if !obj.accessible_by_roles.contains(role) {
                        continue;
                    }
                }
                let qualified_name = format!("{}.{}.{}", db_name, schema_name, obj_name);
                
                if qualified_name.to_uppercase().starts_with(&prefix_upper) {
                    let kind = match obj.object_type {
                        ObjectType::Table => SuggestionKind::Table,
                        ObjectType::View => SuggestionKind::View,
                        ObjectType::Function => SuggestionKind::Function,
                        ObjectType::Procedure => SuggestionKind::Procedure,
                        _ => continue,
                    };
                    
                    suggestions.push(Suggestion {
                        text: qualified_name,  // Keep full name for completion
                        display_text: obj_name.clone(),  // Just object name for display
                        kind,
                        detail: obj.comment.clone(),
                    });
                }
            }
        }
    }
    
    suggestions
}

// Get column suggestions from referenced tables in the buffer
fn get_column_suggestions(buffer: &str, prefix: &str, cache: &SchemaCache) -> Vec<Suggestion> {
    let mut suggestions = Vec::new();
    let prefix_upper = prefix.to_uppercase();
    
    // Use the extract method
    let referenced_tables = Autocomplete::extract_table_references(buffer);
    
    if referenced_tables.is_empty() {
        return suggestions;
    }
    
    // For each referenced table, add its columns
    for table_ref in referenced_tables {
        let parts: Vec<&str> = table_ref.split('.').collect();
        if parts.len() != 3 {
            continue;
        }
        
        let (db_name, schema_name, table_name) = (parts[0], parts[1], parts[2]);
        
        // Find the table in cache
        if let Some(db) = cache.databases.get(db_name) {
            if let Some(schema) = db.schemas.get(schema_name) {
                if let Some(obj) = schema.objects.get(table_name) {
                    // Add columns
                    for column in &obj.columns {
                        if column.name.to_uppercase().starts_with(&prefix_upper) {
                            suggestions.push(Suggestion {
                                text: column.name.clone(),
                                display_text: column.name.clone(),
                                kind: SuggestionKind::Column,
                                detail: Some(format!("{} - {}", table_name, column.data_type)),
                            });
                        }
                    }
                }
            }
        }
    }
    
    suggestions
}