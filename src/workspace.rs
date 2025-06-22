use crate::tile_rowstore::TileRowStore;
use crate::{editor::Editor, results::{Results, ResultsContent}};
use crate::syntax::{ParseState, step, Step};
use crate::tile_rowstore::NULL_SENTINEL;
use crate::batch_generator::BatchGeneratorDialog;
use crate::palette::STYLE;
use crate::autosave::Autosave;
use crate::locked_file::LockedFile;
use crate::editor::GUTTER_WIDTH;
use crate::db_tree::{DbTree, TreeAction};

use odbc::{create_environment_v3, Data, ResultSetState, Statement, Handle};
use odbc::ffi::{SQLCancel, SQLHSTMT};   // raw FFI symbols live in `odbc::ffi`
use std::{
    sync::{Arc, Mutex},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};
use tui::{Terminal, backend::Backend, layout::Rect, Frame};
use tui::layout::{Layout, Direction, Constraint};
use crossterm::event::{KeyEvent, MouseEvent, KeyModifiers, KeyCode, KeyEventKind};
use anyhow::Result;
use directories::UserDirs;
use std::path::PathBuf;


const MIN_ROWS: i16 = 3;

#[derive(Debug)]
pub enum DbWorkerRequest {
    RunQueries(Vec<(String, String)>), // (query, context)
    Cancel,
    Quit,
}

#[derive(Debug)]
pub enum DbWorkerResponse {
    Connected,
    QueryStarted { query_idx: usize, started: Instant, query_context: String },
    QueryFinished { query_idx: usize, elapsed: Duration, result: ResultsContent },
    QueryError { query_idx: usize, elapsed: Duration, message: String },
}

/// Thin wrapper around the raw `SQLHSTMT` pointer that marks it as
/// safe to move/share between threads.  The safety promise we make:
/// * We never dereference the pointer in Rust – it’s only passed back
///   to the driver ( `SQLCancel` ).
/// * The lifetime of the handle is controlled by the ODBC driver; we
///   use it strictly for cancellation while the statement is running.
#[derive(Clone, Copy)]
pub struct SafeStmt(SQLHSTMT);
unsafe impl Send for SafeStmt {}
unsafe impl Sync for SafeStmt {}


pub struct Workspace {
    pub db_tree: DbTree,
    pub editor: Editor,
    pub results: Results,
    pub error: Option<String>,
    pub running: bool,
    pub run_started: Option<Instant>,
    pub run_duration: Option<Duration>,
    pub running_query_idx: Option<usize>,
    pub batch_generator: Option<BatchGeneratorDialog>,

    pub last_editor_area: Option<Rect>,
    pub last_results_area: Option<Rect>,
    pub focus: Focus,
    pub last_esc_down: bool,

    pub status_message: Option<String>,
    pub status_message_time: Option<Instant>,
    
    pub autosave: Autosave,
    pub file_path: Option<std::path::PathBuf>,
    pub file_lock: Option<LockedFile>,

    pub show_help: bool,

    pub db_req_tx: Sender<DbWorkerRequest>,
    pub db_resp_rx: Receiver<DbWorkerResponse>,
    current_stmt: Arc<Mutex<Option<SafeStmt>>>,

    pub total_queries: usize,

    pub split_offset: i16, 
    min_split_offset: i16,
    max_split_offset: i16,
    results_hidden: bool,
    editor_hidden: bool,
    drag_source: Option<Focus>,  // Track which pane started a drag
    pub connected: bool,  // Track Snowflake connection status
    pub frame_counter: u32,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Focus {
    Editor,
    Results,
    DbTree,
}

pub fn start_db_worker(
    conn_str: String,
) -> (
    Sender<DbWorkerRequest>,
    Receiver<DbWorkerResponse>,
    Arc<Mutex<Option<SafeStmt>>>
) {
    let (req_tx,  req_rx)  = mpsc::channel();
    let (resp_tx, resp_rx) = mpsc::channel();

    // shared handle of the statement that is *currently executing*
    let current_stmt: Arc<Mutex<Option<SafeStmt>>> =
        Arc::new(Mutex::new(None));

    // clone into the worker-thread
    let thread_stmt = Arc::clone(&current_stmt);
    thread::spawn(move || {
        // Try to create environment
        let env = match create_environment_v3() {
            Ok(env) => env,
            Err(_) => {
                // Don't exit - keep thread alive but not connected
                loop {
                    match req_rx.recv() {
                        Ok(DbWorkerRequest::Quit) | Err(_) => break,
                        _ => {
                            // Ignore other requests when not connected
                            continue;
                        }
                    }
                }
                return;
            }
        };
        
        // Try to connect
        let conn = match env.connect_with_connection_string(&conn_str) {
            Ok(conn) => {
                // Signal successful connection
                let _ = resp_tx.send(DbWorkerResponse::Connected);
                
                // Enable all secondary roles by default
                if let Ok(stmt) = Statement::with_parent(&conn) {
                    let _ = stmt.exec_direct("USE SECONDARY ROLES ALL");
                }
                
                conn
            }
            Err(_) => {
                // Connection failed - keep thread alive but not connected
                // Don't send an error response here - let the UI handle the "Not Connected" state
                loop {
                    match req_rx.recv() {
                        Ok(DbWorkerRequest::Quit) | Err(_) => break,
                        _ => {
                            // Ignore other requests when not connected
                            continue;
                        }
                    }
                }
                return;
            }
        };

        loop {
            match req_rx.recv() {
                Ok(DbWorkerRequest::RunQueries(queries_with_context)) => {
                    for (i, (query, context)) in queries_with_context.iter().enumerate() {
                        let started = Instant::now();
                        let _ = resp_tx.send(DbWorkerResponse::QueryStarted { query_idx: i, started, query_context: context.clone() });
                        // allocate a fresh statement
                        let stmt = match Statement::with_parent(&conn) {
                            Ok(s) => s,
                            Err(e) => {
                            let msg = format!("Statement Allocation Error: {:?}", e);
                            let _ = resp_tx.send(DbWorkerResponse::QueryError {
                                query_idx: i,
                                elapsed: started.elapsed(),
                                message: msg,
                            });
                            break;
                            }
                        };

                        // 🔗 expose raw HSTMT so Ctrl-Backspace can call SQLCancel
                        unsafe {
                            *thread_stmt.lock().unwrap() =
                                Some(SafeStmt(stmt.handle()));
                        }

                        // Execute the SQL text.
                        let exec_result = stmt.exec_direct(query);

                        match exec_result {
                            Ok(Data(mut stmt)) => {
                                let cols = stmt.num_result_cols().unwrap();
                                let col_names: Vec<String> = (1..=cols)
                                    .map(|i| stmt.describe_col(i as u16).unwrap().name)
                                    .collect();

                                // Stream rows from ODBC using an iterator instead of RowIter struct!
                                let tile_store = match TileRowStore::from_rows(
                                    &col_names,
                                    std::iter::from_fn(|| {
                                        match stmt.fetch().unwrap() {
                                            Some(mut cursor) => {
                                                let mut row = Vec::with_capacity(col_names.len());
                                                for idx in 0..col_names.len() {
                                                let val: Option<String> = cursor.get_data(idx as u16 + 1).unwrap_or(None);
                                                row.push(val.unwrap_or_else(|| NULL_SENTINEL.to_string()));
                                                }
                                                Some(row)
                                            }
                                            None => None,
                                        }
                                    })
                                ) {
                                    Ok(ts) => ts,
                                    Err(e) => {
                                        let msg = format!("TileRowStore error: {e:?}");
                                        let _ = resp_tx.send(DbWorkerResponse::QueryError {
                                            query_idx: i,
                                            elapsed: started.elapsed(),
                                            message: msg,
                                        });
                                        continue;
                                    }
                                };

                                let _ = resp_tx.send(DbWorkerResponse::QueryFinished {
                                    query_idx: i,
                                    elapsed: started.elapsed(),
                                    result: ResultsContent::Table {
                                        headers: col_names,
                                        tile_store,
                                    },
                                });
                            }
                            Ok(ResultSetState::NoData(statement)) => {
                                let msg = {
                                    if let Ok(cnt) = statement.affected_row_count() {
                                        if cnt > 0 {
                                            format!("Statement affected {} row{}", cnt, if cnt == 1 { "" } else { "s" })
                                        } else if cnt == 0 {
                                            "Statement executed successfully (no rows affected).".to_string()
                                        } else {
                                            "Statement executed successfully.".to_string()
                                        }
                                    } else {
                                        "Statement executed successfully.".to_string()
                                    }
                                };
                                let _ = resp_tx.send(DbWorkerResponse::QueryFinished {
                                    query_idx: i,
                                    elapsed: started.elapsed(),
                                    result: ResultsContent::Info { message: msg },
                                });
                            }
                            Err(e) => {
                                // Could be user-cancelled (HY008) or some other error
                                let msg = format!("Execution Error: {:?}", e);
                                let _ = resp_tx.send(DbWorkerResponse::QueryError {
                                    query_idx: i,
                                    elapsed: started.elapsed(),
                                    message: msg,
                                });
                                break;
                            }
                        }
                        // clear handle – we're done with it (success or error)
                        *thread_stmt.lock().unwrap() = None;
                    }
                }
                Ok(DbWorkerRequest::Cancel) => {
                    // user hit Ctrl + Backspace
                    if let Some(h) = *thread_stmt.lock().unwrap() {
                        unsafe { let _ = SQLCancel(h.0); };
                    }
                }
                Ok(DbWorkerRequest::Quit) | Err(_) => {
                    break;
                }
            }
        }
    });

    (req_tx, resp_rx, current_stmt)
}

fn hit(r: Rect, x: u16, y: u16) -> bool {
        x >= r.x && x < r.x + r.width && y >= r.y && y < r.y + r.height
    }

impl Workspace {
    pub fn new(conn_str: String) -> Result<Self> {
        let (db_req_tx, db_resp_rx, current_stmt) = start_db_worker(conn_str.clone());
        let mut editor = Editor::new();
        let db_tree = DbTree::new();
        
        // Initialize schema cache for autocomplete
        if let Some(cache) = db_tree.cache.clone() {
            editor.schema_cache = Some(cache);
        }
        
        Ok(Self {
            editor,
            results: Results::new(),
            error: None,
            running: false,
            batch_generator: None,
            run_started: None,
            run_duration: None,
            running_query_idx: None,
            last_editor_area: None,
            last_results_area: None,
            focus: Focus::Editor,
            last_esc_down: false,
            status_message: None,
            status_message_time: None,
            db_req_tx,
            db_resp_rx,
            current_stmt,
            total_queries: 0,
            split_offset: 0,
            min_split_offset:  0,
            max_split_offset:  0,
            results_hidden: false,
            editor_hidden: false,
            show_help: false,
            drag_source: None,
            autosave: Autosave::new(None),
            file_path: None,
            file_lock: None,
            connected: false,
            db_tree, 
            frame_counter: 0,
        })
    }

    /// Extract a meaningful context/name from a SQL query
    fn extract_query_context(query: &str) -> String {
        let trimmed = query.trim();
        let upper = trimmed.to_uppercase();
        
        // Handle CTEs (WITH clause)
        if upper.starts_with("WITH") {
            return "CTE Query".to_string();
        }
        
        // Handle anonymous blocks
        if upper.starts_with("DECLARE") || upper.starts_with("BEGIN") {
            return "Anonymous Block".to_string();
        }
        
        // Handle CALL statements
        if upper.starts_with("CALL") {
            if let Some(name) = Self::extract_identifier_after(&trimmed, "CALL") {
                return format!("Call {}", Self::clean_identifier(&name));
            }
        }
        
        // Handle CREATE statements
        if upper.starts_with("CREATE") {
            if upper.contains("PROCEDURE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "PROCEDURE") {
                    return format!("Create Proc: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("FUNCTION") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "FUNCTION") {
                    return format!("Create Func: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("TABLE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "TABLE") {
                    if upper.contains("TEMPORARY") || upper.contains("TEMP") {
                        return format!("Create Temp: {}", Self::clean_identifier(&name));
                    } else {
                        return format!("Create Table: {}", Self::clean_identifier(&name));
                    }
                }
            } else if upper.contains("VIEW") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "VIEW") {
                    return format!("Create View: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("STAGE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "STAGE") {
                    return format!("Create Stage: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("TASK") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "TASK") {
                    return format!("Create Task: {}", Self::clean_identifier(&name));
                }
            }
        }
        
        // Handle DROP statements
        if upper.starts_with("DROP") {
            if upper.contains("TABLE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "TABLE") {
                    return format!("Drop Table: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("VIEW") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "VIEW") {
                    return format!("Drop View: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("PROCEDURE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "PROCEDURE") {
                    return format!("Drop Proc: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("FUNCTION") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "FUNCTION") {
                    return format!("Drop Func: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("STAGE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "STAGE") {
                    return format!("Drop Stage: {}", Self::clean_identifier(&name));
                }
            }
        }
        
        // Handle ALTER statements
        if upper.starts_with("ALTER") {
            if upper.contains("TABLE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "TABLE") {
                    return format!("Alter Table: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("VIEW") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "VIEW") {
                    return format!("Alter View: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("WAREHOUSE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "WAREHOUSE") {
                    return format!("Alter WH: {}", Self::clean_identifier(&name));
                }
            }
        }
        
        // Handle SELECT statements
        if upper.starts_with("SELECT") {
            if let Some(table_name) = Self::extract_main_table_from_select(&trimmed) {
                // Check if it's selecting from a temp table (common pattern)
                let clean_name = Self::clean_identifier(&table_name);
                if table_name.to_uppercase().contains("TEMP") || clean_name.to_uppercase().contains("TEMP") {
                    return format!("Select Temp: {}", clean_name);
                } else {
                    return format!("Select: {}", clean_name);
                }
            }
            return "Query".to_string();
        }

        // Handle SHOW statements
        if upper.starts_with("SHOW") {
            if upper.contains("TABLES") {
                return "Show Tables".to_string();
            } else if upper.contains("SCHEMAS") {
                return "Show Schemas".to_string();
            } else if upper.contains("WAREHOUSES") {
                return "Show Warehouses".to_string();
            } else if upper.contains("COLUMNS") {
                return "Show Columns".to_string();
            }
            return "Show".to_string();
        }
        
        // Handle DESCRIBE statements
        if upper.starts_with("DESCRIBE") || upper.starts_with("DESC") {
            if let Some(name) = Self::extract_identifier_after(&trimmed, if upper.starts_with("DESCRIBE") { "DESCRIBE" } else { "DESC" }) {
                return format!("Describe: {}", Self::clean_identifier(&name));
            }
        }
        
        // Handle USE statements
        if upper.starts_with("USE") {
            if upper.contains("DATABASE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "DATABASE") {
                    return format!("Use DB: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("SCHEMA") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "SCHEMA") {
                    return format!("Use Schema: {}", Self::clean_identifier(&name));
                }
            } else if upper.contains("WAREHOUSE") {
                if let Some(name) = Self::extract_identifier_after(&trimmed, "WAREHOUSE") {
                    return format!("Use WH: {}", Self::clean_identifier(&name));
                }
            }
        }
        
        // Handle INSERT statements
        if upper.starts_with("INSERT") {
            if let Some(table_name) = Self::extract_identifier_after(&trimmed, "INTO") {
                return format!("Insert: {}", Self::clean_identifier(&table_name));
            }
        }
        
        // Handle UPDATE statements
        if upper.starts_with("UPDATE") {
            if let Some(table_name) = Self::extract_identifier_after(&trimmed, "UPDATE") {
                return format!("Update: {}", Self::clean_identifier(&table_name));
            }
        }
        
        // Handle DELETE statements
        if upper.starts_with("DELETE") {
            if let Some(table_name) = Self::extract_identifier_after(&trimmed, "FROM") {
                return format!("Delete: {}", Self::clean_identifier(&table_name));
            }
        }
        
        // Handle MERGE statements
        if upper.starts_with("MERGE") {
            if let Some(table_name) = Self::extract_identifier_after(&trimmed, "INTO") {
                return format!("Merge: {}", Self::clean_identifier(&table_name));
            }
        }
        
        // Handle COPY INTO statements
        if upper.starts_with("COPY") {
            if let Some(table_name) = Self::extract_identifier_after(&trimmed, "INTO") {
                return format!("Copy Into: {}", Self::clean_identifier(&table_name));
            }
        }
        
        // Handle PUT/GET statements
        if upper.starts_with("PUT") {
            return "Put File".to_string();
        } else if upper.starts_with("GET") {
            return "Get File".to_string();
        }

        // Handle GRANT statements
        if upper.starts_with("GRANT") {
            // Check if it's granting a role (GRANT role TO user)
            if let Some(to_pos) = upper.find(" TO ") {
                let before_to = &trimmed[5..to_pos]; // Skip "GRANT "
                let role_part = before_to.trim();
                // If there's no "ON" keyword, it's likely a role grant
                if !role_part.to_uppercase().contains(" ON ") {
                    return format!("Grant Role: {}", Self::clean_identifier(role_part));
                }
            }
            // Otherwise it's granting privileges on an object
            if let Some(on_pos) = upper.find(" ON ") {
                let after_on = &trimmed[on_pos + 4..];
                if let Some(obj_name) = Self::extract_object_from_grant(&after_on) {
                    return format!("Grant on: {}", Self::clean_identifier(&obj_name));
                }
            }
            return "Grant".to_string();
        }
        
        // Handle REVOKE statements
        if upper.starts_with("REVOKE") {
            // Check if it's revoking a role (REVOKE role FROM user)
            if let Some(from_pos) = upper.find(" FROM ") {
                let before_from = &trimmed[6..from_pos]; // Skip "REVOKE "
                let role_part = before_from.trim();
                // If there's no "ON" keyword, it's likely a role revoke
                if !role_part.to_uppercase().contains(" ON ") {
                    return format!("Revoke Role: {}", Self::clean_identifier(role_part));
                }
            }
            // Otherwise it's revoking privileges on an object
            if let Some(on_pos) = upper.find(" ON ") {
                let after_on = &trimmed[on_pos + 4..];
                if let Some(obj_name) = Self::extract_object_from_grant(&after_on) {
                    return format!("Revoke on: {}", Self::clean_identifier(&obj_name));
                }
            }
            return "Revoke".to_string();
        }
        
        // Default: use first few words
        let words: Vec<&str> = trimmed.split_whitespace().take(3).collect();
        if words.is_empty() {
            "Query".to_string()
        } else {
            words.join(" ")
        }
    }

    /// Extract identifier after a specific keyword
    fn extract_identifier_after(query: &str, keyword: &str) -> Option<String> {
        let upper = query.to_uppercase();
        let keyword_upper = keyword.to_uppercase();
        
        if let Some(pos) = upper.find(&keyword_upper) {
            let after_keyword = &query[pos + keyword.len()..];
            let tokens: Vec<&str> = after_keyword.split_whitespace().collect();
            
            for token in tokens {
                // Skip SQL keywords
                let token_upper = token.to_uppercase();
                if token_upper == "OR" || token_upper == "IF" || token_upper == "NOT" || 
                   token_upper == "EXISTS" || token_upper == "REPLACE" || token_upper == "TEMPORARY" ||
                   token_upper == "TEMP" {
                    continue;
                }
                
                // Found our identifier
                return Some(token.to_string());
            }
        }
        None
    }

    /// Extract object name from GRANT/REVOKE ... ON object_type object_name
    fn extract_object_from_grant(after_on: &str) -> Option<String> {
        let tokens: Vec<&str> = after_on.split_whitespace().collect();
        
        if tokens.is_empty() {
            return None;
        }
        
        // Common patterns:
        // ON TABLE schema.table TO ...
        // ON SCHEMA schema_name TO ...
        // ON DATABASE db_name TO ...
        // ON WAREHOUSE wh_name TO ...
        // ON schema.table TO ... (without object type)
        
        let first_upper = tokens[0].to_uppercase();
        if first_upper == "TABLE" || first_upper == "VIEW" || first_upper == "SCHEMA" || 
           first_upper == "DATABASE" || first_upper == "WAREHOUSE" || first_upper == "ROLE" ||
           first_upper == "PROCEDURE" || first_upper == "FUNCTION" || first_upper == "STAGE" {
            // Object type specified, take the next token
            tokens.get(1).map(|s| s.to_string())
        } else {
            // No object type, first token is the object name
            Some(tokens[0].to_string())
        }
    }

    /// Extract the main table from a SELECT statement
    fn extract_main_table_from_select(query: &str) -> Option<String> {
        // Split by whitespace to handle multi-line queries
        let tokens: Vec<&str> = query.split_whitespace().collect();
        
        // Find the FROM keyword
        for (i, token) in tokens.iter().enumerate() {
            if token.to_uppercase() == "FROM" {
                // Get the next token (the table name)
                if let Some(table_token) = tokens.get(i + 1) {
                    // Handle subqueries
                    if table_token.starts_with('(') {
                        return None;
                    }
                    
                    // Return the table name
                    return Some(table_token.to_string());
                }
            }
        }
        None
    }

    /// Clean up an identifier (remove schema prefix, quotes, etc.)
    fn clean_identifier(name: &str) -> String {
        // Remove quotes
        let cleaned = name.trim_matches('"').trim_matches('\'').trim_matches('`');
        
        // If it has a schema prefix (e.g., schema.table), take just the table name
        if let Some(last_part) = cleaned.split('.').last() {
            // Limit length for display
            if last_part.len() > 20 {
                format!("{}...", &last_part[..17])
            } else {
                last_part.to_string()
            }
        } else {
            cleaned.to_string()
        }
    }

    fn render_help<B: Backend>(&self, f: &mut Frame<B>, area: Rect) {
        use tui::widgets::*;

        const HELP: &[&str] = &[
            " Frost  –  Key Reference ",
            "",
            "  🔄 Navigation & Focus",
            "      Esc                  Cycle focus between visible panes",
            "      Tab                  Switch between find/replace fields",
            "      [ ]                  Previous/next result tab",
            "",
            "  📝 Editing & Execution", 
            "      Ctrl + Enter         Execute selection or statement at cursor",
            "      Ctrl + Backspace     Cancel running query",
            "      Ctrl + A             Select all",
            "      Ctrl + C             Copy selection",
            "      Ctrl + X             Cut selection", 
            "      Ctrl + V             Paste",
            "      Ctrl + Z             Undo",
            "      Ctrl + Y             Redo",
            "",
            "  🔍 Search & Find",
            "      Ctrl + F             Find text (Editor/Results/Navigator)",
            "      Ctrl + G             Go to next match",
            "      Ctrl + Shift + G     Go to previous match", 
            "      Ctrl + H             Replace current match (Editor only)",
            "      Ctrl + Shift + H     Replace all matches (Editor only)",
            "",
            "  🖼️ Pane Management",
            "      Alt + ←              Shrink Navigator / hide if at minimum",
            "      Alt + →              Show/expand Navigator pane",
            "      Alt + ↑              Expand Results / hide Editor if at minimum", 
            "      Alt + ↓              Expand Editor / hide Results if at minimum",
            "",
            "  💾 Data Export & Tools",
            "      F9                   Export results to CSV",
            "      F10                  Generate batch script",
            "      Ctrl + R             Refresh current database object (Navigator)",
            "      Ctrl + Shift + R     Full schema refresh (Navigator)",
            "      Ctrl + U             Select role filter (Navigator)",
            "",
            "  🖱️ Selection & Navigation",
            "     Shift + Arrows       Extend selection",
            "     Ctrl + Arrows        Fast scroll/move",
            "     Double-click         Select word (Editor) or cell (Results)",
            "     Click row #          Select entire row",
            "     Click column header  Select entire column", 
            "     Ctrl + Click         Toggle row/column in selection",
            "",
            "     F1                   Close this help screen",
            "     Ctrl + Q             Quit Frost",
        ];

        let text = HELP.join("\n");
        use crate::palette::STYLE;
        let block = Block::default()
            .style(STYLE::help_bg())
            .borders(Borders::ALL)
            .title(" Help (F1 to close) ")
            .border_style(STYLE::help_border());

        let p = Paragraph::new(text)
            .block(block)
            .wrap(Wrap { trim: false });

        f.render_widget(Clear, area);          // Clear the entire screen
        f.render_widget(p, area);
    }

    fn render_batch_generator<B: Backend>(&self, f: &mut Frame<B>, area: Rect) {
        use tui::widgets::*;
        use tui::text::*;
        use tui::style::{Style, Modifier, Color};
        
        if let Some(dialog) = &self.batch_generator {
            // Calculate dialog size - make it wider
            let dialog_width = 80.min(area.width - 4);
            let dialog_height = 18.min(area.height - 4);  // Increased for new checkbox
            
            let dialog_area = Rect {
                x: (area.width - dialog_width) / 2,
                y: (area.height - dialog_height) / 2,
                width: dialog_width,
                height: dialog_height,
            };
            
            // Clear background
            f.render_widget(Clear, dialog_area);
            
            // Create dialog content
            let mut lines = vec![
                Spans::from(Span::raw(format!("SQL File: {}", dialog.sql_file.display()))),
                Spans::from(Span::raw("")),
            ];
            
            // Output directory field
            let output_label = if dialog.field_index == 0 {
                Span::styled("Output Directory: ", Style::default().add_modifier(Modifier::BOLD))
            } else {
                Span::raw("Output Directory: ")
            };
            lines.push(Spans::from(vec![
                output_label,
                Span::raw(&dialog.output_dir),
                if dialog.field_index == 0 { Span::styled("_", Style::default().add_modifier(Modifier::REVERSED)) } else { Span::raw("") },
            ]));
            
            // Format field
            let format_label = if dialog.field_index == 1 {
                Span::styled("Format: ", Style::default().add_modifier(Modifier::BOLD))
            } else {
                Span::raw("Format: ")
            };
            let formats = BatchGeneratorDialog::formats();
            let format_options: Vec<Span> = formats.iter().enumerate().map(|(i, fmt)| {
                if i == dialog.format_index && dialog.field_index == 1 {
                    Span::styled(format!(" [{}] ", fmt), Style::default().add_modifier(Modifier::REVERSED))
                } else if i == dialog.format_index {
                    Span::styled(format!(" [{}] ", fmt), Style::default().add_modifier(Modifier::BOLD))
                } else {
                    Span::raw(format!("  {}  ", fmt))
                }
            }).collect();
            
            let mut format_line = vec![format_label];
            format_line.extend(format_options);
            lines.push(Spans::from(format_line));
            
            // Exit on error checkbox
            let exit_label = if dialog.field_index == 2 {
                Span::styled("Exit on Error: ", Style::default().add_modifier(Modifier::BOLD))
            } else {
                Span::raw("Exit on Error: ")
            };
            let exit_checkbox = if dialog.field_index == 2 {
                Span::styled(
                    if dialog.exit_on_error { "[X]" } else { "[ ]" },
                    Style::default().add_modifier(Modifier::REVERSED)
                )
            } else {
                Span::raw(if dialog.exit_on_error { "[X]" } else { "[ ]" })
            };
            lines.push(Spans::from(vec![exit_label, exit_checkbox]));
            
            // Verbose checkbox
            let verbose_label = if dialog.field_index == 3 {
                Span::styled("Verbose Output: ", Style::default().add_modifier(Modifier::BOLD))
            } else {
                Span::raw("Verbose Output: ")
            };
            let verbose_checkbox = if dialog.field_index == 3 {
                Span::styled(
                    if dialog.verbose { "[X]" } else { "[ ]" },
                    Style::default().add_modifier(Modifier::REVERSED)
                )
            } else {
                Span::raw(if dialog.verbose { "[X]" } else { "[ ]" })
            };
            lines.push(Spans::from(vec![verbose_label, verbose_checkbox]));
            
            // Last query only checkbox
            let last_query_label = if dialog.field_index == 4 {
                Span::styled("Export Last Query Result Only: ", Style::default().add_modifier(Modifier::BOLD))
            } else {
                Span::raw("Export Last Query Result Only: ")
            };
            let last_query_checkbox = if dialog.field_index == 4 {
                Span::styled(
                    if dialog.last_query_only { "[X]" } else { "[ ]" },
                    Style::default().add_modifier(Modifier::REVERSED)
                )
            } else {
                Span::raw(if dialog.last_query_only { "[X]" } else { "[ ]" })
            };
            lines.push(Spans::from(vec![last_query_label, last_query_checkbox]));
            
            // Auto-dismiss checkbox (NEW)
            let auto_dismiss_label = if dialog.field_index == 5 {
                Span::styled("Auto-dismiss Terminal: ", Style::default().add_modifier(Modifier::BOLD))
            } else {
                Span::raw("Auto-dismiss Terminal: ")
            };
            let auto_dismiss_checkbox = if dialog.field_index == 5 {
                Span::styled(
                    if dialog.auto_dismiss { "[X]" } else { "[ ]" },
                    Style::default().add_modifier(Modifier::REVERSED)
                )
            } else {
                Span::raw(if dialog.auto_dismiss { "[X]" } else { "[ ]" })
            };
            lines.push(Spans::from(vec![auto_dismiss_label, auto_dismiss_checkbox]));
            
            lines.push(Spans::from(Span::raw("")));
            
            // Message
            if let Some(msg) = &dialog.message {
                lines.push(Spans::from(Span::styled(msg, Style::default().fg(Color::Yellow))));
                lines.push(Spans::from(Span::raw("")));
            }
            
            // Instructions
            lines.push(Spans::from(Span::raw("Space: Toggle checkboxes")));
            lines.push(Spans::from(Span::raw("Enter: Generate script")));
            lines.push(Spans::from(Span::raw("Esc/F10: Cancel")));
            
            let block = Block::default()
                .title(" Generate Batch Script (F10) ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan));
                
            let paragraph = Paragraph::new(lines)
                .block(block)
                .wrap(Wrap { trim: true });
                
            f.render_widget(paragraph, dialog_area);
        }
    }

    fn queries_for_execution(&self) -> Vec<String> {
        /* 1️⃣  highlighted text takes priority */
        if let Some(r) = self.editor.selection_range() {
            if r.start != r.end {
                return Self::split_sql(&self.editor.buffer[r]).collect();
            }
        }

        /* 2️⃣  caret-based logic that uses the shared parser */
        let buf   = &self.editor.buffer;
        let caret = self.editor.caret;

        if let Some(stmt) = Self::statement_at_caret(buf, caret) {
            return Self::split_sql(&stmt).collect();
        }

        /* 3️⃣  nothing to run */
        Vec::new()
    }
    /// Check if auto-wrapping is disabled via comment hint
    fn has_nowrap_hint(stmt: &str) -> bool {
        // Look for --nowrap or /* nowrap */ comment
        stmt.contains("--nowrap") || stmt.contains("--NOWRAP") || 
        stmt.contains("/*nowrap*/") || stmt.contains("/* nowrap */")
    }

    /// Find the complete DECLARE/BEGIN/END block containing the given position
    fn find_enclosing_block(buf: &str, pos: usize) -> Option<(usize, usize)> {
        // Helper to check if a position has a keyword
        let has_keyword_at = |text: &str, pos: usize, keyword: &str| -> bool {
            if pos + keyword.len() > text.len() {
                return false;
            }
            
            let slice = &text[pos..pos + keyword.len()];
            if !slice.eq_ignore_ascii_case(keyword) {
                return false;
            }
            
            // Check word boundaries
            let before_ok = pos == 0 || !text.as_bytes()[pos.saturating_sub(1)].is_ascii_alphanumeric();
            let after_pos = pos + keyword.len();
            let after_ok = after_pos >= text.len() || !text.as_bytes()[after_pos].is_ascii_alphanumeric();
            
            before_ok && after_ok
        };
        
        // Scan entire buffer for DECLARE/BEGIN blocks
        let mut blocks = Vec::new();
        let mut i = 0;
        
        while i < buf.len() {
            // Look for DECLARE
            if has_keyword_at(buf, i, "DECLARE") {
                let declare_start = i;
                let mut j = i + 7;
                let mut begin_count = 0;
                let mut found_begin = false;
                
                // Find the matching END
                while j < buf.len() {
                    if has_keyword_at(buf, j, "BEGIN") {
                        found_begin = true;
                        begin_count += 1;
                        j += 5;
                    } else if found_begin && has_keyword_at(buf, j, "END") {
                        begin_count -= 1;
                        if begin_count == 0 {
                            let mut end_pos = j + 3;
                            
                            // Include semicolon
                            while end_pos < buf.len() && buf.as_bytes()[end_pos].is_ascii_whitespace() {
                                end_pos += 1;
                            }
                            if end_pos < buf.len() && buf.as_bytes()[end_pos] == b';' {
                                end_pos += 1;
                            }
                            
                            blocks.push((declare_start, end_pos));
                            i = end_pos;
                            break;
                        }
                        j += 3;
                    } else {
                        j += 1;
                    }
                }
                
                if j >= buf.len() {
                    break;
                }
            }
            // Look for standalone BEGIN
            else if has_keyword_at(buf, i, "BEGIN") {
                // Make sure it's not already inside a DECLARE block
                let mut inside_declare = false;
                for &(start, end) in &blocks {
                    if i >= start && i < end {
                        inside_declare = true;
                        break;
                    }
                }
                
                if !inside_declare {
                    let begin_start = i;
                    let mut j = i + 5;
                    let mut begin_count = 1;
                    
                    while j < buf.len() && begin_count > 0 {
                        if has_keyword_at(buf, j, "BEGIN") {
                            begin_count += 1;
                            j += 5;
                        } else if has_keyword_at(buf, j, "END") {
                            begin_count -= 1;
                            if begin_count == 0 {
                                let mut end_pos = j + 3;
                                
                                while end_pos < buf.len() && buf.as_bytes()[end_pos].is_ascii_whitespace() {
                                    end_pos += 1;
                                }
                                if end_pos < buf.len() && buf.as_bytes()[end_pos] == b';' {
                                    end_pos += 1;
                                }
                                
                                blocks.push((begin_start, end_pos));
                                i = end_pos;
                                break;
                            }
                            j += 3;
                        } else {
                            j += 1;
                        }
                    }
                    
                    if j >= buf.len() {
                        break;
                    }
                }
            } else {
                i += 1;
            }
        }
        
        // Find which block contains our position
        for &(start, end) in &blocks {
            if pos >= start && pos < end {
                return Some((start, end));
            }
        }
        
        None
    }

    /// Check if a statement is a DDL that can contain a block
    fn is_block_containing_ddl(text: &str) -> bool {
        let trimmed = text.trim().to_uppercase();
        trimmed.starts_with("CREATE PROCEDURE") ||
        trimmed.starts_with("CREATE OR REPLACE PROCEDURE") ||
        trimmed.starts_with("CREATE FUNCTION") ||
        trimmed.starts_with("CREATE OR REPLACE FUNCTION") ||
        trimmed.starts_with("CREATE TASK") ||
        trimmed.starts_with("CREATE OR REPLACE TASK")
    }

    /// Find the end of a DDL statement that contains a block
    fn find_ddl_with_block_end(text: &str, start: usize) -> Option<usize> {
        use crate::syntax::{step, ParseState, Step};
        
        let bytes = text.as_bytes();
        let mut state = ParseState::Normal;
        let mut i = start;
        let mut begin_count = 0;
        let mut in_as_clause = false;
        let mut found_as = false;
        
        // Skip to AS keyword
        while i < bytes.len() {
            let (next, what) = step(bytes, i, &mut state);
            
            if state == ParseState::Normal && i + 2 <= bytes.len() {
                let word = &text[i..].trim_start();
                if word.len() >= 2 && word[..2].eq_ignore_ascii_case("AS") {
                    // Check word boundary
                    if word.len() == 2 || !word.as_bytes()[2].is_ascii_alphanumeric() {
                        found_as = true;
                        in_as_clause = true;
                        i = next;
                        continue;
                    }
                }
            }
            
            if in_as_clause && state == ParseState::Normal {
                // Look for BEGIN/DECLARE after AS
                if i + 5 <= bytes.len() {
                    let chunk = &text[i..i+5];
                    if chunk.eq_ignore_ascii_case("BEGIN") {
                        let before_ok = i == 0 || !bytes[i-1].is_ascii_alphanumeric();
                        let after_ok = i + 5 >= bytes.len() || !bytes[i+5].is_ascii_alphanumeric();
                        if before_ok && after_ok {
                            begin_count = 1;
                            i += 5;
                            break;
                        }
                    }
                }
                if i + 7 <= bytes.len() {
                    let chunk = &text[i..i+7];
                    if chunk.eq_ignore_ascii_case("DECLARE") {
                        let before_ok = i == 0 || !bytes[i-1].is_ascii_alphanumeric();
                        let after_ok = i + 7 >= bytes.len() || !bytes[i+7].is_ascii_alphanumeric();
                        if before_ok && after_ok {
                            // For DECLARE, we need to find the BEGIN
                            i += 7;
                            while i < bytes.len() {
                                let (next2, _) = step(bytes, i, &mut state);
                                if state == ParseState::Normal && i + 5 <= bytes.len() {
                                    let chunk2 = &text[i..i+5];
                                    if chunk2.eq_ignore_ascii_case("BEGIN") {
                                        let before_ok2 = i == 0 || !bytes[i-1].is_ascii_alphanumeric();
                                        let after_ok2 = i + 5 >= bytes.len() || !bytes[i+5].is_ascii_alphanumeric();
                                        if before_ok2 && after_ok2 {
                                            begin_count = 1;
                                            i += 5;
                                            break;
                                        }
                                    }
                                }
                                i = next2;
                            }
                            break;
                        }
                    }
                }
            }
            
            if what == Step::Eof {
                return None;
            }
            i = next;
        }
        
        if !found_as || begin_count == 0 {
            return None;
        }
        
        // Now find the matching END
        while i < bytes.len() && begin_count > 0 {
            let (next, what) = step(bytes, i, &mut state);
            
            if state == ParseState::Normal {
                if i + 5 <= bytes.len() {
                    let chunk = &text[i..i+5];
                    if chunk.eq_ignore_ascii_case("BEGIN") {
                        let before_ok = i == 0 || !bytes[i-1].is_ascii_alphanumeric();
                        let after_ok = i + 5 >= bytes.len() || !bytes[i+5].is_ascii_alphanumeric();
                        if before_ok && after_ok {
                            begin_count += 1;
                            i += 5;
                            continue;
                        }
                    }
                }
                
                if i + 3 <= bytes.len() {
                    let chunk = &text[i..i+3];
                    if chunk.eq_ignore_ascii_case("END") {
                        let before_ok = i == 0 || !bytes[i-1].is_ascii_alphanumeric();
                        let after_ok = i + 3 >= bytes.len() || !bytes[i+3].is_ascii_alphanumeric();
                        if before_ok && after_ok {
                            begin_count -= 1;
                            if begin_count == 0 {
                                // Found the final END, include it and any trailing semicolon
                                let mut end_pos = i + 3;
                                
                                // Skip whitespace
                                while end_pos < bytes.len() && bytes[end_pos].is_ascii_whitespace() {
                                    end_pos += 1;
                                }
                                
                                // Include semicolon if present
                                if end_pos < bytes.len() && bytes[end_pos] == b';' {
                                    end_pos += 1;
                                }
                                
                                return Some(end_pos);
                            }
                            i += 3;
                            continue;
                        }
                    }
                }
            }
            
            if what == Step::Eof {
                break;
            }
            i = next;
        }
        
        None
    }

    /// Enhanced split_sql that handles DDL with blocks and standalone blocks
    pub fn split_sql(text: &str) -> impl Iterator<Item = String> + '_ {
        let mut state = ParseState::Normal;
        let mut stmts = Vec::<String>::new();
        let mut i = 0usize;
        let mut start = 0usize;
        let bytes = text.as_bytes();

        loop {
            // Check if we're at the start of a DDL that contains a block
            if state == ParseState::Normal && i == start {
                let remaining = &text[i..];
                if Self::is_block_containing_ddl(remaining) {
                    if let Some(ddl_end) = Self::find_ddl_with_block_end(text, i) {
                        let stmt = text[i..ddl_end].trim();
                        if !stmt.is_empty() {
                            stmts.push(stmt.to_owned());
                        }
                        i = ddl_end;
                        start = ddl_end;
                        
                        // Skip any trailing whitespace
                        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                            i += 1;
                            start = i;
                        }
                        continue;
                    }
                }
                
                // Check for standalone DECLARE/BEGIN block
                let trimmed = remaining.trim();
                if trimmed.to_uppercase().starts_with("DECLARE") || 
                   (trimmed.to_uppercase().starts_with("BEGIN") && 
                    !trimmed.to_uppercase().contains("TRANSACTION")) {
                    if let Some((block_start, block_end)) = Self::find_enclosing_block(text, i) {
                        let stmt = text[block_start..block_end].trim();
                        if !stmt.is_empty() {
                            stmts.push(stmt.to_owned());
                        }
                        i = block_end;
                        start = block_end;
                        
                        // Skip any trailing whitespace
                        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                            i += 1;
                            start = i;
                        }
                        continue;
                    }
                }
            }
            
            let (next, what) = step(bytes, i, &mut state);
            match what {
                Step::Semi => {
                    let s = text[start..i].trim();
                    if !s.is_empty() {
                        stmts.push(s.to_owned());
                    }
                    start = next;
                }
                Step::Advance => {}
                Step::Eof => {
                    let s = text[start..].trim();
                    if !s.is_empty() {
                        stmts.push(s.to_owned());
                    }
                    break;
                }
            }
            i = next;
        }
        stmts.into_iter()
    }

    /// Enhanced statement_at_caret that handles DDL with blocks
    fn statement_at_caret(buf: &str, caret: usize) -> Option<String> {
        use crate::syntax::{step, ParseState, Step};
        
        // First check if we're inside a DDL that contains a block
        let bytes = buf.as_bytes();
        let mut state = ParseState::Normal;
        let mut i = 0usize;
        let mut stmt_start = 0usize;
        
        loop {
            // At the start of a potential statement
            if i == stmt_start && state == ParseState::Normal {
                let remaining = &buf[i..];
                
                // Check for DDL with block
                if Self::is_block_containing_ddl(remaining) {
                    if let Some(ddl_end) = Self::find_ddl_with_block_end(buf, i) {
                        if caret >= i && caret <= ddl_end {
                            let stmt = buf[i..ddl_end].trim();
                            return if stmt.is_empty() { None } else { Some(stmt.to_owned()) };
                        }
                        i = ddl_end;
                        stmt_start = ddl_end;
                        
                        // Skip whitespace
                        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                            i += 1;
                            stmt_start = i;
                        }
                        continue;
                    }
                }
                
                // Check for standalone block
                if let Some((block_start, block_end)) = Self::find_enclosing_block(buf, i) {
                    if caret >= block_start && caret <= block_end {
                        let block = buf[block_start..block_end].trim();
                        return if block.is_empty() { None } else { Some(block.to_owned()) };
                    }
                    i = block_end;
                    stmt_start = block_end;
                    
                    // Skip whitespace
                    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                        i += 1;
                        stmt_start = i;
                    }
                    continue;
                }
            }
            
            let (next, what) = step(bytes, i, &mut state);
            
            if matches!(what, Step::Semi | Step::Eof) {
                if caret >= stmt_start && caret < next {
                    let stmt = buf[stmt_start..next].trim();
                    return if stmt.is_empty() { None } else { Some(stmt.to_owned()) };
                }
                stmt_start = next;
            }
            
            if what == Step::Eof { break; }
            i = next;
        }
        
        None
    }

    /// Enhanced should_wrap_statement that knows not to wrap DDL statements
    fn should_wrap_statement(stmt: &str) -> bool {
        // User can disable wrapping with a comment
        if Self::has_nowrap_hint(stmt) {
            return false;
        }
        
        let trimmed = stmt.trim().to_uppercase();
        
        // Never wrap DDL statements (they contain their own blocks)
        if Self::is_block_containing_ddl(stmt) {
            return false;
        }
        
        // Already wrapped - definitely don't wrap again
        if trimmed.starts_with("EXECUTE IMMEDIATE") {
            return false;
        }
        
        // Only wrap standalone DECLARE and BEGIN blocks
        if trimmed.starts_with("DECLARE") {
            return true;
        }
        
        if trimmed.starts_with("BEGIN") && !trimmed.contains("TRANSACTION") {
            // But check if it's a simple BEGIN/END with just session variables
            // These work fine without wrapping
            if !stmt.to_uppercase().contains(" LET ") && 
               !stmt.to_uppercase().contains(" CURSOR ") &&
               !stmt.to_uppercase().contains(" EXCEPTION ") {
                // Might be a simple SET/SELECT block, check if it uses := assignment
                if stmt.contains(":=") {
                    return true; // Needs wrapping for := syntax
                }
                // Otherwise, let it through without wrapping
                return false;
            }
            return true;
        }
        
        false
    }

pub fn render<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> Result<()> {
    self.frame_counter = self.frame_counter.wrapping_add(1);
    
    terminal.draw(|f| {
        let size = f.size();

        /* ── 0️⃣  bail early on absurdly small windows (≤ 3 rows / 9 cols) ── */
        if size.height <= 3 || size.width <= 9 {
            return;                   // nothing we can draw safely
        }

        // ── Alternate background color very slightly to force repaint ─────────
        use tui::widgets::Block;
        use tui::style::{Style, Color};
        
        // Alternate between RGB(22,22,22) and RGB(22,22,23) - imperceptible difference
        let bg_color = if self.frame_counter % 2 == 0 {
            Color::Rgb(22, 22, 22)
        } else {
            Color::Rgb(22, 22, 23)
        };
        
        f.render_widget(
            Block::default().style(Style::default().bg(bg_color)), 
            size
        );

        /* ── 1️⃣  Help overlay?  draw + return ───────────────────────────── */
        if self.show_help {
            self.render_help(f, size);
            return;
        }

        /* ── 2️⃣  calculate the split (editor vs. results) safely ────────── */
        let total_h     = size.height as i16;
        let base_editor = (total_h as f32 * 0.66).round() as i16;

        let min_editor  = MIN_ROWS;                       // 5
        let max_editor  = (total_h - MIN_ROWS - 1).max(min_editor);

        /* translate to "offset from the 66 % baseline" space */
        self.min_split_offset = min_editor - base_editor;
        self.max_split_offset = max_editor - base_editor;
        self.split_offset     = self
            .split_offset
            .clamp(self.min_split_offset, self.max_split_offset);

        let editor_h  = (base_editor + self.split_offset)
            .clamp(min_editor, max_editor) as u16;
        
        // NEW: Handle hidden results pane
        let results_h = if self.results_hidden {
            0
        } else {
            size.height.saturating_sub(editor_h + 1)   // status bar
        };

        /* ── if either pane became 0×0, skip this frame (prevents tui panic) */
        if editor_h == 0 {
            return;
        }


        // Calculate main area (potentially split for tree)
        let main_area = if self.db_tree.visible {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(self.db_tree.width_percent),  // Use dynamic width
                    Constraint::Percentage(100 - self.db_tree.width_percent),
                ])
                .split(size);
            
            // Render tree on the left
            self.db_tree.render(f, chunks[0]);
            
            chunks[1]  // Return right side for editor/results
        } else {
            size  // Use full screen when tree is hidden
        };

        /* ── Handle hidden editor ─────────────────────────────────── */
        if self.editor_hidden {
            // Editor is hidden, results takes all space
            let _editor_h = 0u16;
            let _results_h = if self.results_hidden {
                0
            } else {
                main_area.height.saturating_sub(1)  // -1 for status bar
            };
            
            // Layout without editor
            let chunks = if self.results_hidden {
                // Both hidden? Just show status bar
                Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(1),  // Status bar only
                    ])
                    .split(main_area)
            } else {
                Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                    Constraint::Min(1),     // Results takes all space
                        Constraint::Length(1),  // Status bar
                    ])
                    .split(main_area)
            };
            
            // Don't render editor at all
            if !self.results_hidden {
                self.last_results_area = Some(chunks[0]);
                self.results.max_rows = chunks[0].height.saturating_sub(4).max(1) as usize;
                self.results.max_cols = (chunks[0].width / 20).max(2) as usize;
                self.results.render(f, chunks[0], self.total_queries);
            }
            
            // Render status bar
            let status_chunk = if self.results_hidden { chunks[0] } else { chunks[1] };
            
            // helper-closure for the old fallback text
            let default_status = |msg: &Option<String>, err: &Option<String>, connected: bool| -> (String, tui::style::Style) {
                let conn_status = if connected { "[Connected]" } else { "[Not Connected]" };
                if let Some(m) = msg {
                    (format!("{} | {}", conn_status, m), STYLE::status_fg())
                } else if let Some(e) = err {
                    (format!("{} | Press F1 for help | Error: {}", conn_status, e), STYLE::status_fg())
                } else {
                    (format!("{} | Press F1 for help", conn_status), STYLE::status_fg())
                }
            };
            
            // --- results-selection summary when that pane is focused ------------
            if self.focus == Focus::Results {
                if let Some((stats, warn)) = self.results.selection_stats() {
                    use tui::text::{Span, Spans};

                    let spans = if let Some(w) = warn {
                        Spans::from(vec![
                            Span::raw(stats),
                            Span::raw("  |  "),
                            Span::styled(w, STYLE::error_fg()),   // red only for warning
                        ])
                    } else {
                        Spans::from(Span::raw(stats))
                    };

                    let bar = tui::widgets::Paragraph::new(spans).style(STYLE::status_fg());
                    f.render_widget(bar, status_chunk);
                    return;            // nothing else goes into the status bar
                }
            }
            
            // default status (editor focus or no selection) ─────────────────────────
            let (txt, style) = default_status(&self.status_message, &self.error, self.connected);
            let bar = tui::widgets::Paragraph::new(txt).style(style);
            f.render_widget(bar, status_chunk);
            
            /* ── Batch generator overlay? ─────────────────────────────── */
            if self.batch_generator.is_some() {
                self.render_batch_generator(f, size);
            }
            
            return;
        }
        
        // NEW: Adjust layout based on whether results are hidden
        let chunks = if self.results_hidden {
            Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(1),  // Editor takes remaining space
                Constraint::Length(1),  // Status bar
            ])
            .split(main_area)
        } else {
            Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(editor_h),
                Constraint::Length(results_h),
                Constraint::Length(1),  // Status bar
            ])
            .split(main_area)
        };

        /* ── 3️⃣  remember rectangles + update derived limits ───────────── */
        self.last_editor_area  = Some(chunks[0]);
        if !self.results_hidden {
            self.last_results_area = Some(chunks[1]);

            self.results.max_rows = chunks[1].height.saturating_sub(4).max(1) as usize;
            self.results.max_cols = (chunks[1].width / 20).max(2) as usize;
        }

        self.editor.set_viewport_size(
            chunks[0].height.saturating_sub(2) as usize,
            chunks[0].width.saturating_sub(GUTTER_WIDTH + 2) as usize,
        );

        self.editor.focus  = self.focus == Focus::Editor;
        self.results.focus = self.focus == Focus::Results;
        self.db_tree.focused = self.focus == Focus::DbTree;

        /* ── 4️⃣  actual drawing ─────────────────────────────────────────── */
        self.editor.render(f, chunks[0]);
        
        // NEW: Only render results if visible
        if !self.results_hidden {
            self.results.render(f, chunks[1], self.total_queries);
        }

        // helper-closure for the old fallback text
        let default_status = |msg: &Option<String>, err: &Option<String>, connected: bool| -> (String, tui::style::Style) {
            let conn_status = if connected { "[Connected]" } else { "[Not Connected]" };
            if let Some(m) = msg {
                (format!("{} | {}", conn_status, m), STYLE::status_fg())
            } else if let Some(e) = err {
                (format!("{} | Press F1 for help | Error: {}", conn_status, e), STYLE::status_fg())
            } else {
                (format!("{} | Press F1 for help", conn_status), STYLE::status_fg())
            }
        };

        // --- results-selection summary when that pane is focused ------------
        if self.focus == Focus::Results {
            if let Some((stats, warn)) = self.results.selection_stats() {
                use tui::text::{Span, Spans};

                let spans = if let Some(w) = warn {
                    Spans::from(vec![
                        Span::raw(stats),
                        Span::raw("  |  "),
                        Span::styled(w, STYLE::error_fg()),   // red only for warning
                    ])
                } else {
                    Spans::from(Span::raw(stats))
                };

                let bar = tui::widgets::Paragraph::new(spans).style(STYLE::status_fg());
                f.render_widget(bar, chunks[if self.results_hidden { 1 } else { 2 }]);
                return;            // nothing else goes into the status bar
            }
        }

        // default status (editor focus or no selection) ─────────────────────────
        let (txt, style) = default_status(&self.status_message, &self.error, self.connected);
        let bar = tui::widgets::Paragraph::new(txt).style(style);
        f.render_widget(bar, chunks[if self.results_hidden { 1 } else { 2 }]);
        /* ── 5️⃣  Batch generator overlay? ─────────────────────────────── */
        if self.batch_generator.is_some() {
            self.render_batch_generator(f, size);
        }
    })?;
    Ok(())
}



    fn adjust_split(&mut self, delta: i16) {
        let lo = self.min_split_offset.min(self.max_split_offset);
        let hi = self.max_split_offset.max(self.min_split_offset);
        self.split_offset = (self.split_offset + delta).clamp(lo, hi);
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        // Handle Ctrl+Q first (quit)
        if (key.code == KeyCode::Char('q') || key.code == KeyCode::Char('Q')) && key.modifiers.contains(KeyModifiers::CONTROL) {
            return Ok(true);
        }

        // Handle F1 (help)
        if key.kind == KeyEventKind::Press && key.code == KeyCode::F(1) {
            self.show_help = !self.show_help;
            return Ok(false);
        }

        /* block all other keys while help is shown */
        if self.show_help {
            return Ok(false);
        }

        // Handle batch generator dialog - MUST consume ALL key events when active
        if self.batch_generator.is_some() {
            // Only process on Press events to avoid double-processing
            if key.kind != KeyEventKind::Press {
                return Ok(false);
            }
            
            if let Some(dialog) = &mut self.batch_generator {
                if let Some(_config) = dialog.handle_key(key) {
                    // Dialog was closed with success
                    if let Some(msg) = &dialog.message {
                        self.status_message = Some(msg.clone());
                        self.status_message_time = Some(Instant::now());
                    }
                }
                if !dialog.active {
                    self.batch_generator = None;
                }
            }
            return Ok(false);  // ALWAYS consume the event when dialog is active
        }

        // Block ctrl+enter only during running!
        if self.running
           && key.kind == KeyEventKind::Press
           && key.code == KeyCode::Enter
           && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            return Ok(false);          // ignore repeated Ctrl-Enter
        }

        // ── ESC  ───────────────────────────────────────────────
        if key.code == KeyCode::Esc {
            match key.kind {
                KeyEventKind::Press   if !self.last_esc_down => {
                    self.switch_focus();      // flip Editor ⟷ Results
                    self.last_esc_down = true;
                }
                KeyEventKind::Release => {
                    self.last_esc_down = false;
                }
                _ => {}
            }
            return Ok(false);                 // swallow all Esc events
        }

        // Handle other control key combinations
        if key.kind == KeyEventKind::Press
           && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            match key.code {
                KeyCode::Enter => {            // Ctrl-Enter  → execute
                    self.start_execute()?;
                    return Ok(false);
                }
                KeyCode::Backspace => {        // Ctrl-Backspace → cancel
                    if self.running {
                        // ① Call SQLCancel immediately on whatever is running
                        if let Some(h) = *self.current_stmt.lock().unwrap() {
                            unsafe { let _ = SQLCancel(h.0); };
                        }
                        // ② (Optionally) still poke the worker so it knows we intended to cancel.
                        let _ = self.db_req_tx.send(DbWorkerRequest::Cancel);

                        // ③ Update local UI state
                        self.running = false;
                        self.running_query_idx = None;
                        self.run_duration = self.run_started.map(|s| s.elapsed());
                        self.status_message = Some("Query cancellation requested…".into());
                        self.status_message_time = Some(Instant::now());
                    }
                    return Ok(false);
                }
                _ => {}
            }
        }

        // Handle function keys
        if key.kind == KeyEventKind::Press && key.code == KeyCode::F(9) {
            self.export_csv_to_downloads();
            return Ok(false);
        }

        if key.kind == KeyEventKind::Press && key.code == KeyCode::F(10) {
            if let Some(path) = &self.file_path {
                self.batch_generator = Some(BatchGeneratorDialog::new(path.clone()));
            } else {
                self.status_message = Some("Save the file first to generate a batch script".to_string());
                self.status_message_time = Some(Instant::now());
            }
            return Ok(false);
        }

        // NEW: Global Alt+Arrow key handling
        if key.kind == KeyEventKind::Press && key.modifiers == KeyModifiers::ALT {
            match key.code {
                KeyCode::Left => {
                    if self.db_tree.visible {
                        // Tree is visible - decrease width or hide if at minimum
                        if self.db_tree.width_percent <= 10 {
                            // At minimum, hide it
                            self.db_tree.visible = false;
                            self.ensure_focus_on_visible_pane(); // Add this line
                        } else {
                            // Decrease width
                            self.db_tree.width_percent = (self.db_tree.width_percent - 5).max(10);
                        }
                    } else {
                        // Tree is hidden, do nothing on Alt+Left
                    }
                    return Ok(false);
                }
                KeyCode::Right => {
                    if !self.db_tree.visible {
                        // Tree is hidden - show it at minimum width
                        self.db_tree.visible = true;
                        self.db_tree.width_percent = 10;
                        // Give focus to the navigator when it appears
                        self.focus = Focus::DbTree;
                        self.editor.focus = false;
                        self.results.focus = false;
                        self.db_tree.focused = true;
                        // Optionally refresh the cache
                        self.db_tree.on_show();
                    } else {
                        // Tree is visible - increase width
                        self.db_tree.width_percent = (self.db_tree.width_percent + 5).min(90);
                    }
                    return Ok(false);
                }
                KeyCode::Up => {
                    if self.results_hidden {
                        // Results is hidden - show it at minimum size + 1
                        self.results_hidden = false;
                        // Set split offset so results pane has MIN_ROWS + 1 height
                        self.split_offset = self.max_split_offset - 1;
                    } else if self.editor_hidden {
                        // Editor is already hidden (results is full height)
                        // Don't do anything - prevents toggle behavior
                        return Ok(false);
                    } else {
                        // Both visible - make results bigger (editor smaller)
                        self.adjust_split(-1);
                        
                        // Check if editor is now at minimum - if so, hide it
                        if self.split_offset == self.min_split_offset {
                            self.editor_hidden = true;
                            self.ensure_focus_on_visible_pane(); // Add this line
                        }
                    }
                    return Ok(false);
                }
                KeyCode::Down => {
                    if self.results_hidden {
                        return Ok(false);  // Already hidden
                    } else if self.editor_hidden {
                        // Editor is hidden - show it at minimum size + 1
                        self.editor_hidden = false;
                        // Set split offset so editor has MIN_ROWS + 1 height
                        self.split_offset = self.min_split_offset + 1;
                        return Ok(false);
                    } else {
                        // Both visible - make editor bigger (results smaller)
                        self.adjust_split(1);
                        
                        // Check if results is now at minimum - if so, hide it
                        if self.split_offset == self.max_split_offset {
                            self.results_hidden = true;
                            self.ensure_focus_on_visible_pane(); // Add this line
                        }
                    }
                    return Ok(false);
                }
                _ => {}
            }
        }

        // Only process other keys on Press events
        if key.kind != KeyEventKind::Press {
            return Ok(false);
        }

        // If tree is visible and focused, let it handle keys first
        // REMOVED Alt+Left/Right handling from tree since it's now global
        if self.db_tree.visible && self.focus == Focus::DbTree {
            let handled = self.db_tree.handle_key(key);
            
            // Check if tree wants to insert text or change role
            if handled {
                // Check what action the tree wants us to perform
                if let Some(action) = self.db_tree.take_pending_action() {
                    match action {
                        TreeAction::ChangeRole(role) => {
                            let queries = if role == "ALL" {
                                vec!["USE SECONDARY ROLES ALL".to_string()]
                            } else {
                                vec![
                                    "USE SECONDARY ROLES NONE".to_string(),
                                    format!("USE ROLE \"{}\"", role)
                                ]
                            };
                            self.execute_role_change(queries)?;
                        }
                        TreeAction::InsertText(text) => {
                            self.editor.insert(&text);
                        }
                        TreeAction::None => {}
                    }
                }
            }
            
            return Ok(false);
        }

        // Pass key to focused pane
        match self.focus {
            Focus::Editor => {
                self.editor.handle_key(key);
            }
            Focus::Results => {
                self.results.handle_key(key);
            }
            Focus::DbTree => {
                // Tree handles its own keys
            }
        }
        Ok(false)
    }

    pub fn handle_mouse(&mut self, event: MouseEvent) {
        use crossterm::event::MouseEventKind::Down;
        use crossterm::event::MouseEventKind::Up;
        use crossterm::event::MouseEventKind::Drag;
        use crossterm::event::MouseButton;

        if matches!(event.kind, crossterm::event::MouseEventKind::Moved) {
            return;
        }

        // If we're in a drag, only send events to the source pane
        if let (Drag(MouseButton::Left), Some(source)) = (event.kind, self.drag_source) {
            match source {
                Focus::Editor => {
                    if let Some(area) = self.last_editor_area {
                        self.editor.handle_mouse(event, area);
                    }
                }
                Focus::Results => {
                    if let Some(area) = self.last_results_area {
                        self.results.handle_mouse(event, area);
                    }
                }
                Focus::DbTree => {
                    if self.db_tree.visible {
                        let size = self.last_editor_area.map(|r| Rect {
                            x: 0,
                            y: 0,
                            width: r.x + r.width,
                            height: r.y + r.height + 10,
                        }).unwrap_or_default();
                        let tree_width = (size.width as f32 * (self.db_tree.width_percent as f32 / 100.0)) as u16;
                        let tree_area = Rect {
                            x: 0,
                            y: 0,
                            width: tree_width,
                            height: size.height,
                        };
                        self.db_tree.handle_mouse(event, tree_area);
                    }
                }
            }
            return;
        }

        // Clear drag source on mouse up
        if matches!(event.kind, Up(MouseButton::Left)) {
            self.drag_source = None;
        }

        // Determine which pane the mouse is over
        let target = |x, y| -> Option<(Focus, Rect)> {
            self.last_editor_area
                .filter(|r| hit(*r, x, y))
                .map(|r| (Focus::Editor, r))
                .or_else(|| self.last_results_area
                    .filter(|r| hit(*r, x, y))
                    .map(|r| (Focus::Results, r)))
        };

        // If tree is visible, check if click is in tree area
        if self.db_tree.visible {
            let size = self.last_editor_area.map(|r| Rect {
                x: 0,
                y: 0,
                width: r.x + r.width,
                height: r.y + r.height + 10, // rough estimate
            }).unwrap_or_default();
            
            let tree_width = (size.width as f32 * 0.35) as u16;
            
            if event.column < tree_width {
                // Click in tree area
                let tree_area = Rect {
                    x: 0,
                    y: 0,
                    width: tree_width,
                    height: size.height,
                };
                
                self.db_tree.handle_mouse(event, tree_area);
                self.focus = Focus::DbTree;
                
                // Update focused state for all panes
                self.editor.focus = false;
                self.results.focus = false;
                self.db_tree.focused = true;
                
                return;
            }
        }

        let Some((pane, rect)) = target(event.column, event.row) else { return };

        // Set drag source when starting a drag
        if matches!(event.kind, Down(MouseButton::Left)) {
            self.drag_source = Some(pane);
        }

        // switch focus *only* on a real click
        if matches!(event.kind, Down(_)) && self.focus != pane {
            self.focus = pane;
            // Save any pending edits when clicking away from editor
            match pane {
                Focus::Results if self.focus == Focus::Editor => self.editor.on_focus_lost(),
                _ => {}
            }
        }

        // always deliver the event to the pane it landed in
        match pane {
            Focus::Editor  => self.editor.handle_mouse(event, rect),
            Focus::Results => {
                let running = self.results
                    .tabs
                    .get(self.results.tab_idx)
                    .map(|t| t.running)
                    .unwrap_or(false);
                if !running {
                    self.results.handle_mouse(event, rect);
                }
            }
            Focus::DbTree => {
                // Tree handles its own mouse events
            }
        }
    }

    pub fn update(&mut self) {
        /* ── 0️⃣ autosave bookkeeping ────────────────────────── */
        if self.editor.dirty {
            self.autosave.notify_edit();
            self.editor.dirty = false;
        }
        if let Some(msg) =
            self.autosave
                .maybe_flush(&self.editor.buffer, self.editor.last_edit_time)
        {
            self.status_message      = Some(msg);
            self.status_message_time = Some(Instant::now());
        }

        self.db_tree.check_refresh();
        
        /* ── running ──────────────────────────*/
        if self.running {
            self.run_duration = self.run_started.map(|s| s.elapsed());
        }
        if let Some(t) = self.status_message_time {
            if t.elapsed() > Duration::from_secs(5) {
                self.status_message = None;
                self.status_message_time = None;
            }
        }
    }

    /// Called from `main` when the OS asks us to quit *right now*.
    pub fn force_autosave(&mut self) {
        self.autosave.force_flush(&self.editor.buffer);
    }

    /// Call on a *normal* shutdown to replace the real file with the buffer
    /// contents and then remove any `.autosave`.
    pub fn final_save(&mut self) {
        // Save any pending edits before final save
        self.editor.on_focus_lost();
      
        if let Some(lock) = &mut self.file_lock {
            if let Err(_e) = lock.save_and_unlock(&self.editor.buffer) {
                //eprintln!("Final save failed: {e}");
                return;                  // keep .autosave as fallback
            }
            self.autosave.clear();       // success → delete rescue copy
        }
    }

    fn ensure_focus_on_visible_pane(&mut self) {
        // Check if current focus is on a hidden pane
        let focus_is_valid = match self.focus {
            Focus::Editor => !self.editor_hidden,
            Focus::Results => !self.results_hidden,
            Focus::DbTree => self.db_tree.visible,
        };
        
        if focus_is_valid {
            return; // Current focus is fine
        }
        
        // Find a visible pane to focus on
        if !self.editor_hidden {
            self.focus = Focus::Editor;
        } else if !self.results_hidden {
            self.focus = Focus::Results;
        } else if self.db_tree.visible {
            self.focus = Focus::DbTree;
        }
        // If somehow all panes are hidden, focus remains where it was
        
        // Update focused state for all panes
        self.editor.focus = self.focus == Focus::Editor && !self.editor_hidden;
        self.results.focus = self.focus == Focus::Results && !self.results_hidden;
        self.db_tree.focused = self.focus == Focus::DbTree && self.db_tree.visible;
    }

    fn switch_focus(&mut self) {
        // Save any pending edits when switching focus
        if self.focus == Focus::Editor && !self.editor_hidden {
            self.editor.on_focus_lost();
        }
        
        // Determine which panes are actually visible and can receive focus
        let nav_visible = self.db_tree.visible;
        let editor_visible = !self.editor_hidden;
        let results_visible = !self.results_hidden;
        
        // Count visible panes
        let visible_count = (nav_visible as u8) + (editor_visible as u8) + (results_visible as u8);
        
        // If only one pane is visible, stay on it
        if visible_count <= 1 {
            return;
        }
        
        // Cycle through visible panes
        loop {
            self.focus = match self.focus {
                Focus::Editor => {
                    if results_visible {
                        Focus::Results
                    } else if nav_visible {
                        Focus::DbTree
                    } else {
                        Focus::Editor  // Shouldn't happen, but safe fallback
                    }
                }
                Focus::Results => {
                    if nav_visible {
                        Focus::DbTree
                    } else if editor_visible {
                        Focus::Editor
                    } else {
                        Focus::Results  // Shouldn't happen, but safe fallback
                    }
                }
                Focus::DbTree => {
                    if editor_visible {
                        Focus::Editor
                    } else if results_visible {
                        Focus::Results
                    } else {
                        Focus::DbTree  // Shouldn't happen, but safe fallback
                    }
                }
            };
            
            // Verify the new focus is actually visible
            let focus_is_visible = match self.focus {
                Focus::Editor => editor_visible,
                Focus::Results => results_visible,
                Focus::DbTree => nav_visible,
            };
            
            if focus_is_visible {
                break;
            }
            // If not visible, continue cycling
        }
        
        // Update focused state for all panes
        self.editor.focus = self.focus == Focus::Editor && editor_visible;
        self.results.focus = self.focus == Focus::Results && results_visible;
        self.db_tree.focused = self.focus == Focus::DbTree && nav_visible;
    }

    pub fn start_execute(&mut self) -> Result<()> {
        // Save any pending edits before executing
        self.editor.on_focus_lost();
        
        let queries = self.queries_for_execution();

        if queries.is_empty() {
            self.status_message = Some("No SQL statement at caret/selection.".into());
            self.status_message_time = Some(Instant::now());
            return Ok(());                                    // bail out early
        }

        // Extract contexts for each query
        let queries_with_context: Vec<(String, String)> = queries
            .into_iter()
            .map(|q| {
                let context = Self::extract_query_context(&q);
                (q, context)
            })
            .collect();

        self.results.clear();
        self.error = None;
        self.running = true;
        self.run_started = Some(Instant::now());
        self.run_duration = None;
        self.running_query_idx = None;
        self.total_queries = queries_with_context.len();

        let _ = self.db_req_tx.send(DbWorkerRequest::RunQueries(queries_with_context));
        Ok(())
    }

    /// Execute role change commands without adding them to the editor
    fn execute_role_change(&mut self, queries: Vec<String>) -> Result<()> {
        // Prepare queries with context
        let queries_with_context: Vec<(String, String)> = queries
            .into_iter()
            .map(|q| (q, "Role Change".to_string()))
            .collect();

        // Clear any existing results
        self.results.clear();
        self.error = None;
        self.running = true;
        self.run_started = Some(Instant::now());
        self.run_duration = None;
        self.running_query_idx = None;
        self.total_queries = queries_with_context.len();

        // Send to worker
        let _ = self.db_req_tx.send(DbWorkerRequest::RunQueries(queries_with_context));
        
        // Show status
        self.status_message = Some("Changing role...".to_string());
        self.status_message_time = Some(Instant::now());
        
        Ok(())
    }

    /// Call this from your main event loop regularly.
    pub fn poll_db_responses(&mut self) -> bool {
        let mut changed = false;
        // First time check: if editor has no schema cache but db_tree does, copy it
        if self.editor.schema_cache.is_none() {
            if let Some(cache) = self.db_tree.cache.clone() {
                self.editor.schema_cache = Some(cache);
            }
        }
        while let Ok(msg) = self.db_resp_rx.try_recv() {
            match msg {
                DbWorkerResponse::Connected => {
                    self.connected = true;
                    self.db_tree.set_connected(true);
                    self.status_message = Some("Connected to Snowflake".to_string());
                    self.status_message_time = Some(Instant::now());
                    changed = true;
                }
                DbWorkerResponse::QueryStarted { query_idx: _, started, query_context } => {
                    // Add a tab for this query
                    self.results.tabs.push(crate::results::ResultsTab::new_pending_with_start(query_context, started));
                    self.results.tab_idx = self.results.tabs.len() - 1;
                    self.running_query_idx = Some(self.results.tabs.len() - 1);
                    self.running = true;
                    changed = true;
                }
                DbWorkerResponse::QueryFinished { query_idx, elapsed, result } => {
                    if let Some(tab) = self.results.tabs.get_mut(query_idx) {
                        tab.content = result;
                        tab.elapsed = Some(elapsed);
                        tab.running = false;
                        tab.run_started = None;
                    }
                    if query_idx + 1 < self.total_queries {
                        // waiting for next QueryStarted to push the next tab
                    } else {
                        self.running = false;
                        self.running_query_idx = None;
                        self.run_duration = self.run_started.map(|s| s.elapsed());
                    }
                    changed = true;
                }
                DbWorkerResponse::QueryError { query_idx, elapsed, message } => {
                    // Was it a user-cancel?  ODBC returns SQLSTATE HY008 (“Operation cancelled”)
                    let is_cancel = message.contains("HY008");

                    if let Some(tab) = self.results.tabs.get_mut(query_idx) {
                        if is_cancel {
                            tab.content = ResultsContent::Info { message: "Cancelled.".to_string() };
                        } else {
                            tab.content = ResultsContent::Error { 
                                message: message.clone(),
                                cursor: 0,
                                selection: None,
                            };
                        }
                        tab.elapsed = Some(elapsed);
                        tab.running = false;
                        tab.run_started = None;
                    }

                    // Clear running state so UI unlocks
                    self.running = false;
                    self.running_query_idx = None;
                    self.run_duration = self.run_started.map(|s| s.elapsed());

                    if !is_cancel {
                        self.error = Some(message);          // real error still shown in status
                    }
                    changed = true;
                }
            }
            // Update editor's schema cache when tree is refreshed
            if let Some(cache) = self.db_tree.cache.clone() {
                self.editor.schema_cache = Some(cache);
            }
        }
        changed
    }

    pub fn export_csv_to_downloads(&mut self) {
        use crate::results::ResultsContent;
        use crate::results_export::export_entire_result_set;

        let filename = "results_export.csv";
        let download_folder = get_downloads_folder();
        let target_path = download_folder.join(filename);

        if let Some(tab) = self.results.tabs.get_mut(self.results.tab_idx) {
            if let ResultsContent::Table { headers, tile_store } = &mut tab.content {
                // tile_store is now &mut TileRowStore
                let data = match tile_store.get_rows(0, tile_store.nrows) {
                    Ok(rows) => rows,
                    Err(_) => vec![],
                };
                let csv = export_entire_result_set(headers, &data);
                match std::fs::write(&target_path, csv) {
                    Ok(_) => {
                        self.status_message = Some(format!("CSV exported to: {}", target_path.display()));
                        self.status_message_time = Some(Instant::now());
                    },
                    Err(err) => {
                        self.status_message = Some(format!("Error saving CSV: {err}"));
                        self.status_message_time = Some(Instant::now());
                    }
                }
            } else {
                self.status_message = Some("No table loaded, nothing to export.".to_owned());
                self.status_message_time = Some(Instant::now());
            }
        } else {
            self.status_message = Some("No result tab open, nothing to export.".to_owned());
            self.status_message_time = Some(Instant::now());
        }
    }
}

pub fn get_downloads_folder() -> PathBuf {
    if let Some(user_dirs) = UserDirs::new() {
        if let Some(dl) = user_dirs.download_dir() {
            return dl.to_path_buf();
        }
        return user_dirs.home_dir().to_path_buf();
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home);
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}