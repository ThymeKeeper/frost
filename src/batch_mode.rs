use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use anyhow::Result;
use crate::workspace::{DbWorkerRequest, DbWorkerResponse, start_db_worker};
use crate::results::{ResultsContent};
use crate::results_export::export_entire_result_set;

pub struct BatchConfig {
    pub sql_file: PathBuf,
    pub output_dir: PathBuf,
    pub output_format: OutputFormat,
    pub connection_string: String,
    pub exit_on_error: bool,
    pub verbose: bool,
    pub last_query_only: bool
}

#[derive(Clone, Copy)]
pub enum OutputFormat {
    Csv,
    Json,
    Text,
    Xlsx,
}

impl OutputFormat {
    pub fn extension(&self) -> &str {
        match self {
            OutputFormat::Csv => "csv",
            OutputFormat::Json => "json",
            OutputFormat::Text => "txt",
            OutputFormat::Xlsx => "xlsx",
        }
    }
}

pub fn run_batch_mode(config: BatchConfig) -> Result<()> {
    if config.verbose {
        println!("Frost Batch Mode");
        println!("==================");
        println!("SQL File: {}", config.sql_file.display());
        println!("Output Directory: {}", config.output_dir.display());
        println!("Output Format: {:?}", config.output_format.extension());
        if config.last_query_only {
            println!("Mode: Last Query Only");
        }
    }

    // Load SQL file
    let sql_content = fs::read_to_string(&config.sql_file)?;
    if config.verbose {
        println!("Loaded {} bytes from SQL file", sql_content.len());
    }

    // Split into queries
    let queries = crate::workspace::Workspace::split_sql(&sql_content)
        .filter(|q| !q.trim().is_empty())
        .collect::<Vec<_>>();
    
    if queries.is_empty() {
        eprintln!("No SQL statements found in file");
        return Ok(());
    }

    if config.verbose {
        println!("Found {} SQL statement(s)", queries.len());
    }

    // Start DB worker
    let (db_req_tx, db_resp_rx, _) = start_db_worker(config.connection_string.clone());

    // Execute queries
    let queries_with_context: Vec<(String, String)> = queries
        .into_iter()
        .enumerate()
        .map(|(i, q)| {
            let context = format!("Query {}", i + 1);
            (q, context)
        })
        .collect();

    let total_queries = queries_with_context.len();
    let _ = db_req_tx.send(DbWorkerRequest::RunQueries(queries_with_context));

    // Process results
    let mut results_count = 0;
    let mut errors_count = 0;
    let start_time = Instant::now();
    let mut last_result: Option<(usize, ResultsContent)> = None;  // Store last result

    while results_count + errors_count < total_queries {
        match db_resp_rx.recv_timeout(Duration::from_secs(300)) {
            Ok(DbWorkerResponse::Connected) => {
                // Connection established, continue
                continue;
            }
            Ok(DbWorkerResponse::QueryStarted { query_idx: _, query_context, .. }) => {
                if config.verbose {
                    println!("\nExecuting {}", query_context);
                }
            }
            Ok(DbWorkerResponse::QueryFinished { query_idx, elapsed, mut result }) => {
                results_count += 1;
                if config.verbose {
                    println!("  Completed in {:?}", elapsed);
                }
                
                if config.last_query_only {
                    // Store the result for later
                    last_result = Some((query_idx, result));
                } else {
                    // Save results immediately
                    if let Err(e) = save_result(&config, query_idx, &mut result) {
                        eprintln!("Error saving results for query {}: {}", query_idx + 1, e);
                        if config.exit_on_error {
                            return Err(e);
                        }
                    }
                }
            }
            Ok(DbWorkerResponse::QueryError { query_idx, elapsed: _, message }) => {
                errors_count += 1;
                eprintln!("Error in query {}: {}", query_idx + 1, message);
                if config.exit_on_error {
                    return Err(anyhow::anyhow!("Query failed: {}", message));
                }
            }
            Err(_) => {
                eprintln!("Timeout waiting for query results");
                return Err(anyhow::anyhow!("Query execution timeout"));
            }
        }
    }

    // If last_query_only, save only the last successful result
    if config.last_query_only {
        if let Some((_query_idx, mut result)) = last_result {
            if let Err(e) = save_result(&config, 0, &mut result) {  // Save as query_001
                eprintln!("Error saving last query result: {}", e);
                if config.exit_on_error {
                    return Err(e);
                }
            }
        }
    }

    let total_elapsed = start_time.elapsed();
    if config.verbose {
        println!("\nBatch execution completed");
        println!("Total time: {:?}", total_elapsed);
        println!("Successful: {}, Failed: {}", results_count, errors_count);
    }

    // Clean shutdown
    let _ = db_req_tx.send(DbWorkerRequest::Quit);
    
    Ok(())
}

// Update save_result to handle XLSX format:
fn save_result(config: &BatchConfig, query_idx: usize, result: &mut ResultsContent) -> Result<()> {
    match result {
        ResultsContent::Table { headers, tile_store } => {
            // Fetch all rows
            let rows = tile_store.get_rows(0, tile_store.nrows)?;
            
            let filename = format!("query_{:03}.{}", 
                query_idx + 1, 
                config.output_format.extension()
            );
            let output_path = config.output_dir.join(filename);
            
            match config.output_format {
                OutputFormat::Csv => {
                    let csv_content = export_entire_result_set(headers, &rows);
                    fs::write(&output_path, csv_content)?;
                }
                OutputFormat::Json => {
                    let json_data = rows_to_json(headers, &rows);
                    fs::write(&output_path, serde_json::to_string_pretty(&json_data)?)?;
                }
                OutputFormat::Text => {
                    let text_content = format_as_table(headers, &rows);
                    fs::write(&output_path, text_content)?;
                }
                OutputFormat::Xlsx => {
                    write_xlsx(&output_path, headers, &rows)?;
                }
            }
            
            if config.verbose {
                println!("  Saved {} rows to {}", rows.len(), output_path.display());
            }
        }
        ResultsContent::Info { message } => {
            if config.verbose {
                println!("  Info: {}", message);
            }
            // Optionally save info messages
            let filename = format!("query_{:03}_info.txt", query_idx + 1);
            let output_path = config.output_dir.join(filename);
            fs::write(&output_path, message)?;
        }
        ResultsContent::Error { message: _, .. } => {
            // Already handled in the response processing
        }
        ResultsContent::Pending => {
            // Should not happen in batch mode
        }
    }
    Ok(())
}

fn write_xlsx(path: &Path, headers: &[String], rows: &[Vec<String>]) -> Result<()> {
    use rust_xlsxwriter::{Workbook, Format};
    
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    
    // Create a bold format for headers
    let bold_format = Format::new().set_bold();
    
    // Write headers with bold format
    for (col, header) in headers.iter().enumerate() {
        worksheet.write_with_format(0, col as u16, header, &bold_format)?;
    }
    
    // Write data rows
    for (row_idx, row) in rows.iter().enumerate() {
        for (col_idx, cell) in row.iter().enumerate() {
            if cell == crate::tile_rowstore::NULL_SENTINEL {
                // Leave NULL cells empty in Excel
                continue;
            }
            // Try to parse as number first
            if let Ok(num) = cell.parse::<f64>() {
                worksheet.write(row_idx as u32 + 1, col_idx as u16, num)?;
            } else {
                worksheet.write(row_idx as u32 + 1, col_idx as u16, cell)?;
            }
        }
    }
    
    // Auto-fit columns (approximate - rust_xlsxwriter calculates this automatically)
    for col in 0..headers.len() {
        worksheet.set_column_width(col as u16, 15.0)?;
    }
    
    workbook.save(path)?;
    Ok(())
}

fn rows_to_json(headers: &[String], rows: &[Vec<String>]) -> serde_json::Value {
    use serde_json::{json, Value, Map};
    
    let mut result = Vec::new();
    for row in rows {
        let mut obj = Map::new();
        for (i, header) in headers.iter().enumerate() {
            let value = row.get(i)
                .map(|v| {
                    if v == crate::tile_rowstore::NULL_SENTINEL {
                        Value::Null
                    } else {
                        Value::String(v.clone())
                    }
                })
                .unwrap_or(Value::Null);
            obj.insert(header.clone(), value);
        }
        result.push(Value::Object(obj));
    }
    json!(result)
}

fn format_as_table(headers: &[String], rows: &[Vec<String>]) -> String {
    use std::cmp::max;
    
    // Calculate column widths
    let mut widths = vec![0; headers.len()];
    for (i, header) in headers.iter().enumerate() {
        widths[i] = header.len();
    }
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                let display_text = if cell == crate::tile_rowstore::NULL_SENTINEL {
                    "NULL"
                } else {
                    cell
                };
                widths[i] = max(widths[i], display_text.len());
            }
        }
    }
    
    // Build table
    let mut output = String::new();
    
    // Header
    for (i, header) in headers.iter().enumerate() {
        if i > 0 { output.push_str(" | "); }
        output.push_str(&format!("{:<width$}", header, width = widths[i]));
    }
    output.push('\n');
    
    // Separator
    for (i, &width) in widths.iter().enumerate() {
        if i > 0 { output.push_str("-+-"); }
        output.push_str(&"-".repeat(width));
    }
    output.push('\n');
    
    // Rows
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i > 0 { output.push_str(" | "); }
            let display_text = if cell == crate::tile_rowstore::NULL_SENTINEL {
                "NULL"
            } else {
                cell
            };
            output.push_str(&format!("{:<width$}", display_text, width = widths[i]));
        }
        output.push('\n');
    }
    
    output
}