use std::path::{Path, PathBuf};
use std::fs;
use anyhow::Result;
use copypasta::{ClipboardContext, ClipboardProvider};

pub struct BatchScriptConfig {
    pub sql_file: PathBuf,
    pub output_dir: PathBuf,
    pub output_format: String,
    pub Frost_exe: PathBuf,
    pub exit_on_error: bool,
    pub verbose: bool,
    pub last_query_only: bool,
    pub auto_dismiss: bool,
}

impl BatchScriptConfig {
    pub fn new(sql_file: PathBuf) -> Self {
        let output_dir = sql_file.parent()
            .unwrap_or(Path::new("."))
            .join("results");
        
        Self {
            sql_file,
            output_dir,
            output_format: "csv".to_string(),
            Frost_exe: std::env::current_exe().unwrap_or_else(|_| PathBuf::from("Frost")),
            exit_on_error: true,
            verbose: true,
            last_query_only: true,
            auto_dismiss: false,    // Add this field
        }
    }
}

pub fn generate_batch_script(config: &BatchScriptConfig) -> Result<(PathBuf, String)> {
    let script_content = if cfg!(windows) {
        generate_windows_batch(config)
    } else {
        generate_unix_script(config)
    };
    
    let script_extension = if cfg!(windows) { "bat" } else { "sh" };
    let script_name = config.sql_file
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("Frost_batch");
    
    let script_path = config.sql_file
        .parent()
        .unwrap_or(Path::new("."))
        .join(format!("{}_batch.{}", script_name, script_extension));
    
    Ok((script_path, script_content))
}

fn generate_windows_batch(config: &BatchScriptConfig) -> String {
    let mut script = String::new();
    
    script.push_str("@echo off\n");
    script.push_str("REM Frost Batch Script\n");
    script.push_str(&format!("REM Generated for: {}\n", config.sql_file.display()));
    script.push_str(&format!("REM Generated at: {}\n\n", chrono::Local::now().format("%Y-%m-%d %H:%M:%S")));
    
    // Create output directory
    script.push_str(&format!("if not exist \"{}\" mkdir \"{}\"\n\n", 
        config.output_dir.display(), 
        config.output_dir.display()
    ));
    
    // Build command
    script.push_str(&format!("\"{}\" batch ", config.Frost_exe.display()));
    script.push_str(&format!("--sql-file \"{}\" ", config.sql_file.display()));
    script.push_str(&format!("--output-dir \"{}\" ", config.output_dir.display()));
    script.push_str(&format!("--format {} ", config.output_format));
    
    if config.exit_on_error {
        script.push_str("--exit-on-error ");
    }
    if config.verbose {
        script.push_str("--verbose ");
    }
    if config.last_query_only {
        script.push_str("--last-query-only ");
    }
    
    script.push_str("\n\n");
    
    // Error handling
    script.push_str("if %ERRORLEVEL% neq 0 (\n");
    script.push_str("    echo Error: Frost batch execution failed with error code %ERRORLEVEL%\n");
    if !config.auto_dismiss {
        script.push_str("    pause\n");
    }
    script.push_str("    exit /b %ERRORLEVEL%\n");
    script.push_str(")\n\n");
    
    script.push_str("echo Batch execution completed successfully\n");
    if !config.auto_dismiss {
        script.push_str("pause\n");
    }
    
    script
}

fn generate_unix_script(config: &BatchScriptConfig) -> String {
    let mut script = String::new();
    
    script.push_str("#!/bin/bash\n");
    script.push_str("# Frost Batch Script\n");
    script.push_str(&format!("# Generated for: {}\n", config.sql_file.display()));
    script.push_str(&format!("# Generated at: {}\n\n", chrono::Local::now().format("%Y-%m-%d %H:%M:%S")));
    
    // Create output directory
    script.push_str(&format!("mkdir -p \"{}\"\n\n", config.output_dir.display()));
    
    // Build command
    script.push_str(&format!("\"{}\" batch \\\n", config.Frost_exe.display()));
    script.push_str(&format!("    --sql-file \"{}\" \\\n", config.sql_file.display()));
    script.push_str(&format!("    --output-dir \"{}\" \\\n", config.output_dir.display()));
    script.push_str(&format!("    --format {} ", config.output_format));
    
    if config.exit_on_error {
        script.push_str("\\\n    --exit-on-error ");
    }
    if config.verbose {
        script.push_str("\\\n    --verbose ");
    }
    if config.last_query_only {
        script.push_str("\\\n    --last-query-only ");
    }
    
    script.push_str("\n\n");
    
    // Error handling
    script.push_str("if [ $? -ne 0 ]; then\n");
    script.push_str("    echo \"Error: Frost batch execution failed\"\n");
    if !config.auto_dismiss {
        script.push_str("    read -p \"Press Enter to continue...\"\n");
    }
    script.push_str("    exit 1\n");
    script.push_str("fi\n\n");
    
    script.push_str("echo \"Batch execution completed successfully\"\n");
    if !config.auto_dismiss {
        script.push_str("read -p \"Press Enter to continue...\"\n");
    }
    
    script
}

// Dialog for generating batch scripts from the UI
pub struct BatchGeneratorDialog {
    pub active: bool,
    pub sql_file: PathBuf,
    pub output_dir: String,
    pub format_index: usize,
    pub exit_on_error: bool,
    pub verbose: bool,
    pub last_query_only: bool,
    pub auto_dismiss: bool,
    pub field_index: usize,
    pub message: Option<String>,
    clipboard: ClipboardContext,
}

impl BatchGeneratorDialog {
    pub fn new(sql_file: PathBuf) -> Self {
        let default_output = sql_file.parent()
            .unwrap_or(Path::new("."))
            .join("results")
            .to_string_lossy()
            .to_string();
        
        Self {
            active: true,
            sql_file,
            output_dir: default_output,
            format_index: 0, // CSV
            exit_on_error: true,
            verbose: true,
            last_query_only: true,   // Default to true as requested
            auto_dismiss: false,     // Add this field
            field_index: 0,
            message: None,
            clipboard: ClipboardContext::new().unwrap(),
        }
    }
    
    pub fn formats() -> &'static [&'static str] {
        &["csv", "json", "txt", "xlsx"]  // Added xlsx
    }
    
    pub fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> Option<BatchScriptConfig> {
        use crossterm::event::{KeyCode, KeyModifiers, KeyEventKind};
        
        // Only handle Press events to avoid double-processing
        if key.kind != KeyEventKind::Press {
            return None;
        }
        
        match key.code {
            KeyCode::Esc | KeyCode::F(10) => {
                self.active = false;
                None
            }
            KeyCode::Tab | KeyCode::Down => {
                self.field_index = (self.field_index + 1) % 6;  // Now 6 fields
                None
            }
            KeyCode::BackTab | KeyCode::Up => {
                self.field_index = if self.field_index == 0 { 5 } else { self.field_index - 1 };
                None
            }
            KeyCode::Left => {
                if self.field_index == 1 { // Format field
                    self.format_index = if self.format_index == 0 { 
                        Self::formats().len() - 1 
                    } else { 
                        self.format_index - 1 
                    };
                }
                None
            }
            KeyCode::Right => {
                if self.field_index == 1 { // Format field
                    self.format_index = (self.format_index + 1) % Self::formats().len();
                }
                None
            }
            KeyCode::Char(' ') if self.field_index >= 2 && self.field_index <= 5 => {
                match self.field_index {
                    2 => self.exit_on_error = !self.exit_on_error,
                    3 => self.verbose = !self.verbose,
                    4 => self.last_query_only = !self.last_query_only,
                    5 => self.auto_dismiss = !self.auto_dismiss,
                    _ => {}
                }
                None
            }
            KeyCode::Char('v') if key.modifiers.contains(KeyModifiers::CONTROL) && self.field_index == 0 => {
                // Handle paste - replace entire content
                if let Ok(content) = self.clipboard.get_contents() {
                    self.output_dir = content.trim().to_string();
                }
                None
            }
            KeyCode::Char(c) if self.field_index == 0 && !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.output_dir.push(c);
                None
            }
            KeyCode::Backspace if self.field_index == 0 => {
                self.output_dir.pop();
                None
            }
            KeyCode::Enter => {
                // Generate script
                let output_dir = PathBuf::from(&self.output_dir);
                
                let config = BatchScriptConfig {
                    sql_file: self.sql_file.clone(),
                    output_dir,
                    output_format: Self::formats()[self.format_index].to_string(),
                    Frost_exe: std::env::current_exe().unwrap_or_else(|_| PathBuf::from("Frost")),
                    exit_on_error: self.exit_on_error,
                    verbose: self.verbose,
                    last_query_only: self.last_query_only,
                    auto_dismiss: self.auto_dismiss,
                };
                
                match generate_batch_script(&config) {
                    Ok((path, content)) => {
                        match fs::write(&path, content) {
                            Ok(_) => {
                                // Make executable on Unix
                                #[cfg(unix)]
                                {
                                    use std::os::unix::fs::PermissionsExt;
                                    if let Ok(metadata) = fs::metadata(&path) {
                                        let mut perms = metadata.permissions();
                                        perms.set_mode(0o755);
                                        let _ = fs::set_permissions(&path, perms);
                                    }
                                }
                                
                                self.message = Some(format!("Batch script saved to: {}", path.display()));
                                self.active = false;
                                Some(config)
                            }
                            Err(e) => {
                                self.message = Some(format!("Error saving script: {}", e));
                                None
                            }
                        }
                    }
                    Err(e) => {
                        self.message = Some(format!("Error generating script: {}", e));
                        None
                    }
                }
            }
            _ => None
        }
    }
}