//! src/main.rs – Frost launcher with bracketed-paste, dynamic title, and batch mode support
mod editor;
mod results;
mod results_selection;
mod results_export;
mod workspace;
mod tile_rowstore;
mod syntax;
mod palette;
mod autocomplete;
mod config;
mod batch_mode;
mod batch_generator;
mod schema_cache;
mod db_tree;
mod db_navigator;

use std::path::PathBuf;
use std::process;
use std::sync::{Arc, Mutex, OnceLock};
use std::{
    io::{self, Write},
    mem::size_of,
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
use batch_mode::{BatchConfig, OutputFormat};
use editor::normalize_text_for_terminal;

use windows_sys::Win32::{
    Foundation::{GetLastError, HANDLE, INVALID_HANDLE_VALUE},
    Graphics::Gdi::{FW_NORMAL, TMPF_TRUETYPE},
    System::Console::{
        CONSOLE_FONT_INFOEX, GetStdHandle, SetConsoleTitleW, SetCurrentConsoleFontEx,
        STD_OUTPUT_HANDLE,
    },
};

/* ─── modules / crates ─── */
use crate::workspace::Workspace;
use crossterm::event::Event;
use tui::{backend::CrosstermBackend, Terminal};

/* ───── NEW: console-close event support ────────────────────────── */
#[cfg(windows)]
use windows_sys::Win32::System::Console::{SetConsoleCtrlHandler, CTRL_CLOSE_EVENT};
#[cfg(windows)]
use windows_sys::Win32::Foundation::BOOL;

// ───── Unix-only imports ────────────────────────────────────────────
#[cfg(unix)]
use signal_hook::iterator::Signals;
#[cfg(unix)]
use libc::{SIGHUP, SIGTERM};
#[cfg(unix)]
use std::thread;

/*──────────────────────── CLI structures ──────────────────────*/
#[derive(Parser)]
#[command(name = "Frost")]
#[command(about = "A TUI SQL IDE for Snowflake", long_about = None)]
struct Cli {
    /// SQL file to open (interactive mode)
    #[arg(value_name = "FILE")]
    file: Option<PathBuf>,
    
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run in batch mode (non-interactive)
    Batch {
        /// SQL file to execute
        #[arg(short, long, value_name = "FILE")]
        sql_file: PathBuf,
        
        /// Output directory for results
        #[arg(short, long, value_name = "DIR")]
        output_dir: PathBuf,
        
        /// Output format (csv, json, txt, xlsx)
        #[arg(short, long, default_value = "csv")]
        format: String,
        
        /// Exit immediately on first error
        #[arg(short, long)]
        exit_on_error: bool,
        
        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
        
        /// Save only the last query result
        #[arg(short = 'l', long)]
        last_query_only: bool,
    },
}

/*──────────────────────── constants ────────────────────────────*/
const LF_FACESIZE: usize = 32;


/*──────────────────────── helpers ──────────────────────────────*/
/// Change the **current** console font (Windows only).
pub fn set_console_font(face: &[u16], height_px: i16) -> anyhow::Result<()> {
    unsafe {
        let h: HANDLE = GetStdHandle(STD_OUTPUT_HANDLE);
        if h == INVALID_HANDLE_VALUE {
            return Err(anyhow::anyhow!("GetStdHandle failed"));
        }

        let mut info: CONSOLE_FONT_INFOEX = std::mem::zeroed();
        info.cbSize = size_of::<CONSOLE_FONT_INFOEX>() as u32;
        info.nFont = 0;
        info.dwFontSize.Y = height_px;
        info.dwFontSize.X = 0;
        info.FontFamily = TMPF_TRUETYPE as u32;
        info.FontWeight = FW_NORMAL as u32;

        let len = face.len().min(LF_FACESIZE - 1);
        info.FaceName[..len].copy_from_slice(&face[..len]);

        if SetCurrentConsoleFontEx(h, 0, &info) == 0 {
            return Err(anyhow::anyhow!(
                "SetCurrentConsoleFontEx failed ({})",
                GetLastError()
            ));
        }
    }
    Ok(())
}

/// Set console-window title (UTF-16)
fn set_console_title(title: &str) {
    use std::ffi::OsStr;
    use std::os::windows::prelude::*;
    let wide: Vec<u16> = OsStr::new(title)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    unsafe { SetConsoleTitleW(wide.as_ptr()) };
}

/*──────────────────────── main ────────────────────────────────*/
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    /* Load configuration */
    let config_result = crate::config::Config::load();
    
    // Handle batch mode
    if let Some(Commands::Batch { sql_file, output_dir, format, exit_on_error, verbose, last_query_only }) = cli.command {
        let config = config_result?; // For batch mode, we need valid config
        let output_format = match format.as_str() {
            "json" => OutputFormat::Json,
            "txt" | "text" => OutputFormat::Text,
            "xlsx" | "excel" => OutputFormat::Xlsx,
            _ => OutputFormat::Csv,
        };
        
        let batch_config = BatchConfig {
            sql_file,
            output_dir,
            output_format,
            connection_string: config.connection_string,
            exit_on_error,
            verbose,
            last_query_only,
        };
        
        return batch_mode::run_batch_mode(batch_config);
    }
    
    // Interactive mode - handle config error gracefully
    let (config, config_error) = match config_result {
        Ok(cfg) => (cfg, None),
        Err(e) => {
            // Create a default config with empty connection string
            let default_config = crate::config::Config {
                connection_string: String::new(),
                colors: crate::config::ColorConfig::default(),
            };
            (default_config, Some(e.to_string()))
        }
    };
    
    run_interactive_mode(config, cli.file, config_error)
}

fn run_interactive_mode(config: crate::config::Config, file_arg: Option<PathBuf>, config_error: Option<String>) -> anyhow::Result<()> {
    /* ①  pick a monospace font */
    const CONSOLAS_U16: [u16; 9] =
        [0x0043, 0x006f, 0x006e, 0x0073, 0x006f, 0x006c, 0x0061, 0x0073, 0];
    set_console_font(&CONSOLAS_U16, 20).expect("font change failed");

    /* ②  palette guard (noop outside Windows) */
    #[cfg(windows)]
    let _guard = palette::apply_palette()?; // keep guard alive

    /* ③  Workspace + optional file load */
    let mut workspace = Workspace::new(config.connection_string)?;
    // Set initial status message if config had an error
    if let Some(error_msg) = config_error {
        workspace.status_message = Some(error_msg);
        workspace.status_message_time = Some(Instant::now());
    }

    /* ───── Signal handlers for graceful shutdown ───── */
    let should_exit = Arc::new(Mutex::new(false));
    let exit_clone = Arc::clone(&should_exit);
    
    ctrlc::set_handler(move || {
        *exit_clone.lock().unwrap() = true;
    })?;
    
    #[cfg(windows)]
    {
        // Handle console close events on Windows
        let exit_clone2 = Arc::clone(&should_exit);
        unsafe {
            SetConsoleCtrlHandler(Some(console_handler), 1);
        }
        
        unsafe extern "system" fn console_handler(ctrl_type: u32) -> BOOL {
            match ctrl_type {
                CTRL_CLOSE_EVENT => {
                    // Don't exit immediately - let the main loop handle it
                    1  // Return TRUE to indicate we handled it
                }
                _ => 0,
            }
        }
    }

    /* ───── open file if CLI arg given ───── */
    if let Some(path) = file_arg {
        if let Err(e) = workspace.load_file(path.clone()) {
            workspace.status_message = Some(format!("Failed to load file: {}", e));
            workspace.status_message_time = Some(Instant::now());
        }
    } else {
        set_console_title("Frost");
    }

    /* ④  Crossterm / TUI init */
    crossterm::terminal::enable_raw_mode()?;
    if cfg!(windows) {
        print!("\x1b[?2004h");  // Force bracketed paste on
        io::stdout().flush().ok();
    }
    let mut stdout = io::stdout();
    crossterm::execute!(
        stdout,
        crossterm::terminal::EnterAlternateScreen,
        crossterm::event::EnableBracketedPaste,
        crossterm::event::EnableMouseCapture
    )?;
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;

    /* ⑤  event/render loop */
    let anim_tick = Duration::from_millis(100);
    let timer_update = Duration::from_millis(333);
    let full_refresh_interval = Duration::from_millis(100);  // Full refresh every 500ms
    let mut last_anim = Instant::now();
    let mut last_timer = Instant::now();
    let mut last_draw = Instant::now();
    let mut last_full_refresh = Instant::now();
    let mut dirty = true;

    'main: loop {
        // Check if exit was requested via signal
        if *should_exit.lock().unwrap() {
            *should_exit.lock().unwrap() = false;  // Reset flag
            if workspace.request_exit() {
                break 'main;
            }
            dirty = true;  // Show exit dialog
        }
        if workspace.poll_db_responses() {
            dirty = true;
        }

        let timeout = if workspace.running { timer_update } else { anim_tick }
            .saturating_sub(last_anim.elapsed());

        if crossterm::event::poll(timeout)? {
            match crossterm::event::read()? {
                Event::Key(k) if workspace.handle_key(k)? => break 'main, // Ctrl-Q
                Event::Key(_) => dirty = true,
                Event::Mouse(m) => {
                    workspace.handle_mouse(m);
                    dirty = true;
                }
                Event::Paste(s) => {
                    workspace.editor.handle_paste(&s);
                    dirty = true;
                }
                Event::Resize(_, _) => dirty = true,
                _ => {}
            }
        }

        workspace.update();
        if workspace.running && last_timer.elapsed() >= timer_update {
            last_timer = Instant::now();
            dirty = true;
        }

        if dirty && last_draw.elapsed() >= Duration::from_millis(15) {
            workspace.render(&mut term)?;
            last_draw = Instant::now();
            dirty = false;
        }

        // Force a full clear and redraw every 500ms to clear any stray messages
        if last_full_refresh.elapsed() >= full_refresh_interval {
            dirty = true;  // Just mark as dirty to trigger normal render
            last_full_refresh = Instant::now();
        }

        if last_anim.elapsed() >= anim_tick {
            last_anim = Instant::now();
        }
    }

    let mut out = io::stdout();
    crossterm::queue!(
        out,
        crossterm::event::DisableMouseCapture,
        crossterm::event::DisableBracketedPaste,
        crossterm::terminal::LeaveAlternateScreen
    )?;
    out.flush()?;
    crossterm::terminal::disable_raw_mode()?;
    Ok(())
}