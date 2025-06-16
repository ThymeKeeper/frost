use std::path::{Path, PathBuf};
use std::fs;
use std::io;
use directories::UserDirs;

/// Returns the user's Downloads directory, or their Home directory if not available.
/// Falls back to current directory if all else fails.
pub fn get_downloads_folder() -> PathBuf {
    if let Some(user_dirs) = UserDirs::new() {
        if let Some(dl) = user_dirs.download_dir() {
            return dl.to_path_buf();
        }
        // fallback: use home dir
        return user_dirs.home_dir().to_path_buf();
    }
    // fallback: env home
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home);
    }
    // fallback: current directory
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Save a file named `export_filename` (example: "results_export.csv") in downloads folder.
/// Data is a UTF-8 string.
pub fn save_export_to_downloads(export_filename: &str, data: &str) -> io::Result<PathBuf> {
    let mut target = get_downloads_folder();
    target.push(export_filename);

    // Write the data to disk
    fs::write(&target, data)?;
    Ok(target)
}