//! Simple “safety-net” autosave helper.
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub struct Autosave {
    file_path:    Option<PathBuf>,
    last_autosave: Instant,
    idle_timeout:  Duration,
    edit_limit:    usize,
    edit_counter:  usize,
}

impl Autosave {
    pub fn new(path: Option<PathBuf>) -> Self {
        Self {
            file_path:    path,
            last_autosave: Instant::now(),
            idle_timeout:  Duration::from_secs(30), // ① idle-time trigger
            edit_limit:    150,                     // ② edit-count trigger
            edit_counter:  0,
        }
    }

    pub fn set_path(&mut self, p: PathBuf) {
        self.file_path     = Some(p);
        self.last_autosave = Instant::now();
        self.edit_counter  = 0;
    }

    /// Call every time the buffer mutates.
    pub fn notify_edit(&mut self) {
        self.edit_counter += 1;
    }

    /// Tick once per frame; returns a status-bar message on write / error.
    pub fn maybe_flush(
        &mut self,
        buffer: &str,
        last_edit: Option<Instant>,
    ) -> Option<String> {
        let Some(path) = &self.file_path else { return None };

        let need_time  = last_edit
            .map(|t| t.elapsed() >= self.idle_timeout)
            .unwrap_or(false);
        let need_count = self.edit_counter >= self.edit_limit;

        if !(need_time || need_count) {
            return None;
        }

        let target = Self::autosave_path(path);
        let res = std::fs::write(&target, buffer);

        self.last_autosave = Instant::now();
        self.edit_counter  = 0;

        Some(match res {
            Ok(_)  => format!("Autosaved → {}", target.display()),
            Err(e) => format!("Autosave failed: {e}"),
        })
    }

    /// Flush unconditionally (used on forced shutdown).
    pub fn force_flush(&self, buffer: &str) {
        if let Some(p) = &self.file_path {
            let _ = std::fs::write(Self::autosave_path(p), buffer);
        }
    }

    /// Remove the companion file after a clean save.
    pub fn clear(&self) {
        if let Some(p) = &self.file_path {
            let _ = std::fs::remove_file(Self::autosave_path(p));
        }
    }

    pub fn autosave_path(p: &Path) -> PathBuf {
        let mut s = p.as_os_str().to_owned();
        s.push(".autosave");
        PathBuf::from(s)
    }
}
