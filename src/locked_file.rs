// src/locked_file.rs
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct LockedFile(pub File);

impl LockedFile {
    pub fn open_exclusive(path: &Path) -> std::io::Result<(Self, String)> {
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::FILE_SHARE_NONE;

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .share_mode(FILE_SHARE_NONE)      // hard lock
                .open(path)?;

            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            Ok((Self(file), buf))
        }

        #[cfg(unix)]
        {
            use fs2::FileExt;                      // add fs2 below
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            file.lock_exclusive()?;                // advisory lock
            let mut buf = String::new();
            File::read_to_string(&file, &mut buf)?;
            Ok((Self(file), buf))
        }
    }

    pub fn save_and_unlock(&mut self, data: &str) -> std::io::Result<()> {
        self.0.seek(SeekFrom::Start(0))?;
        self.0.set_len(0)?;
        self.0.write_all(data.as_bytes())?;
        self.0.sync_data()?;
        Ok(())
    }
}
