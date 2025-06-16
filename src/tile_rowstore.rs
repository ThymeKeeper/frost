use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write, Seek, SeekFrom,BufWriter},
    sync::Arc,
};
use tempfile::NamedTempFile;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use lru::LruCache;


pub const NULL_SENTINEL: &str = "<Frost-NULL>";

/// Number of rows per tile (can be made configurable)
pub const TILE_SIZE: usize = 1_000;

/// Magic header for file sanity
const MAGIC: &[u8; 4] = b"SNTR";

//------- TileRowStore definition --------
#[derive(Debug)]
pub struct TileRowStore {
    /// Temp file (auto cleaned up)
    temp_file: Option<NamedTempFile>,
    /// We need a persistent file handle for reading (can be reopened by path if needed)
    file: std::io::BufReader<File>,
    /// Offsets of each tile block
    tile_offsets: Vec<u64>,
    /// Row count for each tile (last tile may be short)
    tile_row_counts: Vec<u32>,
    /// Total cols, total rows
    pub ncols: usize,
    pub nrows: usize,
    /// Tile LRU: tile index -> Arc<Vec<Vec<String>>>
    cache: LruCache<usize, Arc<Vec<Vec<String>>>>,
    /// Always hold first/last tile in memory
    first_tile: Option<Arc<Vec<Vec<String>>>>,
    last_tile: Option<Arc<Vec<Vec<String>>>>,
}

impl TileRowStore {
    /// Write entire rowset from an iterator, with column count
    /// Returns: (headers, store)

    pub fn prefetch_for_view(&mut self, view_row: usize, max_rows: usize) {
        let tile_count = self.tile_offsets.len();
        if tile_count == 0 { return; }
        let start_tile = view_row / TILE_SIZE;
        let end_tile = (view_row + max_rows - 1) / TILE_SIZE;
        // One neighbor below and above, bounded by tile_count.
        let prefetch_start = start_tile.saturating_sub(1);
        let prefetch_end = (end_tile+1).min(tile_count-1);
        for t in prefetch_start..=prefetch_end {
            let _ = self.load_tile_arc(t);
        }
    }

    pub fn from_rows<I>(
        headers: &[String],
        rows_iter: I,
    ) -> io::Result<Self>
    where
        I: Iterator<Item = Vec<String>>,
    {
        let mut temp_file = NamedTempFile::new()?;
        let mut file = BufWriter::with_capacity(256 * 1024, temp_file.as_file_mut());
        // Write header
        file.write_all(MAGIC)?;
        file.write_u32::<LittleEndian>(TILE_SIZE as u32)?;
        file.write_u32::<LittleEndian>(headers.len() as u32)?;
        // Placeholders:
        let row_count_pos = file.stream_position()?;
        file.write_u32::<LittleEndian>(0)?;
        let tile_count_pos = file.stream_position()?;
        file.write_u32::<LittleEndian>(0)?;

        // Tiles:
        let mut tile_offsets: Vec<u64> = Vec::new();
        let mut tile_row_counts: Vec<u32> = Vec::new();
        let mut buf_tile: Vec<Vec<String>> = Vec::with_capacity(TILE_SIZE);

        let mut nrows = 0usize;
        for row in rows_iter {
            buf_tile.push(row);
            nrows += 1;
            if buf_tile.len() == TILE_SIZE {
                tile_offsets.push(file.stream_position()?);
                Self::write_tile(&mut file, &buf_tile)?;
                tile_row_counts.push(buf_tile.len() as u32);
                buf_tile.clear();
            }
        }

        // Write last (possibly short) tile
        if !buf_tile.is_empty() {
            tile_offsets.push(file.stream_position()?);
            Self::write_tile(&mut file, &buf_tile)?;
            tile_row_counts.push(buf_tile.len() as u32);
            buf_tile.clear();
        }

        // After data, write the tile offset table:
        let _tile_table_pos = file.stream_position()?;
        for &offset in &tile_offsets {
            file.write_u64::<LittleEndian>(offset)?;
        }
        for &row_count in &tile_row_counts {
            file.write_u32::<LittleEndian>(row_count)?;
        }

        // Patch row count / tile count
        file.seek(SeekFrom::Start(row_count_pos))?;
        file.write_u32::<LittleEndian>(nrows as u32)?;
        file.seek(SeekFrom::Start(tile_count_pos))?;
        file.write_u32::<LittleEndian>(tile_offsets.len() as u32)?;

        // Now re-open as read handle (flush+read)
        drop(file);
        let temp_file_read = OpenOptions::new()
            .read(true)
            .write(true)
            .open(temp_file.path())?;

        let buf_reader = std::io::BufReader::with_capacity(256 * 1024, temp_file_read);


        let mut store = TileRowStore {
            temp_file: Some(temp_file),
            file: buf_reader,
            tile_offsets,
            tile_row_counts,
            ncols: headers.len(),
            nrows,
            cache: LruCache::new(std::num::NonZeroUsize::new(6).unwrap()), // cache size of 6
            first_tile: None,
            last_tile: None,
        };

        // Preload first/last tiles
        if !store.tile_offsets.is_empty() {
            store.first_tile = store.load_tile_arc(0).ok();
            store.last_tile = store.load_tile_arc(store.tile_offsets.len() - 1).ok();
        }

        Ok(store)
    }

    /// Write a full tile (rows) in format:
    /// [row count: u32][col count: u32] then, for row in rows, col in row: [u32(len)][bytes]
    fn write_tile<W: Write>(file: &mut W, rows: &[Vec<String>]) -> io::Result<()> {
        file.write_u32::<LittleEndian>(rows.len() as u32)?;
        file.write_u32::<LittleEndian>(if rows.is_empty() { 0 } else { rows[0].len() as u32 })?;
        for row in rows {
            for col in row {
                let bytes = col.as_bytes();
                file.write_u32::<LittleEndian>(bytes.len() as u32)?;
                file.write_all(bytes)?;
            }
        }
        Ok(())
    }

    /// Loads an Arc'd tile from file (by tile index)
    fn load_tile_arc(&mut self, idx: usize) -> io::Result<Arc<Vec<Vec<String>>>> {
        let offset = *self.tile_offsets.get(idx)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "tile idx out of range"))?;
        self.file.seek(SeekFrom::Start(offset))?;

        let row_count = self.file.read_u32::<LittleEndian>()? as usize;
        let col_count = self.file.read_u32::<LittleEndian>()? as usize;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let mut row = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                let len = self.file.read_u32::<LittleEndian>()? as usize;
                let mut buf = vec![0u8; len];
                self.file.read_exact(&mut buf)?;
                row.push(String::from_utf8_lossy(&buf).to_string());
            }
            rows.push(row);
        }
        Ok(Arc::new(rows))
    }

    /// Fetches rows from start..(start+count).
    /// Rapidly loads tile(s), caches them, always holds first/last tiles.
    pub fn get_rows(&mut self, start: usize, count: usize) -> io::Result<Vec<Vec<String>>> {
        if start >= self.nrows || count == 0 {
            return Ok(Vec::new());
        }
        let end = usize::min(self.nrows, start+count);
        let mut result = Vec::with_capacity(end-start);
        let mut curr = start;
        while curr < end {
            let tile_idx = curr / TILE_SIZE;
            let in_tile = curr % TILE_SIZE;
            let tile = if tile_idx == 0 {
                self.first_tile.as_ref().cloned()
                    .or_else(|| self.load_tile_arc(0).ok())
            } else if tile_idx == self.tile_offsets.len()-1 {
                self.last_tile.as_ref().cloned()
                    .or_else(|| self.load_tile_arc(tile_idx).ok())
            } else {
                if let Some(t) = self.cache.get(&tile_idx) {
                    Some(t.clone())
                } else {
                    let t = self.load_tile_arc(tile_idx)?;
                    self.cache.put(tile_idx, t.clone());
                    Some(t)
                }
            }.ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to load tile"))?;
            let end_in_tile = usize::min(tile.len(), in_tile + (end-curr));
            for row in &tile[in_tile..end_in_tile] {
                result.push(row.clone());
            }
            curr += end_in_tile - in_tile;
        }
        Ok(result)
    }
}

/// To allow ResultsTab or tile cache to auto-clean up temp files:
impl Drop for TileRowStore {
    fn drop(&mut self) {
        // NamedTempFile's Drop will remove the file
        // file is auto-closed
    }
}