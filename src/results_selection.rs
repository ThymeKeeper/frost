//! Selection/state/render/summary logic for Results table pane

use tui::style::{Style, Modifier};
use crate::palette::STYLE;
use crate::palette::KANAGAWA as k;   // needed for grey "NULL" style
use crate::palette::{rgb, CONFIG_COLORS};

use tui::{
    backend::Backend, Frame,
    text::*, widgets::*,
};

use tui::layout::Rect as UiRect;

use crossterm::event::{
    KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind, KeyEventKind,
};
use std::cmp::{max, min};
use std::collections::HashMap;
use copypasta::ClipboardProvider;

use crate::results::{Results, ResultsContent, ScrollDirection};
use crate::results_export::copy_selection;

/// Column width limits
const MIN_COL_WIDTH: u16 = 8;
const MAX_COL_WIDTH: u16 = 50;
const INDEX_COL_WIDTH: u16 = 10;

#[derive(Debug)]
struct WrappedLine {
    start: usize,      // Byte offset in original string
    end: usize,        // Byte offset in original string
    char_start: usize, // Character position in original string
    char_end: usize,   // Character position in original string
}

fn wrap_text(text: &str, max_width: usize) -> Vec<WrappedLine> {
    let mut lines = Vec::new();
    let mut current_line_start = 0;
    let mut current_line_width = 0;
    let mut char_pos = 0;
    let mut last_space_byte = None;
    let mut last_space_char = None;
    let mut line_char_start = 0;
    
    let mut i = 0;
    let bytes = text.as_bytes();
    
    while i < bytes.len() {
        let ch_bytes = if bytes[i] < 128 {
            1
        } else if bytes[i] < 224 {
            2
        } else if bytes[i] < 240 {
            3
        } else {
            4
        };
        
        let ch = std::str::from_utf8(&bytes[i..i + ch_bytes]).unwrap_or(" ").chars().next().unwrap_or(' ');
        
        if ch == ' ' {
            last_space_byte = Some(i);
            last_space_char = Some(char_pos);
        }
        
        if ch == '\n' || current_line_width >= max_width {
            let (line_end_byte, line_end_char) = if ch == '\n' {
                (i, char_pos)
            } else if let (Some(space_byte), Some(space_char)) = (last_space_byte, last_space_char) {
                // Break at last space
                (space_byte, space_char)
            } else {
                // Break at current position
                (i, char_pos)
            };
            
            lines.push(WrappedLine {
                start: current_line_start,
                end: line_end_byte,
                char_start: line_char_start,
                char_end: line_end_char,
            });
            
            if ch == '\n' {
                current_line_start = i + 1;
                line_char_start = char_pos + 1;
            } else if line_end_byte < i {
                // We broke at a space, skip the space
                current_line_start = line_end_byte + 1;
                line_char_start = line_end_char + 1;
            } else {
                current_line_start = i;
                line_char_start = char_pos;
            }
            
            current_line_width = 0;
            last_space_byte = None;
            last_space_char = None;
            
            if ch == '\n' {
                i += 1;
                char_pos += 1;
                continue;
            }
        }
        
        current_line_width += 1;
        i += ch_bytes;
        char_pos += 1;
    }
    
    // Don't forget the last line
    if current_line_start < bytes.len() {
        lines.push(WrappedLine {
            start: current_line_start,
            end: bytes.len(),
            char_start: line_char_start,
            char_end: char_pos,
        });
    }
    
    lines
}

/// Pretty-prints a floating number with
///   • adaptive #decimals (big → fewer, tiny → more)
///   • naive repeating-decimal notation using a Unicode overline
fn fmt_num(n: f64) -> String {
    // 1️⃣  choose decimals by magnitude
    let abs = n.abs();
    let decs = if abs >= 1_000.0 { 2 }       // big → 0-2 dp
               else if abs >= 1.0 { 4 }      // mid → 4 dp
               else { 8 };                   // tiny → up to 8 dp
    let mut s = format!("{:.*}", decs, n);

    // 2️⃣  trim trailing zeros + lone dot
    if let Some(_dot) = s.find('.') {
        while s.ends_with('0') { s.pop(); }
        if s.ends_with('.') { s.pop(); }
    }

    // 3️⃣  very simple repeating pattern detection (period ≤3, ≥6 repeats)
    if let Some(dot) = s.find('.') {
        let dec = &s[dot + 1..];
        for period in 1..=3 {
            if dec.len() < period * 6 { continue; }
            let pat = &dec[..period];
            if dec.as_bytes().chunks(period).all(|c| c == pat.as_bytes()) {
                // build e.g. "0.3̅"
                let mut out = s[..=dot].to_string();
                for ch in pat.chars() {
                    out.push(ch);
                    out.push('\u{0305}'); // combining overline
                }
                return out;
            }
        }
    }
    s
}

#[derive(Clone, Debug)]
pub struct FindMatch {
    pub row: usize,
    pub col: usize,
    pub is_header: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectionKind {
    Rect,
    FullRowSet { anchor: usize, cursor: usize },
    FullColSet { anchor: usize, cursor: usize },
    FullRowVec(Vec<usize>),
    FullColVec(Vec<usize>),
    None,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultSelection {
    pub kind: SelectionKind,
    pub anchor: Option<(usize, usize)>,
    pub cursor: Option<(usize, usize)>,
}

/// Column width information
#[derive(Clone, Debug)]
pub struct ColumnWidths {
    /// Width for each column (including index column at position 0)
    pub widths: Vec<u16>,
}

/// Information about visible columns with partial rendering
#[derive(Clone, Debug)]
pub struct VisibleColumns {
    /// Starting X offset in characters (for partial first column)
    pub start_offset: u16,
    /// List of (column_index, x_position, visible_width, full_width)
    pub columns: Vec<(usize, u16, u16, u16)>,
}

impl ColumnWidths {
    pub fn new() -> Self {
        Self {
            widths: vec![INDEX_COL_WIDTH],
        }
    }
    
    /// Calculate optimal column widths based on headers and sample data
    pub fn calculate(headers: &[String], sample_rows: &[Vec<String>]) -> Self {
        let mut widths = vec![INDEX_COL_WIDTH]; // Index column
        
        // For each data column, find the maximum width needed
        for (col_idx, header) in headers.iter().enumerate() {
            // Start with header width
            let mut max_width = header.len() as u16 + 2; // +2 for padding
            
            // Check sample rows for this column
            for row in sample_rows.iter().take(100) { // Sample more rows for better width estimation
                if let Some(cell) = row.get(col_idx) {
                    let cell_str = if cell == crate::tile_rowstore::NULL_SENTINEL {
                        "NULL"
                    } else {
                        cell
                    };
                    // Account for display length
                    let display_len = cell_str.chars().take(MAX_COL_WIDTH as usize).count();
                    max_width = max_width.max(display_len as u16 + 2);
                }
            }
            
            // Clamp to limits
            let width = max_width.clamp(MIN_COL_WIDTH, MAX_COL_WIDTH);
            widths.push(width);
        }
        
        Self { widths }
    }
    
    /// Calculate visible columns based on a fixed scroll position (for mouse hit testing)
    pub fn get_visible_at_scroll(&self, scroll_x: u16, viewport_width: u16) -> VisibleColumns {
        let mut visible = VisibleColumns {
            start_offset: 0,
            columns: vec![],
        };
        
        // Always include index column
        visible.columns.push((0, 0, INDEX_COL_WIDTH, INDEX_COL_WIDTH));
        let _used_width = INDEX_COL_WIDTH;
        
        // Now render columns starting from the scroll offset
        let mut current_x = 0u16;
        for i in 1..self.widths.len() {
            let col_width = self.widths[i];
            let col_start = current_x;
            let col_end = current_x + col_width;
            
            // Skip columns that are completely before the viewport
            if col_end <= scroll_x {
                current_x += col_width;
                continue;
            }
            
            // Stop if we're past the viewport
            if col_start >= scroll_x + viewport_width - INDEX_COL_WIDTH {
                break;
            }
            
            // Calculate visible portion of this column
            let visible_start = col_start.max(scroll_x);
            let visible_end = col_end.min(scroll_x + viewport_width - INDEX_COL_WIDTH);
            let x_in_viewport = INDEX_COL_WIDTH + visible_start - scroll_x;
            
            // How much of the column to skip at the beginning
            let skip_chars = if col_start < scroll_x {
                scroll_x - col_start
            } else {
                0
            };
            
            visible.columns.push((
                i,
                x_in_viewport,
                visible_end - visible_start,
                col_width,
            ));
            
            // Set start_offset for the first data column if it's partially visible
            if visible.columns.len() == 2 && skip_chars > 0 {
                visible.start_offset = skip_chars;
            }
            
            current_x += col_width;
        }
        
        visible
    }
    
    /// Calculate visible columns based on cursor position and viewport width
    /// This is the key method that needs to handle partial column rendering
    pub fn calculate_visible_columns(
        &self,
        cursor_col: usize,
        viewport_width: u16,
        moving_right: bool,
    ) -> (u16, VisibleColumns) {
        // Special case: cursor on index column or no data columns
        if cursor_col == 0 || self.widths.len() <= 1 {
            return (0, self.get_visible_at_scroll(0, viewport_width));
        }
        
        // Calculate position of cursor column
        let mut cursor_start_x = 0u16;
        for i in 0..cursor_col {
            if i < self.widths.len() {
                cursor_start_x += self.widths[i];
            }
        }
        let cursor_end_x = cursor_start_x + self.widths.get(cursor_col).copied().unwrap_or(0);
        
        // Determine the scroll offset based on movement direction
        let scroll_x = if moving_right {
            // When moving right, position cursor column at the right edge
            cursor_end_x.saturating_sub(viewport_width)
        } else {
            // When moving left, position cursor column at the left edge (after index)
            cursor_start_x.saturating_sub(INDEX_COL_WIDTH)
        };
        
        // Use the common method to get visible columns at this scroll position
        let visible = self.get_visible_at_scroll(scroll_x, viewport_width);
        
        (scroll_x, visible)
    }
    
    /// Calculate minimal scroll adjustment to make cursor visible
    pub fn ensure_cursor_visible(
        &self,
        cursor_col: usize,
        current_scroll: u16,
        viewport_width: u16,
    ) -> u16 {
        if cursor_col == 0 || self.widths.len() <= 1 {
            return 0;
        }
        
        // Calculate position of cursor column (accounting for index column)
        let mut cursor_start_x = INDEX_COL_WIDTH; // Start after index column
        for i in 1..cursor_col {
            if i < self.widths.len() {
                cursor_start_x += self.widths[i];
            }
        }
        let cursor_width = self.widths.get(cursor_col).copied().unwrap_or(0);
        let cursor_end_x = cursor_start_x + cursor_width;
        
        // Adjust positions to account for index column always being visible
        let effective_cursor_start = cursor_start_x.saturating_sub(INDEX_COL_WIDTH);
        let effective_cursor_end = cursor_end_x.saturating_sub(INDEX_COL_WIDTH);
        
        // Calculate viewport bounds (accounting for index column)
        let viewport_start = current_scroll;
        let viewport_end = current_scroll + viewport_width - INDEX_COL_WIDTH;
        
        // Check if cursor is fully visible
        if effective_cursor_start >= viewport_start && effective_cursor_end <= viewport_end {
            // Already fully visible, no change needed
            current_scroll
        } else if effective_cursor_start < viewport_start {
            // Cursor is to the left of viewport, scroll left just enough
            effective_cursor_start
        } else {
            // Cursor is to the right of viewport, scroll right just enough
            effective_cursor_end.saturating_sub(viewport_width - INDEX_COL_WIDTH)
        }
    }
}

impl ResultSelection {
    pub fn none() -> Self {
        ResultSelection {
            kind: SelectionKind::None,
            anchor: None,
            cursor: None,
        }
    }
}

/* ────────────────────────────────────────────────────────────────────────── */
/*  Adjust every row index in a ResultSelection by –delta                    */
/*  (used when we copy only part of the table for a column selection).       */

/// Shift all row-coordinates in `sel` *down* by `delta` (i.e. subtract
/// `delta`).  Column indices are left untouched.  Returns a brand-new
/// selection – the original is not modified.
pub fn shift_rows(mut sel: ResultSelection, delta: usize) -> ResultSelection {
    if delta == 0 { return sel; }

    match &mut sel.kind {
        SelectionKind::FullRowSet { anchor, cursor } => {
            *anchor  = anchor.saturating_sub(delta);
            *cursor  = cursor.saturating_sub(delta);
        }
        SelectionKind::FullRowVec(rows) => {
            for r in rows.iter_mut() {
                *r = r.saturating_sub(delta);
            }
        }
        SelectionKind::Rect => {
            if let Some((r, c)) = sel.anchor {
                sel.anchor = Some((r.saturating_sub(delta), c));
            }
            if let Some((r, c)) = sel.cursor {
                sel.cursor = Some((r.saturating_sub(delta), c));
            }
        }
        _ => {}
    }
    sel
}
/* ────────────────────────────────────────────────────────────────────────── */

fn handle_find_input(results: &mut Results, key: KeyEvent) -> bool {
    if !results.find_active { return false; }
    
    match key.code {
        KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            results.find_query.push(ch);
            if results.find_query.len() >= 2 {
                update_find_matches(results);
                if !results.find_matches.is_empty() {
                    jump_to_find_match(results, results.find_current);
                }
            }
            return true;
        }
        KeyCode::Backspace => {
            results.find_query.pop();
            if results.find_query.len() >= 2 {
                update_find_matches(results);
                if !results.find_matches.is_empty() {
                    jump_to_find_match(results, results.find_current);
                }
            } else {
                // Clear matches if query is too short
                results.find_matches.clear();
                results.find_current = 0;
            }
            return true;
        }
        KeyCode::Esc => {
            results.find_active = false;
            results.find_query.clear();
            results.find_matches.clear();
            results.find_current = 0;
            return true;
        }
        _ => {}
    }
    false
}

fn update_find_matches(results: &mut Results) {
    const SEARCH_WINDOW: usize = 2_000; // Reasonable window size
    
    results.find_matches.clear();
    if results.find_query.len() < 2 { return; } // Minimum 2 characters
    
    if results.tabs.is_empty() { return; }
    let tab = &mut results.tabs[results.tab_idx];
    
    if let ResultsContent::Table { headers, tile_store } = &mut tab.content {
        let query_lower = results.find_query.to_lowercase();
        
        // Search headers - NOW INSIDE THE if let BLOCK
        for (col, header) in headers.iter().enumerate() {
            if header.to_lowercase().contains(&query_lower) {
                results.find_matches.push(FindMatch {
                    row: 0,
                    col: col + 1,
                    is_header: true,
                });
            }
        }
        
        // Search data - window around cursor position
        let start_row = tab.cursor_row.saturating_sub(SEARCH_WINDOW / 2);
        let end_row = (start_row + SEARCH_WINDOW).min(tile_store.nrows);
        
        let rows = tile_store.get_rows(start_row, end_row - start_row)
            .unwrap_or_default();
        
        for (offset, row_data) in rows.iter().enumerate() {
            let row_idx = start_row + offset;
            for (col, cell) in row_data.iter().enumerate() {
                if cell != crate::tile_rowstore::NULL_SENTINEL && cell.to_lowercase().contains(&query_lower) {
                    results.find_matches.push(FindMatch {
                        row: row_idx,
                        col: col + 1,
                        is_header: false,
                    });
                    // Still limit total matches to prevent memory issues
                    if results.find_matches.len() >= 500 {
                        return;
                    }
                }
            }
        }
    }
}

fn jump_to_find_match(results: &mut Results, idx: usize) {
    if let Some(m) = results.find_matches.get(idx) {
        let tab = &mut results.tabs[results.tab_idx];
        tab.cursor_row = m.row;
        tab.cursor_col = m.col;
        
        if let ResultsContent::Table { headers: _, tile_store } = &tab.content {
            // Center the match vertically
            if results.max_rows > 0 {
                let center_row = results.max_rows / 2;
                if m.row >= center_row {
                    tab.view_row = m.row - center_row;
                } else {
                    tab.view_row = 0;
                }
                // Ensure we don't scroll past the end
                let max_view_row = tile_store.nrows.saturating_sub(results.max_rows);
                tab.view_row = tab.view_row.min(max_view_row);
            }
            
            // Center the match horizontally
            if let Some(ref widths) = tab.column_widths_cache {
                let viewport_width = (results.wrap_width as u16).saturating_sub(2);
                
                // Calculate position of cursor column
                let mut cursor_start_x = INDEX_COL_WIDTH; // Start after index column
                for i in 1..m.col {
                    if i < widths.widths.len() {
                        cursor_start_x += widths.widths[i];
                    }
                }
                let cursor_width = widths.widths.get(m.col).copied().unwrap_or(20);
                let cursor_center = cursor_start_x + cursor_width / 2;
                
                // Center the column in the viewport
                let viewport_center = viewport_width / 2;
                if cursor_center > viewport_center + INDEX_COL_WIDTH {
                    // Need to scroll right
                    tab.scroll_x = (cursor_center - viewport_center - INDEX_COL_WIDTH).min(
                        // Don't scroll past the end
                        widths.widths.iter().sum::<u16>().saturating_sub(viewport_width)
                    );
                } else {
                    // Column is near the start, minimal or no scroll needed
                    tab.scroll_x = 0;
                }
            }
            
            // Reset scroll direction to prevent additional adjustments
            tab.scroll_direction = ScrollDirection::None;
        }
    }
}

pub fn handle_key(results: &mut Results, key: KeyEvent) {

    if key.kind != KeyEventKind::Press || results.tabs.is_empty() {
        return;
    }
    
    // Handle find mode input first
    if results.find_active && handle_find_input(results, key) {
        return;
    }

    // -- Tab cycling among available tabs only --
    match key.code {
        KeyCode::Char('[') => {
            let n = results.tabs.len();
            if n > 1 {
                results.tab_idx = if results.tab_idx == 0 { n - 1 } else { results.tab_idx - 1 };
                // Reset scroll direction when switching tabs
                if let Some(tab) = results.tabs.get_mut(results.tab_idx) {
                    tab.scroll_direction = ScrollDirection::None;
                }
            }
            return;
        }
        KeyCode::Char(']') => {
            let n = results.tabs.len();
            if n > 1 {
                results.tab_idx = (results.tab_idx + 1) % n;
                // Reset scroll direction when switching tabs
                if let Some(tab) = results.tabs.get_mut(results.tab_idx) {
                    tab.scroll_direction = ScrollDirection::None;
                }
            }
            return;
        }
        _ => {}
    }
    
    // Find/search key bindings
    match (key.code, key.modifiers) {
        (KeyCode::Char('f') | KeyCode::Char('F'), KeyModifiers::CONTROL) => {
            results.find_active = !results.find_active;
            if results.find_active {
                // Only update matches if query is long enough
                if results.find_query.len() >= 2 {
                    update_find_matches(results);
                }
            } else {
                results.find_query.clear();
                results.find_matches.clear();
                results.find_current = 0;
            }
            return;
        }
        (KeyCode::Char('g') | KeyCode::Char('G'), KeyModifiers::CONTROL) => {
            if !results.find_matches.is_empty() {
                results.find_current = (results.find_current + 1) % results.find_matches.len();
                jump_to_find_match(results, results.find_current);
            } else if results.find_query.len() >= 2 {
                // Try to update matches if we have a valid query
                update_find_matches(results);
                if !results.find_matches.is_empty() {
                    jump_to_find_match(results, results.find_current);
                }
            }
            return;
        }
        (KeyCode::Char(ch), mods) if (ch == 'g' || ch == 'G') && 
            mods.contains(KeyModifiers::CONTROL) && 
            mods.contains(KeyModifiers::SHIFT) => {
            if !results.find_matches.is_empty() {
                results.find_current = results.find_current.checked_sub(1)
                    .unwrap_or(results.find_matches.len() - 1);
                jump_to_find_match(results, results.find_current);
            } else if results.find_query.len() >= 2 {
                // Try to update matches if we have a valid query
                update_find_matches(results);
                if !results.find_matches.is_empty() {
                    results.find_current = results.find_matches.len() - 1;
                    jump_to_find_match(results, results.find_current);
                }
            }
            return;
        }
        _ => {}
    }

    let tab = &mut results.tabs[results.tab_idx];

    match &mut tab.content {
        ResultsContent::Table { headers, tile_store } => {
            let row_count = tile_store.nrows;
            let col_count = headers.len();
            let visible_cols = col_count + 1;
            let sel = &mut tab.selection;

            let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
            let shift = key.modifiers.contains(KeyModifiers::SHIFT);

            /* ───────────────────── keyboard handling inside a Table tab ───────────────── */
            match key.code {
                /* ── COPY ─────────────────────────────────────────────────────────────── */
                KeyCode::Char('c') if ctrl => {
                    use crate::results_selection::{SelectionKind, shift_rows};

                    const WINDOW: usize = 10_000;        // ±10 000 rows for column copy

                    let _selection = sel.clone();
                    let headers   = headers.clone();

                    // 0️⃣  Promote a plain caret to a 1 × 1 rectangular selection
                    let mut selection = sel.clone();
                    if matches!(selection.kind, SelectionKind::None) {
                        let cell = (tab.cursor_row, tab.cursor_col);
                        selection.kind   = SelectionKind::Rect;
                        selection.anchor = Some(cell);
                        selection.cursor = Some(cell);
                    }

                    /* column-only? decide row window */
                    let col_only = matches!(
                        selection.kind,
                        SelectionKind::FullColSet { .. } | SelectionKind::FullColVec(_)
                    );

                    let total_rows               = tile_store.nrows;
                    let (first_row, rows_to_get) = if col_only {
                        let start = tab.view_row.saturating_sub(WINDOW);
                        let end   = (tab.view_row + WINDOW).min(total_rows);
                        (start, end - start)
                    } else {
                        (0, total_rows)
                    };

                    let data = tile_store.get_rows(first_row, rows_to_get).unwrap_or_default();

                    /* shift row indices if we sliced */
                    let sel_shifted = if col_only && first_row > 0 {
                        shift_rows(selection.clone(), first_row)
                    } else {
                        selection.clone()
                    };

                    let txt = copy_selection(&sel_shifted, &headers, &data);
                    if !txt.is_empty() {
                        let _ = results.clipboard.set_contents(txt);
                    }
                }

                /* ── COLUMN selection expansion (Shift + ←/→) ─────────────────────────── */
                KeyCode::Left | KeyCode::Right if shift => {
                    let dir = if key.code == KeyCode::Left { -1isize } else { 1 };
                    match &sel.kind {
                        SelectionKind::FullColSet { anchor, cursor } => {
                            let active = *cursor;
                            let proposal = (active as isize + dir)
                                .clamp(1, (visible_cols - 1) as isize) as usize;
                            let new_cursor = if proposal == *anchor { *anchor } else { proposal };
                            sel.kind      = SelectionKind::FullColSet { anchor: *anchor, cursor: new_cursor };
                            tab.cursor_col = new_cursor;
                            tab.scroll_direction = if dir > 0 { ScrollDirection::Right } else { ScrollDirection::Left };
                        }
                        SelectionKind::Rect => {
                            let new_col = (tab.cursor_col as isize + dir)
                                .clamp(1, (visible_cols - 1) as isize) as usize;
                            tab.cursor_col = new_col;
                            sel.cursor = Some((tab.cursor_row, new_col));
                            tab.scroll_direction = if dir > 0 { ScrollDirection::Right } else { ScrollDirection::Left };
                        }
                        _ => {
                            let anchor = (tab.cursor_row, tab.cursor_col);
                            let new_col = (tab.cursor_col as isize + dir)
                                .clamp(1, (visible_cols - 1) as isize) as usize;
                            sel.kind   = SelectionKind::Rect;
                            sel.anchor = Some(anchor);
                            sel.cursor = Some((tab.cursor_row, new_col));
                            tab.cursor_col = new_col;
                            tab.scroll_direction = if dir > 0 { ScrollDirection::Right } else { ScrollDirection::Left };
                        }
                    }
                }

                /* ── ROW selection expansion (Shift + ↑/↓) ────────────────────────────── */
                KeyCode::Up | KeyCode::Down if shift => {
                    let dir = if key.code == KeyCode::Up { -1isize } else { 1 };
                    match &sel.kind {
                        SelectionKind::FullRowSet { anchor, cursor } => {
                            let active = *cursor;
                            let proposal = (active as isize + dir)
                                .clamp(0, (row_count - 1) as isize) as usize;
                            let new_cursor = if proposal == *anchor { *anchor } else { proposal };
                            sel.kind       = SelectionKind::FullRowSet { anchor: *anchor, cursor: new_cursor };
                            tab.cursor_row = new_cursor;
                        }
                        SelectionKind::Rect => {
                            let new_row = (tab.cursor_row as isize + dir)
                                .clamp(0, (row_count - 1) as isize) as usize;
                            tab.cursor_row = new_row;
                            sel.cursor = Some((new_row, tab.cursor_col));
                        }
                        _ => {
                            let anchor = (tab.cursor_row, tab.cursor_col);
                            let new_row = (tab.cursor_row as isize + dir)
                                .clamp(0, (row_count - 1) as isize) as usize;
                            sel.kind   = SelectionKind::Rect;
                            sel.anchor = Some(anchor);
                            sel.cursor = Some((new_row, tab.cursor_col));
                            tab.cursor_row = new_row;
                        }
                    }
                }

                /* ── Plain cursor moves (no Shift) ─────────────────────────────────────── */
                KeyCode::Left => {
                    if tab.cursor_col > 1 {
                        tab.cursor_col -= 1;
                        tab.scroll_direction = ScrollDirection::Left;
                    }
                    sel.kind = SelectionKind::None;
                }
                KeyCode::Right => {
                    if tab.cursor_col < visible_cols - 1 {
                        tab.cursor_col += 1;
                        tab.scroll_direction = ScrollDirection::Right;
                    }
                    sel.kind = SelectionKind::None;
                }
                KeyCode::Up if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    tab.cursor_row = tab.cursor_row.saturating_sub(results.max_rows * 10);
                    sel.kind = SelectionKind::None;
                }
                KeyCode::Down if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    tab.cursor_row =
                        min(tab.cursor_row + results.max_rows * 10, row_count.saturating_sub(1));
                    sel.kind = SelectionKind::None;
                }
                KeyCode::Home if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    tab.cursor_row = 0;
                    sel.kind = SelectionKind::None;
                }
                KeyCode::End if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    tab.cursor_row = row_count.saturating_sub(1);
                    sel.kind = SelectionKind::None;
                }
                KeyCode::Up => {
                    if tab.cursor_row > 0 { tab.cursor_row -= 1; }
                    sel.kind = SelectionKind::None;
                }
                KeyCode::Down => {
                    if tab.cursor_row + 1 < row_count { tab.cursor_row += 1; }
                    sel.kind = SelectionKind::None;
                }
                KeyCode::Home => {
                    tab.cursor_col = 1;
                    tab.scroll_x = 0; // Reset scroll position to leftmost
                    sel.kind = SelectionKind::None;
                }
                KeyCode::End => {
                    tab.cursor_col = visible_cols - 1;
                    // Set scroll direction to trigger ensure_cursor_visible
                    tab.scroll_direction = ScrollDirection::Right;
                    sel.kind = SelectionKind::None;
                }
                KeyCode::PageUp => {
                    tab.cursor_row = tab.cursor_row.saturating_sub(results.max_rows);
                    sel.kind = SelectionKind::None;
                }
                KeyCode::PageDown => {
                    tab.cursor_row =
                        min(tab.cursor_row + results.max_rows, row_count.saturating_sub(1));
                    sel.kind = SelectionKind::None;
                }

                /* ── default ──────────────────────────────────────────────────────────── */
                _ => {}
            }   // ← closes match key.code

            tab.nudge_viewport(results.max_rows, results.max_cols, row_count, visible_cols);
        }
        ResultsContent::Error { message, cursor, selection } => {
            let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
            let shift = key.modifiers.contains(KeyModifiers::SHIFT);
            
            match key.code {
                // Copy selection
                KeyCode::Char('c') if ctrl => {
                    if let Some((start, end)) = selection {
                        let (s, e) = if start < end { (*start, *end) } else { (*end, *start) };
                        let text = message.chars().skip(s).take(e - s).collect::<String>();
                        let _ = results.clipboard.set_contents(text);
                    }
                }
                // Select all
                KeyCode::Char('a') if ctrl => {
                    *selection = Some((0, message.chars().count()));
                    *cursor = message.chars().count();
                }
                // Navigation
                KeyCode::Left => {
                    if shift {
                        let anchor = selection.map(|(a, _)| a).unwrap_or(*cursor);
                        *cursor = cursor.saturating_sub(1);
                        *selection = Some((anchor, *cursor));
                    } else {
                        *cursor = cursor.saturating_sub(1);
                        *selection = None;
                    }
                }
                KeyCode::Right => {
                    let max = message.chars().count();
                    if shift {
                        let anchor = selection.map(|(a, _)| a).unwrap_or(*cursor);
                        *cursor = (*cursor + 1).min(max);
                        *selection = Some((anchor, *cursor));
                    } else {
                        *cursor = (*cursor + 1).min(max);
                        *selection = None;
                    }
                }
                KeyCode::Up => {
                    // Move cursor up one visual line in wrapped text
                    // (Implementation would need wrapped line positions)
                }
                KeyCode::Down => {
                    // Move cursor down one visual line in wrapped text
                }
                KeyCode::Home => {
                    if shift {
                        let anchor = selection.map(|(a, _)| a).unwrap_or(*cursor);
                        *cursor = 0;
                        *selection = Some((anchor, *cursor));
                    } else {
                        *cursor = 0;
                        *selection = None;
                    }
                }
                KeyCode::End => {
                    let max = message.chars().count();
                    if shift {
                        let anchor = selection.map(|(a, _)| a).unwrap_or(*cursor);
                        *cursor = max;
                        *selection = Some((anchor, *cursor));
                    } else {
                        *cursor = max;
                        *selection = None;
                    }
                }
                _ => {}
            }
        }
        ResultsContent::Pending => {}
        ResultsContent::Info { .. } => {}
    }
}

pub fn handle_mouse(results: &mut Results, event: MouseEvent, area: UiRect) {
    if results.tabs.is_empty() || !results.focus {
        return;
    }
    let tab = &mut results.tabs[results.tab_idx];
    
    match &mut tab.content {
        ResultsContent::Table { headers, tile_store } => {
            let row_count = tile_store.nrows;
            let col_count = headers.len();
            
            let vx = area.x + 1;
            let header_row = area.y + 2;
            let vy = area.y + 3;
            let mx = event.column;
            let my = event.row;

            if mx < vx || my < area.y + 2 {
                return;
            }

            // Get column from mouse position using the stored scroll position
            let rel_x = (mx - vx) as u16;
            
            let col = if let Some(ref widths) = tab.column_widths_cache {
                // Get visible columns at the current scroll position
                let viewport_width = area.width - 2;
                let visible_cols = widths.get_visible_at_scroll(tab.scroll_x, viewport_width);
                
                // Find which column was clicked
                visible_cols.columns.iter()
                    .find(|&&(_, x, width, _)| rel_x >= x && rel_x < x + width)
                    .map(|&(idx, _, _, _)| idx)
                    .unwrap_or(0)
            } else {
                // Fallback to fixed width calculation
                (rel_x / 20) as usize
            };
            
            if col > col_count {
                return;
            }

            // Map mouse y to data row
            let row = (my - vy) as usize + tab.view_row;

            // Area checks
            let in_table = (my >= vy)
                && row < row_count
                && col < col_count + 1
                && (col > 0 || col == 0)
                && my > header_row;
            let in_index = row < row_count && col == 0 && my > header_row;
            let in_header = my == header_row && col > 0 && col < col_count + 1;
            let in_index_header = my == header_row && col == 0;

            let sel = &mut tab.selection;
            let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
            let shift = event.modifiers.contains(KeyModifiers::SHIFT);

            match event.kind {
                MouseEventKind::Down(MouseButton::Left) => {
                    /* ─── Ctrl-click row toggle (add/subtract) ─────────── */
                    if in_index && ctrl {
                        // Start with current selection (Set or Vec) → Vec
                        let mut rows_vec = match &sel.kind {
                            SelectionKind::FullRowVec(v) => v.clone(),
                            SelectionKind::FullRowSet { anchor, cursor } => {
                                let (a, c) = (*anchor.min(cursor), *anchor.max(cursor));
                                (a..=c).collect()
                            }
                            _ => Vec::new(),
                        };

                        // Toggle clicked row
                        if rows_vec.contains(&row) {
                            rows_vec.retain(|&r| r != row);
                        } else {
                            rows_vec.push(row);
                        }

                        sel.kind = SelectionKind::FullRowVec(rows_vec);
                        tab.cursor_row = row;
                        tab.cursor_col = 1;
                    } else if in_index && !ctrl {
                        match &sel.kind {
                            // already have a contiguous row range
                            SelectionKind::FullRowSet { anchor, .. } => {
                                sel.kind = SelectionKind::FullRowSet { anchor: *anchor, cursor: row };
                            }
                            _ => {
                                sel.kind = SelectionKind::FullRowSet { anchor: row, cursor: row };
                            }
                        }
                        sel.anchor = None;
                        sel.cursor = None;
                        tab.cursor_row = row;
                        tab.cursor_col = 1;
                    } else if in_index_header {
                        sel.kind = SelectionKind::Rect;
                        sel.anchor = Some((0, 1));
                        sel.cursor = Some((row_count.saturating_sub(1), col_count));
                        tab.cursor_row = 0;
                        tab.cursor_col = 1;
                    } else if in_header && ctrl {
                        // always toggle col in a Vec; convert range → Vec first
                        let mut cols_vec = match &sel.kind {
                            SelectionKind::FullColVec(v) => v.clone(),
                            SelectionKind::FullColSet { anchor, cursor } => {
                                let (a, c) = (*anchor.min(cursor), *anchor.max(cursor));
                                (a..=c).collect()
                            }
                            _ => Vec::new(),
                        };
                        if cols_vec.contains(&col) {
                            cols_vec.retain(|&c| c != col);
                        } else {
                            cols_vec.push(col);
                        }
                        sel.kind = SelectionKind::FullColVec(cols_vec);
                        tab.cursor_row = tab.view_row;
                        tab.cursor_col = col;
                        // Don't change scroll direction for header clicks
                        tab.scroll_direction = ScrollDirection::None;
                    } else if in_header && !ctrl {
                        match &sel.kind {
                            // already have a contiguous col range → keep the same anchor
                            SelectionKind::FullColSet { anchor, .. } => {
                                sel.kind = SelectionKind::FullColSet { anchor: *anchor, cursor: col };
                            }
                            _ => {
                                // start a new range anchored at the current column
                                sel.kind = SelectionKind::FullColSet { anchor: col, cursor: col };
                            }
                        }
                        sel.anchor = None;
                        sel.cursor = None;
                        tab.cursor_col = col;
                        tab.cursor_row = tab.view_row;
                        // Don't change scroll direction for header clicks
                        tab.scroll_direction = ScrollDirection::None;
                    } else if in_table {
                        if shift && sel.anchor.is_some() {
                            // Shift+click extends selection
                            sel.kind = SelectionKind::Rect;
                            sel.cursor = Some((row, col));
                        } else {
                            // Regular click - just move cursor
                            // Clear any existing selection
                            sel.kind = SelectionKind::None;
                            // Store anchor for potential drag selection
                            sel.anchor = Some((row, col));
                            sel.cursor = Some((row, col));
                            // Reset scroll direction to prevent viewport jumps
                            tab.scroll_direction = ScrollDirection::None;
                        }
                        
                        // Always move cursor
                        tab.cursor_row = row;
                        tab.cursor_col = col;
                    } else {
                        sel.kind = SelectionKind::None;
                        sel.anchor = None;
                        sel.cursor = None;
                    }
                }
                MouseEventKind::Drag(MouseButton::Left) | MouseEventKind::Up(MouseButton::Left) => {
                    if sel.anchor.is_some() && in_table {
                        // Only create a selection if we dragged to a different cell
                        if let Some(anchor) = sel.anchor {
                            if anchor != (row, col) {
                                sel.kind = SelectionKind::Rect;
                                sel.cursor = Some((row, col));
                            }
                        }
                        tab.cursor_row = row;
                        tab.cursor_col = col;
                        // Don't change scroll direction during drag
                        tab.scroll_direction = ScrollDirection::None;
                    } else if matches!(sel.kind, SelectionKind::FullColSet { .. }) && in_header {
                        // update continuous column drag
                        if let SelectionKind::FullColSet { anchor, cursor: _ } = sel.kind {
                            sel.kind = SelectionKind::FullColSet { anchor, cursor: col };
                        }
                        tab.cursor_col = col;
                        tab.scroll_direction = ScrollDirection::None;
                    } else if matches!(sel.kind, SelectionKind::FullRowSet { .. }) && in_index {
                        // update continuous row drag
                        if let SelectionKind::FullRowSet { anchor, cursor: _ } = sel.kind {
                            sel.kind = SelectionKind::FullRowSet { anchor, cursor: row };
                        }
                        tab.cursor_row = row;
                    }
                }
                MouseEventKind::ScrollDown => {
                    if ctrl {
                        // Horizontal scroll right - just scroll viewport
                        if let Some(ref widths) = tab.column_widths_cache {
                            // Scroll by a fixed amount (e.g., 20 chars)
                            tab.scroll_x = tab.scroll_x.saturating_add(20);
                            // Clamp to reasonable maximum
                            let total_width: u16 = widths.widths.iter().sum();
                            let max_scroll = total_width.saturating_sub(area.width - 2);
                            tab.scroll_x = tab.scroll_x.min(max_scroll);
                        }
                    } else {
                        // Vertical scroll down
                        tab.view_row = (tab.view_row + 1).min(row_count.saturating_sub(results.max_rows));
                    }
                }
                MouseEventKind::ScrollUp => {
                    if ctrl {
                        // Horizontal scroll left - just scroll viewport
                        tab.scroll_x = tab.scroll_x.saturating_sub(20);
                    } else {
                        // Vertical scroll up
                        tab.view_row = tab.view_row.saturating_sub(1);
                    }
                }
                MouseEventKind::ScrollRight => {
                    // Horizontal scroll right
                    if let Some(ref widths) = tab.column_widths_cache {
                        tab.scroll_x = tab.scroll_x.saturating_add(20);
                        let total_width: u16 = widths.widths.iter().sum();
                        let max_scroll = total_width.saturating_sub(area.width - 2);
                        tab.scroll_x = tab.scroll_x.min(max_scroll);
                    }
                }
                MouseEventKind::ScrollLeft => {
                    // Horizontal scroll left
                    tab.scroll_x = tab.scroll_x.saturating_sub(20);
                }
                _ => {}
            }
        }
        ResultsContent::Error { message, cursor, selection } => {
            // Account for tab bar (+1) and borders (+1)
            let content_area = UiRect { x: area.x, y: area.y + 1, width: area.width, height: area.height - 1 };
            let inner = content_area.inner(&tui::layout::Margin { horizontal: 1, vertical: 1 });
           
            let mx = event.column;
            let my = event.row;
            
            // Check if click is inside the error text area
            if mx >= inner.x && mx < inner.x + inner.width &&
               my >= inner.y && my < inner.y + inner.height {
                
                // Calculate wrapped lines to get accurate position
                let wrap_width = inner.width as usize;
                let wrapped_lines = wrap_text(message, wrap_width);
                
                let rel_x = (mx - inner.x) as usize;
                let rel_y = (my - inner.y) as usize;
                
                // Find which character was clicked
                let clicked_pos = if rel_y < wrapped_lines.len() {
                    let line_info = &wrapped_lines[rel_y];
                    let chars_in_line: Vec<char> = message[line_info.start..line_info.end].chars().collect();
                    let click_char = rel_x.min(chars_in_line.len());
                    line_info.char_start + click_char
                } else {
                    message.chars().count()
                };
                
                match event.kind {
                    MouseEventKind::Down(MouseButton::Left) => {
                        *cursor = clicked_pos;
                        *selection = Some((*cursor, *cursor));
                    }
                    MouseEventKind::Drag(MouseButton::Left) => {
                        if let Some((anchor, _)) = selection {
                            *cursor = clicked_pos;
                            *selection = Some((*anchor, *cursor));
                        }
                    }
                    MouseEventKind::ScrollDown => {
                        // Could implement scrolling for long error messages
                    }
                    MouseEventKind::ScrollUp => {
                        // Could implement scrolling for long error messages
                    }
                    _ => {}
                }
            }
        }
        ResultsContent::Pending => {}
        ResultsContent::Info { .. } => {}
    }
}

fn format_duration_hms(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

pub fn render<B: Backend>(
    results: &mut Results,
    f: &mut Frame<B>,
    area: UiRect,
    total_queries: usize,
) {
    let mut tabtitles: Vec<Spans> = Vec::new();
    for (i, t) in results.tabs.iter().enumerate() {
        let tabname = format!("{}/{}", i + 1, total_queries);
        let label = match &t.content {
            ResultsContent::Table { .. } | ResultsContent::Pending | ResultsContent::Info { .. } => tabname,
            ResultsContent::Error { .. } => format!("Error {}", tabname),
        };
        if i == results.tab_idx {
            tabtitles.push(Spans::from(Span::styled(
                format!("[{}]", label),
                STYLE::tab_active(),
            )));
        } else {
            tabtitles.push(Spans::from(Span::raw(format!(" {} ", label))));
        }
    }

    let wrap_width = (area.width as usize).saturating_sub(4).max(6);
    results.wrap_width = wrap_width;

    f.render_widget(
        Tabs::new(tabtitles)
            .block(Block::default().borders(Borders::NONE))
            .select(results.tab_idx)
            .style(Style::default()),
        UiRect {
            x: area.x,
            y: area.y,
            width: area.width,
            height: 1,
        },
    );

    if results.tabs.is_empty() {
        let p = Paragraph::new("No results")
            .block(Block::default()
                .title(Span::styled(
                    "Results",
                    STYLE::results_border_focus()  // Always use active color for title text
                ))
                .borders(Borders::ALL)
                .border_style(
                            if results.focus {
                                STYLE::results_border_focus()
                            } else {
                                STYLE::results_border()
                            }
                        ));
        f.render_widget(
            p,
            UiRect {
                x: area.x,
                y: area.y + 1,
                width: area.width,
                height: area.height - 1,
            },
        );
        return;
    }

    let tab = &mut results.tabs[results.tab_idx];

    let border_label = if tab.running {
        if let Some(start) = tab.run_started {
            let hms = format_duration_hms(start.elapsed());
            format!("{} (running: {})", tab.query_context, hms)
        } else {
            format!("{} (running)", tab.query_context)
        }
    } else if let Some(elapsed) = tab.elapsed {
        let hms = format_duration_hms(elapsed);
        format!("{} ({})", tab.query_context, hms)
    } else {
        tab.query_context.clone()
    };

    match &mut tab.content {
        ResultsContent::Table { headers, tile_store } => {
            // ---------- early-out if the result-set is empty ----------
            let total_rows = tile_store.nrows;
            if headers.is_empty() || total_rows == 0 {
                let p = Paragraph::new("No rows returned (this statement did not produce a table)")
                    .block(
                    Block::default()
                        .title(Span::styled(
                            border_label,
                            STYLE::results_border_focus()  // Always use active color for title text
                        ))
                        .borders(Borders::ALL)
                        .border_style(
                            if results.focus {
                                STYLE::results_border_focus()
                            } else {
                                STYLE::results_border()
                            }
                        )
                )
                    .style(STYLE::info_fg());
                f.render_widget(
                    p,
                    UiRect { x: area.x, y: area.y + 1, width: area.width, height: area.height - 1 },
                );
                return;
            }

            /* ---- 1️⃣  Copy every tab field we'll need later ---- */
            let view_row      = tab.view_row;
            let cursor_row    = tab.cursor_row;
            let cursor_col    = tab.cursor_col;
            let selection     = tab.selection.clone();
            let headers_vec   = headers.clone();

            /* ---- 2️⃣  Fetch the visible rows ---- */
            let visible_rows  = tile_store
                .get_rows(view_row, results.max_rows)
                .unwrap_or_default();
            tile_store.prefetch_for_view(view_row, results.max_rows);

            /* ---- 3️⃣  Calculate column widths if needed ---- */
            if tab.column_widths_cache.is_none() {
                // Fetch sample rows for width calculation
                let sample_rows = tile_store
                    .get_rows(0, 100) // Sample first 100 rows
                    .unwrap_or_default();
                tab.column_widths_cache = Some(ColumnWidths::calculate(&headers_vec, &sample_rows));
            }
            
            let col_widths = tab.column_widths_cache.as_ref().unwrap();
            let viewport_width = area.width - 2; // Subtract borders
            
            // Check if cursor is already fully visible at current scroll
            let cursor_fully_visible = {
                let current_visible = col_widths.get_visible_at_scroll(tab.scroll_x, viewport_width);
                current_visible.columns.iter().any(|&(idx, _, vis_width, full_width)| {
                    idx == cursor_col && vis_width == full_width
                })
            };
            
            // Calculate visible columns
            let visible_cols = if cursor_fully_visible && tab.scroll_direction == ScrollDirection::None {
                // Cursor is fully visible and no keyboard navigation, keep current scroll
                col_widths.get_visible_at_scroll(tab.scroll_x, viewport_width)
            } else if tab.scroll_direction == ScrollDirection::Left || tab.scroll_direction == ScrollDirection::Right {
                // Keyboard navigation - adjust scroll minimally to ensure cursor is visible
                let new_scroll = col_widths.ensure_cursor_visible(cursor_col, tab.scroll_x, viewport_width);
                tab.scroll_x = new_scroll;
                tab.view_col = new_scroll as usize;
                col_widths.get_visible_at_scroll(new_scroll, viewport_width)
            } else if !cursor_fully_visible {
                // Cursor not visible (e.g., from find or initial load) - center it if possible
                let new_scroll = col_widths.ensure_cursor_visible(cursor_col, tab.scroll_x, viewport_width);
                tab.scroll_x = new_scroll;
                tab.view_col = new_scroll as usize;
                col_widths.get_visible_at_scroll(new_scroll, viewport_width)
            } else {
                // Default case
                col_widths.get_visible_at_scroll(tab.scroll_x, viewport_width)
            };
            
            // Reset scroll direction after using it
            tab.scroll_direction = ScrollDirection::None;
            
            // ---------- render custom table with partial columns ----------
            let res_border = if results.focus {
                STYLE::results_border_focus()
            } else {
                STYLE::results_border()
            };
            
            let table_area = UiRect { 
                x: area.x, 
                y: area.y + 1, 
                width: area.width, 
                height: area.height - 1 
            };
            
            // Draw the border first
            let block = Block::default()
                .title(Span::styled(
                    border_label,
                    STYLE::results_border_focus()  // Always use active color for title text
                ))
                .borders(Borders::ALL)
                .border_style(res_border);
            f.render_widget(block, table_area);
            
            // Inner area (inside borders)
            let inner = UiRect {
                x: table_area.x + 1,
                y: table_area.y + 1,
                width: table_area.width - 2,
                height: table_area.height - 2,
            };
            
            // Render header row
            let mut header_spans = Vec::new();
            
            for &(col_idx, _col_x, visible_width, _full_width) in &visible_cols.columns {
                if col_idx == 0 {
                    // Index column header
                    let text = "#";
                    let padded = format!("{:width$}", text, width = visible_width as usize);
                    let style = Style::default().add_modifier(Modifier::BOLD);
                    header_spans.push(Span::styled(padded, style));
                } else {
                    // Data column header
                    let header_text = &headers_vec[col_idx - 1];
                    
                    // Calculate how much to skip if this is the first data column
                    let skip_chars = if visible_cols.columns.len() > 1 && 
                                       visible_cols.columns[1].0 == col_idx && 
                                       visible_cols.start_offset > 0 {
                        visible_cols.start_offset as usize
                    } else {
                        0
                    };
                    
                    // Get the visible portion of the header
                    let visible_text = if skip_chars > 0 {
                        // Partial column - skip first characters
                        header_text.chars().skip(skip_chars).collect::<String>()
                    } else {
                        header_text.clone()
                    };
                    
                    // Truncate if still too long
                    let display_text = if visible_text.len() > visible_width as usize {
                        format!("{}…", &visible_text[..visible_width as usize - 1])
                    } else {
                        visible_text
                    };
                    
                    let padded = format!("{:width$}", display_text, width = visible_width as usize);
                    
                    let mut style = Style::default().add_modifier(Modifier::BOLD);
                    
                    // Check for find matches in headers
                    if results.find_active && !results.find_matches.is_empty() {
                        let is_header_match = results.find_matches.iter().any(|m| 
                            m.row == 0 && m.col == col_idx && m.is_header
                        );
                        let is_current_header = results.find_current < results.find_matches.len() &&
                            results.find_matches.get(results.find_current)
                                .map(|m| m.row == 0 && m.col == col_idx && m.is_header)
                                .unwrap_or(false);
                        
                        if is_current_header {
                            style = style
                                .fg(rgb(CONFIG_COLORS.find_current_fg))
                                .bg(rgb(CONFIG_COLORS.find_current_bg));
                        } else if is_header_match {
                            style = style
                                .fg(rgb(CONFIG_COLORS.find_match_fg))
                                .bg(rgb(CONFIG_COLORS.find_match_bg));
                        }
                    }
                    
                    if cell_in_selection(usize::MAX, col_idx, &selection) {
                        style = STYLE::table_sel_bg();
                    }
                    
                    header_spans.push(Span::styled(padded, style));
                }
            }
            
            // Render header row
            let header_line = Spans::from(header_spans);
            f.render_widget(
                Paragraph::new(header_line).style(STYLE::header_row()),
                UiRect { x: inner.x, y: inner.y, width: inner.width, height: 1 }
            );
            
            // Render data rows
            let data_area_y = inner.y + 1;
            let max_data_rows = (inner.height - 1) as usize;
            
            for (row_offset, row_idx) in (view_row..view_row + visible_rows.len().min(max_data_rows)).enumerate() {
                let mut row_spans = Vec::new();
                
                for &(col_idx, _col_x, visible_width, _full_width) in &visible_cols.columns {
                    if col_idx == 0 {
                        // Index column
                        let text = (row_idx + 1).to_string();
                        let padded = format!("{:width$}", text, width = visible_width as usize);
                        let mut style = Style::default();
                        if cell_in_selection(row_idx, 0, &selection) {
                            style = STYLE::table_sel_bg();
                        }
                        row_spans.push(Span::styled(padded, style));
                    } else if row_offset < visible_rows.len() && col_idx - 1 < visible_rows[row_offset].len() {
                        // Data column
                        let cell_value = &visible_rows[row_offset][col_idx - 1];
                        let mut cell_text = cell_value.replace('\n', " ");
                        
                        // Check if this cell is a find match
                        let is_find_match = if results.find_active && !results.find_matches.is_empty() {
                            results.find_matches.iter().any(|m| 
                                m.row == row_idx && m.col == col_idx && !m.is_header
                            )
                        } else {
                            false
                        };
                        let is_current_find = if results.find_active && results.find_current < results.find_matches.len() {
                            results.find_matches.get(results.find_current)
                                .map(|m| m.row == row_idx && m.col == col_idx && !m.is_header)
                                .unwrap_or(false)
                        } else {
                            false
                        };
    
                        let is_null = cell_text == crate::tile_rowstore::NULL_SENTINEL;
                        if is_null { cell_text = "NULL".into(); }
                        
                        // Calculate how much to skip if this is the first data column
                        let skip_chars = if visible_cols.columns.len() > 1 && 
                                           visible_cols.columns[1].0 == col_idx && 
                                           visible_cols.start_offset > 0 {
                            visible_cols.start_offset as usize
                        } else {
                            0
                        };
                        
                        // Get the visible portion of the cell
                        let visible_text = if skip_chars > 0 {
                            cell_text.chars().skip(skip_chars).collect::<String>()
                        } else {
                            cell_text.clone()
                        };
                        
                        // Truncate if still too long
                        let display_text = if visible_text.len() > visible_width as usize {
                            format!("{}…", &visible_text[..visible_width as usize - 1])
                        } else {
                            visible_text
                        };
                        
                        let padded = format!("{:width$}", display_text, width = visible_width as usize);
                        
                        let base_style = if is_null {
                            Style::default().fg(k::STEEL_VIOLET)
                        } else {
                            Style::default()
                        };

                        let selected = cell_in_selection(row_idx, col_idx, &selection);
                        let caret_here = results.focus && row_idx == cursor_row && col_idx == cursor_col;

                        let style = if is_current_find {
                            Style::default()
                                .fg(rgb(CONFIG_COLORS.find_current_fg))
                                .bg(rgb(CONFIG_COLORS.find_current_bg))
                        } else if is_find_match {
                            Style::default()
                                .fg(rgb(CONFIG_COLORS.find_match_fg))
                                .bg(rgb(CONFIG_COLORS.find_match_bg))
                        } else if selected {
                            STYLE::table_sel_bg()
                        } else if caret_here {
                            STYLE::table_caret_bg()
                        } else {
                            base_style
                        };
                        
                        row_spans.push(Span::styled(padded, style));
                    } else {
                        // Empty cell
                        let padded = format!("{:width$}", "", width = visible_width as usize);
                        row_spans.push(Span::raw(padded));
                    }
                }
                
                // Render the row
                let row_line = Spans::from(row_spans);
                f.render_widget(
                    Paragraph::new(row_line),
                    UiRect { 
                        x: inner.x, 
                        y: data_area_y + row_offset as u16, 
                        width: inner.width, 
                        height: 1 
                    }
                );
            }
            
            // Overlay search text on bottom border if active
            if results.find_active {
                let match_info = if results.find_query.len() < 2 {
                    "(min 2 chars)".to_string()
                } else {
                    format!("[{}/{}]", 
                        if results.find_matches.is_empty() { 0 } else { results.find_current + 1 },
                        results.find_matches.len()
                    )
                };
                
                let search_text = format!(" Find: {} {} ",
                    results.find_query,
                    match_info
                );
                
                let rect = UiRect { x: area.x, y: area.y + 1, width: area.width, height: area.height - 1 };
                let search_x = rect.x + (rect.width.saturating_sub(search_text.len() as u16)) / 2;
                let search_y = rect.y + rect.height - 1;
                
                let search_style = Style::default()
                    .fg(rgb(CONFIG_COLORS.find_current_fg))
                    .bg(rgb(CONFIG_COLORS.find_current_bg));
                
                let search_area = UiRect {
                    x: search_x,
                    y: search_y,
                    width: search_text.len() as u16,
                    height: 1,
                };
                
                f.render_widget(Paragraph::new(search_text).style(search_style), search_area);
            }
        }

        ResultsContent::Error { message, cursor, selection } => {
            // Calculate available width for wrapping
            let wrap_width = area.width.saturating_sub(2) as usize;
            // Use the same wrapping logic as mouse handling
            let wrapped_lines = wrap_text(message, wrap_width);
            
            // Build styled lines with cursor and selection
            let mut lines = Vec::new();
            for line_info in &wrapped_lines {
                let mut spans = Vec::new();
                let line_text = &message[line_info.start..line_info.end];
                
                for (char_idx, ch) in line_text.chars().enumerate() {
                    let char_pos = line_info.char_start + char_idx;
                    let style = if let Some((start, end)) = selection {
                        let (s, e) = if start < end { (*start, *end) } else { (*end, *start) };
                        if char_pos >= s && char_pos < e {
                            STYLE::selection_bg()
                        } else if char_pos == *cursor && results.focus {
                            STYLE::table_caret_bg()
                        } else {
                            STYLE::error_fg()
                        }
                    } else if char_pos == *cursor && results.focus {
                        STYLE::table_caret_bg()
                    } else {
                        STYLE::error_fg()
                    };
                    
                    spans.push(Span::styled(ch.to_string(), style));
                }
                
                lines.push(Spans::from(spans));
            }
            
            let p = Paragraph::new(lines)
                .block(Block::default()
                    .title(Span::styled(
                        border_label,
                        STYLE::results_border_focus()  // Always use active color for title text
                    ))
                    .borders(Borders::ALL)
                    .border_style(if results.focus {
                        STYLE::results_border_focus()
                    } else {
                        STYLE::results_border()
                    }));
            f.render_widget(
                p,
                UiRect {
                    x: area.x,
                    y: area.y+1,
                    width: area.width,
                    height: area.height - 1,
                },
            );
        }
        ResultsContent::Info { message } => {
            let p = Paragraph::new(message.as_str())
                .block(Block::default()
                    .title(Span::styled(
                        border_label,
                        STYLE::results_border_focus()  // Always use active color for title text
                    ))
                    .borders(Borders::ALL)
                    .border_style(if results.focus {
                        STYLE::results_border_focus()
                    } else {
                        STYLE::results_border()
                    }))
                .style(STYLE::info_fg());
            f.render_widget(
                p,
                UiRect {
                    x: area.x,
                    y: area.y + 1,
                    width: area.width,
                    height: area.height - 1,
                },
            );
        }
        ResultsContent::Pending => {
            let p = Paragraph::new("")
                .block(Block::default()
                    .title(Span::styled(
                        border_label,
                        STYLE::results_border_focus()  // Always use active color for title text
                    ))
                    .borders(Borders::ALL)
                    .border_style(if results.focus {
                        STYLE::results_border_focus()
                    } else {
                        STYLE::results_border()
                    }));
            f.render_widget(
                p,
                UiRect {
                    x: area.x,
                    y: area.y + 1,
                    width: area.width,
                    height: area.height - 1,
                },
            );
        }
    }
}

pub fn cell_in_selection(row: usize, col: usize, sel: &ResultSelection) -> bool {
    match &sel.kind {
        SelectionKind::FullRowSet { anchor, cursor } => {
            let (start, end) = (min(*anchor, *cursor), max(*anchor, *cursor));
            row >= start && row <= end
        }
        SelectionKind::FullRowVec(rows) => rows.contains(&row),
        SelectionKind::FullColSet { anchor, cursor } => {
            let (start, end) = (min(*anchor, *cursor), max(*anchor, *cursor));
            col >= start && col <= end
        }
        SelectionKind::FullColVec(cols) => cols.contains(&col),
        SelectionKind::Rect => {
            if let (Some(anchor), Some(cursor)) = (sel.anchor, sel.cursor) {
                let ra = min(anchor.0, cursor.0);
                let ca = min(anchor.1, cursor.1);
                let rb = max(anchor.0, cursor.0);
                let cb = max(anchor.1, cursor.1);
                row >= ra && row <= rb && col >= ca && col <= cb
            } else {
                false
            }
        }
        SelectionKind::None => false,
    }
}

// ────────────────────────────────────────────────────────────────
//  Fast, sample-based summary of the *current* selection
// ────────────────────────────────────────────────────────────────
pub fn compute_selection_summary(
    sel: &ResultSelection,
    headers: &[String],
    tile_store: &mut crate::tile_rowstore::TileRowStore,
) -> Option<(String /*stats*/, Option<String> /*warning*/)> {
    /// Hard-cap: analyse at most this many **rows**
    const MAX_SUMMARY_ROWS: usize = 10_000;

    if matches!(sel.kind, SelectionKind::None) {
        return None;                           // nothing selected
    }

    let nrows = tile_store.nrows;
    let ncols = headers.len();

    /* helper: iterator over every logical (row,col) pair in the selection */
    fn cells<'a>(
        sel: &'a ResultSelection,
        nrows: usize,
        ncols: usize,
    ) -> Box<dyn Iterator<Item = (usize, usize)> + 'a> {
        use SelectionKind::*;
        match &sel.kind {
            FullRowSet { anchor, cursor } => {
                let (s, e) = (*anchor.min(cursor), *anchor.max(cursor));
                Box::new((s..=e).flat_map(move |r| (1..=ncols).map(move |c| (r, c))))
            }
            FullRowVec(rows) => Box::new(
                rows.clone()
                    .into_iter()
                    .flat_map(move |r| (1..=ncols).map(move |c| (r, c))),
            ),
            FullColSet { anchor, cursor } => {
                let (s, e) = (*anchor.min(cursor), *anchor.max(cursor));
                Box::new((0..nrows).flat_map(move |r| (s..=e).map(move |c| (r, c))))
            }
            FullColVec(cols) => Box::new(
                cols.clone()
                    .into_iter()
                    .flat_map(move |c| (0..nrows).map(move |r| (r, c))),
            ),
            Rect => {
                if let (Some(a), Some(b)) = (sel.anchor, sel.cursor) {
                    let r0 = a.0.min(b.0);
                    let r1 = a.0.max(b.0);
                    let c0 = a.1.min(b.1).max(1);   // skip "#" index col
                    let c1 = a.1.max(b.1);
                    Box::new((r0..=r1).flat_map(move |r| (c0..=c1).map(move |c| (r, c))))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            None => Box::new(std::iter::empty()),
        }
    }

    // ── accumulators ─────────────────────────────────────────────
    use std::collections::HashSet;

    let mut rows_seen: HashSet<usize> = HashSet::with_capacity(MAX_SUMMARY_ROWS);
    let mut total_cells   = 0usize;
    let mut null_cells    = 0usize;
    let mut numeric_sum   = 0f64;
    let mut numeric_cnt   = 0usize;
    let mut freq:    HashMap<String, usize> = HashMap::new();
    let mut uniques: HashSet<String>        = HashSet::new();
    let mut whitespace = false;

    // ── main walk over the selection ─────────────────────────────
    for (r, c) in cells(sel, nrows, ncols) {
        // stop once we've processed the desired number of **rows**
        if rows_seen.len() >= MAX_SUMMARY_ROWS && !rows_seen.contains(&r) {
            break;
        }
        rows_seen.insert(r);

        // fetch the single row we need
        let row_vec = tile_store.get_rows(r, 1).ok()?.pop()?;
        let idx = (c - 1) as usize;            // 1-based → 0-based
        if idx >= row_vec.len() { continue; }
        let cell = row_vec[idx].clone();
        total_cells += 1;

        // true NULL?
        if cell == crate::tile_rowstore::NULL_SENTINEL
            || cell.eq_ignore_ascii_case("null")
        {
            null_cells += 1;
            continue;
        }

        let trimmed = cell.trim();

        if let Ok(n) = trimmed.parse::<f64>() {
            numeric_sum += n;
            numeric_cnt += 1;
        }

        whitespace |= trimmed.len() != cell.len();

        if uniques.len() < MAX_SUMMARY_ROWS {
            uniques.insert(trimmed.to_owned());
        }
        if freq.len() < MAX_SUMMARY_ROWS {
            *freq.entry(trimmed.to_owned()).or_insert(0) += 1;
        }
    }

    if rows_seen.is_empty() {
        return None;
    }

    // ── compute summary numbers ─────────────────────────────────
    let mode_raw = freq
        .iter()
        .max_by_key(|&(_, c)| c)
        .map(|(v, _)| v.as_str())
        .unwrap_or("");

    let mode = match mode_raw.parse::<f64>() {
        Ok(n) => fmt_num(n),
        Err(_) => mode_raw.to_string(),
    };

    let sum_str = if numeric_cnt > 0 { fmt_num(numeric_sum) } else { "n/a".into() };
    let avg_str = if numeric_cnt > 0 { fmt_num(numeric_sum / numeric_cnt as f64) } else { "n/a".into() };

    let null_pct = (null_cells as f64) * 100.0 / (total_cells as f64);

    // main stats line (count now == distinct rows scanned)
    let stats = format!(
        "sum: {sum}   avg: {avg}   mode: {mode}   rows: {rows}   uniq: {uniq}   null: {null:.1}%",
        sum  = sum_str,
        avg  = avg_str,
        mode = mode,
        rows = rows_seen.len(),
        uniq = uniques.len(),
        null = null_pct,
    );

    // optional warning
    let warning = if whitespace {
        Some("leading/trailing spaces detected".to_string())
    } else {
        None
    };

    Some((stats, warning))
}