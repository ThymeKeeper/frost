use tui::{layout::*, backend::Backend, Frame, style::{Style, Color}};
use crossterm::event::{KeyEvent, KeyCode, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind, MouseButton};
use copypasta::{ClipboardProvider, ClipboardContext};
use std::cmp::min;
use std::ops::Range;
use std::time::{Duration, Instant};
use crate::syntax::{highlight_line, ParseState};
use tui::widgets::Clear;

use crate::palette::STYLE;
use crate::palette::{rgb, CONFIG_COLORS};

/*────────────── configurable gutter ──────────────*/
/// Total characters the line-number gutter occupies **including** the
/// trailing space (e.g. `4` → "-123␠").  Bump this to 5, 6, … if you
/// need to show more digits.
pub const GUTTER_WIDTH: u16 = 7;

const V_SCROLL_MARGIN: usize = 1;     // 1 → 2   shows one extra line at bottom
const H_SCROLL_MARGIN: usize = 4;     // unchanged
const V_SCROLL_STEP:   usize = 4;     // wheel ticks
const H_SCROLL_STEP:   usize = 4;
const INDENT: &str = "    ";



#[derive(Clone, Debug)]
enum EditOp {
    Insert { pos: usize, text: String },
    Delete { pos: usize, text: String },
}

#[derive(Clone, Debug)]
struct EditGroup {
    ops: Vec<EditOp>,
    selection_before: Option<(usize, usize)>,
    selection_after: Option<(usize, usize)>,
    caret_before: usize,
    caret_after: usize,
}

impl EditGroup {
    fn new(caret: usize, selection: Option<(usize, usize)>) -> Self {
        Self {
            ops: Vec::new(),
            selection_before: selection,
            selection_after: selection,
            caret_before: caret,
            caret_after: caret,
        }
    }
    
    fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
    
    fn add_op(&mut self, op: EditOp, caret_after: usize, selection_after: Option<(usize, usize)>) {
        self.ops.push(op);
        self.caret_after = caret_after;
        self.selection_after = selection_after;
    }
    
    fn apply(&self, buffer: &mut String) -> (usize, Option<(usize, usize)>) {
        for op in &self.ops {
            match op {
                EditOp::Insert { pos, text } => {
                    buffer.insert_str(*pos, text);
                }
                EditOp::Delete { pos, text } => {
                    buffer.replace_range(*pos..*pos + text.len(), "");
                }
            }
        }
        (self.caret_after, self.selection_after)
    }
    
    fn revert(&self, buffer: &mut String) -> (usize, Option<(usize, usize)>) {
        // Apply operations in reverse order with inverted actions
        for op in self.ops.iter().rev() {
            match op {
                EditOp::Insert { pos, text } => {
                    // To revert an insert, we delete
                    buffer.replace_range(*pos..*pos + text.len(), "");
                }
                EditOp::Delete { pos, text } => {
                    // To revert a delete, we insert
                    buffer.insert_str(*pos, text);
                }
            }
        }
        (self.caret_before, self.selection_before)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum LastAction {
    Typing,
    Delete,
    Paste,
    Other,
}

pub struct Editor {
    pub buffer: String,
    pub caret: usize,
    pub selection: Option<(usize, usize)>,
    pub view_row: usize,
    pub view_col: usize, // horizontal scroll column (in chars)
    preferred_col: usize, // "goal column" for vertical movement
    pub focus: bool,
    pub dirty: bool,
    pub autocomplete: crate::autocomplete::Autocomplete,
    pub schema_cache: Option<crate::schema_cache::SchemaCache>,
    last_autocomplete_update: Option<Instant>,
    autocomplete_pending: bool,
    drag_anchor: Option<usize>,
    clipboard: ClipboardContext,
    
    // Delta-based undo/redo
    undo_stack: Vec<EditGroup>,
    redo_stack: Vec<EditGroup>,
    current_group: Option<EditGroup>,
    last_action: Option<LastAction>,
    last_action_time: Option<Instant>,
    
    // Paste detection
    in_find_jump: bool,
    
    pub last_edit_time: Option<Instant>,
    last_clip: Option<String>,
    pub find_cursor_pos: usize,
    pub find_active: bool,
    pub find_query: String,
    pub find_matches: Vec<(usize, usize)>, // (start, end) byte offsets
    pub find_current: usize,
    pub replace_query: String,
    pub find_replace_mode: bool, // false = editing find, true = editing replace
    pub viewport_height: usize,
    pub viewport_width: usize,
    /* ─── double-click helper ─────────────────────────────── */
    last_click_time: Option<Instant>,
    last_click_cell: Option<(usize /*row*/, usize /*col*/ )>,
    /* ─── bracket matching ─────────────────────────────── */
    bracket_match: Option<(usize, usize)>, // (start, end) of matched bracket content
}

impl Editor {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            caret: 0,
            selection: None,
            view_row: 0,
            view_col: 0,
            preferred_col: 0,
            focus: true,
            dirty: false,
            drag_anchor: None,
            clipboard: ClipboardContext::new().unwrap(),
            last_autocomplete_update: None,
            autocomplete_pending: false,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            current_group: None,
            last_action: None,
            last_action_time: None,
            in_find_jump: false,
            last_edit_time: None,
            last_clip: None,
            find_cursor_pos: 0,
            find_active: false,
            find_query: String::new(),
            find_matches: Vec::new(),
            find_current: 0,
            replace_query: String::new(),
            find_replace_mode: false,
            viewport_height: 7,
            viewport_width: 120,
            last_click_time: None,
            last_click_cell: None,
            bracket_match: None,
            autocomplete: crate::autocomplete::Autocomplete::new(),
            schema_cache: None,
        }
    }

    fn line_start_offset(&self, line_idx: usize) -> usize {
        self.buffer
            .lines()
            .take(line_idx)
            .map(|l| l.len() + 1)          // +1 for '\n'
            .sum()
    }

    /// Convert byte offset within a line to character column
    fn byte_offset_to_char_col(&self, line: &str, byte_offset: usize) -> usize {
        let mut char_col = 0;
        let mut byte_pos = 0;
        
        for ch in line.chars() {
            if byte_pos >= byte_offset {
                break;
            }
            byte_pos += ch.len_utf8();
            char_col += 1;
        }
        char_col
    }


    pub fn set_viewport_size(&mut self, h: usize, w: usize) {
        self.viewport_height = h;
        self.viewport_width = w;
    }
    
    /// Call this when the editor loses focus
    pub fn on_focus_lost(&mut self) {
        // Commit any pending group and break action grouping
        self.commit_current_group();
        self.last_action = None;
    }
    
    /*─────────────────────────────────────────────────────────────
      Indent every line in [start_line , end_line]               */
    fn indent_lines(&mut self, start_line: usize, end_line: usize) {
        self.start_group();
        
        // Calculate how much the selection needs to be adjusted
        let mut added_before_anchor = 0usize;
        let mut added_before_cursor = 0usize;
        
        if let Some((anchor, cursor)) = self.selection {
            // Count how many characters will be added before anchor and cursor
            for line in start_line..=end_line {
                let line_start = self.line_start_offset(line);
                if line_start <= anchor {
                    added_before_anchor += INDENT.len();
                }
                if line_start <= cursor {
                    added_before_cursor += INDENT.len();
                }
            }
        }
        
        // Do the actual indenting (bottom-up to keep offsets valid)
        for line in (start_line..=end_line).rev() {
            let off = self.line_start_offset(line);
            self.insert_at(off, INDENT);
        }
        
        // Adjust selection to account for inserted characters
        if let Some((anchor, cursor)) = self.selection {
            self.selection = Some((
                anchor + added_before_anchor,
                cursor + added_before_cursor
            ));
        }
        
        // Adjust caret position
        if self.caret > 0 {
            let mut added_before_caret = 0usize;
            for line in start_line..=end_line {
                let line_start = self.line_start_offset(line);
                if line_start <= self.caret {
                    added_before_caret += INDENT.len();
                }
            }
            self.caret += added_before_caret;
        }
        
        self.commit_current_group();
    }

    /*─────────────────────────────────────────────────────────────
      Dedent every line in [start_line , end_line] (≤ 4 spaces)   */
    fn dedent_lines(&mut self, start_line: usize, end_line: usize) {
        self.start_group();
        
        // Calculate how much will be removed and adjust selection
        let mut removed_before_anchor = 0usize;
        let mut removed_before_cursor = 0usize;
        let mut removed_before_caret = 0usize;
        
        // First pass: calculate removals
        for line in start_line..=end_line {
            let off = self.line_start_offset(line);
            let mut to_remove = 0usize;
            while to_remove < INDENT.len()
                && off + to_remove < self.buffer.len()
                && self.buffer.as_bytes()[off + to_remove] == b' '
            {
                to_remove += 1;
            }
            
            if to_remove > 0 {
                if let Some((anchor, cursor)) = self.selection {
                    if off <= anchor {
                        removed_before_anchor += to_remove;
                    }
                    if off <= cursor {
                        removed_before_cursor += to_remove;
                    }
                }
                if off <= self.caret {
                    removed_before_caret += to_remove;
                }
            }
        }
        
        // Second pass: do the actual dedenting (bottom-up)
        for line in (start_line..=end_line).rev() {
            let off = self.line_start_offset(line);
            let mut removed = 0usize;
            while removed < INDENT.len()
                && off + removed < self.buffer.len()
                && self.buffer.as_bytes()[off + removed] == b' '
            {
                removed += 1;
            }
            if removed == 0 { continue; }
            let text = self.buffer[off..off + removed].to_string();
            self.delete_range(off, off + removed, text);
        }
        
        // Adjust selection
        if let Some((anchor, cursor)) = self.selection {
            self.selection = Some((
                anchor.saturating_sub(removed_before_anchor),
                cursor.saturating_sub(removed_before_cursor)
            ));
        }
        
        // Adjust caret
        self.caret = self.caret.saturating_sub(removed_before_caret);
        
        self.commit_current_group();
    }

    /// Convert any absolute **byte offset** in `self.buffer` to (line, col).
    fn offset_to_line_col(&self, offset: usize) -> (usize, usize) {
        let mut idx  = 0;
        let mut line = 0;

        for l in self.buffer.lines() {
            let len = l.len();
            if offset <= idx + len {
                let byte_offset = offset - idx;
                return (line, self.byte_offset_to_char_col(l, byte_offset));
            }
            idx  += len + 1;                         // +1 for '\n'
            line += 1;
        }
        (line, 0)                                    // offset is EOF
    }

    /* ────────────────────────────────────────────────────────────────
       Helper: byte-offset range of the paragraph that contains `line`
    ─────────────────────────────────────────────────────────────── */
    fn paragraph_byte_range(&self, line: usize) -> (usize, usize) {
        let lines: Vec<&str> = self.buffer.lines().collect();

        /* walk ↑ while we're still inside a non-blank line */
        let mut start_line = line;
        while start_line > 0 && !lines[start_line - 1].trim().is_empty() {
            start_line -= 1;
        }

        /* walk ↓ the same way */
        let mut end_line = line;
        while end_line + 1 < lines.len() && !lines[end_line + 1].trim().is_empty() {
            end_line += 1;
        }

        /* convert [start_line , end_line] → byte offsets */
        let mut start = 0usize;
        for l in 0..start_line       { start += lines[l].len() + 1; }
        let mut end = start;
        for l in start_line..=end_line { end += lines[l].len() + 1; }
        // keep the trailing '\n' – makes d/y line-wise
        (start, end.min(self.buffer.len()))
    }

    fn update_find_matches(&mut self) {
        self.find_matches.clear();
        if self.find_query.len() < 2 { return; } // Minimum 2 characters
        
        // For very large files, limit search to a reasonable window
        let query_lower = self.find_query.to_lowercase();
        
        // Search entire buffer if it's reasonably sized
        if self.buffer.len() < 1_000_000 { // 1MB
            let buffer_lower = self.buffer.to_lowercase();
            let mut start = 0;
            while let Some(pos) = buffer_lower[start..].find(&query_lower) {
                let match_start = start + pos;
                let match_end = match_start + self.find_query.len();
                self.find_matches.push((match_start, match_end));
                start = match_start + 1;
            }
        } else {
            // For larger files, search a window around the current position
            let center = self.caret;
            let search_start = center.saturating_sub(100_000); // 100KB before
            let search_end = (center + 100_000).min(self.buffer.len()); // 100KB after
            
            let search_slice = &self.buffer[search_start..search_end];
            let search_lower = search_slice.to_lowercase();
            
            let mut start = 0;
            while let Some(pos) = search_lower[start..].find(&query_lower) {
                let match_start = search_start + start + pos;
                let match_end = match_start + self.find_query.len();
                self.find_matches.push((match_start, match_end));
                start = start + pos + 1;
            }
        }
        
        // Position current match at or after caret
        self.find_current = 0;
        for (i, &(start, _)) in self.find_matches.iter().enumerate() {
            if start >= self.caret {
                self.find_current = i;
                break;
            }
        }
    }
    
    fn jump_to_match(&mut self, idx: usize) {
        if let Some(&(start, end)) = self.find_matches.get(idx) {
            self.caret = start;  // Put cursor at start of match
            self.selection = Some((end, start));  // Select from end to start
            
            // Center the match in the viewport
            let (line, _col) = self.offset_to_line_col(start);

            // For horizontal centering, we need the character column, not byte offset
            let line_text = self.buffer.lines().nth(line).unwrap_or("");
            let char_col = self.byte_offset_to_char_col(line_text, start - self.line_start_offset(line));
            
            // Vertical centering
            if self.viewport_height > 0 {
                let center_row = self.viewport_height / 2;
                self.view_row = line.saturating_sub(center_row);
            }
            
            // Horizontal centering
            if self.viewport_width > 0 {
                // Calculate match length in characters, not bytes
                let match_text = &self.buffer[start..end];
                let match_char_len = match_text.chars().count();
                let match_center = char_col + match_char_len / 2;
                let center_col = self.viewport_width / 2;
                self.view_col = match_center.saturating_sub(center_col);
            }
            
            // Set flag to prevent nudge_view_to_caret from overriding
            self.in_find_jump = true;
            self.update_bracket_match();
        }
    }

    fn replace_current(&mut self) {
        if self.find_matches.is_empty() || self.find_current >= self.find_matches.len() {
            return;
        }
        
        // Save checkpoint before replace
        self.checkpoint(LastAction::Other);
      
        let (start, end) = self.find_matches[self.find_current];
        let replace_text = self.replace_query.clone(); // Clone to avoid borrow issues
        
        self.start_group();
        let old_text = self.buffer[start..end].to_string();
        self.delete_range(start, end, old_text);
        self.insert_at(start, &replace_text);
        self.commit_current_group();
        
        // Recalculate matches after replacement
        self.update_find_matches();
        
        // Move to next match if available
        if !self.find_matches.is_empty() {
            // Find the next match at or after current position
            for (i, &(match_start, _)) in self.find_matches.iter().enumerate() {
                if match_start >= self.caret {
                    self.find_current = i;
                    self.jump_to_match(i);
                    return;
                }
            }
            // If no match after cursor, stay at last match
            self.find_current = self.find_matches.len() - 1;
            self.jump_to_match(self.find_current);
        }
    }

    fn replace_all(&mut self) {
        if self.find_matches.is_empty() {
            return;
        }
        
        // Save checkpoint before replace all
        self.checkpoint(LastAction::Other);
        self.start_group();
        
        // Clone the data we need to avoid borrow conflicts
        let matches: Vec<(usize, usize)> = self.find_matches.iter().rev().copied().collect();
        let replace_text = self.replace_query.clone();
        
        // Replace from end to start to maintain indices
        for (start, end) in matches {
            let old_text = self.buffer[start..end].to_string();
            self.delete_range(start, end, old_text);
            self.insert_at(start, &replace_text);
        }
        
        self.commit_current_group();
        
        // Clear matches after replacing all
        self.find_matches.clear();
        self.find_current = 0;
    }
    
    fn handle_find_input(&mut self, key: KeyEvent) -> bool {
        if !self.find_active { return false; }
        
        match key.code {
            KeyCode::Tab => {
                self.find_replace_mode = !self.find_replace_mode;
                self.find_cursor_pos = if self.find_replace_mode {
                    self.replace_query.len()
                } else {
                    self.find_query.len()
                };
                return true;
            }
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                if self.find_replace_mode {
                    self.replace_query.insert(self.find_cursor_pos, ch);
                    self.find_cursor_pos += 1;
                } else {
                    self.find_query.insert(self.find_cursor_pos, ch);
                    self.find_cursor_pos += 1;
                    if self.find_query.len() >= 2 {
                        self.update_find_matches();
                        if !self.find_matches.is_empty() {
                            self.jump_to_match(self.find_current);
                        }
                    }
                }
                return true;
            }
            KeyCode::Backspace => {
                if self.find_replace_mode {
                    if self.find_cursor_pos > 0 {
                        self.find_cursor_pos -= 1;
                        self.replace_query.remove(self.find_cursor_pos);
                    }
                } else {
                    if self.find_cursor_pos > 0 {
                        self.find_cursor_pos -= 1;
                        self.find_query.remove(self.find_cursor_pos);
                        if self.find_query.len() >= 2 {
                            self.update_find_matches();
                            if !self.find_matches.is_empty() {
                                self.jump_to_match(self.find_current);
                            }
                        } else {
                            // Clear matches if query is too short
                            self.find_matches.clear();
                            self.find_current = 0;
                        }
                    }
                }
                return true;
            }
            KeyCode::Esc => {
                self.find_active = false;
                self.find_query.clear();
                self.replace_query.clear();
                self.find_matches.clear();
                self.find_current = 0;
                self.find_replace_mode = false;
                return true;
            }
            _ => {}
        }
        false
    }

    // Delta-based undo/redo operations
    fn start_group(&mut self) {
        if self.current_group.is_none() {
            self.current_group = Some(EditGroup::new(self.caret, self.selection));
        }
    }
    
    fn commit_current_group(&mut self) {
        if let Some(group) = self.current_group.take() {
            if !group.is_empty() {
                self.undo_stack.push(group);
                if self.undo_stack.len() > 1000 { // Limit stack size
                    self.undo_stack.remove(0);
                }
                self.redo_stack.clear();
            }
        }
    }
    
    fn insert_at(&mut self, pos: usize, text: &str) {
        self.buffer.insert_str(pos, text);
        if let Some(ref mut group) = self.current_group {
            group.add_op(
                EditOp::Insert { pos, text: text.to_string() },
                self.caret,
                self.selection
            );
        }
    }
    
    fn delete_range(&mut self, start: usize, end: usize, deleted_text: String) {
        self.buffer.replace_range(start..end, "");
        if let Some(ref mut group) = self.current_group {
            group.add_op(
                EditOp::Delete { pos: start, text: deleted_text },
                self.caret,
                self.selection
            );
        }
    }
    
    // Simplified checkpoint system
    fn checkpoint(&mut self, action: LastAction) {
        // Decide if we should merge with the last group
        let should_merge = match (&self.last_action, &action, &self.current_group) {
            // Continue with current group for same action type within time window
            (Some(LastAction::Typing), LastAction::Typing, Some(_)) => {
                self.last_action_time
                    .map(|t| t.elapsed() < Duration::from_secs(1))
                    .unwrap_or(false)
            }
            (Some(LastAction::Delete), LastAction::Delete, Some(_)) => {
                self.last_action_time
                    .map(|t| t.elapsed() < Duration::from_secs(1))
                    .unwrap_or(false)
            }
            // Start new group for different actions
            _ => false,
        };
        
        if !should_merge {
            self.commit_current_group();
            self.start_group();
        }
        
        self.last_action = Some(action);
        self.last_action_time = Some(Instant::now());
        self.last_edit_time = Some(Instant::now());
    }
    
    fn undo(&mut self) {
        self.commit_current_group();
        
        if let Some(group) = self.undo_stack.pop() {
            let (new_caret, new_selection) = group.revert(&mut self.buffer);
            self.caret = new_caret;
            self.selection = new_selection;
            self.redo_stack.push(group);
        }
        
        self.last_action = None;
        self.nudge_view_to_caret();
        self.update_bracket_match();
    }
    
    fn redo(&mut self) {
        if let Some(group) = self.redo_stack.pop() {
            let (new_caret, new_selection) = group.apply(&mut self.buffer);
            self.caret = new_caret;
            self.selection = new_selection;
            self.undo_stack.push(group);
        }
        
        self.last_action = None;
        self.nudge_view_to_caret();
        self.update_bracket_match();
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if key.kind != KeyEventKind::Press {
            return false;
        }

        // Handle autocomplete mode
        if self.autocomplete.active {
            match key.code {
                KeyCode::Up => { self.autocomplete.move_up(); return false; }
                KeyCode::Down => { self.autocomplete.move_down(); return false; }
                KeyCode::Tab | KeyCode::Enter => {
                    if let Some((start, end, completion)) = self.autocomplete.accept_suggestion() {
                        self.checkpoint(LastAction::Other);
                        self.start_group();
                        if end > start {
                            let deleted = self.buffer[start..end].to_string();
                            self.delete_range(start, end, deleted);
                        }
                        self.caret = start;
                        self.insert_at(start, &completion);
                        self.caret = start + completion.len();
                        self.commit_current_group();
                        self.autocomplete.active = false;
                        self.nudge_view_to_caret();
                    }
                    return false;
                }
                KeyCode::Esc => { self.autocomplete.active = false; return false; }
                _ => { self.autocomplete.active = false; }
            }
        }

        // Handle find mode completely separately to avoid borrow conflicts
        if self.find_active {
            match (key.code, key.modifiers) {
                (KeyCode::Char('f'), KeyModifiers::CONTROL) => {
                    self.find_active = false;
                    self.find_query.clear();
                    self.find_matches.clear();
                    self.find_current = 0;
                    return false;
                }
                (KeyCode::Char(ch), mods) if (ch == 'g' || ch == 'G') && 
                    mods.contains(KeyModifiers::CONTROL) && 
                    mods.contains(KeyModifiers::SHIFT) => {
                    if !self.find_matches.is_empty() {
                        self.find_current = self.find_current.checked_sub(1)
                            .unwrap_or(self.find_matches.len() - 1);
                        self.jump_to_match(self.find_current);
                    } else if self.find_query.len() >= 2 {
                        // Try to update matches if we have a valid query
                        self.update_find_matches();
                        if !self.find_matches.is_empty() {
                            self.find_current = self.find_matches.len() - 1;
                            self.jump_to_match(self.find_current);
                        }
                    }
                    return false;
                }

                (KeyCode::Char(ch), mods) if (ch == 'h' || ch == 'H') && 
                    mods.contains(KeyModifiers::CONTROL) && 
                    mods.contains(KeyModifiers::SHIFT) => {
                    self.replace_all();
                    self.find_active = false;
                    self.find_query.clear();
                    self.replace_query.clear();
                    self.find_matches.clear();
                    self.find_current = 0;
                    self.find_replace_mode = false;
                    return false;
                }
                (KeyCode::Char('g') | KeyCode::Char('G'), KeyModifiers::CONTROL) => {
                    if !self.find_matches.is_empty() {
                        self.find_current = (self.find_current + 1) % self.find_matches.len();
                        self.jump_to_match(self.find_current);
                    } else if self.find_query.len() >= 2 {
                        // Try to update matches if we have a valid query
                        self.update_find_matches();
                        if !self.find_matches.is_empty() {
                            self.jump_to_match(self.find_current);
                        }
                    }
                    return false;
                }
                (KeyCode::Char('h') | KeyCode::Char('H'), KeyModifiers::CONTROL) => {
                    if self.find_query.len() >= 2 {
                        self.replace_current();
                    }
                    return false;
                }
                _ => {
                    if self.handle_find_input(key) {
                        return false;
                    }
                }
            }
        }

        let old_caret = self.caret;
        let shift = key.modifiers.intersects(KeyModifiers::SHIFT);
      
        // Navigation keys break action grouping
        match key.code {
            KeyCode::Left | KeyCode::Right | KeyCode::Up | KeyCode::Down |
            KeyCode::Home | KeyCode::End | KeyCode::PageUp | KeyCode::PageDown => {
                self.commit_current_group();
                self.last_action = None;
            }
            _ => {}
        }
        
        let lines: Vec<&str> = self.buffer.lines().collect();
        let (cur_line, cur_col) = self.caret_line_col();
        
        // Find/search key bindings (when not in find mode)
        match (key.code, key.modifiers) {
            (KeyCode::Char('f') | KeyCode::Char('F'), KeyModifiers::CONTROL) => {
                self.find_active = true;
                self.commit_current_group();
                self.last_action = None; // Break any action grouping
                // Pre-fill with current selection if any
                if let Some(sel) = self.selection_range() {
                    if sel.end - sel.start < 100 { // reasonable size
                        self.find_query = self.buffer[sel].to_string();
                        self.find_cursor_pos = self.find_query.len();
                    }
                } else {
                    self.find_cursor_pos = self.find_query.len();
                }
                // Only update matches if query is long enough
                if self.find_query.len() >= 2 {
                    self.update_find_matches();
                }
                return false;
            }
            _ => {}
        }

        match (key.code, key.modifiers) {
            (KeyCode::Char('a') | KeyCode::Char('A'), KeyModifiers::CONTROL) => {
                if !self.buffer.is_empty() {
                    self.selection = Some((0, self.buffer.len()));
                    self.caret = self.buffer.len();
                }
                self.preferred_col = cur_col;
            }
            (KeyCode::Char('z') | KeyCode::Char('Z'), KeyModifiers::CONTROL) => {
                self.undo();
                self.preferred_col = self.caret_line_col().1;
            }
            (KeyCode::Char('y') | KeyCode::Char('Y'), KeyModifiers::CONTROL) => {
                self.redo();
                self.preferred_col = self.caret_line_col().1;
            }
            (KeyCode::Char(' '), KeyModifiers::CONTROL) => {
                // Manually trigger autocomplete
                self.autocomplete.update_suggestions(&self.buffer, self.caret, self.schema_cache.as_ref());
                self.preferred_col = cur_col;
            }
            (KeyCode::Char('v') | KeyCode::Char('V'), KeyModifiers::CONTROL) => {
                // 1️⃣  Get clipboard text via copypasta
                let mut clip = self.clipboard.get_contents().unwrap_or_default();

                // 2️⃣  Fall back to the "last_clip" cache (Wayland / RDP quirks)
                if clip.is_empty() {
                    if let Some(ref cached) = self.last_clip {
                        clip = cached.clone();
                    }
                }

                clip = clip.replace("\r\n", "\n");

                // 3️⃣  If it's ONLY whitespace, strip CR/LF but keep spaces/tabs
                if clip.chars().all(|c| matches!(c, ' ' | '\t' | '\r' | '\n')) {
                    clip.retain(|c| c != '\r' && c != '\n');
                }

                // 4️⃣  Nothing left?  Bail out early.
                if clip.is_empty() {
                    return false;
                }

                // 5️⃣  Insert pasted text directly as a single operation
                self.checkpoint(LastAction::Paste);
                self.start_group();
                self.erase_selection();
                self.insert_at(self.caret, &clip);
                self.caret += clip.len();
                self.commit_current_group();
                
                self.last_clip = Some(clip);
                self.preferred_col = self.caret_line_col().1;
                self.nudge_view_to_caret();
                self.update_bracket_match();
            }

            (KeyCode::Char('x') | KeyCode::Char('X'), KeyModifiers::CONTROL) => {
                if self.try_delete_selection() {
                    // selection already copied inside try_delete_selection()
                } else { /* … existing "cut current line" fallback … */ }
            }

            (KeyCode::Char('c') | KeyCode::Char('C'), KeyModifiers::CONTROL) => {
                if let Some(sel) = self.selection_range() {
                    if sel.start != sel.end {
                        let text = self.buffer[sel.clone()].to_string();
                        let _ = self.clipboard.set_contents(text.clone());
                        self.last_clip = Some(text);
                    }
                }
                self.preferred_col = cur_col;
            }
            (KeyCode::Char(ch), mods)
                if !mods.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER) =>
            {
                self.checkpoint(LastAction::Typing);
                self.insert(&ch.to_string());
                self.clear_sel();
                self.preferred_col = self.caret_line_col().1;

                // Trigger autocomplete
                if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' {
                    // Debounce autocomplete updates
                    self.autocomplete_pending = true;
                    if let Some(last) = self.last_autocomplete_update {
                        if last.elapsed() > Duration::from_millis(150) {
                            self.autocomplete.update_suggestions(&self.buffer, self.caret, self.schema_cache.as_ref());
                            self.last_autocomplete_update = Some(Instant::now());
                            self.autocomplete_pending = false;
                        }
                    } else {
                        self.autocomplete.update_suggestions(&self.buffer, self.caret, self.schema_cache.as_ref());
                        self.last_autocomplete_update = Some(Instant::now());
                    }
                }
            }

            (KeyCode::Backspace, _) => {
                self.checkpoint(LastAction::Delete);
                if !self.erase_selection() {
                    self.backspace();
                }
                self.clear_sel();
                self.preferred_col = self.caret_line_col().1;
            }
            (KeyCode::Delete, _) => {
                self.checkpoint(LastAction::Delete);
                if !self.erase_selection() {
                    self.delete();
                }
                self.clear_sel();
                self.preferred_col = self.caret_line_col().1;
            }
            (KeyCode::Enter, _) => {
                self.checkpoint(LastAction::Other);
                self.insert("\n");
                self.clear_sel();
                self.preferred_col = self.caret_line_col().1;
            }
            (KeyCode::Left, _) => {
                if shift {
                    /* regular "extend selection to the left" behaviour */
                    let prev = self.buffer[..self.caret]
                        .char_indices()
                        .rev()
                        .next()
                        .map(|(i, _)| i)
                        .unwrap_or(0);
                    if self.selection.is_none() {
                        self.selection = Some((self.caret, prev));
                    } else if let Some((anchor, _)) = self.selection {
                        self.selection = Some((anchor, prev));
                    }
                    self.caret = prev;
                } else {
                    /* NEW: collapse to *start* of selection if it exists */
                    if let Some(sel) = self.selection_range() {
                        self.caret = sel.start;
                    } else {
                        let prev = self.buffer[..self.caret]
                            .char_indices()
                            .rev()
                            .next()
                            .map(|(i, _)| i)
                            .unwrap_or(0);
                        self.caret = prev;
                    }
                    self.clear_sel();
                }
                self.preferred_col = self.caret_line_col().1;
            }
            (KeyCode::Right, _) => {
                if !shift {
                    if let Some(sel) = self.selection_range() {
                        self.caret = sel.end;
                    } else {
                        self.caret = self.buffer[self.caret..]
                            .char_indices()
                            .nth(1)
                            .map(|(i, _)| self.caret + i)
                            .unwrap_or(self.buffer.len());
                    }
                    self.clear_sel();
                } else {
                    let next = self.buffer[self.caret..]
                        .char_indices()
                        .nth(1)
                        .map(|(i, _)| self.caret + i)
                        .unwrap_or(self.buffer.len());
                    if self.selection.is_none() {
                        self.selection = Some((self.caret, next));
                    } else if let Some((anchor, _)) = self.selection {
                        self.selection = Some((anchor, next));
                    }
                    self.caret = next;
                }
                self.preferred_col = self.caret_line_col().1;
            }
            // ────────────────────────── Arrow keys ───────────────────────────
            (KeyCode::Up, _) => {
                if cur_line == 0 {
                    // already on the first line
                    if shift {
                        let anchor = self.selection.map(|(a, _)| a).unwrap_or(self.caret);
                        self.caret = 0;
                        self.selection = Some((anchor, self.caret));
                    } else {
                        self.caret = 0;
                        self.clear_sel();
                    }
                    self.preferred_col = 0;
                } else {
                    // move one line up, keep preferred column
                    let target_line = cur_line - 1;
                    let new_col     = min(self.preferred_col, lines[target_line].len());

                    // absolute offset of (target_line, new_col)
                    let mut offset = 0;
                    for (i, l) in lines.iter().enumerate() {
                            if i == target_line { 
                                // Convert character column to byte offset
                                let mut byte_offset = 0;
                                let mut char_pos = 0;
                                for ch in l.chars() {
                                    if char_pos >= new_col {
                                        break;
                                    }
                                    byte_offset += ch.len_utf8();
                                    char_pos += 1;
                                }
                                offset += byte_offset;
                                break; 
                            }
                        offset += l.len() + 1;              // +1 for '\n'
                    }

                    if shift {
                        let anchor = self.selection.map(|(a, _)| a).unwrap_or(self.caret);
                        self.caret = offset;
                        self.selection = Some((anchor, self.caret));
                    } else {
                        self.caret = offset;
                        self.clear_sel();
                    }
                }
            }

            (KeyCode::Down, _) => {
                if cur_line + 1 >= lines.len() {
                    // already on the last line
                    let eof_col = lines.last().map(|l| l.len()).unwrap_or(0); // compute BEFORE mut-borrow

                    if shift {
                        let anchor = self.selection.map(|(a, _)| a).unwrap_or(self.caret);
                        self.caret = self.buffer.len();
                        self.selection = Some((anchor, self.caret));
                    } else {
                        self.caret = self.buffer.len();
                        self.clear_sel();                           // mutable borrow happens here
                    }
                    self.preferred_col = eof_col;                   // immutable data already saved
                } else {
                    // move one line down, keep preferred column
                    let target_line = cur_line + 1;
                    let new_col     = min(self.preferred_col, lines[target_line].len());

                    // absolute offset of (target_line, new_col)
                    let mut offset = 0;
                    for (i, l) in lines.iter().enumerate() {
                            if i == target_line { 
                                // Convert character column to byte offset
                                let mut byte_offset = 0;
                                let mut char_pos = 0;
                                for ch in l.chars() {
                                    if char_pos >= new_col {
                                        break;
                                    }
                                    byte_offset += ch.len_utf8();
                                    char_pos += 1;
                                }
                                offset += byte_offset;
                                break; 
                            }
                        offset += l.len() + 1;                      // +1 for '\n'
                    }

                    if shift {
                        let anchor = self.selection.map(|(a, _)| a).unwrap_or(self.caret);
                        self.caret = offset;
                        self.selection = Some((anchor, self.caret));
                    } else {
                        self.caret = offset;
                        self.clear_sel();
                    }
                }
            }

            (KeyCode::Home, _) => {
                let (line, _) = self.caret_line_col();
                let lines: Vec<_> = self.buffer.lines().collect();
                let offset = lines.iter().take(line).map(|l| l.len() + 1).sum::<usize>();
                let _line_len = lines[line].len();
                if shift {
                    if self.selection.is_none() {
                        self.selection = Some((self.caret, offset));
                    } else if let Some((anchor, _)) = self.selection {
                        self.selection = Some((anchor, offset));
                    }
                    self.caret = offset;
                } else {
                    self.caret = offset;
                    self.preferred_col = 0;
                    self.clear_sel();
                }
            }
            (KeyCode::End, _) => {
                let (line, _) = self.caret_line_col();
                let lines: Vec<_> = self.buffer.lines().collect();
                let line_len = lines[line].len();
                let offset = lines.iter().take(line).map(|l| l.len() + 1).sum::<usize>() + line_len;
                if shift {
                    if self.selection.is_none() {
                        self.selection = Some((self.caret, offset));
                    } else if let Some((anchor, _)) = self.selection {
                        self.selection = Some((anchor, offset));
                    }
                    self.caret = offset;
                } else {
                    self.caret = offset;
                    self.preferred_col = line_len;
                    self.clear_sel();
                }
            }
            (KeyCode::PageUp, _) => {
                let (line, _) = self.caret_line_col();
                let lines: Vec<_> = self.buffer.lines().collect();
                let new_line = line.saturating_sub(self.viewport_height.max(1));
                let new_col = min(self.preferred_col, lines.get(new_line).map(|l| l.len()).unwrap_or(0));
                let offset = lines.iter().take(new_line).map(|l| l.len() + 1).sum::<usize>() + new_col;
                if shift {
                    if self.selection.is_none() {
                        self.selection = Some((self.caret, offset));
                    } else if let Some((anchor, _)) = self.selection {
                        self.selection = Some((anchor, offset));
                    }
                    self.caret = offset;
                } else {
                    self.caret = offset;
                    self.clear_sel();
                }
            }
            (KeyCode::PageDown, _) => {
                let (line, _) = self.caret_line_col();
                let lines: Vec<_> = self.buffer.lines().collect();
                let new_line = min(line + self.viewport_height.max(1), lines.len() - 1);
                let new_col = min(self.preferred_col, lines.get(new_line).map(|l| l.len()).unwrap_or(0));
                let offset = lines.iter().take(new_line).map(|l| l.len() + 1).sum::<usize>() + new_col;
                if shift {
                    if self.selection.is_none() {
                        self.selection = Some((self.caret, offset));
                    } else if let Some((anchor, _)) = self.selection {
                        self.selection = Some((anchor, offset));
                    }
                    self.caret = offset;
                } else {
                    self.caret = offset;
                    self.clear_sel();
                }
            }
            /*──────────────── Tab / Shift+Tab ────────────────*/
            (KeyCode::Tab, mods) | (KeyCode::BackTab, mods) => {
                let shift = mods.intersects(KeyModifiers::SHIFT);
                
                self.checkpoint(LastAction::Other);

                if let Some(sel) = self.selection_range() {
                    // 1️⃣  Inclusive first / last offsets of the visible block
                    let (start_line, _) = self.offset_to_line_col(sel.start);
                    let (end_line, _) = self.offset_to_line_col(sel.end.saturating_sub(1));

                    if shift { self.dedent_lines(start_line, end_line); }
                    else     { self.indent_lines(start_line, end_line); }
                } else {
                    // no multi-line selection → single-line / caret mode
                    if shift {
                        let (line, _col) = self.caret_line_col();
                        self.dedent_lines(line, line);
                    } else {
                        self.insert(INDENT);                       // inserts 4 spaces
                    }
                }

                self.preferred_col = self.caret_line_col().1;
            }
            _ => {}
        }
        // Follow the caret only if it actually moved (arrow keys, edits, etc.)
        if self.caret != old_caret && !self.in_find_jump {
            self.nudge_view_to_caret();
        }
        self.in_find_jump = false; // Reset flag
        
        // Update bracket match if caret moved
        if self.caret != old_caret {
            self.update_bracket_match();
        }
      
        false
    }

    pub fn handle_paste(&mut self, pasted: &str) {
        // This is called for bracketed paste mode (when terminal supports it)
        // Our Ctrl+V handling is separate and handles paste directly
        
        // 1️⃣  First try the real clipboard
        let mut clip = self.clipboard.get_contents().unwrap_or_default();

        // 2️⃣  If that failed (Wayland / RDP edge-cases), fall back to the
        //     string the terminal gave us.
        if clip.is_empty() {
            clip = pasted.to_owned();
        }
        // 3️⃣  If *that* is still empty, fall back to what we copied last.
        if clip.is_empty() {
            if let Some(ref cached) = self.last_clip {
                clip = cached.clone();
            }
        }

        // 4️⃣  Special-case "just whitespace + newlines" → strip the newlines
        if clip.chars().all(|c| matches!(c, ' ' | '\t' | '\r' | '\n')) {
            clip.retain(|c| matches!(c, ' ' | '\t'));
        }
        clip = clip.replace("\r\n", "\n");
        if clip.is_empty() { return; }   // nothing left → bail

        // For bracketed paste, use the same delta approach
        self.checkpoint(LastAction::Paste);
        self.start_group();
        self.erase_selection();
        self.insert_at(self.caret, &clip);
        self.caret += clip.len();
        self.commit_current_group();
        
        self.last_clip = Some(clip);
        self.preferred_col = self.caret_line_col().1;
        self.nudge_view_to_caret();
        self.update_bracket_match();
    }

    pub fn handle_mouse(&mut self, event: MouseEvent, area: Rect) {
        use MouseEventKind::*;
        let old_caret = self.caret;

        // ── 1️⃣  Pure scroll events – no hit-test needed ────────────────────
        match event.kind {
            ScrollLeft |
            ScrollUp    if event.modifiers.contains(KeyModifiers::SHIFT) => {
                self.view_col = self.view_col.saturating_sub(H_SCROLL_STEP);
                return;                         // ← skip caret-nudge
            }
            ScrollRight |
            ScrollDown   if event.modifiers.contains(KeyModifiers::SHIFT) => {
                self.view_col += H_SCROLL_STEP;
                return;                         // ← skip caret-nudge
            }
            ScrollUp => {
                self.view_row = self.view_row.saturating_sub(V_SCROLL_STEP);
                return;                         // ← skip caret-nudge
            }
            ScrollDown => {
                let total_lines = self.buffer.lines().count();
                let max_row = total_lines.saturating_sub(self.viewport_height);
                self.view_row = (self.view_row + V_SCROLL_STEP).min(max_row);
                return;                         // ← skip caret-nudge
            }
            _ => {}              // fall through for click / drag logic
        }
        
        // End any active action grouping on mouse click
        if matches!(event.kind, Down(MouseButton::Left)) {
            self.commit_current_group();
            self.last_action = None;
        }

        // ── 2️⃣  Hit-test for click / drag inside the text rectangle ───────
        /* ─── auto-scroll tuning ──────────────────────────────────────── */
        const DRAG_V_SCROLL_STEP: usize = 1;   // rows per tick while dragging past top/bottom
        const DRAG_H_SCROLL_STEP: usize = 3;   // cols per tick while dragging past sides
        const DRAG_V_HOTZONE:     u16   = 1;   // cell band at top/bottom that triggers v-scroll
        const DRAG_H_HOTZONE:     u16   = 4;   // cell band at left/right that triggers h-scroll

        let lines = self.buffer.lines().collect::<Vec<_>>();
        let mx = event.column;
        let my = event.row;

        // text area (inside borders, after the configurable gutter)
        let vx = area.x + GUTTER_WIDTH + 1;         // +1 for the left border
        let vy = area.y + 1;

        // Calculate the actual width of the text area
        let text_area_width = area.width.saturating_sub(GUTTER_WIDTH + 2); // -2 for borders, -GUTTER_WIDTH for gutter

        // Stay strict for non-drag events, but let Drag(..) through so that
        // we can auto-scroll even when the mouse leaves the editor pane.
        let inside_text_rect =
            mx >= vx
            && mx < vx + text_area_width  // FIX: Use the actual text area width
            && my >= vy
            && my < vy + area.height - 2;

        if !inside_text_rect && !matches!(event.kind, Drag(MouseButton::Left)) {
            return;
        }

        // When the pointer is outside the rectangle, clamp the relative
        // co-ordinates so we never underflow (mx < vx) or overflow.
        // relative mouse-coords in *pane cells* (always usize to match view_* types)
        let rel_x: usize = if mx < vx {
            0
        } else if mx >= vx + text_area_width {  // FIX: Use text_area_width instead of complex calculation
            text_area_width.saturating_sub(1) as usize
        } else {
            (mx - vx) as usize
        };
        // Map mouse y to data row
        let rel_y = (my - vy) as usize;

        let mut line_idx = self.view_row + rel_y as usize;
        let mut col_idx  = self.view_col + rel_x as usize;

        // clamp to the buffer size after potential viewport shift
        line_idx = line_idx.min(lines.len().saturating_sub(1));
        let line = lines.get(line_idx).copied().unwrap_or("");
        col_idx = col_idx.min(line.chars().count());  // FIX: Use character count, not byte length

        // compute absolute buffer offset of (line_idx, col_idx)
        let mut offset = 0;
        for (i, l) in lines.iter().enumerate() {
            if i == line_idx {
                // Convert visual column to byte offset
                let mut byte_offset = 0;
                let mut visual_col = 0;
                for ch in l.chars() {
                    if visual_col >= col_idx {
                        break;
                    }
                    byte_offset += ch.len_utf8();
                    visual_col += 1;
                }
                offset += byte_offset;
                break;
            }
            offset += l.len() + 1;  // +1 for '\n'
        }
        offset = offset.min(self.buffer.len());  // Add this line!

        match event.kind {
            Down(MouseButton::Left) => {
                /* ─── double-click detection ───────────────────────────── */
                let now = Instant::now();
                let clicked_cell = (line_idx, col_idx);
                let is_double = self
                    .last_click_time
                    .map_or(false, |t| now.duration_since(t) < Duration::from_millis(300))
                    && self.last_click_cell == Some(clicked_cell);
                self.last_click_time = Some(now);
                self.last_click_cell = Some(clicked_cell);

                if is_double {
                    /* Select the whole word under caret */
                    let mut start = offset.min(self.buffer.len());
                    let mut end   = start;
                    let is_word = |c: char| c.is_alphanumeric() || c == '_';
                    while start > 0 && is_word(self.buffer[..start].chars().rev().next().unwrap()) {
                        start = self.buffer[..start]
                            .char_indices()
                            .rev()
                            .nth(0)
                            .map(|(i, _)| i)
                            .unwrap_or(0);
                    }
                    while end < self.buffer.len() {
                        let ch = self.buffer[end..].chars().next().unwrap();
                        if is_word(ch) {
                            end += ch.len_utf8();
                        } else {
                            break;
                        }
                    }
                    self.selection = Some((start, end));
                    self.caret = end;
                    self.preferred_col = col_idx;
                    self.nudge_view_to_caret();
                    return;
                }

                self.caret     = offset.min(self.buffer.len());
                self.selection = Some((self.caret, self.caret));
                self.drag_anchor = Some(self.caret);
                self.preferred_col = col_idx;
            }
            Drag(MouseButton::Left) => {
                /* ── 1️⃣  auto-scroll if mouse is outside the text box ───── */
                /* ──  vertical auto-scroll  ───────────────────────── */
                if my < vy + DRAG_V_HOTZONE {                           // near/above top edge
                    self.view_row = self.view_row.saturating_sub(DRAG_V_SCROLL_STEP);
                } else if my >= vy + area.height - 2 - DRAG_V_HOTZONE { // near/below bottom
                    let total_lines = self.buffer.lines().count();
                    let max_row = total_lines.saturating_sub(self.viewport_height);
                    self.view_row = (self.view_row + DRAG_V_SCROLL_STEP).min(max_row);
                }
                /* ──  horizontal auto-scroll  ───────────────────────── */
                if mx <  vx + DRAG_H_HOTZONE {                             // near left edge
                    self.view_col = self.view_col.saturating_sub(DRAG_H_SCROLL_STEP);
                } else if mx >= vx + area.width - (GUTTER_WIDTH + 2) - DRAG_H_HOTZONE { // near right edge
                    self.view_col += DRAG_H_SCROLL_STEP;
                }

                /* ── 2️⃣  update caret & selection ──────────────────────── */
                // recompute buffer coords after any viewport shift
                let rel_x = (mx.saturating_sub(vx)) as usize;
                let rel_y = (my.saturating_sub(vy)) as usize;

                let mut line_idx = self.view_row + rel_y.min((area.height - 3) as usize);
                line_idx = line_idx.min(lines.len().saturating_sub(1));

                let line = lines.get(line_idx).copied().unwrap_or("");

                let mut col_idx = self.view_col + rel_x;
                col_idx = col_idx.min(line.chars().count());

                let mut new_offset = 0;
                for (i, l) in lines.iter().enumerate() {
                    if i == line_idx {
                        // Convert visual column to byte offset
                        let mut byte_offset = 0;
                        let mut visual_col = 0;
                        for ch in l.chars() {
                            if visual_col >= col_idx {
                                break;
                            }
                            byte_offset += ch.len_utf8();
                            visual_col += 1;
                        }
                        new_offset += byte_offset;
                        break;
                    }
                    new_offset += l.len() + 1;
                }
                new_offset = new_offset.min(self.buffer.len());  // Add this line!

                self.caret = new_offset.min(self.buffer.len());
                if let Some(anchor) = self.drag_anchor {
                    self.selection = Some((anchor, self.caret));
                }
                self.preferred_col = col_idx;
            }
            Up(MouseButton::Left) => {
                self.drag_anchor = None;
            }
            _ => {}
        }
        /* nudge only if the caret actually moved (click, drag, auto-scroll) */
        if self.caret != old_caret {
            self.nudge_view_to_caret();
            self.update_bracket_match();
        }
    }


    pub fn insert(&mut self, s: &str) {
        self.erase_selection();
        self.dirty = true;
        
        // Use delta operations
        if self.current_group.is_none() {
            self.start_group();
        }
        
        self.insert_at(self.caret, s);
        self.caret += s.len();
        self.clear_sel();
    }
    
    fn backspace(&mut self) {
        self.dirty = true;
        if self.caret == 0 {
            return;
        }
        
        if self.current_group.is_none() {
            self.start_group();
        }
        
        let prev = self.buffer[..self.caret].char_indices().rev().next().map(|(i, _)| i).unwrap_or(0);
        let deleted = self.buffer[prev..self.caret].to_string();
        self.delete_range(prev, self.caret, deleted);
        self.caret = prev;
        self.update_bracket_match();
    }
    
    fn delete(&mut self) {
        self.dirty = true;
        if self.caret >= self.buffer.len() {
            return;
        }
        
        if self.current_group.is_none() {
            self.start_group();
        }
        
        let next = self.buffer[self.caret..].char_indices().nth(1).map(|(i, _)| self.caret + i).unwrap_or(self.buffer.len());
        let deleted = self.buffer[self.caret..next].to_string();
        self.delete_range(self.caret, next, deleted);
        self.update_bracket_match();
    }

    /// Delete current selection **without** touching the clipboard.
    fn erase_selection(&mut self) -> bool {
        if let Some(sel) = self.selection_range() {
            if sel.start != sel.end {
                if self.current_group.is_none() {
                    self.start_group();
                }
                let deleted = self.buffer[sel.clone()].to_string();
                self.delete_range(sel.start, sel.end, deleted);
                self.caret = sel.start;
                self.clear_sel();
                self.dirty = true;
                self.update_bracket_match();
                return true;
            }
        }
        false
    }

    pub fn try_delete_selection(&mut self) -> bool {
        if let Some(sel) = self.selection_range() {
            if sel.start != sel.end {
                let text = self.buffer[sel.clone()].to_string();
                let _ = self.clipboard.set_contents(text.clone());
                self.last_clip = Some(text);

                if self.current_group.is_none() {
                    self.start_group();
                }
                let deleted = self.buffer[sel.clone()].to_string();
                self.delete_range(sel.start, sel.end, deleted);
                self.caret = sel.start;
                self.clear_sel();
                self.dirty = true;
                return true;
            }
        }
        false
    }
    
    pub fn clear_sel(&mut self) {
        self.selection = None;
        self.drag_anchor = None;
    }
    
    pub fn selection_range(&self) -> Option<Range<usize>> {
        self.selection.map(|(a, b)| if a < b { a..b } else { b..a })
    }

    /// Find the innermost matching brackets around the caret
    fn find_bracket_match(&self, caret: usize) -> Option<(usize, usize)> {
        const MAX_SEARCH: usize = 2000;
        
        let bytes = self.buffer.as_bytes();
        if bytes.is_empty() || caret > bytes.len() {
            return None;
        }
        
        // First check if caret is right at a bracket
        // Case 1: Caret is right after a closing bracket
        if caret > 0 {
            match bytes.get(caret - 1) {
                Some(b')') => return self.find_matching_open(caret - 1, b'(', b')'),
                Some(b']') => return self.find_matching_open(caret - 1, b'[', b']'),
                Some(b'}') => return self.find_matching_open(caret - 1, b'{', b'}'),
                _ => {}
            }
        }
        
        // Case 2: Caret is right before an opening bracket
        if caret < bytes.len() {
            match bytes.get(caret) {
                Some(b'(') => return self.find_matching_close(caret, b'(', b')'),
                Some(b'[') => return self.find_matching_close(caret, b'[', b']'),
                Some(b'{') => return self.find_matching_close(caret, b'{', b'}'),
                _ => {}
            }
        }
        
    // Case 3: Find the innermost bracket pair that contains the caret
        let search_start = caret.saturating_sub(MAX_SEARCH);
        let search_end = (caret + MAX_SEARCH).min(bytes.len());
        
        // Parse all brackets in the range and build a proper nesting structure
        #[derive(Debug)]
        struct BracketSpan {
            start: usize,
            end: usize,
            bracket_type: u8, // '(', '[', or '{'
        }
        
        let mut spans: Vec<BracketSpan> = Vec::new();
        let mut stack: Vec<(usize, u8)> = Vec::new();
        
        // Build complete bracket structure
        for i in search_start..search_end {
            match bytes[i] {
                b'(' | b'[' | b'{' => {
                    stack.push((i, bytes[i]));
                }
                b')' => {
                    if let Some(pos) = stack.iter().rposition(|(_, ch)| *ch == b'(') {
                        let (start, _) = stack.remove(pos);
                        spans.push(BracketSpan { start, end: i + 1, bracket_type: b'(' });
                    }
                }
                b']' => {
                    if let Some(pos) = stack.iter().rposition(|(_, ch)| *ch == b'[') {
                        let (start, _) = stack.remove(pos);
                        spans.push(BracketSpan { start, end: i + 1, bracket_type: b'[' });
                    }
                }
                b'}' => {
                    if let Some(pos) = stack.iter().rposition(|(_, ch)| *ch == b'{') {
                        let (start, _) = stack.remove(pos);
                        spans.push(BracketSpan { start, end: i + 1, bracket_type: b'{' });
                    }
                }
                _ => {}
            }
        }
        
        // Find the smallest span that contains the caret
        spans.iter()
            .filter(|span| span.start <= caret && span.end > caret)
            .min_by_key(|span| span.end - span.start)
            .map(|span| (span.start, span.end))
    }
    
    /// Find matching open bracket for a close bracket at the given position
    fn find_matching_open(&self, close_pos: usize, open_ch: u8, close_ch: u8) -> Option<(usize, usize)> {
        const MAX_SEARCH: usize = 200000;
        let bytes = self.buffer.as_bytes();
        let search_start = close_pos.saturating_sub(MAX_SEARCH);
        let mut depth = 1;
        
        for i in (search_start..close_pos).rev() {
            if bytes[i] == close_ch {
                depth += 1;
            } else if bytes[i] == open_ch {
                depth -= 1;
                if depth == 0 {
                    return Some((i, close_pos + 1));
                }
            }
        }
        None
    }
    
    /// Find matching close bracket for an open bracket at the given position
    fn find_matching_close(&self, open_pos: usize, open_ch: u8, close_ch: u8) -> Option<(usize, usize)> {
        const MAX_SEARCH: usize = 200000;
        let bytes = self.buffer.as_bytes();
        let search_end = (open_pos + 1 + MAX_SEARCH).min(bytes.len());
        let mut depth = 1;
        
        for i in open_pos + 1..search_end {
            if bytes[i] == open_ch {
                depth += 1;
            } else if bytes[i] == close_ch {
                depth -= 1;
                if depth == 0 {
                    return Some((open_pos, i + 1));
                }
            }
        }
        None
    }
    
    /// Update bracket match when caret moves
    fn update_bracket_match(&mut self) {
        self.bracket_match = self.find_bracket_match(self.caret);
    }
    
    pub fn caret_line_col(&self) -> (usize, usize) {
        let mut idx = 0;
        let mut line = 0;
        for l in self.buffer.lines() {
            let len = l.len();
            if self.caret <= idx + len {
                let byte_offset = self.caret - idx;
                return (line, self.byte_offset_to_char_col(l, byte_offset));
            }
            idx += l.len() + 1;
            line += 1;
        }
        (line, 0)
    }
    
    fn nudge_view_to_caret(&mut self) {
        let (cy, cx) = self.caret_line_col();

        /* ── vertical ─────────────────────────────────────────────── */
        if cy < self.view_row {
            self.view_row = cy;
        }
        let v_rows = self.viewport_height;
        if cy + V_SCROLL_MARGIN >= self.view_row + v_rows {
            self.view_row = cy + V_SCROLL_MARGIN + 1 - v_rows;
        }

        /* ── horizontal ───────────────────────────────────────────── */
        let v_cols = self.viewport_width;
        if cx < self.view_col + H_SCROLL_MARGIN {
            self.view_col = cx.saturating_sub(H_SCROLL_MARGIN);
        }
        if cx + H_SCROLL_MARGIN >= self.view_col + v_cols {
            self.view_col = cx + H_SCROLL_MARGIN + 1 - v_cols;
        }
    }

    pub fn render<B: Backend>(&self, f: &mut Frame<B>, area: Rect) {
        use tui::{text::*, widgets::*};

        let lines: Vec<&str>   = self.buffer.lines().collect();
        let sel_range          = self.selection_range();
        let (caret_l, caret_c) = self.caret_line_col();
        let vwidth             = self.viewport_width;
        let vheight            = self.viewport_height;

        let mut parse_state = ParseState::Normal;
        let mut rows: Vec<Spans> = Vec::with_capacity(vheight);

        for scr_i in 0..vheight {
            let line_idx   = self.view_row + scr_i;
            let is_caret   = line_idx == caret_l;
            let src_line   = lines.get(line_idx).copied().unwrap_or("");

            // Check if any find matches are on this line
            let line_start_offset = self.line_start_offset(line_idx);
            let line_end_offset = line_start_offset + src_line.len();
            
            // Only collect matches that are actually on this visible line
            let line_matches: Vec<_> = if self.find_active && !self.find_matches.is_empty() {
                self.find_matches.iter()
                    .filter(|(start, end)| *end > line_start_offset && *start < line_end_offset)
                    .map(|(start, end)| (
                        start.saturating_sub(line_start_offset), 
                        (*end).min(line_end_offset) - line_start_offset
                    ))
                    .collect()
            } else {
                Vec::new()
            };
            
            // Check if current match is on this line
            let current_match_on_line = if self.find_active && self.find_current < self.find_matches.len() {
                let (start, end) = self.find_matches[self.find_current];
                end > line_start_offset && start < line_end_offset
            } else {
                false
            };

            // ---------- gutter (abs / rel numbers) ----------
            let abs = (line_idx + 1).to_string();
            let rel = if is_caret {
                abs.clone()
            } else {
                ((line_idx as isize) - (caret_l as isize)).abs().to_string()
            };
            let gutter_style = if is_caret { STYLE::gutter_cur() } else { STYLE::gutter_rel() };
            /*──── gutter (abs/rel numbers) ────*/
            let gutter_digits = (GUTTER_WIDTH as usize).saturating_sub(1);
            let gutter_txt    = format!("{:>width$} ", rel, width = gutter_digits);
            let mut spans: Vec<Span> =
                vec![Span::styled(gutter_txt, gutter_style)];

            // ---------- highlight whole line ----------
            let tokens = highlight_line(src_line, &mut parse_state);

            // absolute buffer offset of first char in full line
            let mut buf_base = 0usize;
            for li in 0..line_idx.min(lines.len()) {
                buf_base += lines[li].len() + 1; // +1 for '\n'
            }

            // ---------- render, skipping view_col ----------
            let mut col      = 0usize;         // col in *full* line
            let mut visible  = 0usize;         // how many columns emitted
            let col_off      = self.view_col;  // horizontal scroll

            for (token, style) in tokens {
                for ch in token.chars() {
                    // skip invisible part
                    if col < col_off { 
                        col += 1; 
                        buf_base += ch.len_utf8();
                        continue; 
                    }
                    if visible >= vwidth { break; }
                    // Check if this character is part of bracket match
                    let in_bracket_match = self.bracket_match
                        .as_ref()
                        .map_or(false, |(start, end)| {
                            buf_base == *start || buf_base == *end - 1  // Only highlight the brackets themselves
                        });
 
                    let mut is_find_match = false;
                    let mut is_current_find = false;
                    
                    // Check if this character is part of a find match
                    if !line_matches.is_empty() {
                        for &(match_start, match_end) in &line_matches {
                            if col >= match_start && col < match_end {
                                is_find_match = true;
                                if current_match_on_line && self.find_current < self.find_matches.len() {
                                    let (curr_start, curr_end) = self.find_matches[self.find_current];
                                    let curr_start_in_line = curr_start.saturating_sub(line_start_offset);
                                    let curr_end_in_line = curr_end.min(line_end_offset) - line_start_offset;
                                    if col >= curr_start_in_line && col < curr_end_in_line {
                                        is_current_find = true;
                                    }
                                }
                                break;
                            }
                        }
                    }
                   
                    // Determine style priority: find match > selection > syntax
                    let mut st = style;
                    if sel_range
                        .as_ref()
                        .map_or(false, |r| r.contains(&(buf_base)))
                    {
                        st = STYLE::selection_bg();
                    } else if in_bracket_match {
                        st = if st.bg.is_some() {
                            // Preserve foreground color from syntax highlighting
                            st.bg(STYLE::bracket_match().bg.unwrap())
                        } else {
                            st.patch(STYLE::bracket_match())
                        };
                    }

                    // ── caret cell? highlight with yellow block ─────────────
                    let caret_here = is_caret && col == caret_c;
                    let cell_style = if is_current_find {
                        Style::default()
                            .fg(rgb(CONFIG_COLORS.find_current_fg))
                            .bg(rgb(CONFIG_COLORS.find_current_bg))
                    } else if is_find_match {
                        Style::default()
                            .fg(rgb(CONFIG_COLORS.find_match_fg))
                            .bg(rgb(CONFIG_COLORS.find_match_bg))
                    } else if caret_here {
                        STYLE::caret_cell()
                    } else {
                        st
                    };

                    spans.push(Span::styled(ch.to_string(), cell_style));

                    col      += 1;
                    buf_base += ch.len_utf8();
                    visible  += 1;
                }
                if visible >= vwidth { break; }
            }
            // caret after end-of-line → draw a yellow block in the "empty" cell
            if is_caret && caret_c >= col && caret_c - col_off < vwidth {
                let pos = caret_c - col_off;
                while spans.len() < pos + 1 { spans.push(Span::raw(" ")); } // +1 for gutter
                spans.insert(
                    pos + 1,
                    Span::styled(" ", STYLE::caret_cell()),
                );
            }

            rows.push(Spans::from(spans));
        }

        /* pick border colour by focus */
        let border_st = if self.focus {
            STYLE::editor_border_focus()
        } else {
            STYLE::editor_border()
        };

        let block = Block::default()
            .title("Editor")
            .borders(Borders::ALL)
            .border_style(border_st);

        // draw the main editor first …
        f.render_widget(Paragraph::new(rows).block(block), area);

        // Render autocomplete dropdown
        // Render autocomplete dropdown
        if self.autocomplete.active && !self.autocomplete.suggestions.is_empty() {
            // Get the word position from autocomplete state
            let word_start_offset = self.autocomplete.word_start;
            let (word_line, word_col) = self.offset_to_line_col(word_start_offset);
            
            // Only render if the word is visible in the viewport
            if word_line >= self.view_row && word_line < self.view_row + self.viewport_height {
                // Calculate dropdown position anchored at word start
                let mut dropdown_y = area.y + 1 + (word_line - self.view_row) as u16 + 1;
                let mut dropdown_x = area.x + GUTTER_WIDTH + 1 + (word_col.saturating_sub(self.view_col)) as u16;
                
                // Calculate dropdown dimensions first
                let max_height = (area.y + area.height - dropdown_y - 1).min(10);
                let suggestions_to_show = self.autocomplete.suggestions.len().min(max_height as usize);
                let visible_suggestions = suggestions_to_show.min(8); // Max 8 visible at once
                
                // Calculate the actual display width including icons and separators
                let max_text_len = self.autocomplete.suggestions.iter()
                    .skip(self.autocomplete.view_offset)
                    .take(visible_suggestions)
                    .map(|s| {
                        // Icon (2) + space (1) + text + potential detail
                        3 + s.text.len() + s.detail.as_ref().map_or(0, |d| d.len() + 3)
                    })
                    .max()
                    .unwrap_or(80);
                
                // Use a more generous width range
                let dropdown_width = (max_text_len as u16 + 6).clamp(70, 200);
                
                // Calculate column positions for alignment
                let max_main_text_len = self.autocomplete.suggestions.iter()
                    .skip(self.autocomplete.view_offset)
                    .take(visible_suggestions)
                    .map(|s| s.text.len())
                    .max()
                    .unwrap_or(20);
                
                // For columns with details, find the longest detail prefix
                let detail_parts: Vec<_> = self.autocomplete.suggestions.iter()
                    .skip(self.autocomplete.view_offset)
                    .take(visible_suggestions)
                    .filter_map(|s| s.detail.as_ref())
                    .filter_map(|d| {
                        if d.contains(" - ") {
                            let parts: Vec<&str> = d.splitn(2, " - ").collect();
                            Some((parts[0].len(), parts.get(1).map(|s| s.to_string())))
                        } else {
                            None
                        }
                    })
                    .collect();
                
                let max_table_len = detail_parts.iter()
                    .map(|(len, _)| *len)
                    .max()
                    .unwrap_or(0);
                
                let dropdown_height = visible_suggestions as u16 + 2;
                
                // Adjust position to keep dropdown within bounds
                if dropdown_x + dropdown_width > area.x + area.width {
                    dropdown_x = (area.x + area.width).saturating_sub(dropdown_width);
                }
                if dropdown_y + dropdown_height > area.y + area.height {
                    // Try to position above the word if there's more room
                    let space_above = (word_line - self.view_row) as u16;
                    if space_above > dropdown_height {
                        dropdown_y = area.y + 1 + (word_line - self.view_row) as u16 - dropdown_height;
                    } else {
                        // Otherwise just clamp it
                        dropdown_y = (area.y + area.height).saturating_sub(dropdown_height);
                    }
                }
                
                // Don't render if dropdown would be outside the editor area
                if dropdown_x >= area.x && dropdown_y >= area.y {
                    // Build suggestion lines
                    let mut lines = Vec::new();
                    
                    for i in 0..visible_suggestions {
                        let suggestion_idx = self.autocomplete.view_offset + i;
                        if suggestion_idx >= self.autocomplete.suggestions.len() {
                            break;
                        }
                        
                        let suggestion = &self.autocomplete.suggestions[suggestion_idx];
                        let is_selected = suggestion_idx == self.autocomplete.selected;
                        
                        // Icon based on suggestion type
                        let icon = match suggestion.kind {
                            crate::autocomplete::SuggestionKind::Keyword => "󰌋",
                            crate::autocomplete::SuggestionKind::Database => "󰆼",
                            crate::autocomplete::SuggestionKind::Schema => "󰙅",
                            crate::autocomplete::SuggestionKind::Table => "󰓫",
                            crate::autocomplete::SuggestionKind::View => "󰈈",
                            crate::autocomplete::SuggestionKind::Column => "󰠵",
                            crate::autocomplete::SuggestionKind::Function => "󰡱",
                            crate::autocomplete::SuggestionKind::Procedure => "󰊕",
                            crate::autocomplete::SuggestionKind::Variable => "󰀫",
                        };
                        
                        // Abbreviate long qualified names
                        let display_text = &suggestion.display_text;
                        
                        // Build formatted text with alignment
                        let mut text = format!("{} {}", icon, display_text);
                        
                        // Add padding for alignment
                        let padding_needed = max_main_text_len + 2 - display_text.len();
                        if padding_needed > 0 && suggestion.detail.is_some() {
                            text.push_str(&" ".repeat(padding_needed));
                        }
                        
                        // Add detail with column alignment
                        if let Some(detail) = &suggestion.detail {
                            if detail.contains(" - ") {
                                let parts: Vec<&str> = detail.splitn(2, " - ").collect();
                                if parts.len() == 2 {
                                    // This is a column with table and type info
                                    let table_padding = max_table_len - parts[0].len();
                                    text.push_str(&format!("│ {} {} - {}", parts[0], " ".repeat(table_padding), parts[1]));
                                } else {
                                    text.push_str(&format!("│ {}", detail));
                                }
                            } else {
                                // Other details without the separator
                                text.push_str(&format!("│ {}", detail));
                            }
                        }
                        
                        // Properly truncate UTF-8 strings if too long
                        if text.len() > dropdown_width as usize - 2 {
                            let max_len = dropdown_width as usize - 5;
                            let mut truncated = String::new();
                            for ch in text.chars() {
                                if truncated.len() + ch.len_utf8() > max_len {
                                    break;
                                }
                                truncated.push(ch);
                            }
                            truncated.push_str("...");
                            text = truncated;
                        }
                        
                        let style = if is_selected {
                            STYLE::autocomplete_selected()
                        } else {
                            STYLE::autocomplete_text()
                        };
                        
                        lines.push(Spans::from(Span::styled(text, style)));
                    }
                    
                    let dropdown_area = Rect {
                        x: dropdown_x,
                        y: dropdown_y,
                        width: dropdown_width,
                        height: dropdown_height,
                    };
                    
                    let block = Block::default()
                        .borders(Borders::ALL)
                        .border_style(STYLE::autocomplete_border());
                    
                    f.render_widget(Clear, dropdown_area); // Clear background
                    
                    let inner = block.inner(dropdown_area);
                    f.render_widget(block, dropdown_area);
                    
                    let paragraph = Paragraph::new(lines)
                        .style(STYLE::autocomplete_bg());
                    f.render_widget(paragraph, inner);
                }
            }
        }
        // Overlay search text on bottom border if active
        if self.find_active {
            let find_text = &self.find_query;
            let replace_text = &self.replace_query;
            
            // Build the search display with focus indicators
            let match_info = if self.find_query.len() < 2 {
                "(min 2 chars)".to_string()
            } else {
                format!("[{}/{}]", 
                    if self.find_matches.is_empty() { 0 } else { self.find_current + 1 },
                    self.find_matches.len()
                )
            };
            
            let search_display = if self.find_replace_mode {
                // Replace field has focus
                format!(" Find: {} │ Replace: [{}] {} ", 
                    find_text,
                    replace_text,
                    match_info
                )
            } else {
                // Find field has focus
                format!(" Find: [{}] │ Replace: {} {} ", 
                    find_text,
                    replace_text,
                    match_info
                )
            };
            
            let search_x = area.x + (area.width.saturating_sub(search_display.len() as u16)) / 2;
            let search_y = area.y + area.height - 1;
            
            // Render the complete search bar
            let search_area = Rect {
                x: search_x,
                y: search_y,
                width: search_display.len() as u16,
                height: 1,
            };
            
            // For now, use a simple approach - render the whole bar with current focus style
            let bar_style = Style::default()
                .fg(rgb(CONFIG_COLORS.find_current_fg))
                .bg(rgb(CONFIG_COLORS.find_current_bg));
            
            f.render_widget(Paragraph::new(search_display).style(bar_style), search_area);
        }
    }

}