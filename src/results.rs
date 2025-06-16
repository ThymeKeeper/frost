pub use crate::results_selection::*;
use crate::results_selection;

use tui::{
    backend::Backend,
    Frame, 
};

use copypasta::ClipboardContext;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum ResultsContent {
    Table {
        headers: Vec<String>,
        tile_store: crate::tile_rowstore::TileRowStore,
    },
    Error {
        message: String,
        cursor: usize,           // Add cursor position
        selection: Option<(usize, usize)>, // Add selection range
    },
    Info {
        message: String, // <-- New variant
    },
    Pending, // For a tab that's running and not yet populated
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScrollDirection {
    None,
    Left,
    Right,
}

pub struct ResultsTab {
    pub content: ResultsContent,
    pub cursor_row: usize,
    pub cursor_col: usize,
    pub view_row: usize,
    pub view_col: usize,
    pub selection: ResultSelection,
    pub running: bool,
    pub elapsed: Option<Duration>,
    pub run_started: Option<Instant>,
    pub visible_cache: Option<(usize, Vec<Vec<String>>)>,
    pub summary_cache: Option<(
        crate::results_selection::ResultSelection,
        (String, Option<String>)
    )>,
    pub query_context: String,
    pub column_widths_cache: Option<crate::results_selection::ColumnWidths>,
    pub scroll_direction: ScrollDirection,
    pub scroll_x: u16,
}

impl ResultsTab {
    pub fn new_pending(query_context: String) -> Self {
        Self::new_pending_with_start(query_context, Instant::now())
    }
    
    pub fn new_pending_with_start(query_context: String, started: Instant) -> Self {
        Self {
            content: ResultsContent::Pending,
            cursor_row: 0,
            cursor_col: 1,
            view_row: 0,
            view_col: 0,
            selection: ResultSelection::none(),
            running: true,
            elapsed: None,
            run_started: Some(started),
            visible_cache: None,
            summary_cache: None,
            query_context,
            column_widths_cache: None,
            scroll_direction: ScrollDirection::None,
            scroll_x: 0,
        }
    }
}

pub struct Results {
    pub tabs: Vec<ResultsTab>,
    pub tab_idx: usize,
    pub focus: bool,
    pub max_rows: usize,
    pub max_cols: usize,
    pub clipboard: ClipboardContext,
    pub wrap_width: usize,
    pub find_active: bool,
    pub find_query: String,
    pub find_matches: Vec<crate::results_selection::FindMatch>,
    pub find_current: usize,
}

impl ResultsTab {
    pub fn nudge_viewport(
        &mut self,
        max_view_rows: usize,
        _max_view_cols: usize,
        row_count: usize,
        col_count: usize,
    ) {
        // Vertical viewport adjustment
        if self.cursor_row < self.view_row {
            self.view_row = self.cursor_row;
        }
        if self.cursor_row >= self.view_row + max_view_rows {
            self.view_row = self.cursor_row + 1 - max_view_rows;
        }
        
        // Clamp cursor to valid bounds
        self.cursor_row = self.cursor_row.min(row_count.saturating_sub(1));
        self.cursor_col = self.cursor_col.min(col_count);
        
        // Note: Horizontal viewport adjustment is now handled directly in the keyboard handler
        // to work properly with variable column widths
    }
    
    pub fn visible_rows<'a>(
        &'a mut self,
        tile_store: &mut crate::tile_rowstore::TileRowStore,
        max_rows: usize,
    ) -> &'a [Vec<String>] {
        let need_fetch = match self.visible_cache {
            Some((cached_start, ref v)) => cached_start != self.view_row || v.is_empty(),
            None => true,
        };
        if need_fetch {
            let rows = tile_store
                .get_rows(self.view_row, max_rows)
                .unwrap_or_default();
            self.visible_cache = Some((self.view_row, rows));
        }
        // Safe: we keep the Vec inside self.visible_cache for the whole frame
        &self.visible_cache.as_ref().unwrap().1
    }
} 

impl Results {
    pub fn new() -> Self {
        Self {
            tabs: Vec::new(),
            tab_idx: 0,
            focus: false,
            max_rows: 13,
            max_cols: 6,
            clipboard: ClipboardContext::new().unwrap(),
            wrap_width: 72,
            find_active: false,
            find_query: String::new(),
            find_matches: Vec::new(),
            find_current: 0,
        }
    }

    pub fn selection_stats(&mut self) -> Option<(String, Option<String>)> {
        if self.tabs.is_empty() {
            return None;
        }
        let tab = &mut self.tabs[self.tab_idx];
        if let ResultsContent::Table { headers, tile_store } = &mut tab.content {
            // return cached if unchanged
            if let Some((ref old_sel, ref tup)) = tab.summary_cache {
                if old_sel == &tab.selection {
                    return Some(tup.clone());
                }
            }
            // otherwise recompute & cache
            let new_sum = crate::results_selection::compute_selection_summary(
                &tab.selection,
                headers,
                tile_store,
            );
            if let Some(ref s) = new_sum {
                tab.summary_cache = Some((tab.selection.clone(), s.clone()));
            }
            new_sum
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.tabs.clear();
        self.tab_idx = 0;
    }

    /// Call this to create an empty tab for a pending/running query.
    pub fn add_pending_tab(&mut self, query_context: String) {
        self.tabs.push(ResultsTab::new_pending(query_context));
        self.tab_idx = self.tabs.len() - 1;
    }

    /// When a query finishes, fill in the tab with its data or error.
    pub fn finish_tab(&mut self, tab_idx: usize, content: ResultsContent) {
        if let Some(tab) = self.tabs.get_mut(tab_idx) {
            tab.content = content;
            tab.elapsed = tab.run_started.map(|start| start.elapsed());
            tab.running = false;
            tab.run_started = None;
            tab.summary_cache = None;
            // Note: column_widths_cache will be calculated when the table is rendered
        }
    }

    pub fn handle_key(&mut self, key: crossterm::event::KeyEvent) {
        results_selection::handle_key(self, key)
    }
    pub fn handle_mouse(&mut self, event: crossterm::event::MouseEvent, area: tui::layout::Rect) {
        results_selection::handle_mouse(self, event, area)
    }

    // CHANGE: now requires total_queries argument and passes it down.
    pub fn render<B: Backend>(&mut self, f: &mut Frame<B>, area: tui::layout::Rect, total_queries: usize) {
        results_selection::render(self, f, area, total_queries)
    }
}