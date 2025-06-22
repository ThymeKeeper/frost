// src/db_tree.rs
use tui::{
    backend::Backend,
    layout::Rect,
    text::{Span, Spans},
    widgets::{Block, Borders, Paragraph},
    Frame,
};
use std::collections::HashSet;
use crate::schema_cache::{SchemaCache, ObjectType, SchemaObject};
use crate::palette::STYLE;
use crate::palette::{rgb, CONFIG_COLORS};
use crate::palette::KANAGAWA as k;
use tui::style::{Style, Modifier};

#[derive(Debug, Clone, PartialEq)]
pub enum TreeNode {
    Database(String),
    Schema(String, String), // (database, schema)
    ObjectType(String, String, ObjectType), // (database, schema, object_type)
    Object(String, String, String, ObjectType), // (database, schema, object, type)
    Column(String, String, String, String, String), // (db, schema, table, column, type)
}

pub enum TreeAction {
    None,
    InsertText(String),
    ChangeRole(String), // "ALL" for all roles, or specific role name
}

pub struct DbTree {
    pub visible: bool,
    pub focused: bool,
    pub cache: Option<SchemaCache>,
    
    // Tree state
    expanded: HashSet<String>, // Keys like "DB", "DB.SCHEMA"
    selected_index: usize,
    view_offset: usize,
    
    // Flattened view for rendering
    visible_nodes: Vec<(TreeNode, usize)>, // (node, depth)
    
    // Navigation
    db_navigator: crate::db_navigator::DbNavigator,
    
    // Search functionality
    pub find_active: bool,
    pub find_query: String,
    pub find_cursor_pos: usize,
    search_results: Vec<(TreeNode, String)>, // (node, fully_qualified_name)
    
    // Width control
    pub width_percent: u16, // Percentage of screen width (10-90)
    
    // Role selection
    pub role_selection_mode: bool,
    pub selected_role_index: usize,
    pending_action: Option<TreeAction>,
    connected: bool,
    needs_refresh: bool, 
}

impl DbTree {
    pub fn new() -> Self {
        let mut tree = Self {
            visible: false,
            focused: false,
            cache: None,
            expanded: HashSet::new(),
            selected_index: 0,
            view_offset: 0,
            visible_nodes: Vec::new(),
            db_navigator: crate::db_navigator::DbNavigator::new(),
            find_active: false,
            find_query: String::new(),
            find_cursor_pos: 0,
            search_results: Vec::new(),
            width_percent: 35,
            role_selection_mode: false,
            selected_role_index: 0,
            pending_action: None,
            connected: false,
            needs_refresh: true,
        };

        
        // Since we use "USE SECONDARY ROLES ALL" on startup, ensure UI reflects this
        if let Some(cache) = &mut tree.cache {
            cache.current_role = None;  // None = "All Roles"
        }
        tree.selected_role_index = 0;  // 0 = "All Roles" in the menu

        tree.cache = Some(SchemaCache::new());
        tree
    }

    pub fn check_refresh(&mut self) {
        if self.needs_refresh && self.visible && self.connected {  // Add connected check
            self.needs_refresh = false;
            // Clear both caches to force reload from disk
            self.cache = None;
            self.db_navigator.clear_cache();
            self.refresh_cache();
        }
    }

    pub fn set_connected(&mut self, connected: bool) {
        self.connected = connected;
    }

    fn get_object_text(&self, node: &TreeNode) -> Option<String> {
        match node {
            TreeNode::Object(db, schema, table, ObjectType::Table) |
            TreeNode::Object(db, schema, table, ObjectType::View) => {
                Some(format!("{}.{}.{}\n", db, schema, table))
            }
            TreeNode::Object(db, schema, obj, _) => {
                Some(format!("{}.{}.{}\n", db, schema, obj))
            }
            TreeNode::Column(_, _, _, column, _) => {
                Some(format!("{}\n", column))
            }
            _ => None
        }
    }
    
    pub fn toggle_visible(&mut self) {
        self.visible = !self.visible;
        if self.visible {
            self.on_show();
        }
    }

    pub fn on_show(&mut self) {
        // Called when tree becomes visible
        self.focused = true;
        // Don't refresh immediately - just mark that we need to
        self.needs_refresh = true;
    }
    
    fn refresh_cache(&mut self) {
        // Don't try to refresh if not connected
        if !self.connected {
            // Just show empty tree
            self.cache = Some(SchemaCache::new());
            self.rebuild_visible_nodes();
            return;
        }
        
        // Try to load cache
        match self.db_navigator.load_cache() {
            Ok(cache) => {
                self.cache = Some(cache.clone());
                // Since we use "USE SECONDARY ROLES ALL" on startup, ensure cache reflects this
                if let Some(cache) = &mut self.cache {
                    cache.current_role = None;  // None = "All Roles"
                }
                self.selected_role_index = 0;  // 0 = "All Roles" in the menu
                self.rebuild_visible_nodes();
            }
            Err(_) => {
                // Cache doesn't exist, request full refresh only if connected
                if self.connected {
                    let _ = self.db_navigator.request_refresh("REFRESH ALL");
                } else {
                    // Just show empty tree
                    self.cache = Some(SchemaCache::new());
                    self.rebuild_visible_nodes();
                }
            }
        }
    }
    
    /// Check if a node is accessible by the current role
    fn is_node_accessible(&self, node: &TreeNode) -> bool {
        let Some(cache) = &self.cache else { return true };
        let current_role = cache.current_role.as_deref();
        
        match node {
            TreeNode::Database(db) => {
                if let Some(database) = cache.databases.get(db) {
                    if let Some(role) = current_role {
                        database.accessible_by_roles.contains(role)
                    } else {
                        !database.accessible_by_roles.is_empty()
                    }
                } else {
                    false
                }
            }
            TreeNode::Schema(db, schema) => {
                if let Some(database) = cache.databases.get(db) {
                    if let Some(schema_obj) = database.schemas.get(schema) {
                        if let Some(role) = current_role {
                            schema_obj.accessible_by_roles.contains(role)
                        } else {
                            !schema_obj.accessible_by_roles.is_empty()
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            TreeNode::ObjectType(db, schema, _) => {
                // Object types are accessible if the schema is accessible
                self.is_node_accessible(&TreeNode::Schema(db.clone(), schema.clone()))
            }
            TreeNode::Object(db, schema, obj, _) => {
                if let Some(database) = cache.databases.get(db) {
                    if let Some(schema_obj) = database.schemas.get(schema) {
                        if let Some(object) = schema_obj.objects.get(obj) {
                            object.is_accessible_by_role(current_role)
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            TreeNode::Column(db, schema, table, _, _) => {
                // Columns are accessible if their parent object is accessible
                self.is_node_accessible(&TreeNode::Object(db.clone(), schema.clone(), table.clone(), ObjectType::Table))
            }
        }
    }
    
    fn rebuild_visible_nodes(&mut self) {
        self.visible_nodes.clear();
        
        let Some(cache) = &self.cache else { return };
        
        // Sort databases for consistent ordering
        let mut databases: Vec<_> = cache.databases.keys().collect();
        databases.sort();
        
        for db_name in databases {
            let db_key = db_name.to_string();
            self.visible_nodes.push((TreeNode::Database(db_name.to_string()), 0));
            
            if self.expanded.contains(&db_key) {
                // Show schemas
                let db = &cache.databases[db_name];
                let mut schemas: Vec<_> = db.schemas.keys().collect();
                schemas.sort();
                
                for schema_name in schemas {
                    let schema_key = format!("{}.{}", db_name, schema_name);
                    self.visible_nodes.push((
                        TreeNode::Schema(db_name.to_string(), schema_name.to_string()), 
                        1
                    ));
                    
                    if self.expanded.contains(&schema_key) {
                        // Group objects by type
                        let schema = &db.schemas[schema_name];
                        
                        // Collect all object types
                        let mut tables: Vec<(&String, &SchemaObject)> = Vec::new();
                        let mut views: Vec<(&String, &SchemaObject)> = Vec::new();
                        let mut procedures: Vec<(&String, &SchemaObject)> = Vec::new();
                        let mut functions: Vec<(&String, &SchemaObject)> = Vec::new();
                        let mut tasks: Vec<(&String, &SchemaObject)> = Vec::new();
                        let mut stages: Vec<(&String, &SchemaObject)> = Vec::new();
                        let mut streams: Vec<(&String, &SchemaObject)> = Vec::new();
                        let mut sequences: Vec<(&String, &SchemaObject)> = Vec::new();
                        
                        for (name, obj) in &schema.objects {
                            match &obj.object_type {
                                ObjectType::Table => tables.push((name, obj)),
                                ObjectType::View => views.push((name, obj)),
                                ObjectType::Procedure => procedures.push((name, obj)),
                                ObjectType::Function => functions.push((name, obj)),
                                ObjectType::Task => tasks.push((name, obj)),
                                ObjectType::Stage => stages.push((name, obj)),
                                ObjectType::Stream => streams.push((name, obj)),
                                ObjectType::Sequence => sequences.push((name, obj)),
                            }
                        }
                        
                        // Sort each type
                        tables.sort_by_key(|(name, _)| name.to_string());
                        views.sort_by_key(|(name, _)| name.to_string());
                        procedures.sort_by_key(|(name, _)| name.to_string());
                        functions.sort_by_key(|(name, _)| name.to_string());
                        tasks.sort_by_key(|(name, _)| name.to_string());
                        stages.sort_by_key(|(name, _)| name.to_string());
                        streams.sort_by_key(|(name, _)| name.to_string());
                        sequences.sort_by_key(|(name, _)| name.to_string());
                        
                        // Add nodes for each type that has objects
                        let type_groups = [
                            (ObjectType::Table, tables),
                            (ObjectType::View, views),
                            (ObjectType::Procedure, procedures),
                            (ObjectType::Function, functions),
                            (ObjectType::Task, tasks),
                            (ObjectType::Stage, stages),
                            (ObjectType::Stream, streams),
                            (ObjectType::Sequence, sequences),
                        ];
                        
                        for (obj_type, objects) in type_groups.iter() {
                            if !objects.is_empty() {
                                let type_key = format!("{}.{}.{:?}", db_name, schema_name, obj_type);
                                
                                // Add object type node
                                self.visible_nodes.push((
                                    TreeNode::ObjectType(
                                        db_name.to_string(),
                                        schema_name.to_string(),
                                        obj_type.clone()
                                    ),
                                    2
                                ));
                                
                                // If this type is expanded, show its objects
                                if self.expanded.contains(&type_key) {
                                    for (obj_name, obj) in objects {
                                        let node = TreeNode::Object(
                                            db_name.to_string(),
                                            schema_name.to_string(),
                                            obj_name.to_string(),
                                            obj.object_type.clone()
                                        );
                                        
                                        self.visible_nodes.push((node.clone(), 3));
                                        
                                        // Check if this object (table/view) is expanded to show columns
                                        if matches!(obj.object_type, ObjectType::Table | ObjectType::View) {
                                            let object_key = Self::get_node_key(&node);
                                            
                                            if self.expanded.contains(&object_key) && !obj.columns.is_empty() {
                                                for column in &obj.columns {
                                                    self.visible_nodes.push((
                                                        TreeNode::Column(
                                                            db_name.to_string(),
                                                            schema_name.to_string(),
                                                            obj_name.to_string(),
                                                            column.name.clone(),
                                                            column.data_type.clone()
                                                        ),
                                                        4
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    fn get_node_key(node: &TreeNode) -> String {
        match node {
            TreeNode::Database(db) => db.clone(),
            TreeNode::Schema(db, schema) => format!("{}.{}", db, schema),
            TreeNode::ObjectType(db, schema, obj_type) => format!("{}.{}.{:?}", db, schema, obj_type),
            TreeNode::Object(db, schema, obj, _) => format!("{}.{}.{}", db, schema, obj),
            TreeNode::Column(db, schema, table, col, _) => {
                format!("{}.{}.{}.{}", db, schema, table, col)
            }
        }
    }
    
    fn get_icon(object_type: &ObjectType) -> &'static str {
        match object_type {
            ObjectType::Table => "󰓫",
            ObjectType::View => "󰈈",
            ObjectType::Procedure => "󰊕",
            ObjectType::Function => "󰡱",
            ObjectType::Task => "󰔟",
            ObjectType::Stage => "󰉺",
            ObjectType::Stream => "󱑞",
            ObjectType::Sequence => "󰓹",
        }
    }
    
    fn handle_role_selection(&mut self, key: crossterm::event::KeyEvent) -> bool {
        use crossterm::event::KeyCode;
        
        match key.code {
            KeyCode::Up => {
                if self.selected_role_index > 0 {
                    self.selected_role_index -= 1;
                }
            }
            KeyCode::Down => {
                if let Some(cache) = &self.cache {
                    let max_index = cache.available_roles.len(); // +1 for "All Roles" option
                    if self.selected_role_index < max_index {
                        self.selected_role_index += 1;
                    }
                }
            }
            KeyCode::Enter => {
                // Apply role selection - get the new role first
                let new_role = if self.selected_role_index == 0 {
                    None  // "All Roles"
                } else {
                    self.cache.as_ref()
                        .and_then(|cache| cache.available_roles.get(self.selected_role_index - 1))
                        .cloned()
                };
                
                // Update the cache with the new role
                if let Some(cache) = &mut self.cache {
                    cache.current_role = new_role.clone();
                }
                
                self.role_selection_mode = false;
                self.rebuild_visible_nodes();

                // Set pending action
                self.pending_action = Some(TreeAction::ChangeRole(
                    if self.selected_role_index == 0 {
                        "ALL".to_string()
                    } else {
                        new_role.unwrap_or_default()
                    }
                ));
                
                self.role_selection_mode = false;

                return true;
            }
            KeyCode::Esc => {
                self.role_selection_mode = false;
            }
            _ => {}
        }
        
        false
    }

    /// Get and clear any pending action
    pub fn take_pending_action(&mut self) -> Option<TreeAction> {
        self.pending_action.take()
    }
    
    fn search_objects(&mut self) {
        self.search_results.clear();
        
        if self.find_query.len() < 3 {
            return;
        }
        
        let Some(cache) = &self.cache else { return };
        let query_lower = self.find_query.to_lowercase();
        
        for (db_name, db) in &cache.databases {
            for (schema_name, schema) in &db.schemas {
                for (obj_name, obj) in &schema.objects {
                    // Match on object name
                    if obj_name.to_lowercase().contains(&query_lower) {
                        let node = TreeNode::Object(
                            db_name.clone(),
                            schema_name.clone(),
                            obj_name.clone(),
                            obj.object_type.clone()
                        );
                        let qualified_name = format!("{}.{}.{}", db_name, schema_name, obj_name);
                        self.search_results.push((node.clone(), qualified_name));
                    }
                    
                    // Also search column names for tables/views
                    if matches!(obj.object_type, ObjectType::Table | ObjectType::View) {
                        for column in &obj.columns {
                            if column.name.to_lowercase().contains(&query_lower) {
                                let node = TreeNode::Column(
                                    db_name.clone(),
                                    schema_name.clone(),
                                    obj_name.clone(),
                                    column.name.clone(),
                                    column.data_type.clone()
                                );
                                let qualified_name = format!("{}.{}.{}.{}", db_name, schema_name, obj_name, column.name);
                                self.search_results.push((node, qualified_name));
                            }
                        }
                    }
                }
            }
        }
        
        // Reset selection if we have results
        if !self.search_results.is_empty() {
            self.selected_index = 0;
            self.view_offset = 0;
        }
    }
    
    fn handle_find_input(&mut self, key: crossterm::event::KeyEvent) -> bool {
        use crossterm::event::{KeyCode, KeyModifiers};
        
        if !self.find_active {
            return false;
        }
        
        match key.code {
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.find_query.insert(self.find_cursor_pos, ch);
                self.find_cursor_pos += 1;
                self.search_objects();
                return true;
            }
            KeyCode::Backspace => {
                if self.find_cursor_pos > 0 {
                    self.find_cursor_pos -= 1;
                    self.find_query.remove(self.find_cursor_pos);
                    self.search_objects();
                }
                return true;
            }
            KeyCode::Esc => {
                self.find_active = false;
                self.find_query.clear();
                self.find_cursor_pos = 0;
                self.search_results.clear();
                self.rebuild_visible_nodes();
                return true;
            }
            _ => {}
        }
        false
    }
    
    pub fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> bool {
        use crossterm::event::{KeyCode, KeyModifiers, KeyEventKind};
        
        // Only process key press events
        if key.kind != KeyEventKind::Press {
            return false;
        }
        
        // Handle role selection mode
        if self.role_selection_mode {
            return self.handle_role_selection(key);
        }
        
        // Handle find mode input first
        if self.find_active && self.handle_find_input(key) {
            return false;
        }
        
        // Check for Ctrl+F to activate search
        if key.code == KeyCode::Char('f') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.find_active = !self.find_active;
            if self.find_active {
                self.find_cursor_pos = self.find_query.len();
                if self.find_query.len() >= 3 {
                    self.search_objects();
                }
            } else {
                self.find_query.clear();
                self.find_cursor_pos = 0;
                self.search_results.clear();
                self.rebuild_visible_nodes();
            }
            return false;
        }
        
        // Check for Ctrl+U to activate role selection
        if key.code == KeyCode::Char('u') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.role_selection_mode = true;
            // Set selected index based on current role
            if let Some(cache) = &self.cache {
                if let Some(ref current_role) = cache.current_role {
                    self.selected_role_index = cache.available_roles
                        .iter()
                        .position(|r| r == current_role)
                        .map(|i| i + 1)
                        .unwrap_or(0);
                } else {
                    self.selected_role_index = 0; // "All Roles"
                }
            }
            return false;
        }
        
        // In search mode, handle navigation differently
        if self.find_active && self.find_query.len() >= 3 && !self.search_results.is_empty() {
            match key.code {
                KeyCode::Up => {
                    if self.selected_index > 0 {
                        self.selected_index -= 1;
                        self.ensure_visible();
                    }
                }
                KeyCode::Down => {
                    if self.selected_index + 1 < self.search_results.len() {
                        self.selected_index += 1;
                        self.ensure_visible();
                    }
                }
                KeyCode::Enter => {
                    // Insert selected search result
                    if let Some((node, _)) = self.search_results.get(self.selected_index) {
                        match node {
                            TreeNode::Object(_, _, _, _) => {
                                return true;
                            }
                            TreeNode::Column(_, _, _, _, _) => {
                                return true;
                            }
                            _ => {}
                        }
                    }
                }
                KeyCode::Esc => {
                    self.find_active = false;
                    self.find_query.clear();
                    self.find_cursor_pos = 0;
                    self.search_results.clear();
                    self.rebuild_visible_nodes();
                    return true;
                }
                _ => {}
            }
            return false;
        }
        
        // Normal tree navigation when not searching
        if self.visible_nodes.is_empty() {
            return false;
        }
        
        match key.code {
            KeyCode::Up => {
                if self.selected_index > 0 {
                    self.selected_index -= 1;
                    self.ensure_visible();
                }
            }
            KeyCode::Down => {
                if self.selected_index + 1 < self.visible_nodes.len() {
                    self.selected_index += 1;
                    self.ensure_visible();
                }
            }
            KeyCode::Left => {
                // Collapse current node
                if let Some((node, _)) = self.visible_nodes.get(self.selected_index) {
                    let key = Self::get_node_key(node);
                    if self.expanded.contains(&key) {
                        self.expanded.remove(&key);
                        self.rebuild_visible_nodes();
                    }
                }
            }
            KeyCode::Right => {
                // Expand current node (but don't insert text)
                if let Some((node, _)) = self.visible_nodes.get(self.selected_index) {
                    match node {
                        TreeNode::Database(_) | TreeNode::Schema(_, _) | TreeNode::ObjectType(_, _, _) => {
                            let key = Self::get_node_key(node);
                            if !self.expanded.contains(&key) {
                                self.expanded.insert(key);
                                self.rebuild_visible_nodes();
                            }
                        }
                        TreeNode::Object(_, _, _, obj_type) => {
                            if matches!(obj_type, ObjectType::Table | ObjectType::View) {
                                let key = Self::get_node_key(node);
                                if !self.expanded.contains(&key) {
                                    self.expanded.insert(key);
                                    self.rebuild_visible_nodes();
                                }
                            }
                        }
                        TreeNode::Column(_, _, _, _, _) => {
                            // Columns can't be expanded
                        }
                    }
                }
            }
            KeyCode::Enter => {
                // Insert text based on node type
                if let Some((node, _)) = self.visible_nodes.get(self.selected_index) {
                    match node {
                        TreeNode::Database(_) | TreeNode::Schema(_, _) | TreeNode::ObjectType(_, _, _) => {
                            // Don't insert text for these organizational nodes
                            // Just expand/collapse them
                            let key = Self::get_node_key(node);
                            if self.expanded.contains(&key) {
                                self.expanded.remove(&key);
                            } else {
                                self.expanded.insert(key);
                            }
                            self.rebuild_visible_nodes();
                        }
                        TreeNode::Object(_, _, _, obj_type) => {
                            if matches!(obj_type, ObjectType::Table | ObjectType::View) {
                                    if let Some(text) = self.get_object_text(node) {
                                        self.pending_action = Some(TreeAction::InsertText(text));
                                        return true;
                                    }
                            } else {
                                    if let Some(text) = self.get_object_text(node) {
                                        self.pending_action = Some(TreeAction::InsertText(text));
                                        return true;
                                    }
                                }
                            }
                            TreeNode::Column(_, _, _, _, _) => {
                            if let Some(text) = self.get_object_text(node) {
                                self.pending_action = Some(TreeAction::InsertText(text));
                                return true;
                            }
                        }
                    }
                }
            }
            // For Ctrl+R (refresh current node)
            KeyCode::Char('r') | KeyCode::Char('R') if key.modifiers.contains(KeyModifiers::CONTROL) && !key.modifiers.contains(KeyModifiers::SHIFT) => {
                // Refresh current node
                if !self.connected {
                    // Don't refresh if not connected
                    return false;
                }
                if let Some((node, _)) = self.visible_nodes.get(self.selected_index) {
                    match node {
                        TreeNode::Database(db) => {
                            let _ = self.db_navigator.request_refresh(&format!("REFRESH DATABASE {}", db));
                        }
                        TreeNode::Schema(db, schema) => {
                            let _ = self.db_navigator.request_refresh(&format!("REFRESH SCHEMA {}.{}", db, schema));
                        }
                        TreeNode::Object(db, schema, obj, _) => {
                            let _ = self.db_navigator.request_refresh(&format!("REFRESH TABLE {}.{}.{}", db, schema, obj));
                        }
                        TreeNode::ObjectType(db, schema, _) => {
                            let _ = self.db_navigator.request_refresh(&format!("REFRESH SCHEMA {}.{}", db, schema));
                        }
                        TreeNode::Column(db, schema, table, _, _) => {
                            let _ = self.db_navigator.request_refresh(&format!("REFRESH TABLE {}.{}.{}", db, schema, table));
                        }
                    }
                }
            }

            // For Ctrl+Shift+R (full refresh)
            KeyCode::Char('r') | KeyCode::Char('R') if key.modifiers.contains(KeyModifiers::CONTROL | KeyModifiers::SHIFT) => {
                if !self.connected {
                    // Don't refresh if not connected
                    return false;
                }
                let _ = self.db_navigator.request_refresh("REFRESH ALL");
                self.cache = None;
                return false;
            }
            KeyCode::Esc => {
                self.visible = false;
                self.focused = false;
                return true;
            }
            _ => {}
        }
        
        false
    }
    
    pub fn handle_mouse(&mut self, event: crossterm::event::MouseEvent, area: Rect) {
        use crossterm::event::{MouseEventKind, MouseButton};
        
        let inner = area.inner(&tui::layout::Margin { horizontal: 1, vertical: 1 });
        let rel_y = event.row.saturating_sub(inner.y) as usize;
        
        match event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                if event.column >= inner.x && event.column < inner.x + inner.width &&
                   event.row >= inner.y && event.row < inner.y + inner.height {
                    // Click inside tree area
                    let clicked_index = self.view_offset + rel_y;
                    if clicked_index < self.visible_nodes.len() {
                        self.selected_index = clicked_index;
                        
                        // Double-click behavior (expand/collapse)
                        if let Some((node, _)) = self.visible_nodes.get(clicked_index) {
                            match node {
                                TreeNode::Database(_) | TreeNode::Schema(_, _) | TreeNode::ObjectType(_, _, _) => {
                                    let key = Self::get_node_key(node);
                                    if self.expanded.contains(&key) {
                                        self.expanded.remove(&key);
                                    } else {
                                        self.expanded.insert(key);
                                    }
                                    self.rebuild_visible_nodes();
                                }
                                TreeNode::Object(_, _, _, obj_type) if matches!(obj_type, ObjectType::Table | ObjectType::View) => {
                                    let key = Self::get_node_key(node);
                                    if self.expanded.contains(&key) {
                                        self.expanded.remove(&key);
                                    } else {
                                        self.expanded.insert(key);
                                    }
                                    self.rebuild_visible_nodes();
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            MouseEventKind::ScrollUp => {
                if self.view_offset > 0 {
                    self.view_offset -= 1;
                }
            }
            MouseEventKind::ScrollDown => {
                let max_offset = self.visible_nodes.len().saturating_sub(inner.height as usize);
                if self.view_offset < max_offset {
                    self.view_offset += 1;
                }
            }
            _ => {}
        }
    }
    
    fn ensure_visible(&mut self) {
        // Adjust view_offset to keep selected item visible
        // This will be called after render to know the actual height
    }
    
    pub fn ensure_visible_with_height(&mut self, visible_height: usize) {
        if self.selected_index < self.view_offset {
            self.view_offset = self.selected_index;
        } else if self.selected_index >= self.view_offset + visible_height {
            self.view_offset = self.selected_index - visible_height + 1;
        }
    }
    
    fn render_highlighted_spans<'a>(&self, text: &'a str, icon: &str, base_style: Style, is_selected: bool) -> Vec<Span<'a>> {
        let query_lower = self.find_query.to_lowercase();
        let text_lower = text.to_lowercase();
        
        let style = if is_selected {
            if self.focused {
                STYLE::table_caret_bg()
            } else {
                STYLE::table_sel_bg()
            }
        } else {
            base_style
        };
        
        let mut spans = vec![Span::styled(format!("{} ", icon), style)];
        
        // Find the match position in the text
        if let Some(match_start) = text_lower.find(&query_lower) {
            // Add text before match
            if match_start > 0 {
                spans.push(Span::styled(&text[..match_start], style));
            }
            
            // Add highlighted match
            let match_end = match_start + self.find_query.len();
            let highlight_style = if is_selected {
                style.add_modifier(Modifier::UNDERLINED | Modifier::BOLD)
            } else {
                Style::default()
                    .fg(rgb(CONFIG_COLORS.find_current_fg))
                    .bg(rgb(CONFIG_COLORS.find_current_bg))
            };
            spans.push(Span::styled(&text[match_start..match_end], highlight_style));
            
            // Add text after match
            if match_end < text.len() {
                spans.push(Span::styled(&text[match_end..], style));
            }
        } else {
            // No match in this part, just show the text
            spans.push(Span::styled(text, style));
        }
        
        spans
    }
    
    pub fn render<B: Backend>(&mut self, f: &mut Frame<B>, area: Rect) {
        // Build title with role info
        let title = if let Some(cache) = &self.cache {
            if let Some(ref role) = cache.current_role {
                format!(" Database Navigator [Role: {}] ", role)
            } else {
                " Database Navigator [Using All Secondary Roles] ".to_string()
            }
        } else {
            " Database Navigator ".to_string()
        };
        
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(if self.focused {
                STYLE::editor_border_focus()
            } else {
                STYLE::editor_border()
            });
        
        let inner = block.inner(area);
        f.render_widget(block, area);
        
        // If role selection mode is active, show role selector
        if self.role_selection_mode {
            self.render_role_selector(f, inner);
            return;
        }
        
        // Build the lines to display
        let mut lines = Vec::new();
        let visible_height = inner.height as usize;
        
        // Show search results if searching with 3+ characters
        if self.find_active && self.find_query.len() >= 3 {
            // Show search results
            self.ensure_visible_with_height(visible_height);
            
            for i in self.view_offset..self.view_offset + visible_height {
                if i >= self.search_results.len() {
                    break;
                }
                
                let (node, qualified_name) = &self.search_results[i];
                let is_selected = i == self.selected_index;
                let is_accessible = self.is_node_accessible(node);
                
                let icon = match node {
                    TreeNode::Object(_, _, _, obj_type) => Self::get_icon(obj_type),
                    TreeNode::Column(_, _, _, _, _) => "󰠵",
                    _ => "",
                };
                
                // Choose style based on accessibility
                let base_style = if is_accessible {
                    STYLE::plain()
                } else {
                    // Dimmed style for inaccessible objects
                    Style::default().fg(k::STEEL_VIOLET)
                };
                
                let spans = self.render_highlighted_spans(qualified_name, icon, base_style, is_selected);
                lines.push(Spans::from(spans));
            }
        } else {
            // Show normal tree view
            self.ensure_visible_with_height(visible_height);
            
            for i in self.view_offset..self.view_offset + visible_height {
                if i >= self.visible_nodes.len() {
                    break;
                }
                
                let (node, depth) = &self.visible_nodes[i];
                let is_selected = i == self.selected_index;
                let is_accessible = self.is_node_accessible(node);
                
                let indent = "  ".repeat(*depth);
                let (prefix, text, default_style) = match node {
                    TreeNode::Database(name) => {
                        let expanded = self.expanded.contains(name);
                        let prefix = if expanded { "▼ " } else { "▶ " };
                        (prefix, format!("󰆼 {}", name), STYLE::info_fg())
                    }
                    TreeNode::Schema(_, name) => {
                        let key = Self::get_node_key(node);
                        let expanded = self.expanded.contains(&key);
                        let prefix = if expanded { "▼ " } else { "▶ " };
                        (prefix, format!("󰙅 {}", name), STYLE::status_fg())
                    }
                    TreeNode::ObjectType(_, _, obj_type) => {
                        let expanded = self.expanded.contains(&Self::get_node_key(node));
                        let prefix = if expanded { "▼ " } else { "▶ " };
                        let icon = Self::get_icon(obj_type);
                        let type_name = match obj_type {
                            ObjectType::Table => "Tables",
                            ObjectType::View => "Views",
                            ObjectType::Procedure => "Procedures",
                            ObjectType::Function => "Functions",
                            ObjectType::Task => "Tasks",
                            ObjectType::Stage => "Stages",
                            ObjectType::Stream => "Streams",
                            ObjectType::Sequence => "Sequences",
                        };
                        (prefix, format!("{} {}", icon, type_name), STYLE::info_fg())
                    }
                    TreeNode::Object(_, _, name, obj_type) => {
                        let icon = Self::get_icon(obj_type);
                        let prefix = if matches!(obj_type, ObjectType::Table | ObjectType::View) {
                            let expanded = self.expanded.contains(&Self::get_node_key(node));
                            if expanded { "▼ " } else { "▶ " }
                        } else {
                            "  "
                        };
                        (prefix, format!("{} {}", icon, name), STYLE::plain())
                    }
                    TreeNode::Column(_, _, _, col_name, data_type) => {
                        ("  ", format!("󰠵 {} : {}", col_name, data_type), STYLE::status_fg())
                    }
                };
                
                let full_text = format!("{}{}{}", indent, prefix, text);
                
                // Apply style based on accessibility and selection
                let style = if !is_accessible {
                    // Inaccessible objects shown in dimmed color
                    Style::default().fg(k::STEEL_VIOLET)
                } else if is_selected {
                    if self.focused {
                        STYLE::table_caret_bg()
                    } else {
                        STYLE::table_sel_bg()
                    }
                } else {
                    default_style
                };
                
                lines.push(Spans::from(Span::styled(full_text, style)));
            }
        }
        
        let paragraph = Paragraph::new(lines);
        f.render_widget(paragraph, inner);
        
        // Overlay search text on bottom border if active
        if self.find_active {
            let search_text = if self.find_query.len() < 3 {
                format!(" Find: {} (min 3 chars) ", self.find_query)
            } else if self.search_results.is_empty() && self.find_query.len() >= 3 {
                format!(" Find: {} (no results) ", self.find_query)
            } else {
                format!(" Find: {} [{} results] ", self.find_query, self.search_results.len())
            };
            
            let search_x = area.x + (area.width.saturating_sub(search_text.len() as u16)) / 2;
            let search_y = area.y + area.height - 1;
            
            let search_style = Style::default()
                .fg(rgb(CONFIG_COLORS.find_current_fg))
                .bg(rgb(CONFIG_COLORS.find_current_bg));
            
            let search_area = Rect {
                x: search_x,
                y: search_y,
                width: search_text.len() as u16,
                height: 1,
            };
            
            f.render_widget(Paragraph::new(search_text).style(search_style), search_area);
        }
        
        // Show hint for role selection
        if !self.find_active && !self.role_selection_mode {
            let hint_text = " Ctrl+U: Select Role ";
            let hint_x = area.x + area.width.saturating_sub(hint_text.len() as u16 + 1);
            let hint_y = area.y + area.height - 1;
            
            let hint_area = Rect {
                x: hint_x,
                y: hint_y,
                width: hint_text.len() as u16,
                height: 1,
            };
            
            f.render_widget(
                Paragraph::new(hint_text).style(STYLE::status_fg()), 
                hint_area
            );
        }
    }
    
    fn render_role_selector<B: Backend>(&self, f: &mut Frame<B>, area: Rect) {
        let Some(cache) = &self.cache else { return };
        
        let mut lines = vec![
            Spans::from(Span::styled("Select Role:", Style::default().add_modifier(Modifier::BOLD))),
            Spans::from(Span::raw("")),
        ];
        
        // Add "All Roles" option
        let style = if self.selected_role_index == 0 {
            STYLE::table_caret_bg()
        } else {
            STYLE::plain()
        };
        lines.push(Spans::from(Span::styled("  [All Roles]", style)));
        
        // Add each available role
        for (i, role) in cache.available_roles.iter().enumerate() {
            let style = if self.selected_role_index == i + 1 {
                STYLE::table_caret_bg()
            } else {
                STYLE::plain()
            };
            lines.push(Spans::from(Span::styled(format!("  {}", role), style)));
        }
        
        lines.push(Spans::from(Span::raw("")));
        lines.push(Spans::from(Span::raw("Enter: Select, Esc: Cancel")));
        
        let paragraph = Paragraph::new(lines);
        f.render_widget(paragraph, area);
    }
}