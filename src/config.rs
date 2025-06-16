use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Snowflake connection string
    pub connection_string: String,
    
    /// Theme colors (all RGB values)
    pub colors: ColorConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColorConfig {
    // Editor colors
    pub editor_border: [u8; 3],
    pub editor_border_focus: [u8; 3],
    pub gutter_current: [u8; 3],
    pub gutter_relative: [u8; 3],
    pub caret_cell_fg: [u8; 3],
    pub caret_cell_bg: [u8; 3],
    pub selection_fg: [u8; 3],
    pub selection_bg: [u8; 3],
    pub bracket_match_bg: [u8; 3],
    
    // Results colors
    pub results_border: [u8; 3],
    pub results_border_focus: [u8; 3],
    pub tab_active: [u8; 3],
    pub header_row: [u8; 3],
    pub table_sel_fg: [u8; 3],
    pub table_sel_bg: [u8; 3],
    pub table_caret_fg: [u8; 3],
    pub table_caret_bg: [u8; 3],

    // Find/Search colors
    pub find_match_fg: [u8; 3],
    pub find_match_bg: [u8; 3],
    pub find_current_fg: [u8; 3],
    pub find_current_bg: [u8; 3],

    // Autocomplete colors
    pub autocomplete_bg: [u8; 3],
    pub autocomplete_border: [u8; 3],
    pub autocomplete_selected_fg: [u8; 3],
    pub autocomplete_selected_bg: [u8; 3],
    
    // UI colors
    pub help_bg: [u8; 3],
    pub help_border: [u8; 3],
    pub status_fg: [u8; 3],
    pub error_fg: [u8; 3],
    pub info_fg: [u8; 3],
    pub default_bg: [u8; 3],
    
    // Syntax highlighting
    pub syntax_keyword: [u8; 3],
    pub syntax_number: [u8; 3],
    pub syntax_string: [u8; 3],
    pub syntax_comment: [u8; 3],
    pub syntax_cast: [u8; 3],
    pub syntax_function: [u8; 3],
    pub syntax_variable: [u8; 3],
    pub syntax_plain: [u8; 3],
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connection_string: String::from(
                "Driver=SnowflakeDSIIDriver;\
                Server=your-account.snowflakecomputing.com;\
                UID=your-email@example.com;\
                Authenticator=externalbrowser;\
                Role=your_role;\
                Warehouse=your_warehouse;\
                Database=your_database;\
                Schema=your_schema;"
            ),
            colors: ColorConfig::default(),
        }
    }
}

impl Default for ColorConfig {
    fn default() -> Self {
        Self {
            // Editor colors
            editor_border: [42, 42, 55],           // INDIGO_SHADOW
            editor_border_focus: [120, 120, 145],  // SILVER_VIOLET
            gutter_current: [177, 135, 166],       // DIRTY_SAKURA_PETAL
            gutter_relative: [84, 84, 109],        // STEEL_VIOLET
            caret_cell_fg: [22, 22, 22],           // INKSTONE
            caret_cell_bg: [238, 185, 225],        // SAKURA_PETAL
            selection_fg: [200, 200, 200],         // RICE_PAPER
            selection_bg: [54, 54, 70],            // DUSKY_SLATE
            bracket_match_bg: [45, 79, 103],       // DEEP_SEA (bluish for bracket matching)
            
            // Results colors
            results_border: [42, 42, 55],          // INDIGO_SHADOW
            results_border_focus: [120, 120, 145], // SILVER_VIOLET
            tab_active: [126, 156, 216],           // SKY_GLAZE
            header_row: [147, 138, 169],           // LAVENDER_HAZE
            table_sel_fg: [200, 200, 200],         // RICE_PAPER
            table_sel_bg: [84, 84, 109],           // STEEL_VIOLET
            table_caret_fg: [22, 22, 22],          // INKSTONE
            table_caret_bg: [238, 185, 225],       // SAKURA_PETAL
            
            // Find/Search colors
            find_match_fg: [22, 22, 22],           // INKSTONE
            find_match_bg: [84, 54, 77],           // Dark sakura (muted purple-pink)
            find_current_fg: [22, 22, 22],         // INKSTONE  
            find_current_bg: [238, 185, 225],      // SAKURA_PETAL (same as caret)

            // Autocomplete colors
            autocomplete_bg: [30, 31, 40],      // Dark background (OBSIDIAN_FOG)
            autocomplete_border: [84, 84, 109], // STEEL_VIOLET
            autocomplete_selected_fg: [22, 22, 22], // INKSTONE
            autocomplete_selected_bg: [238, 185, 225], // OCHRE_SAND

            // UI colors
            help_bg: [30, 31, 40],                 // OBSIDIAN_FOG
            help_border: [84, 84, 109],            // STEEL_VIOLET
            status_fg: [84, 84, 109],              // STEEL_VIOLET
            error_fg: [232, 36, 36],               // TORII_VERMILION
            info_fg: [126, 156, 216],              // SKY_GLAZE
            default_bg: [22, 22, 22],              // INKSTONE
            
            // Syntax highlighting
            syntax_keyword: [126, 156, 216],       // SKY_GLAZE
            syntax_number: [149, 127, 184],        // TWILIGHT_WISTERIA
            syntax_string: [106, 149, 137],        // PINE_NEEDLE
            syntax_comment: [84, 84, 109],         // STEEL_VIOLET
            syntax_cast: [255, 160, 102],          // SUNSET_APRICOT
            syntax_function: [210, 126, 153],      // SAKURA_BLOSSOM
            syntax_variable: [230, 195, 132],      // OCHRE_SAND
            syntax_plain: [200, 200, 200],         // RICE_PAPER
        }
    }
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        let config_path = Self::config_path()?;
        
        if !config_path.exists() {
            Self::create_default_config(&config_path)?;
        }
        
        let contents = fs::read_to_string(&config_path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
    
    fn config_path() -> anyhow::Result<PathBuf> {
        let exe_path = std::env::current_exe()?;
        let exe_dir = exe_path.parent()
            .ok_or_else(|| anyhow::anyhow!("Could not find executable directory"))?;
        Ok(exe_dir.join("Frost.toml"))
    }
    
    fn create_default_config(path: &PathBuf) -> anyhow::Result<()> {
        let default_toml = r#"# Frost Configuration
# Place this file in the same directory as the Frost executable

# Snowflake connection string
connection_string = """
Driver=SnowflakeDSIIDriver;
Server=your-account.snowflakecomputing.com;
UID=your-email@example.com;
Authenticator=externalbrowser;
Role=your_role;
Warehouse=your_warehouse;
Database=your_database;
Schema=your_schema;
"""

# Theme colors - all values are RGB arrays [red, green, blue]
# You can customize any of these colors to your preference

[colors]
# Editor colors
editor_border = [42, 42, 55]            # Border when editor is not focused
editor_border_focus = [120, 120, 145]   # Border when editor is focused (brighter)
gutter_current = [177, 135, 166]        # Current line number in gutter
gutter_relative = [84, 84, 109]         # Relative line numbers in gutter
caret_cell_fg = [22, 22, 22]            # Cursor foreground color
caret_cell_bg = [238, 185, 225]         # Cursor background color
selection_fg = [200, 200, 200]          # Selected text foreground
selection_bg = [54, 54, 70]             # Selected text background
bracket_match_bg = [45, 79, 103]        # Background for matching brackets

# Results pane colors
results_border = [42, 42, 55]           # Border when results pane is not focused
results_border_focus = [120, 120, 145]  # Border when results pane is focused
tab_active = [126, 156, 216]            # Active tab text color
header_row = [147, 138, 169]            # Table header row color
table_sel_fg = [200, 200, 200]          # Selected cell foreground
table_sel_bg = [84, 84, 109]            # Selected cell background
table_caret_fg = [22, 22, 22]           # Cursor cell foreground
table_caret_bg = [238, 185, 225]        # Cursor cell background

# Find/Search colors
find_match_fg = [22, 22, 22]            # Search match foreground
find_match_bg = [84, 54, 77]            # Search match background (dark sakura)
find_current_fg = [22, 22, 22]          # Current search match foreground  
find_current_bg = [238, 185, 225]       # Current search match background (sakura petal)

# Autocomplete colors
autocomplete_bg = [30, 31, 40]           # Dark background
autocomplete_border = [84, 84, 109]      # Border color
autocomplete_selected_fg = [22, 22, 22]  # Selected text color
autocomplete_selected_bg = [230, 195, 132] # Selected background color

# UI colors
help_bg = [30, 31, 40]                  # Help screen background
help_border = [84, 84, 109]             # Help screen border
status_fg = [84, 84, 109]               # Status bar text
error_fg = [232, 36, 36]                # Error message color
info_fg = [126, 156, 216]               # Info message color
default_bg = [22, 22, 22]               # Default background color

# Syntax highlighting
syntax_keyword = [126, 156, 216]        # SQL keywords (SELECT, FROM, etc.)
syntax_number = [149, 127, 184]         # Numeric literals
syntax_string = [106, 149, 137]         # String literals
syntax_comment = [84, 84, 109]          # Comments
syntax_cast = [255, 160, 102]           # Type casts (::)
syntax_function = [210, 126, 153]       # Function names
syntax_variable = [230, 195, 132]       # Variables and parameters
syntax_plain = [200, 200, 200]          # Plain text
"#;
        fs::write(path, default_toml)?;
        eprintln!("Created config file at: {}", path.display());
        eprintln!("Please edit it with your Snowflake connection details and restart.");
        std::process::exit(1);
    }
}