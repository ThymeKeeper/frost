use tui::style::{Color, Modifier, Style};
use once_cell::sync::Lazy;

/*──────────────────────── 1. Kanagawa palette (RGB) ───────────────────────*/
/// Named Kanagawa tones as **true RGB** – exactly the same on every OS.
///
///    ╭ index only matters if you still want to call `color(idx)`
///    │   (0‑15 map in the order below)
pub mod KANAGAWA {
    use tui::style::Color;

    pub const INKSTONE:          Color = Color::Rgb( 22,  22,  22);
    pub const OBSIDIAN_FOG:      Color = Color::Rgb( 30,  31,  40);
    pub const INDIGO_SHADOW:     Color = Color::Rgb( 42,  42,  55);
    pub const DUSKY_SLATE:       Color = Color::Rgb( 54,  54,  70);
    pub const STEEL_VIOLET:      Color = Color::Rgb( 84,  84, 109);
    pub const SILVER_VIOLET:     Color = Color::Rgb(120, 120, 145);
    pub const RICE_PAPER:        Color = Color::Rgb(200, 200, 200);
    pub const PUMICE:            Color = Color::Rgb(114, 113, 105);
    pub const WAVE_CREST:        Color = Color::Rgb( 34,  50,  73);
    pub const DEEP_SEA:          Color = Color::Rgb( 45,  79, 103);
    pub const LAVENDER_HAZE:     Color = Color::Rgb(147, 138, 169);
    pub const TWILIGHT_WISTERIA: Color = Color::Rgb(149, 127, 184);
    pub const SKY_GLAZE:         Color = Color::Rgb(126, 156, 216);
    pub const SEAFOAM_JADE:      Color = Color::Rgb(122, 168, 159);
    pub const SAKURA_BLOSSOM:    Color = Color::Rgb(210, 126, 153);
    pub const SAKURA_PETAL:      Color = Color::Rgb(238, 185, 225);
    pub const DIRTY_SAKURA_PETAL:      Color = Color::Rgb(177, 135, 166);
    pub const TORII_VERMILION:   Color = Color::Rgb(232,  36,  36);
    pub const SURF_BLUE:         Color = Color::Rgb(127, 180, 202);
    pub const MOSS_GREEN:        Color = Color::Rgb(152, 187, 108);
    pub const PEACH_BLUSH:       Color = Color::Rgb(228, 104, 118);
    pub const SUNSET_APRICOT:    Color = Color::Rgb(255, 160, 102);
    pub const PINE_NEEDLE:       Color = Color::Rgb(106, 149, 137);
    pub const OCHRE_SAND:        Color = Color::Rgb(230, 195, 132);
    pub const TEA_BISCUIT:       Color = Color::Rgb(192, 163, 110);
    pub const PEONY_RED:         Color = Color::Rgb(255,  93,  98);
    pub const PERIWINKLE_MIST:   Color = Color::Rgb(156, 171, 202);
    pub const SLATE_HARBOR:      Color = Color::Rgb(101, 133, 148);
}

/*──────────────────────── 2. Helper for legacy code ───────────────────────*/
/// Legacy helper: fetch one of the first 16 Kanagawa tones by index.
///
/// Only used by a couple of older call‑sites; feel free to delete when all
/// references are gone.
pub fn color(idx: u8) -> Color {
    use KANAGAWA::*;
    const TABLE: [Color; 16] = [
        INKSTONE, SKY_GLAZE, TWILIGHT_WISTERIA, PINE_NEEDLE, STEEL_VIOLET,
        SUNSET_APRICOT, SAKURA_BLOSSOM, OCHRE_SAND, RICE_PAPER, LAVENDER_HAZE,
        SEAFOAM_JADE, DEEP_SEA, TORII_VERMILION, SURF_BLUE, MOSS_GREEN,
        TEA_BISCUIT,
    ];
    TABLE[idx as usize & 0x0F]
}

/*──────────────────────── Load configured colors ──────────────────────────*/
pub static CONFIG_COLORS: Lazy<crate::config::ColorConfig> = Lazy::new(|| {
    crate::config::Config::load()
        .map(|c| c.colors)
        .unwrap_or_else(|_| crate::config::ColorConfig::default())
});

// Helper to convert RGB array to Color
#[inline]
pub fn rgb(arr: [u8; 3]) -> Color {
    Color::Rgb(arr[0], arr[1], arr[2])
}


/*──────────────────────── 3. Style shortcuts used throughout the UI ───────*/
/// Central place to theme every widget.
pub mod STYLE {
    use super::{Modifier, Style};
    const EMPTY: Modifier = Modifier::empty();

    /* ─── new: global background ─── */
    pub fn default_bg() -> Style {
        Style { fg: None, bg: Some(super::rgb(super::CONFIG_COLORS.default_bg)), add_modifier: EMPTY, sub_modifier: EMPTY }
    }

    /* ─── syntax highlighter ─── */
    pub fn kw() -> Style   { Style { fg: Some(super::rgb(super::CONFIG_COLORS.syntax_keyword)), bg: None, add_modifier: Modifier::BOLD, sub_modifier: EMPTY } }
    pub fn num() -> Style  { Style { fg: Some(super::rgb(super::CONFIG_COLORS.syntax_number)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn str_() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.syntax_string)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn cmt() -> Style  { Style { fg: Some(super::rgb(super::CONFIG_COLORS.syntax_comment)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn cast() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.syntax_cast)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn func() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.syntax_function)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn var_() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.syntax_variable)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn plain() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.syntax_plain)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
 
    /* ─── editor widget ─── */
    pub fn gutter_cur() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.gutter_current)), bg: None, add_modifier: Modifier::BOLD, sub_modifier: EMPTY } }
    pub fn gutter_rel() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.gutter_relative)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn caret_cell() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.caret_cell_fg)), bg: Some(super::rgb(super::CONFIG_COLORS.caret_cell_bg)), add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn selection_bg() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.selection_fg)), bg: Some(super::rgb(super::CONFIG_COLORS.selection_bg)), add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn bracket_match() -> Style { Style { fg: None, bg: Some(super::rgb(super::CONFIG_COLORS.bracket_match_bg)), add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn editor_border() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.editor_border)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn editor_border_focus() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.editor_border_focus)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
 
    /* ─── autocomplete dropdown ─── */
    pub fn autocomplete_bg() -> Style { Style { fg: None, bg: Some(super::rgb(super::CONFIG_COLORS.autocomplete_bg)), add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn autocomplete_border() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.autocomplete_border)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn autocomplete_selected() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.autocomplete_selected_fg)), bg: Some(super::rgb(super::CONFIG_COLORS.autocomplete_selected_bg)), add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn autocomplete_text() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.syntax_plain)), bg: Some(super::rgb(super::CONFIG_COLORS.autocomplete_bg)), add_modifier: EMPTY, sub_modifier: EMPTY } }

    /* ─── help / status ─── */
    pub fn help_bg() -> Style { Style { fg: None, bg: Some(super::rgb(super::CONFIG_COLORS.help_bg)), add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn help_border() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.help_border)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn status_fg() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.status_fg)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
 
    /* ─── results pane ─── */
    pub fn results_border() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.results_border)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn results_border_focus() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.results_border_focus)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn tab_active() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.tab_active)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn header_row() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.header_row)), bg: None, add_modifier: Modifier::BOLD, sub_modifier: EMPTY } }
    pub fn table_sel_bg() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.table_sel_fg)), bg: Some(super::rgb(super::CONFIG_COLORS.table_sel_bg)), add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn table_caret_bg() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.table_caret_fg)), bg: Some(super::rgb(super::CONFIG_COLORS.table_caret_bg)), add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn error_fg() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.error_fg)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
    pub fn info_fg() -> Style { Style { fg: Some(super::rgb(super::CONFIG_COLORS.info_fg)), bg: None, add_modifier: EMPTY, sub_modifier: EMPTY } }
}

/*──────────────────────── 4. Dummy palette guard ─────────────────────────*/
/// We no longer patch any system palette, but callers might still keep the
/// guard around.  Provide a no‑op stub to keep the API intact.
#[derive(Debug, Default)]
pub struct PaletteGuard;

pub fn apply_palette() -> anyhow::Result<PaletteGuard> { Ok(PaletteGuard) }

//#  RGB Suggested name
//1   22 22 22    INKSTONE
//2   30 31 40    OBSIDIAN_FOG
//3   42 42 55    INDIGO_SHADOW
//4   54 54 70    DUSKY_SLATE
//5   84 84 109   STEEL_VIOLET
//6   200 200 200 RICE_PAPER
//7   114 113 105 PUMICE
//8   34 50 73    WAVE_CREST
//9   45 79 103   DEEP_SEA
//10  147 138 169 LAVENDER_HAZE
//11  149 127 184 TWILIGHT_WISTERIA
//12  126 156 216 SKY_GLAZE
//13  122 168 159 SEAFOAM_JADE
//14  210 126 153 SAKURA_BLOSSOM
//15  232 36 36   TORII_VERMILION
//16  127 180 202 SURF_BLUE
//17  152 187 108 MOSS_GREEN
//18  228 104 118 PEACH_BLUSH
//19  255 160 102 SUNSET_APRICOT
//20  106 149 137 PINE_NEEDLE
//21  230 195 132 OCHRE_SAND
//22  192 163 110 TEA_BISCUIT
//23  255 93 98   PEONY_RED
//24  156 171 202 PERIWINKLE_MIST
//25  101 133 148 SLATE_HARBOR

