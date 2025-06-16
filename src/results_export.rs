//! Clipboard, CSV, and export helpers for Results pane
//! Handles the NULL sentinel <Frost-NULL> so that
//!   • clipboard/tab‐copy shows empty cells
//!   • CSV export writes empty fields

use crate::results_selection::{ResultSelection, SelectionKind};
use crate::tile_rowstore::NULL_SENTINEL;

use std::cmp::{max, min};

/// Convert sentinel → empty string (all other values passthrough)
#[inline]
fn clean<'a>(s: &'a str) -> &'a str {
    if s == NULL_SENTINEL { "" } else { s }
}

/// RFC-4180 CSV escaping
pub fn escape_csv(field: &str) -> String {
    let needs_quotes = field.contains(',') || field.contains('"')
        || field.contains('\n') || field.contains('\r');
    if needs_quotes {
        let escaped = field.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        field.to_owned()
    }
}

/// Copy the current selection as **tab-separated text** (for clipboard)
pub fn copy_selection(
    sel: &ResultSelection,
    headers: &Vec<String>,
    data: &Vec<Vec<String>>,
) -> String {
    use SelectionKind::*;

    match &sel.kind {
        /* ───────────── column range (contiguous) ───────────── */
        FullColSet { anchor, cursor } => {
            let s = min(*anchor, *cursor);
            let e = max(*anchor, *cursor);
            let mut out = vec![ headers[s - 1 ..= e - 1]
                .iter().map(|h| clean(h)).collect::<Vec<_>>().join("\t") ];

            for row in data {
                out.push(
                    row[s - 1 ..= e - 1]
                        .iter()
                        .map(|c| clean(c))
                        .collect::<Vec<_>>()
                        .join("\t"),
                );
            }
            out.join("\n")
        }

        /* ───────────── column vector (non-contiguous) ───────── */
        FullColVec(cols) => {
            let mut cols = cols.clone(); cols.sort_unstable();

            let mut out = vec![ cols.iter().filter(|&&c| c > 0)
                .map(|&c| clean(&headers[c - 1])).collect::<Vec<_>>().join("\t") ];

            for row in data {
                out.push(
                    cols.iter().filter(|&&c| c > 0)
                        .map(|&c| clean(&row[c - 1])).collect::<Vec<_>>().join("\t"),
                );
            }
            out.join("\n")
        }

        /* ───────────── row range (contiguous) ──────────────── */
        FullRowSet { anchor, cursor } => {
            let s = min(*anchor, *cursor);
            let e = max(*anchor, *cursor);

            let mut out = vec![ headers.iter().map(|h| clean(h)).collect::<Vec<_>>().join("\t") ];

            for r in s..=e {
                if r < data.len() {
                    out.push(
                        data[r].iter().map(|c| clean(c)).collect::<Vec<_>>().join("\t")
                    );
                }
            }
            out.join("\n")
        }

        /* ───────────── row vector (non-contiguous) ─────────── */
        FullRowVec(rows) => {
            let mut rows = rows.clone(); rows.sort_unstable();

            let mut out = vec![ headers.iter().map(|h| clean(h)).collect::<Vec<_>>().join("\t") ];

            for &r in &rows {
                if r < data.len() {
                    out.push(
                        data[r].iter().map(|c| clean(c)).collect::<Vec<_>>().join("\t")
                    );
                }
            }
            out.join("\n")
        }

        /* ───────────── rectangular selection ───────────────── */
        Rect => {
            if let (Some(a), Some(b)) = (sel.anchor, sel.cursor) {
                let r0 = min(a.0, b.0); let r1 = max(a.0, b.0);
                let c0 = min(a.1, b.1).max(1); let c1 = max(a.1, b.1);

                let range_cols: Vec<usize> = (c0..=c1).collect();
                let range_rows: Vec<usize> = (r0..=r1).collect();

                /* single cell → just return the contents */
                if range_cols.len() == 1 && range_rows.len() == 1 {
                    let rc = range_rows[0]; let cc = range_cols[0];
                    return clean(&data[rc][cc - 1]).to_owned();
                }

                let mut out = Vec::new();
                /* header row for the block */
                out.push(
                    range_cols.iter()
                        .map(|&c| clean(&headers[c - 1]))
                        .collect::<Vec<_>>()
                        .join("\t"),
                );

                for r in range_rows {
                    if r >= data.len() { continue; }
                    let row_items = range_cols.iter()
                        .map(|&c| clean(&data[r][c - 1]))
                        .collect::<Vec<_>>()
                        .join("\t");
                    out.push(row_items);
                }
                out.join("\n")
            } else {
                String::new()
            }
        }

        None => String::new(),
    }
}

/// Export the **entire** result set as CSV (RFC-4180)
pub fn export_entire_result_set(
    headers: &Vec<String>,
    data: &Vec<Vec<String>>,
) -> String {
    let mut csv_lines = Vec::with_capacity(data.len() + 1);

    /* header */
    csv_lines.push(
        headers.iter()
            .map(|h| escape_csv(clean(h)))
            .collect::<Vec<_>>()
            .join(","),
    );

    /* rows */
    for row in data {
        csv_lines.push(
            row.iter()
                .map(|c| escape_csv(clean(c)))
                .collect::<Vec<_>>()
                .join(","),
        );
    }

    csv_lines.join("\r\n")
}
