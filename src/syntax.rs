use once_cell::sync::Lazy;
use regex::Regex;
use std::{borrow::Cow, collections::HashSet};
use tui::style::Style;
use crate::palette::STYLE;

/*──── shortcuts to palette styles ───────────────────────────────*/
#[inline] fn kw()   -> Style { STYLE::kw() }
#[inline] fn num()  -> Style { STYLE::num() }
#[inline] fn str_() -> Style { STYLE::str_() }
#[inline] fn cmt()  -> Style { STYLE::cmt() }
#[inline] fn cast() -> Style { STYLE::cast() }
#[inline] fn func() -> Style { STYLE::func() }
#[inline] fn var_() -> Style { STYLE::var_() }
#[inline] fn plain()-> Style { STYLE::plain() }

/*──── keyword set ────────────────────────────────────────────────*/
static KEYWORDS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    [
        "SELECT","FROM","WHERE","INSERT","COPY","INTO","REMOVE","PUT","GET","VALUES","UPDATE","SET","DELETE",
        "CREATE","TABLE","VIEW","AS","DROP","USE","ALTER","ADD","DISTINCT","ORDER",
        "GROUP","BY","HAVING","JOIN","LEFT","RIGHT","FULL","INNER","OUTER","ON",
        "UNION","ALL","CASE","WHEN","THEN","ELSE","END","NULL","NOT","AND","OR","IF","FILE","FORMAT",
        "LIKE","ILIKE","IN","BETWEEN","IS","EXISTS","TRUE","FALSE","LIMIT",
        "OFFSET","WAREHOUSE","SCHEMA","DATABASE","FUNCTION","PROCEDURE","RETURN",
        "BEGIN","WITH","RETURNS","LANGUAGE","EXECUTE","REPLACE","TEMPORARY",
        "QUALIFY", "DECLARE", "CROSS","APPLY", "PARTITION", "OVER", "CALL", "DESC", "ASC", 
        "TOP", "TASK","RESUME","SUSPEND","PRECEDING","FOLLOWING","CURRENT","UNBOUNDED","RANGE","ROW","ROWS",
        "GRANT","REVOKE","TO","ROLE", "USER","USING","TEMPLATE",
        "DAY","WEEK","MONTH","QUARTER","YEAR",
        "DATE","DATETIME","TIMESTAMP","VARCHAR","STRING","INT","INTEGER","BIGINT","DOUBLE","CHAR","PRECISION"
    ]
    .into_iter()
    .collect()
});

/*──── compact regex for tokenising a single line ─────────────────*/
static TOKEN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(\$\$)|::|:[A-Za-z_][A-Za-z0-9_$]*|@@?[A-Za-z_][A-Za-z0-9_$]*|\$[A-Za-z_][A-Za-z0-9_$]*|--[^\r\n]*|/\*|\*/|'(?:[^'\\]|\\.|'')*'?|"(?:[^"\\]|\\.|"")*"?|\b[0-9]+(?:\.[0-9]+)?|\b[A-Za-z_][A-Za-z0-9_$]*|\s+|."#
    ).unwrap()
});

/*──── finite-state machine states ────────────────────────────────*/
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ParseState {
    Normal,
    InSingle,   // inside '…'
    InDouble,   // inside "…"
    InBlock,    // inside /* … */
    InLine,     // inside -- … to end-of-line
    InDollar,   // inside $$…$$
}

/*──── one-byte scanner result codes (splitter & caret use these) ─*/
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Step { Semi, Advance, Eof }

/*────────────────────────────────────────────────────────────────*/
pub type Seg<'a> = (Cow<'a, str>, Style);

/*──── public: high-light a single line, update state ────────────*/
pub fn highlight_line<'a>(line: &'a str, state: &mut ParseState) -> Vec<Seg<'a>> {
    let mut segs: Vec<Seg<'a>> = Vec::new();
    let mut start = 0usize;

    /*—— 1️⃣ finish an unterminated multi-line construct quickly ———*/
    match *state {
        ParseState::InSingle => {
            if let Some(pos) = line[start..].find('\'') {
                segs.push((Cow::Borrowed(&line[..start + pos + 1]), str_()));
                *state = ParseState::Normal;
                start += pos + 1;
            } else {
                segs.push((Cow::Borrowed(&line[start..]), str_()));
                return segs;
            }
        }
        ParseState::InDouble => {
            if let Some(pos) = line[start..].find('"') {
                segs.push((Cow::Borrowed(&line[..start + pos + 1]), str_()));
                *state = ParseState::Normal;
                start += pos + 1;
            } else {
                segs.push((Cow::Borrowed(&line[start..]), str_()));
                return segs;
            }
        }
        ParseState::InBlock => {
            if let Some(pos) = line[start..].find("*/") {
                segs.push((Cow::Borrowed(&line[..start + pos + 2]), cmt()));
                *state = ParseState::Normal;
                start += pos + 2;
            } else {
                segs.push((Cow::Borrowed(&line[start..]), cmt()));
                return segs;
            }
        }
        ParseState::InLine => {
            segs.push((Cow::Borrowed(&line[start..]), cmt()));
            *state = ParseState::Normal;
            return segs;
        }
        ParseState::InDollar => {
            if let Some(pos) = line[start..].find("$$") {
                segs.push((Cow::Borrowed(&line[..start + pos + 2]), str_()));
                *state = ParseState::Normal;
                start += pos + 2;
            } else {
                segs.push((Cow::Borrowed(&line[start..]), str_()));
                return segs;
            }
        }
        ParseState::Normal => {}
    }

    /*—— 2️⃣ tokenise the remainder (Normal mode) ———————————————*/
    let toks: Vec<&str> = TOKEN_RE
        .find_iter(&line[start..])
        .map(|m| &line[start + m.start()..start + m.end()])
        .collect();

    let mut idx = 0usize;
    while idx < toks.len() {
        let tok = toks[idx];

        /* line comment  -- … */
        if tok.starts_with("--") {
            segs.push((Cow::Borrowed(tok), cmt()));
            break;                              // rest of row is comment
        }

        /* dollar-quoted string $$ ... $$ */
        if tok == "$$" {
            segs.push((Cow::Borrowed(tok), str_()));
            *state = ParseState::InDollar;
            idx += 1;
            continue;
        }

        /* cast  :: ident */
        if tok == "::" {
            segs.push((Cow::Borrowed(tok), cast()));
            if let Some(id) = toks.get(idx + 1) {
                segs.push((Cow::Borrowed(*id), cast()));
                idx += 1;                       // skip ident
            }
            idx += 1;
            continue;
        }

        /* bind / session variable  :var  @var  @@var  $var */
        if tok.starts_with(':') || tok.starts_with("@@") || tok.starts_with('@') || tok.starts_with('$') {
            segs.push((Cow::Borrowed(tok), var_()));
            idx += 1;
            continue;
        }

        /* function  ident(  */
        if is_ident(tok) && toks.get(idx + 1) == Some(&"(") {
            segs.push((Cow::Borrowed(tok), func()));
            idx += 1;
            continue;
        }

        /* block comment open / close */
        if tok == "/*" {
            segs.push((Cow::Borrowed(tok), cmt()));
            *state = ParseState::InBlock;
            idx += 1;
            continue;
        }
        if tok == "*/" {
            segs.push((Cow::Borrowed(tok), cmt()));
            *state = ParseState::Normal;
            idx += 1;
            continue;
        }

        /* single-quoted literal */
        if tok.starts_with('\'') {
            classify_string(tok, '\'', &mut segs, state);
            idx += 1;
            continue;
        }

        /* double-quoted identifier */
        if tok.starts_with('"') {
            classify_string(tok, '"', &mut segs, state);
            idx += 1;
            continue;
        }

        /* number */
        if tok.chars().all(|c| c.is_ascii_digit() || c == '.') {
            segs.push((Cow::Borrowed(tok), num()));
            idx += 1;
            continue;
        }

        /* keyword */
        if KEYWORDS.contains(tok.to_ascii_uppercase().as_str()) {
            segs.push((Cow::Borrowed(tok), kw()));
            idx += 1;
            continue;
        }

        /* default */
        segs.push((Cow::Borrowed(tok), plain()));
        idx += 1;
    }
    segs
}

/*──── shared one-byte scanner (splitter / caret) ─────────────────*/
pub fn step(bytes: &[u8], mut i: usize, state: &mut ParseState) -> (usize, Step) {
    if i >= bytes.len() {
        return (i, Step::Eof);
    }
    macro_rules! next { () => { i += 1 } }

    match *state {
        /*—— Normal ———————————————————————————————*/
        ParseState::Normal => match bytes[i] {
            b'\''                                 => { *state = ParseState::InSingle; next!(); }
            b'"'                                  => { *state = ParseState::InDouble; next!(); }
            b'/' if bytes.get(i+1) == Some(&b'*') => { *state = ParseState::InBlock;  i += 2; }
            b'-' if bytes.get(i+1) == Some(&b'-') => { *state = ParseState::InLine;   i += 2; }
            b'$' if bytes.get(i+1) == Some(&b'$') => { *state = ParseState::InDollar; i += 2; }
            b';'                                  => { next!(); return (i, Step::Semi); }
            _                                     => { next!(); }
        },

        /*—— inside '…' ———————————————————————————*/
        ParseState::InSingle => {
            next!();
            match bytes[i - 1] {
                b'\'' => {
                    if bytes.get(i) == Some(&b'\'') { next!(); }   // doubled ''
                    else { *state = ParseState::Normal; }          // <-- change here
                }
                b'\\' if bytes.get(i) == Some(&b'\'') => { next!(); } // skip \'
                _ => {}
            }
        }

        /*—— inside "…" ———————————————————————————*/
        ParseState::InDouble => {
            next!();
            match bytes[i - 1] {
                b'"' => {                              // possible terminator
                    if bytes.get(i) == Some(&b'"') {   // doubled ""
                        next!();                       // skip escaped quote
                    } else {
                        *state = ParseState::Normal;   // <-- fixed line
                    }
                }
                b'\\' if bytes.get(i) == Some(&b'"') => { // NEW: escape \"
                    next!();                           // skip the escaped quote
                }
                _ => {}
            }
        }

        /*—— inside /* … */ ——————————————————————*/
        ParseState::InBlock => {
            next!();
            if bytes[i - 1] == b'*' && bytes.get(i) == Some(&b'/') {
                *state = ParseState::Normal; next!();
            }
        }

        /*—— inside -- … \n —————————————————————————*/
        ParseState::InLine => {
            next!();
            if bytes[i - 1] == b'\n' { *state = ParseState::Normal; }
        }

        /*—— inside $$…$$ ———————————————————————————*/
        ParseState::InDollar => {
            next!();
            if bytes[i - 1] == b'$' && bytes.get(i) == Some(&b'$') {
                *state = ParseState::Normal; next!();
            }
        }
    }
    (i, Step::Advance)
}

/*──── token helpers ─────────────────────────────────────────────*/
fn classify_string<'a>(
    tok: &'a str,
    quote: char,
    segs: &mut Vec<Seg<'a>>,
    state: &mut ParseState,
) {
    segs.push((Cow::Borrowed(&tok[..1]), str_()));      // opening quote
    let rest = &tok[1..];

    // NEW: locate the first *unescaped* quote
    let mut pos = None;
    let mut i = 0;
    let bytes = rest.as_bytes();
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {               // skip back‑slash escape
                i += 2; continue;
            }
            b'\'' | b'"' if bytes[i] == quote as u8 => {
                if bytes.get(i + 1) == Some(&bytes[i]) {   // doubled quote ''
                    i += 2; continue;
                }
                pos = Some(i);
                break;
            }
            _ => i += 1,
        }
    }

    match pos {
        Some(p) => {
            if p > 0 { segs.push((Cow::Borrowed(&rest[..p]), str_())); }
            segs.push((Cow::Borrowed(&rest[p..=p]), str_()));          // closing quote
            let tail = &rest[p + 1..];
            if !tail.is_empty() {
                let st = if tail.chars().all(|c| c.is_ascii_digit() || c == '.') {
                    num()
                } else if KEYWORDS.contains(tail.to_ascii_uppercase().as_str()) {
                    kw()
                } else { plain() };
                segs.push((Cow::Borrowed(tail), st));
            }
        }
        None => {
            if !rest.is_empty() { segs.push((Cow::Borrowed(rest), str_())); }
            *state = if quote == '\'' { ParseState::InSingle } else { ParseState::InDouble };
        }
    }
}

fn is_ident(tok: &str) -> bool {
    tok.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
}
