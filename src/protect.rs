//! What the model must not translate (mentions, links, code, custom emoji, spoilers) is replaced
//! by numbered placeholders `@@N@@` before translation and put back afterwards.

use super::*;
use ruma_html::{Html, NodeData, NodeRef};

/// A message's text: the plain `body` and, when it has one, the HTML `formatted_body`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SourceContent {
    pub(crate) plain: String,
    pub(crate) html: Option<String>,
}

/// The format a text is sent to the backend in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TextFormat {
    Html,
    Text,
}

impl TextFormat {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Html => "html",
            Self::Text => "text",
        }
    }
}

/// One protected piece; placeholder `@@N@@` stands for `segments[N - 1]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Segment {
    /// What it is in the plain body.
    pub(crate) plain: String,
    /// What it is in the HTML body (escaped for an attribute value when it was one).
    pub(crate) html: String,
    /// Block code: on a line of its own in the plain text.
    pub(crate) block: bool,
}

/// The source with placeholders: `html` for a formatted message, `plain` always (for a formatted
/// message the text of the protected HTML, for the retry without formatting).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Protected {
    pub(crate) plain: String,
    pub(crate) html: Option<String>,
}

/// A translation with the protected pieces back in place; `html` is sanitised.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Restored {
    pub(crate) plain: String,
    pub(crate) html: String,
}

/// A placeholder of the sent text is missing from the translation, or occurs a different number
/// of times.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Lost;

/// Plain text of a spoiler: its content stays hidden in the plain body too.
pub(crate) const SPOILER_PLAIN: &str = "[Spoiler]";

pub(crate) fn placeholder(number: usize) -> String {
    format!("@@{number}@@")
}

/// Length of the placeholder `@@<digits>@@` at the start of `text`, and its number.
fn placeholder_at(text: &str) -> Option<(usize, usize)> {
    let rest = text.strip_prefix("@@")?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 || digits > 6 || !rest[digits..].starts_with("@@") {
        return None;
    }
    Some((digits + 4, rest[..digits].parse().ok()?))
}

/// Each placeholder in `text`: its byte offset, length and number.
fn find_placeholders(text: &str) -> Vec<(usize, usize, usize)> {
    let mut found = Vec::new();
    let mut i = 0;
    while let Some(offset) = text[i..].find("@@") {
        let start = i + offset;
        match placeholder_at(&text[start..]) {
            Some((len, number)) => {
                found.push((start, len, number));
                i = start + len;
            }
            None => i = start + 1,
        }
    }
    found
}

/// Plain `text` with what [`protect`] would keep (URLs, Matrix IDs, code, placeholders)
/// replaced by spaces.
pub(crate) fn without_protected(text: &str) -> String {
    scan_text(text, true)
        .into_iter()
        .map(|piece| match piece {
            Piece::Text(text) => text,
            Piece::Kept(_) => " ",
        })
        .collect()
}

fn placeholder_counts(text: &str) -> BTreeMap<usize, usize> {
    let mut counts = BTreeMap::new();
    for (_, _, number) in find_placeholders(text) {
        *counts.entry(number).or_insert(0) += 1;
    }
    counts
}

/// `text` with each placeholder replaced by `piece(number)` and the text between them by
/// `between(text)`. `None` when `piece` has nothing for a number.
fn substitute(
    text: &str,
    piece: impl Fn(usize) -> Option<String>,
    between: impl Fn(&str) -> String,
) -> Option<String> {
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for (start, len, number) in find_placeholders(text) {
        out.push_str(&between(&text[last..start]));
        out.push_str(&piece(number)?);
        last = start + len;
    }
    out.push_str(&between(&text[last..]));
    Some(out)
}

#[derive(Default)]
struct Segments(Vec<Segment>);

impl Segments {
    /// The placeholder for a piece; the same piece twice gets the same number.
    fn add(&mut self, plain: &str, html: String, block: bool) -> String {
        let number = match self
            .0
            .iter()
            .position(|s| s.plain == plain && s.html == html)
        {
            Some(index) => index + 1,
            None => {
                self.0.push(Segment {
                    plain: plain.to_owned(),
                    html,
                    block,
                });
                self.0.len()
            }
        };
        placeholder(number)
    }
}

/// Replaces the protected pieces of `source` by placeholders.
pub(crate) fn protect(source: &SourceContent) -> (Protected, Vec<Segment>) {
    let mut segments = Segments::default();
    let protected = match &source.html {
        Some(html) => {
            let mut out = String::new();
            for child in Html::parse(html).children() {
                protect_node(&child, &mut out, &mut segments);
            }
            let html = out.trim().to_owned();
            let plain = blocks_on_own_lines(&html_to_plain(&html), &segments.0);
            Protected {
                plain,
                html: Some(html),
            }
        }
        None => {
            let mut plain = String::new();
            for piece in scan_text(&source.plain, true) {
                match piece {
                    Piece::Text(text) => plain.push_str(text),
                    Piece::Kept(kept) => plain.push_str(&segments.add(
                        kept,
                        html_escape(kept).replace('\n', "<br>"),
                        false,
                    )),
                }
            }
            Protected { plain, html: None }
        }
    };
    (protected, segments.0)
}

/// Puts the translation back together. `sent` is what went to the backend, in `format`; each of
/// its placeholders must occur in `translated` exactly as often.
pub(crate) fn restore(
    translated: &str,
    sent: &str,
    format: TextFormat,
    segments: &[Segment],
) -> Result<Restored, Lost> {
    if placeholder_counts(translated) != placeholder_counts(sent) {
        return Err(Lost);
    }
    let segment = |number: usize| number.checked_sub(1).and_then(|i| segments.get(i));
    match format {
        TextFormat::Html => {
            let html = substitute(
                translated,
                |n| Some(segment(n)?.html.clone()),
                str::to_owned,
            )
            .ok_or(Lost)?;
            let html = outgoing_html(&html);
            Ok(Restored {
                plain: html_to_plain(&html),
                html,
            })
        }
        // The translation of plain text is text, not markup: escaped for the HTML body, so a
        // `<b>`, a link or an `&` in it shows as written instead of being interpreted.
        TextFormat::Text => {
            let plain = substitute(
                translated,
                |n| Some(segment(n)?.plain.clone()),
                str::to_owned,
            )
            .ok_or(Lost)?;
            let html = substitute(
                translated,
                |n| Some(segment(n)?.html.clone()),
                |text| html_escape(text).replace('\n', "<br>"),
            )
            .ok_or(Lost)?;
            Ok(Restored {
                plain,
                html: outgoing_html(&html),
            })
        }
    }
}

enum Piece<'a> {
    Text(&'a str),
    Kept(&'a str),
}

/// Splits plain text into translatable text and what is kept: URLs, Matrix IDs, literal
/// placeholders and, when `code` is set, inline and fenced code.
fn scan_text(text: &str, code: bool) -> Vec<Piece<'_>> {
    let mut pieces = Vec::new();
    let mut text_start = 0;
    let mut i = 0;
    let mut word_start = true;
    while i < text.len() {
        let rest = &text[i..];
        let mut kept = None;
        if code && rest.starts_with('`') {
            let fence = if rest.starts_with("```") { "```" } else { "`" };
            if let Some(end) = rest[fence.len()..].find(fence) {
                kept = Some((i, fence.len() + end + fence.len()));
            }
        }
        if kept.is_none() {
            if let Some((len, _)) = placeholder_at(rest) {
                kept = Some((i, len));
            }
        }
        if kept.is_none() && word_start {
            let word_len = rest.find(char::is_whitespace).unwrap_or(rest.len());
            if let Some((offset, len)) = link_or_id_in_word(&rest[..word_len]) {
                kept = Some((i + offset, len));
            }
        }
        match kept {
            Some((start, len)) => {
                if start > text_start {
                    pieces.push(Piece::Text(&text[text_start..start]));
                }
                pieces.push(Piece::Kept(&text[start..start + len]));
                i = start + len;
                text_start = i;
                word_start = false;
            }
            None => {
                let c = rest.chars().next().expect("not at the end");
                word_start = c.is_whitespace();
                i += c.len_utf8();
            }
        }
    }
    if text_start < text.len() {
        pieces.push(Piece::Text(&text[text_start..]));
    }
    pieces
}

/// The URL or Matrix ID a word is, without surrounding brackets, quotes and final punctuation:
/// its offset and length in the word.
fn link_or_id_in_word(word: &str) -> Option<(usize, usize)> {
    let core = word.trim_start_matches(['(', '[', '<', '"', '\'']);
    let offset = word.len() - core.len();
    let core = core.trim_end_matches([
        '(', ')', '[', ']', '<', '>', '"', '\'', '.', ',', ';', ':', '!', '?',
    ]);
    if core.contains("@@") {
        return None;
    }
    let is_url = core.starts_with("www.")
        || core.split_once("://").is_some_and(|(scheme, rest)| {
            !scheme.is_empty()
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '+')
                && !rest.is_empty()
        });
    let is_id = core
        .strip_prefix(['@', '#', '!'])
        .and_then(|id| id.split_once(':'))
        .is_some_and(|(local, server)| {
            !local.is_empty()
                && !server.is_empty()
                && server
                    .chars()
                    .all(|c| c.is_alphanumeric() || matches!(c, '.' | '-' | ':' | '[' | ']'))
        });
    (is_url || is_id).then_some((offset, core.len()))
}

fn attr(node: &NodeRef, name: &str) -> Option<String> {
    let element = node.as_element()?;
    let attrs = element.attrs.borrow();
    attrs
        .iter()
        .find(|a| &*a.name.local == name)
        .map(|a| a.value.to_string())
}

fn element_name(node: &NodeRef) -> Option<String> {
    node.as_element().map(|e| e.name.local.to_string())
}

fn is_mention_link(href: &str) -> bool {
    href.starts_with("https://matrix.to/") || href.starts_with("matrix:")
}

fn protect_node(node: &NodeRef, out: &mut String, segments: &mut Segments) {
    match node.data() {
        NodeData::Text(text) => {
            for piece in scan_text(&text.borrow(), false) {
                match piece {
                    Piece::Text(text) => out.push_str(&html_escape(text)),
                    Piece::Kept(kept) => {
                        out.push_str(&segments.add(kept, html_escape(kept), false))
                    }
                }
            }
        }
        NodeData::Element(_) => {
            let name = element_name(node).unwrap_or_default();
            let whole = |plain: String, block: bool, out: &mut String, segments: &mut Segments| {
                let mut html = String::new();
                write_node(node, &mut html);
                out.push_str(&segments.add(&plain, html, block));
            };
            match name.as_str() {
                "mx-reply" => {}
                "a" => match attr(node, "href") {
                    Some(href) => {
                        let text = text_content(node);
                        if is_mention_link(&href)
                            || text.trim() == href
                            || link_or_id_in_word(text.trim())
                                .is_some_and(|(_, len)| len == text.trim().len())
                        {
                            whole(text, false, out, segments);
                        } else {
                            let href_placeholder = segments.add(&href, html_escape(&href), false);
                            write_start_tag(node, out, Some(&href_placeholder));
                            for child in node.children() {
                                protect_node(&child, out, segments);
                            }
                            out.push_str("</a>");
                        }
                    }
                    None => protect_children(node, &name, out, segments),
                },
                "code" => whole(text_content(node), false, out, segments),
                "pre" => whole(text_content(node), true, out, segments),
                "img" if attr(node, "data-mx-emoticon").is_some() => {
                    let alt = attr(node, "alt").or_else(|| attr(node, "title"));
                    whole(alt.unwrap_or_default(), false, out, segments)
                }
                "span" if attr(node, "data-mx-spoiler").is_some() => {
                    whole(SPOILER_PLAIN.to_owned(), false, out, segments)
                }
                _ => protect_children(node, &name, out, segments),
            }
        }
        _ => {}
    }
}

fn protect_children(node: &NodeRef, name: &str, out: &mut String, segments: &mut Segments) {
    write_start_tag(node, out, None);
    if !is_void(name) {
        for child in node.children() {
            protect_node(&child, out, segments);
        }
        out.push_str(&format!("</{name}>"));
    }
}

/// Block segments on lines of their own in the plain text.
fn blocks_on_own_lines(plain: &str, segments: &[Segment]) -> String {
    let mut out = String::with_capacity(plain.len());
    let mut last = 0;
    for (start, len, number) in find_placeholders(plain) {
        out.push_str(&plain[last..start]);
        let block = segments.get(number - 1).is_some_and(|s| s.block);
        if block && !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str(&plain[start..start + len]);
        last = start + len;
        if block && last < plain.len() && !plain[last..].starts_with('\n') {
            out.push('\n');
        }
    }
    out.push_str(&plain[last..]);
    out
}

pub(crate) fn is_void(name: &str) -> bool {
    matches!(
        name,
        "br" | "hr"
            | "img"
            | "wbr"
            | "area"
            | "base"
            | "col"
            | "embed"
            | "input"
            | "link"
            | "meta"
            | "source"
            | "track"
    )
}

/// The start tag of an element; `href` replaces the value of its `href` attribute.
fn write_start_tag(node: &NodeRef, out: &mut String, href: Option<&str>) {
    let Some(element) = node.as_element() else {
        return;
    };
    out.push('<');
    out.push_str(&element.name.local);
    for a in element.attrs.borrow().iter() {
        out.push(' ');
        out.push_str(&a.name.local);
        out.push_str("=\"");
        match href {
            Some(href) if &*a.name.local == "href" => out.push_str(href),
            _ => out.push_str(&html_escape(&a.value)),
        }
        out.push('"');
    }
    out.push('>');
}

/// Serialises a node as HTML.
pub(crate) fn write_node(node: &NodeRef, out: &mut String) {
    match node.data() {
        NodeData::Text(text) => out.push_str(&html_escape(&text.borrow())),
        NodeData::Element(element) => {
            write_start_tag(node, out, None);
            let name = &*element.name.local;
            if !is_void(name) {
                for child in node.children() {
                    write_node(&child, out);
                }
                out.push_str("</");
                out.push_str(name);
                out.push('>');
            }
        }
        _ => {}
    }
}

/// The text of a node and its descendants, as written.
pub(crate) fn text_content(node: &NodeRef) -> String {
    match node.data() {
        NodeData::Text(text) => text.borrow().to_string(),
        _ => node.children().map(|child| text_content(&child)).collect(),
    }
}

pub(crate) fn html_escape(text: &str) -> String {
    mxbot_common::format::html_escape(text)
}
