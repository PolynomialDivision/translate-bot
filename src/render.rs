//! Building translation messages: flags, HTML to text, sanitising, reply relations.

use super::*;
use ruma_html::{Html, ListBehavior, NodeRef, PropertiesNames, SanitizerConfig};

pub(crate) fn flag_for_lang(lang: &str) -> &'static str {
    match lang {
        "en" => "🇬🇧",
        "de" => "🇩🇪",
        "uk" => "🇺🇦",
        "fr" => "🇫🇷",
        "es" => "🇪🇸",
        "it" => "🇮🇹",
        "pt" => "🇵🇹",
        "pl" => "🇵🇱",
        "nl" => "🇳🇱",
        "ru" => "🇷🇺",
        "zh" => "🇨🇳",
        "ja" => "🇯🇵",
        "ko" => "🇰🇷",
        "ar" => "🇸🇦",
        "tr" => "🇹🇷",
        "sv" => "🇸🇪",
        _ => "🌐",
    }
}
pub(crate) fn build_translation_bodies(lines: &[TranslatedLine]) -> (String, String) {
    let plain_lines = lines
        .iter()
        .map(|line| format!("{} {}", flag_for_lang(&line.target), line.plain))
        .collect::<Vec<_>>();
    let html_lines = lines
        .iter()
        .map(|line| format!("{} {}", flag_for_lang(&line.target), line.html))
        .collect::<Vec<_>>();

    (plain_lines.join("\n"), html_lines.join("<br>\n"))
}

/// Top-level paragraphs collapsed into one line of a Matrix message: their content, separated by
/// `<br>`. Other elements stay as they are.
pub(crate) fn inline_html(html: &Html) -> String {
    let children: Vec<NodeRef> = html.children().collect();
    let is_block = |node: Option<&NodeRef>| {
        node.and_then(|n| n.as_element())
            .is_some_and(|e| is_block_element(&e.name.local))
    };
    #[derive(PartialEq)]
    enum Last {
        Nothing,
        Paragraph,
        Inline,
        Block,
    }
    let mut out = String::new();
    let mut last = Last::Nothing;
    for (i, child) in children.iter().enumerate() {
        if let Some(text) = child.as_text() {
            let blank = text.borrow().trim().is_empty();
            let at_edge = i == 0 || i + 1 == children.len();
            if blank && (at_edge || is_block(children.get(i - 1)) || is_block(children.get(i + 1)))
            {
                continue;
            }
        }
        let name = child.as_element().map(|e| e.name.local.to_string());
        match name.as_deref() {
            Some("p") => {
                let mut content = String::new();
                for grandchild in child.children() {
                    protect::write_node(&grandchild, &mut content);
                }
                if content.trim().is_empty() {
                    continue;
                }
                if matches!(last, Last::Paragraph | Last::Inline) {
                    out.push_str("<br>");
                }
                out.push_str(content.trim());
                last = Last::Paragraph;
            }
            Some(name) if is_block_element(name) => {
                protect::write_node(child, &mut out);
                last = Last::Block;
            }
            _ => {
                if last == Last::Paragraph {
                    out.push_str("<br>");
                }
                protect::write_node(child, &mut out);
                last = Last::Inline;
            }
        }
    }
    out.trim().to_owned()
}

fn is_block_element(name: &str) -> bool {
    matches!(
        name,
        "p" | "pre"
            | "ul"
            | "ol"
            | "blockquote"
            | "div"
            | "h1"
            | "h2"
            | "h3"
            | "h4"
            | "h5"
            | "h6"
            | "table"
            | "hr"
            | "details"
    )
}

/// The HTML a translation is posted with: the Matrix subset (strict), without reply fallback,
/// scripts or styles, custom emoji kept; paragraphs collapsed by [`inline_html`].
pub(crate) fn outgoing_html(html: &str) -> String {
    let html = Html::parse(html);
    html.sanitize_with(
        &SanitizerConfig::strict()
            .remove_reply_fallback()
            .remove_elements(["script", "style"])
            .allow_attributes(
                [PropertiesNames {
                    parent: "img",
                    properties: &["data-mx-emoticon"],
                }],
                ListBehavior::Add,
            ),
    );
    inline_html(&html)
}

/// The plain-text form of HTML, for the Matrix `body`: block ends become line breaks, a custom
/// emoji its `alt` text, a spoiler [`protect::SPOILER_PLAIN`]; the reply fallback is left out.
pub(crate) fn html_to_plain(html: &str) -> String {
    let mut out = String::new();
    for child in Html::parse(html).children() {
        push_plain(&child, &mut out);
    }
    out.trim().to_owned()
}

fn push_plain(node: &NodeRef, out: &mut String) {
    if let Some(text) = node.as_text() {
        let text = text.borrow();
        // Whitespace between blocks is no content of its own; line breaks after one neither.
        if out.is_empty() || out.ends_with('\n') {
            if !text.trim().is_empty() {
                out.push_str(text.trim_start_matches(['\n', '\r']));
            }
        } else {
            out.push_str(&text);
        }
        return;
    }
    let Some(element) = node.as_element() else {
        return;
    };
    let name = &*element.name.local;
    let has_attr = |attr: &str| {
        element
            .attrs
            .borrow()
            .iter()
            .any(|a| &*a.name.local == attr)
    };
    let line_break = |out: &mut String| {
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
    };
    match name {
        "br" => out.push('\n'),
        "mx-reply" => {}
        "img" => {
            let attrs = element.attrs.borrow();
            if let Some(alt) = attrs.iter().find(|a| &*a.name.local == "alt") {
                out.push_str(&alt.value);
            }
        }
        "span" if has_attr("data-mx-spoiler") => out.push_str(protect::SPOILER_PLAIN),
        _ => {
            let block = is_block_element(name) || matches!(name, "li" | "tr");
            if block {
                line_break(out);
            }
            for child in node.children() {
                push_plain(&child, out);
            }
            if name == "p" {
                line_break(out);
                out.push('\n');
            } else if block {
                line_break(out);
            }
        }
    }
}

/// The leading Matrix reply-fallback block of a plain-text body (the `>` lines at the top
/// and the blank lines after them) is removed; a blockquote later in the message stays.
pub(crate) use mxbot_common::format::strip_reply_fallback;

/// One target's line from the backend's answer to `sent` (in `format`, with placeholders): checked
/// for emptiness, runaway output and lost placeholders, then restored.
pub(crate) fn build_line(
    target: &str,
    sent: &str,
    translated: &str,
    format: TextFormat,
    segments: &[Segment],
) -> Result<TranslatedLine, TranslationError> {
    let translated = translated.trim();
    let (source_plain, translated_plain) = match format {
        TextFormat::Html => (html_to_plain(sent), html_to_plain(translated)),
        TextFormat::Text => (sent.to_owned(), translated.to_owned()),
    };
    if translated_plain.is_empty() {
        return Err(TranslationError::new(
            TranslationErrorKind::Empty,
            "translation produced empty text",
        ));
    }
    if let Some(reason) = translation_rejection_reason(&source_plain, &translated_plain) {
        return Err(TranslationError::new(
            TranslationErrorKind::Rejected,
            reason,
        ));
    }
    let restored = restore(translated, sent, format, segments).map_err(|Lost| {
        TranslationError::new(
            TranslationErrorKind::Rejected,
            "translation lost a placeholder",
        )
    })?;
    Ok(TranslatedLine {
        target: target.to_owned(),
        plain: restored.plain,
        html: restored.html,
    })
}

/// Compute the `relates_to` relation for a translation message.
///
/// | reply_to_original | in_thread | thread_replies | result              |
/// |-------------------|-----------|----------------|---------------------|
/// | false             | *         | *              | None (standalone)   |
/// | true              | true      | *              | m.thread            |
/// | true              | false     | false          | m.in_reply_to reply |
/// | true              | false     | true           | m.thread            |
///
/// `in_thread`: whether the original event is already inside a thread.
///   When true the bot always thread-replies into that same thread, regardless
///   of `thread_replies` config.  `thread_replies = false` only suppresses the
///   bot from *opening* new threads on standalone messages.
///
/// `event_id`    – the event being translated (used as reply target / thread in_reply_to).
/// `thread_root` – the thread root to use; pass `resolve_thread_root(event)` at call sites.
pub(crate) fn make_relation(
    event_id: &OwnedEventId,
    thread_root: &OwnedEventId,
    reply_to_original: bool,
    thread_replies: bool,
    in_thread: bool,
) -> Option<Relation<RoomMessageEventContentWithoutRelation>> {
    if !reply_to_original {
        return None;
    }
    if in_thread || thread_replies {
        Some(Relation::Thread(Thread::reply(
            thread_root.clone(),
            event_id.clone(),
        )))
    } else {
        Some(Relation::Reply(Reply::new(InReplyTo::new(
            event_id.clone(),
        ))))
    }
}

/// Build a translated message content with the right msgtype.
/// `silent = true` → `m.notice` (suppressed push, muted styling in most clients).
/// `silent = false` → `m.text` (default behaviour).
/// Carries an empty `m.mentions`: a name or `@room` in a translation notifies nobody.
pub(crate) fn make_translation_content(
    plain: String,
    html: String,
    silent: bool,
) -> RoomMessageEventContent {
    let mut content = if silent {
        RoomMessageEventContent::notice_html(plain, html)
    } else {
        RoomMessageEventContent::text_html(plain, html)
    };
    content.mentions = Some(Mentions::new());
    content
}

/// An `m.replace` of the bot's translation `bot_event_id` with `lines`, keeping its msgtype.
pub(crate) fn make_translation_edit(
    lines: &[TranslatedLine],
    silent: bool,
    bot_event_id: OwnedEventId,
) -> RoomMessageEventContent {
    let (body, html) = build_translation_bodies(lines);
    let msgtype = if silent {
        MessageType::Notice(NoticeMessageEventContent::html(body.clone(), html.clone()))
    } else {
        MessageType::Text(TextMessageEventContent::html(body.clone(), html.clone()))
    };
    let mut new_content = RoomMessageEventContentWithoutRelation::new(msgtype);
    new_content.mentions = Some(Mentions::new());
    let mut edit = make_translation_content(format!("* {body}"), html, silent);
    edit.relates_to = Some(Relation::Replacement(Replacement::new(
        bot_event_id,
        new_content,
    )));
    edit
}
