//! Building translation messages: flags, Markdown/HTML rendering, reply relations.

use super::*;

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

/// Parse `markdown` and return the indices + full text of every `Event::Text`
/// node that sits outside a code block.  These are the nodes that should be
/// translated; all other events (code blocks, inline code, HTML, URLs) are
/// left untouched.
#[allow(dead_code)]
pub(crate) fn render_html(markdown: &str) -> String {
    let opts = Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TABLES
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES;
    let mut html = String::new();
    push_html(&mut html, Parser::new_ext(markdown, opts));
    html
}

/// Collapse block-level `<p>` tags into inline content suitable for embedding
/// inside a single Matrix message line. Inter-paragraph breaks become `<br>`.
pub(crate) fn inline_html(html: &str) -> String {
    html.trim()
        .replace("</p>\n<p>", "<br>")
        .replace("</p><p>", "<br>")
        .replace("</p>", "")
        .replace("<p>", "")
        .trim()
        .to_owned()
}

/// Strip HTML tags to produce a plain-text fallback for Matrix `body`.
/// Block-level closing tags are replaced with newlines for readability.
pub(crate) fn html_to_plain(html: &str) -> String {
    let s = html
        .replace("</p>", "\n\n")
        .replace("</li>", "\n")
        .replace("<br>", "\n")
        .replace("<br/>", "\n")
        .replace("<br />", "\n")
        .replace("</blockquote>", "\n");
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    out.trim()
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

/// Strip the `<mx-reply>…</mx-reply>` fallback block that Matrix clients prepend
/// to `formatted_body` when a message is a reply.  Returns the remaining HTML.
pub(crate) fn strip_mx_reply(html: &str) -> String {
    if let Some(start) = html.find("<mx-reply>") {
        if let Some(rel_end) = html[start..].find("</mx-reply>") {
            return html[start + rel_end + "</mx-reply>".len()..]
                .trim_start()
                .to_owned();
        }
    }
    html.to_owned()
}

/// Strip the leading Matrix reply-fallback block from a plain-text body:
/// consecutive "> " lines at the top, followed by a blank separator line.
/// Only the top block is removed so intentional blockquotes later in the
/// message are preserved.
pub(crate) fn strip_reply_fallback(raw: &str) -> String {
    if raw.starts_with("> ") {
        raw.lines()
            .skip_while(|l| l.starts_with("> "))
            .skip_while(|l| l.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        raw.to_owned()
    }
}

pub(crate) fn build_text_line(
    target: &str,
    source_plain: &str,
    translated: String,
) -> Result<TranslatedLine, TranslationError> {
    let translated = translated.trim().to_owned();
    if translated.is_empty() {
        return Err(TranslationError::new(
            TranslationErrorKind::Empty,
            "translation produced empty text",
        ));
    }
    if let Some(reason) = translation_rejection_reason(source_plain, &translated) {
        return Err(TranslationError::new(
            TranslationErrorKind::Rejected,
            reason,
        ));
    }
    Ok(TranslatedLine {
        target: target.to_owned(),
        plain: translated.clone(),
        html: translated,
    })
}

pub(crate) fn build_html_line(
    target: &str,
    source_plain: &str,
    translated_html: String,
) -> Result<TranslatedLine, TranslationError> {
    let plain = html_to_plain(&translated_html);
    if plain.is_empty() {
        return Err(TranslationError::new(
            TranslationErrorKind::Empty,
            "translation produced empty text",
        ));
    }
    if let Some(reason) = translation_rejection_reason(source_plain, &plain) {
        return Err(TranslationError::new(
            TranslationErrorKind::Rejected,
            reason,
        ));
    }
    Ok(TranslatedLine {
        target: target.to_owned(),
        plain,
        html: inline_html(&translated_html),
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
pub(crate) fn make_translation_content(
    plain: String,
    html: String,
    silent: bool,
) -> RoomMessageEventContent {
    if silent {
        RoomMessageEventContent::notice_html(plain, html)
    } else {
        RoomMessageEventContent::text_html(plain, html)
    }
}
