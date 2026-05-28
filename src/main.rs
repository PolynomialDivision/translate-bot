use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use mxbot_common::config::{MatrixConfig, SecurityConfig};

fn flag_for_lang(lang: &str) -> &'static str {
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

use anyhow::Result;
use pulldown_cmark::{Options, Parser};
use pulldown_cmark::html::push_html;
use matrix_sdk::{
    Client, Room, RoomState,
    config::SyncSettings,
    ruma::{
        OwnedEventId, OwnedServerName, OwnedUserId, RoomOrAliasId,
        api::client::filter::FilterDefinition,
        events::{
            key::verification::request::ToDeviceKeyVerificationRequestEvent,
            relation::{Replacement, Thread},
            room::{
                member::StrippedRoomMemberEvent,
                message::{
                    MessageType, OriginalSyncRoomMessageEvent,
                    Relation, RoomMessageEventContent,
                    RoomMessageEventContentWithoutRelation, TextMessageEventContent,
                },
                redaction::OriginalSyncRoomRedactionEvent,
            },
        },
    },
};
use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Mutex, time::sleep};
use tracing::{error, info, warn};

#[derive(Deserialize)]
struct Config {
    matrix: MatrixConfig,
    libretranslate: LibreTranslateConfig,
    #[serde(default)]
    translation: TranslationConfig,
    #[serde(default)]
    security: SecurityConfig,
}

#[derive(Deserialize)]
struct LibreTranslateConfig {
    url: String,
    api_key: Option<String>,
}

#[derive(Deserialize)]
struct TranslationConfig {
    #[serde(default = "default_langs")]
    langs: Vec<String>,
    #[serde(default = "default_min_confidence")]
    min_confidence: f64,
    /// Post translations inside the Matrix thread of the original message (default: true).
    /// Set to false to post as a plain room message instead.
    #[serde(default = "default_true")]
    thread_replies: bool,
}

fn default_langs() -> Vec<String> {
    vec!["en".to_owned(), "de".to_owned()]
}

fn default_min_confidence() -> f64 {
    0.5
}

fn default_true() -> bool { true }

impl Default for TranslationConfig {
    fn default() -> Self {
        Self { langs: default_langs(), min_confidence: default_min_confidence(), thread_replies: true }
    }
}

#[derive(Serialize)]
struct DetectRequest<'a> {
    q: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key: Option<&'a str>,
}

#[derive(Deserialize)]
struct DetectResult {
    language: String,
    confidence: f64,
}

#[derive(Serialize)]
struct TranslateRequest<'a> {
    q: &'a str,
    source: &'a str,
    target: &'a str,
    format: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key: Option<&'a str>,
}

#[derive(Deserialize)]
struct TranslateResponse {
    #[serde(rename = "translatedText")]
    translated_text: String,
}

#[derive(Clone)]
struct BotState {
    lt_url: String,
    lt_api_key: Option<String>,
    langs: Vec<String>,
    min_confidence: f64,
    thread_replies: bool,
    http: reqwest::Client,
    bot_user_id: OwnedUserId,
    admin_users: HashSet<OwnedUserId>,
    allowed_inviters: HashSet<OwnedUserId>,
    // Users allowed to re-verify despite already having a verified device.
    // Populated by !reset-trust from an admin, cleared after use.
    reset_allowed: Arc<Mutex<HashSet<OwnedUserId>>>,
    // Maps user's original event_id → bot's translation event_id.
    // Used to edit the bot's translation when the user edits their message.
    translation_map: Arc<RwLock<HashMap<OwnedEventId, OwnedEventId>>>,
}

impl BotState {
    async fn detect(&self, text: &str) -> Option<(String, f64)> {
        let resp = self
            .http
            .post(format!("{}/detect", self.lt_url))
            .json(&DetectRequest { q: text, api_key: self.lt_api_key.as_deref() })
            .send()
            .await
            .ok()?;
        let results: Vec<DetectResult> = resp.json().await.ok()?;
        results
            .into_iter()
            .max_by(|a, b| {
                a.confidence.partial_cmp(&b.confidence).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|r| (r.language, r.confidence))
    }

    async fn translate(&self, text: &str, source: &str, target: &str, format: &str) -> Option<String> {
        let resp = self
            .http
            .post(format!("{}/translate", self.lt_url))
            .json(&TranslateRequest {
                q: text,
                source,
                target,
                format,
                api_key: self.lt_api_key.as_deref(),
            })
            .send()
            .await
            .ok()?;
        let result: TranslateResponse = resp.json().await.ok()?;
        Some(result.translated_text)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config_path = std::env::args().nth(1).unwrap_or_else(|| "config.toml".to_owned());
    let config_str = fs::read_to_string(&config_path)
        .await
        .unwrap_or_else(|_| std::fs::read_to_string("config.toml").expect("config.toml not found"));
    let config: Config = toml::from_str(&config_str)?;

    let store_path = PathBuf::from(
        std::env::var("STORE_PATH").unwrap_or_else(|_| "store".to_owned()),
    );
    let (client, user_id) = mxbot_common::session::build_and_restore(
        &config.matrix,
        &store_path,
        config.security.encryption_strategy.into(),
    ).await?;

    let admin_users: HashSet<OwnedUserId> = config
        .security
        .admin_users
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();

    let allowed_inviters: HashSet<OwnedUserId> = config
        .security
        .allowed_inviters
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();

    if admin_users.is_empty() {
        warn!("No admin_users configured — !reset-trust command is disabled");
    } else {
        info!("Admin users: {:?}", admin_users);
    }

    if allowed_inviters.is_empty() {
        warn!("No allowed_inviters configured — bot will accept invites from anyone");
    } else {
        info!("Allowed inviters: {:?}", allowed_inviters);
    }

    let state = BotState {
        lt_url: config.libretranslate.url.trim_end_matches('/').to_owned(),
        lt_api_key: config.libretranslate.api_key,
        langs: config.translation.langs,
        min_confidence: config.translation.min_confidence,
        thread_replies: config.translation.thread_replies,
        http: reqwest::Client::new(),
        bot_user_id: user_id,
        admin_users,
        allowed_inviters,
        reset_allowed: Arc::new(Mutex::new(HashSet::new())),
        translation_map: Arc::new(RwLock::new(HashMap::new())),
    };

    // Auto-join invited rooms (only from allowed_inviters)
    client.add_event_handler({
        let state = state.clone();
        move |ev: StrippedRoomMemberEvent, room: Room, client: Client| {
            let state = state.clone();
            async move {
                if ev.state_key != state.bot_user_id {
                    return;
                }
                if !state.allowed_inviters.is_empty() && !state.allowed_inviters.contains(&ev.sender) {
                    warn!("Rejecting invite from {} (not in allowed_inviters)", ev.sender);
                    room.leave().await.ok();
                    return;
                }
                info!("Accepted invite from {} to {}", ev.sender, room.room_id());
                let room_id = room.room_id().to_owned();
                let mut via: Vec<OwnedServerName> = vec![ev.sender.server_name().to_owned()];
                if let Some(s) = room_id.server_name() {
                    let s = s.to_owned();
                    if !via.contains(&s) {
                        via.push(s);
                    }
                }
                let room_or_alias = match RoomOrAliasId::parse(room_id.as_str()) {
                    Ok(id) => id,
                    Err(e) => {
                        error!("Invalid room ID {room_id}: {e}");
                        return;
                    }
                };
                tokio::spawn(async move {
                    let mut delay = 2u64;
                    const MAX_ATTEMPTS: u32 = 8;
                    for attempt in 1..=MAX_ATTEMPTS {
                        match client.join_room_by_id_or_alias(&room_or_alias, &via).await {
                            Ok(_) => {
                                info!("Joined {room_id}");
                                return;
                            }
                            Err(ref e) if mxbot_common::verify::is_join_terminal(e) => {
                                warn!("Join failed (terminal) for {room_id}: {e}");
                                return;
                            }
                            Err(e) if attempt == MAX_ATTEMPTS => {
                                warn!("Join failed after {MAX_ATTEMPTS} attempts for {room_id}: {e}");
                            }
                            Err(e) => {
                                warn!("Join attempt {attempt}/{MAX_ATTEMPTS} failed for {room_id}: {e}; retry in {delay}s");
                                sleep(Duration::from_secs(delay)).await;
                                delay = (delay * 2).min(300);
                            }
                        }
                    }
                });
            }
        }
    });

    // To-device verification requests
    client.add_event_handler({
        let state = state.clone();
        move |ev: ToDeviceKeyVerificationRequestEvent, client: Client| {
            let state = state.clone();
            async move {
                let Some(request) = client
                    .encryption()
                    .get_verification_request(&ev.sender, &ev.content.transaction_id)
                    .await
                else {
                    warn!("to-device verification request object not found");
                    return;
                };
                tokio::spawn(mxbot_common::verify::handle_verification_request(
                    client, Arc::clone(&state.reset_allowed), request,
                ));
            }
        }
    });

    // In-room messages: verification requests, admin commands, and translation
    client.add_event_handler({
        let state = state.clone();
        move |ev: OriginalSyncRoomMessageEvent, room: Room, client: Client| {
            let state = state.clone();
            async move {
                info!("Received room message from {} in {}", ev.sender, room.room_id());

                if let MessageType::VerificationRequest(_) = &ev.content.msgtype {
                    let Some(request) = client
                        .encryption()
                        .get_verification_request(&ev.sender, &ev.event_id)
                        .await
                    else {
                        warn!("in-room verification request object not found");
                        return;
                    };
                    tokio::spawn(mxbot_common::verify::handle_verification_request(
                    client, Arc::clone(&state.reset_allowed), request,
                ));
                    return;
                }

                if ev.sender == state.bot_user_id || room.state() != RoomState::Joined {
                    return;
                }

                tokio::spawn(handle_message(state, room, ev));
            }
        }
    });

    // Redactions: if a user deletes their original message, delete the bot's
    // translation too — including thread replies that would otherwise be orphaned.
    client.add_event_handler({
        let state = state.clone();
        move |ev: OriginalSyncRoomRedactionEvent, room: Room| {
            let state = state.clone();
            async move {
                // `redacts` can be None in some room versions / federation edge cases.
                let redacted_id = match ev.redacts {
                    Some(ref id) => id.clone(),
                    None => {
                        warn!("Redaction event has no `redacts` field — ignoring");
                        return;
                    }
                };

                // Check whether we have a translation for this event.
                let bot_event_id = match state.translation_map.read() {
                    Ok(map) => map.get(&redacted_id).cloned(),
                    Err(e)  => {
                        warn!("translation_map lock poisoned on redaction read: {e}");
                        return;
                    }
                };

                let Some(bot_event_id) = bot_event_id else { return };

                info!(
                    "Original message {redacted_id} was redacted — \
                     redacting bot translation {bot_event_id}"
                );

                if let Err(e) = room.redact(&bot_event_id, None, None).await {
                    warn!("Failed to redact translation {bot_event_id}: {e}");
                    return;
                }

                // Remove from map so we don't attempt a double-redact.
                match state.translation_map.write() {
                    Ok(mut map) => { map.remove(&redacted_id); }
                    Err(e)      => warn!("translation_map lock poisoned on redaction write: {e}"),
                }
            }
        }
    });

    info!("Starting sync...");
    let filter = FilterDefinition::with_lazy_loading();
    client
        .sync_once(SyncSettings::default().filter(filter.clone().into()))
        .await?;

    // Drain pending invites from prior sessions (StrippedRoomMemberEvent only fires for new
    // invites, not ones already persisted in the SQLite store).
    let invited = client.invited_rooms();
    if !invited.is_empty() {
        info!("Pending invite(s) found after initial sync — joining {} room(s)", invited.len());
        for room in invited {
            let room_id = room.room_id().to_owned();
            let via: Vec<OwnedServerName> = room_id
                .server_name()
                .map(|s| vec![s.to_owned()])
                .unwrap_or_default();
            match RoomOrAliasId::parse(room_id.as_str()) {
                Ok(room_or_alias) => {
                    match client.join_room_by_id_or_alias(&room_or_alias, &via).await {
                        Ok(_) => info!("Joined pending invite room {room_id}"),
                        Err(e) => warn!("Failed to join pending invite room {room_id}: {e}"),
                    }
                }
                Err(e) => warn!("Invalid room ID in pending invite {room_id}: {e}"),
            }
        }
    }

    loop {
        match client.sync(SyncSettings::default().filter(filter.clone().into())).await {
            Ok(()) => warn!("Sync loop exited cleanly — reconnecting"),
            Err(e) => warn!("Sync loop error: {e} — reconnecting in 5s"),
        }
        sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_html_bold_italic() {
        let html = render_html("This is **bold** and *italic*");
        assert!(html.contains("<strong>bold</strong>"));
        assert!(html.contains("<em>italic</em>"));
    }

    #[test]
    fn render_html_code_block_preserved() {
        let html = render_html("```rust\nlet x = 5;\n```");
        assert!(html.contains("let x = 5;"));
        assert!(html.contains("<code"));
    }

    #[test]
    fn render_html_inline_code_preserved() {
        let html = render_html("Run `cargo build` to compile");
        assert!(html.contains("<code>cargo build</code>"));
    }

    #[test]
    fn html_to_plain_strips_tags() {
        let plain = html_to_plain("<p><strong>Hello</strong> world</p>");
        assert_eq!(plain, "Hello world");
    }

    #[test]
    fn html_to_plain_newlines_on_paragraphs() {
        let plain = html_to_plain("<p>First</p><p>Second</p>");
        assert!(plain.contains("First"));
        assert!(plain.contains("Second"));
        // paragraphs separated by newlines
        assert!(plain.contains('\n'));
    }

    #[test]
    fn html_to_plain_list_items() {
        let plain = html_to_plain("<ul><li>Alpha</li><li>Beta</li></ul>");
        assert!(plain.contains("Alpha"));
        assert!(plain.contains("Beta"));
    }

    #[test]
    fn html_to_plain_empty() {
        assert!(html_to_plain("").is_empty());
        assert!(html_to_plain("<p></p>").is_empty());
    }

    #[test]
    fn render_and_strip_roundtrip() {
        let plain = html_to_plain(&render_html("Hello **world**"));
        assert_eq!(plain, "Hello world");
    }

    #[test]
    fn html_to_plain_decodes_entities() {
        let plain = html_to_plain(&render_html("a & b < c > d"));
        assert_eq!(plain, "a & b < c > d");
    }
}


/// Parse `markdown` and return the indices + full text of every `Event::Text`
/// node that sits outside a code block.  These are the nodes that should be
/// translated; all other events (code blocks, inline code, HTML, URLs) are
/// left untouched.
fn render_html(markdown: &str) -> String {
    let opts = Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TABLES
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES;
    let mut html = String::new();
    push_html(&mut html, Parser::new_ext(markdown, opts));
    html
}

/// Strip HTML tags to produce a plain-text fallback for Matrix `body`.
/// Block-level closing tags are replaced with newlines for readability.
fn html_to_plain(html: &str) -> String {
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

/// Translate a Markdown message by rendering it to HTML first, then sending
/// the HTML to LibreTranslate in HTML mode (one call, full context preserved).
/// LibreTranslate's HTML mode skips <code> and <pre> content automatically.
///
/// Returns `(plain_body, formatted_html)`.
async fn translate_message(
    state: &BotState,
    markdown: &str,
    source: &str,
    target: &str,
) -> (String, String) {
    let html_input = render_html(markdown);
    match state.translate(&html_input, source, target, "html").await {
        Some(translated_html) => {
            let plain = html_to_plain(&translated_html);
            (plain, translated_html)
        }
        None => {
            warn!("Translation failed for target={target} — keeping original");
            (markdown.to_owned(), html_input)
        }
    }
}

async fn handle_image_caption(
    state: BotState,
    room: Room,
    event: OriginalSyncRoomMessageEvent,
    caption: String,
) {
    let Some((lang, confidence)) = state.detect(&caption).await else {
        warn!("Language detection failed for image caption ({})", event.sender);
        return;
    };

    info!("image caption lang={lang} conf={confidence:.2} sender={} room={}", event.sender, room.room_id());

    if confidence < state.min_confidence || !state.langs.contains(&lang) {
        return;
    }

    let targets: Vec<&String> = state.langs.iter().filter(|t| t.as_str() != lang).collect();
    if targets.is_empty() { return; }

    let mut plain_lines = Vec::new();
    let mut html_lines = Vec::new();
    for target in &targets {
        let flag = flag_for_lang(target);
        let (plain, html) = translate_message(&state, &caption, &lang, target).await;
        plain_lines.push(format!("{flag} {plain}"));
        html_lines.push(format!("{flag} {html}"));
    }

    let plain_body = plain_lines.join("\n");
    let mut content = RoomMessageEventContent::text_html(plain_body, html_lines.join("<br>\n"));

    if state.thread_replies {
        let thread_root = resolve_thread_root(&event);
        content.relates_to = Some(Relation::Thread(Thread::reply(
            thread_root,
            event.event_id.clone(),
        )));
    }

    if let Err(e) = room.send(content).await {
        error!("Failed to send image caption translation: {e}");
    }
}

async fn handle_message(state: BotState, room: Room, event: OriginalSyncRoomMessageEvent) {
    // Edits arrive as m.replace — route them to handle_edit and stop.
    // Must be checked BEFORE the msgtype guard: the top-level body of an edit
    // is only a fallback ("* new text") for old clients and must NOT be translated.
    if let Some(Relation::Replacement(ref replacement)) = event.content.relates_to {
        let original_event_id = replacement.event_id.clone();
        let new_content = replacement.new_content.clone();
        handle_edit(state, room, original_event_id, new_content).await;
        return;
    }

    // Handle image messages — translate caption if present.
    if let MessageType::Image(img) = &event.content.msgtype {
        let caption = img.caption()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        // img borrow ends here; event can be moved below
        if let Some(caption) = caption {
            handle_image_caption(state, room, event, caption).await;
        }
        return;
    }

    let MessageType::Text(text_content) = &event.content.msgtype else { return };

    let raw = text_content.body.trim();

    // Admin command: !reset-trust @user:server
    if let Some(target) = raw.strip_prefix("!reset-trust ") {
        if state.admin_users.contains(&event.sender) {
            match target.trim().parse::<OwnedUserId>() {
                Ok(target_user) => {
                    state.reset_allowed.lock().await.insert(target_user.clone());
                    info!("Trust reset allowed for {} (by {})", target_user, event.sender);
                }
                Err(_) => warn!("!reset-trust: invalid user ID '{}'", target.trim()),
            }
        } else {
            warn!("!reset-trust from non-admin {} — ignored", event.sender);
        }
        return;
    }

    // Strip reply fallback lines from plain text
    let text: String = raw
        .lines()
        .filter(|l| !l.starts_with("> "))
        .collect::<Vec<_>>()
        .join("\n");
    let text = text.trim();
    if text.is_empty() {
        return;
    }

    let Some((lang, confidence)) = state.detect(text).await else {
        warn!("Language detection failed ({})", event.sender);
        return;
    };

    info!("lang={lang} conf={confidence:.2} sender={} room={}", event.sender, room.room_id());

    if confidence < state.min_confidence || !state.langs.contains(&lang) {
        return;
    }

    let targets: Vec<&String> = state.langs.iter().filter(|t| t.as_str() != lang).collect();
    if targets.is_empty() {
        return;
    }

    let mut plain_lines = Vec::new();
    let mut html_lines = Vec::new();

    for target in &targets {
        let flag = flag_for_lang(target);
        let (plain, html) = translate_message(&state, text, &lang, target).await;
        plain_lines.push(format!("{flag} {plain}"));
        html_lines.push(format!("{flag} {html}"));
    }

    let plain_body = plain_lines.join("\n");
    let mut content = RoomMessageEventContent::text_html(plain_body, html_lines.join("<br>\n"));

    if state.thread_replies {
        let thread_root = resolve_thread_root(&event);
        info!("thread_root={} for event={}", thread_root, event.event_id);
        // reply_to = event.event_id so the translation quotes the specific
        // message it translated, not always the thread root.
        content.relates_to = Some(Relation::Thread(Thread::reply(
            thread_root,
            event.event_id.clone(),
        )));
    }

    match room.send(content).await {
        Ok(resp) => {
            state.translation_map.write().unwrap()
                .insert(event.event_id.clone(), resp.response.event_id);
        }
        Err(e) => error!("Failed to send translation: {e}"),
    }
}

/// Determine the Matrix thread root for a given incoming event using the
/// priority defined in the Matrix spec:
///
/// 1. Event is already in a thread (`m.thread`) → use that thread's root.
/// 2. Event is a plain reply (`m.in_reply_to`) → treat the replied-to event
///    as the root (avoids a round-trip to fetch and inspect the parent).
/// 3. Otherwise → this event starts a new thread; use its own event_id.
fn resolve_thread_root(event: &OriginalSyncRoomMessageEvent) -> OwnedEventId {
    match &event.content.relates_to {
        Some(Relation::Thread(thread)) => thread.event_id.clone(),
        Some(Relation::Reply(reply)) => reply.in_reply_to.event_id.clone(),
        _ => event.event_id.clone(),
    }
}

/// Called when a user edits a message the bot previously translated.
/// Re-translates the new content and edits the bot's existing translation
/// in-place using m.replace — no new message is sent, thread context is preserved.
async fn handle_edit(
    state: BotState,
    room: Room,
    original_event_id: OwnedEventId,
    new_content: RoomMessageEventContentWithoutRelation,
) {
    // Look up whether we have a translation for this event.
    let bot_event_id = state.translation_map.read().unwrap()
        .get(&original_event_id)
        .cloned();

    let Some(bot_event_id) = bot_event_id else {
        info!("Edit for unknown event {original_event_id} — no cached translation, ignoring");
        return;
    };

    // Use ONLY m.new_content as the source of truth (full replacement, not a diff).
    let MessageType::Text(text_content) = &new_content.msgtype else { return };
    let raw = text_content.body.trim();

    // Strip reply fallback lines (same as normal message handling).
    let text: String = raw.lines()
        .filter(|l| !l.starts_with("> "))
        .collect::<Vec<_>>()
        .join("\n");
    let text = text.trim();
    if text.is_empty() { return; }

    let Some((lang, confidence)) = state.detect(text).await else {
        warn!("Language detection failed for edit of {original_event_id}");
        return;
    };

    if confidence < state.min_confidence || !state.langs.contains(&lang) { return; }

    let targets: Vec<&String> = state.langs.iter().filter(|t| t.as_str() != lang).collect();
    if targets.is_empty() { return; }

    let mut plain_lines = Vec::new();
    let mut html_lines = Vec::new();
    for target in &targets {
        let flag = flag_for_lang(target);
        let (plain, html) = translate_message(&state, text, &lang, target).await;
        plain_lines.push(format!("{flag} {plain}"));
        html_lines.push(format!("{flag} {html}"));
    }

    let new_body = plain_lines.join("\n");
    let new_html = html_lines.join("<br>\n");

    // Build m.replace pointing at the bot's existing translation event.
    // The thread membership is inherited from bot_event_id — no thread relation needed here.
    let new_without = RoomMessageEventContentWithoutRelation::new(
        MessageType::Text(TextMessageEventContent::html(new_body.clone(), new_html.clone())),
    );
    let mut edit_content = RoomMessageEventContent::text_html(format!("* {new_body}"), new_html);
    edit_content.relates_to = Some(Relation::Replacement(Replacement::new(
        bot_event_id.clone(),
        new_without,
    )));

    info!("Editing bot translation {bot_event_id} for edit of {original_event_id}");
    if let Err(e) = room.send(edit_content).await {
        error!("Failed to send translation edit: {e}");
    }
}

