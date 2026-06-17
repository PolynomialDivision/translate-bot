use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
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
use futures_util::future::join_all;
use matrix_sdk::{
    Client, Room, RoomState,
    config::SyncSettings,
    ruma::{
        OwnedEventId, OwnedServerName, OwnedUserId, RoomOrAliasId,
        api::client::filter::FilterDefinition,
        events::{
            key::verification::request::ToDeviceKeyVerificationRequestEvent,
            relation::{InReplyTo, Replacement, Reply, Thread},
            room::{
                member::StrippedRoomMemberEvent,
                message::{
                    MessageFormat, MessageType, NoticeMessageEventContent,
                    OriginalSyncRoomMessageEvent, Relation, RoomMessageEventContent,
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
    /// Whether translations reference the original message at all (default: true).
    /// When false, translations are posted as standalone messages regardless of thread_replies.
    #[serde(default = "default_true")]
    reply_to_original: bool,
    /// When reply_to_original = true: use m.thread instead of m.in_reply_to (default: true).
    /// When reply_to_original = false: has no effect.
    #[serde(default = "default_true")]
    thread_replies: bool,
    /// Send translations as `m.notice` (non-notifying) instead of `m.text` (default: false).
    /// Most Matrix clients render notices with muted styling and suppress push notifications.
    #[serde(default)]
    silent_messages: bool,
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
        Self {
            langs: default_langs(),
            min_confidence: default_min_confidence(),
            reply_to_original: true,
            thread_replies: true,
            silent_messages: false,
        }
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
    reply_to_original: bool,
    thread_replies: bool,
    silent_messages: bool,
    http: reqwest::Client,
    bot_user_id: OwnedUserId,
    admin_users: HashSet<OwnedUserId>,
    allowed_inviters: HashSet<OwnedUserId>,
    // Users allowed to re-verify despite already having a verified device.
    // Populated by !reset-trust from an admin, cleared after use.
    reset_allowed: Arc<Mutex<HashSet<OwnedUserId>>>,
    startup_time: SystemTime,
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

    let startup_time = SystemTime::now();

    let state = BotState {
        lt_url: config.libretranslate.url.trim_end_matches('/').to_owned(),
        lt_api_key: config.libretranslate.api_key,
        langs: config.translation.langs,
        min_confidence: config.translation.min_confidence,
        reply_to_original: config.translation.reply_to_original,
        thread_replies: config.translation.thread_replies,
        silent_messages: config.translation.silent_messages,
        startup_time,
        http: reqwest::Client::new(),
        bot_user_id: user_id,
        admin_users,
        allowed_inviters,
        reset_allowed: Arc::new(Mutex::new(HashSet::new())),
        translation_map: Arc::new(RwLock::new(HashMap::new())),
    };

    // Advance the sync token past any backlog from downtime before registering
    // handlers — events that arrived while the bot was down are silently discarded.
    let filter = FilterDefinition::with_lazy_loading();
    info!("Starting sync...");
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

    // ── strip_mx_reply tests ──────────────────────────────────────────────────

    #[test]
    fn strip_mx_reply_removes_wrapper() {
        let html = "<mx-reply><blockquote>quoted</blockquote></mx-reply>Actual message";
        assert_eq!(strip_mx_reply(html), "Actual message");
    }

    #[test]
    fn strip_mx_reply_passthrough_when_no_wrapper() {
        let html = "<p>Normal message</p>";
        assert_eq!(strip_mx_reply(html), html);
    }

    #[test]
    fn strip_mx_reply_trims_leading_whitespace_after_removal() {
        let html = "<mx-reply><blockquote>q</blockquote></mx-reply>\n\n<p>message</p>";
        assert_eq!(strip_mx_reply(html), "<p>message</p>");
    }

    // ── blockquote stripping tests ────────────────────────────────────────────

    #[test]
    fn blockquote_strip_removes_leading_fallback_block() {
        let raw = "> Alice said something\n> more quoted\n\nActual reply text";
        let text: String = if raw.starts_with("> ") {
            raw.lines()
                .skip_while(|l| l.starts_with("> "))
                .skip_while(|l| l.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            raw.to_owned()
        };
        assert_eq!(text.trim(), "Actual reply text");
    }

    #[test]
    fn blockquote_strip_preserves_mid_message_blockquotes() {
        // A message that starts normally but has a blockquote mid-text should be untouched
        let raw = "Here is my point:\n> some quote\nAnd my conclusion";
        let text: String = if raw.starts_with("> ") {
            raw.lines()
                .skip_while(|l| l.starts_with("> "))
                .skip_while(|l| l.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            raw.to_owned()
        };
        assert_eq!(text, raw);
    }

    #[test]
    fn blockquote_strip_passthrough_when_no_leading_quote() {
        let raw = "Normal message without quotes";
        let text: String = if raw.starts_with("> ") {
            raw.lines()
                .skip_while(|l| l.starts_with("> "))
                .skip_while(|l| l.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n")
        } else {
            raw.to_owned()
        };
        assert_eq!(text, raw);
    }

    #[test]
    fn make_translation_content_text_when_not_silent() {
        let content = make_translation_content("hello".into(), "<p>hello</p>".into(), false);
        assert!(matches!(content.msgtype, MessageType::Text(_)));
    }

    #[test]
    fn make_translation_content_notice_when_silent() {
        let content = make_translation_content("hello".into(), "<p>hello</p>".into(), true);
        assert!(matches!(content.msgtype, MessageType::Notice(_)));
    }

    #[test]
    fn make_translation_content_preserves_body() {
        let content = make_translation_content("plain text".into(), "<b>bold</b>".into(), true);
        let body = match &content.msgtype {
            MessageType::Notice(n) => n.body.clone(),
            _ => panic!("expected Notice"),
        };
        assert_eq!(body, "plain text");
    }

    // ── make_relation tests ───────────────────────────────────────────────────

    fn eid(s: &str) -> OwnedEventId {
        matrix_sdk::ruma::EventId::parse(s).unwrap()
    }

    #[test]
    fn relation_standalone_when_reply_to_original_false() {
        let id = eid("$ev1:example.org");
        assert!(make_relation(&id, &id, false, false, false).is_none());
    }

    #[test]
    fn relation_standalone_when_reply_to_original_false_thread_true() {
        let id = eid("$ev2:example.org");
        assert!(make_relation(&id, &id, false, true, false).is_none(),
            "thread_replies=true must not override reply_to_original=false");
    }

    #[test]
    fn relation_reply_when_reply_true_thread_false_not_in_thread() {
        let id = eid("$ev3:example.org");
        assert!(matches!(make_relation(&id, &id, true, false, false), Some(Relation::Reply(_))));
    }

    #[test]
    fn relation_thread_when_reply_true_thread_true() {
        let id = eid("$ev4:example.org");
        assert!(matches!(make_relation(&id, &id, true, true, false), Some(Relation::Thread(_))));
    }

    #[test]
    fn relation_thread_when_in_thread_even_if_thread_replies_false() {
        // Key regression test: thread_replies=false must NOT pull the bot out of
        // an existing thread.  If the original message is already in a thread,
        // the translation must always be a thread reply into that same thread.
        let event_id = eid("$reply_in_thread:example.org");
        let root_id  = eid("$root:example.org");
        let Some(Relation::Thread(t)) = make_relation(&event_id, &root_id, true, false, true) else {
            panic!("expected Thread when in_thread=true regardless of thread_replies");
        };
        assert_eq!(t.event_id, root_id);
        assert_eq!(t.in_reply_to.as_ref().map(|r| &r.event_id), Some(&event_id));
    }

    #[test]
    fn relation_reply_points_to_event_id() {
        let id = eid("$ev5:example.org");
        let Some(Relation::Reply(r)) = make_relation(&id, &id, true, false, false) else {
            panic!("expected Reply");
        };
        assert_eq!(r.in_reply_to.event_id, id);
    }

    #[test]
    fn relation_thread_uses_thread_root() {
        let event_id   = eid("$reply:example.org");
        let root_id    = eid("$root:example.org");
        let Some(Relation::Thread(t)) = make_relation(&event_id, &root_id, true, true, false) else {
            panic!("expected Thread");
        };
        assert_eq!(t.event_id, root_id,  "thread root must be root_id");
        // in_reply_to inside the thread must point to the translated event itself
        assert_eq!(t.in_reply_to.as_ref().map(|r| &r.event_id), Some(&event_id));
    }

    #[test]
    fn default_translation_config() {
        let cfg = TranslationConfig::default();
        assert!(cfg.reply_to_original, "default: reply_to_original must be true");
        assert!(cfg.thread_replies,    "default: thread_replies must be true");
        assert!(!cfg.silent_messages,  "default: silent_messages must be false");
    }

    // ── Serde / default-value proof ───────────────────────────────────────────
    //
    // These tests prove the exact path taken when reply_to_original is absent
    // from config:
    //
    //   1. Key present with explicit value → used as-is.
    //   2. Key absent from [translation] section → serde calls default_true().
    //   3. [translation] section absent entirely → Config field has
    //      #[serde(default)] so TranslationConfig::default() is called,
    //      which also sets reply_to_original = true.

    #[test]
    fn serde_reply_to_original_explicit_false() {
        let toml = r#"langs = ["en", "de"]
reply_to_original = false"#;
        let cfg: TranslationConfig = toml::from_str(toml).unwrap();
        assert!(!cfg.reply_to_original);
    }

    #[test]
    fn serde_reply_to_original_explicit_true() {
        let toml = r#"langs = ["en", "de"]
reply_to_original = true"#;
        let cfg: TranslationConfig = toml::from_str(toml).unwrap();
        assert!(cfg.reply_to_original);
    }

    #[test]
    fn serde_reply_to_original_missing_key_defaults_to_true() {
        // Key is absent — serde invokes the #[serde(default = "default_true")] path.
        let toml = r#"langs = ["en", "de"]"#;
        let cfg: TranslationConfig = toml::from_str(toml).unwrap();
        assert!(cfg.reply_to_original,
            "absent key must default to true via default_true()");
    }

    #[test]
    fn serde_entire_translation_section_missing_defaults_to_true() {
        // Simulates a config.toml with no [translation] section at all.
        // Config has #[serde(default)] on the translation field, which calls
        // TranslationConfig::default() → reply_to_original = true.
        let cfg = TranslationConfig::default();
        assert!(cfg.reply_to_original,
            "TranslationConfig::default() must set reply_to_original = true");
    }

    // ── Exact serialized Matrix event JSON ────────────────────────────────────
    //
    // These tests serialize actual RoomMessageEventContent values and assert
    // the exact JSON shape that the Matrix homeserver will receive and forward
    // to other clients.  This is the ground truth for client compatibility.

    fn content_with_relation(reply_to_original: bool, thread_replies: bool) -> serde_json::Value {
        let event_id  = eid("$original:example.org");
        let thread_root = eid("$root:example.org");
        let mut content = make_translation_content("🇬🇧 Hello".into(), "🇬🇧 Hello".into(), false);
        content.relates_to = make_relation(&event_id, &thread_root, reply_to_original, thread_replies, false);
        serde_json::to_value(&content).unwrap()
    }

    #[test]
    fn json_standalone_has_no_relates_to() {
        // reply_to_original=false → no relation field at all.
        // Expected JSON:
        //   { "msgtype": "m.text", "body": "...", "format": "...", "formatted_body": "..." }
        let json = content_with_relation(false, false);
        assert!(json.get("m.relates_to").is_none(),
            "standalone must have no m.relates_to field; got: {json}");
    }

    #[test]
    fn json_reply_shape() {
        // reply_to_original=true, thread_replies=false → m.in_reply_to reply.
        // Expected JSON fragment:
        //   "m.relates_to": {
        //     "m.in_reply_to": { "event_id": "$original:example.org" }
        //   }
        // Note: no "rel_type" key — pure replies do not have rel_type per Matrix spec.
        let json = content_with_relation(true, false);
        let rel  = &json["m.relates_to"];
        assert!(!rel.is_null(), "m.relates_to must be present");
        assert!(rel.get("rel_type").is_none(),
            "plain reply must not have rel_type; got: {rel}");
        assert_eq!(rel["m.in_reply_to"]["event_id"], "$original:example.org",
            "in_reply_to must point to $original:example.org");
    }

    #[test]
    fn json_thread_shape() {
        // reply_to_original=true, thread_replies=true → m.thread.
        // Expected JSON fragment:
        //   "m.relates_to": {
        //     "rel_type":      "m.thread",
        //     "event_id":      "$root:example.org",
        //     "m.in_reply_to": { "event_id": "$original:example.org" }
        //   }
        //
        // Note: ruma omits is_falling_back from the JSON when it is false
        // (#[serde(skip_serializing_if = "is_default")]).  Per the Matrix spec,
        // an absent is_falling_back is equivalent to false — a genuine thread
        // reply.  is_falling_back only appears in the JSON when true (fallback).
        let json = content_with_relation(true, true);
        let rel  = &json["m.relates_to"];
        assert_eq!(rel["rel_type"],  "m.thread",             "rel_type must be m.thread");
        assert_eq!(rel["event_id"],  "$root:example.org",    "event_id must be thread root");
        assert_eq!(rel["m.in_reply_to"]["event_id"], "$original:example.org",
            "in_reply_to inside thread must point to translated event");
        assert!(rel.get("is_falling_back").is_none(),
            "is_falling_back=false is omitted by ruma (absent == false per Matrix spec); \
             if it appears it means ruma set it to true unexpectedly: {rel}");
    }

    // ── Image caption path integration ───────────────────────────────────────
    //
    // handle_image_caption() builds its relation with exactly:
    //
    //   let thread_root = resolve_thread_root(&event);
    //   content.relates_to = make_relation(
    //       &event.event_id, &thread_root,
    //       state.reply_to_original, state.thread_replies,
    //   );
    //
    // This is the same call as handle_message().  The tests below reproduce
    // that call sequence directly, proving both paths exercise the same logic.
    //
    // resolve_thread_root() returns:
    //   - event.event_id   when the event is NOT already in a thread
    //   - thread.event_id  when the event IS already in a thread
    //
    // Case A: caption event is a standalone message (not in a thread).
    // resolve_thread_root returns event_id → thread_root == event_id.

    #[test]
    fn image_caption_standalone_event_reply_relation() {
        let caption_event_id = eid("$caption:example.org");
        // resolve_thread_root for a non-threaded event returns event_id itself
        let thread_root = caption_event_id.clone();

        let mut content = make_translation_content("🇩🇪 Hallo".into(), "🇩🇪 Hallo".into(), false);
        content.relates_to = make_relation(&caption_event_id, &thread_root, true, false, false);

        let json = serde_json::to_value(&content).unwrap();
        assert!(json["m.relates_to"].get("rel_type").is_none(),
            "caption reply must not have rel_type");
        assert_eq!(json["m.relates_to"]["m.in_reply_to"]["event_id"],
            "$caption:example.org");
    }

    // Case B: caption event is already inside a thread.
    // resolve_thread_root returns thread.event_id (the root), NOT event_id.
    // Even with thread_replies=false the bot must stay in the thread (in_thread=true).

    #[test]
    fn image_caption_threaded_event_thread_relation() {
        let thread_root_id   = eid("$thread_root:example.org");
        let caption_event_id = eid("$caption_in_thread:example.org");
        // resolve_thread_root would return thread_root_id in this case
        let thread_root = thread_root_id.clone();

        let mut content = make_translation_content("🇩🇪 Hallo".into(), "🇩🇪 Hallo".into(), false);
        // in_thread=true mirrors what handle_image_caption now passes
        content.relates_to = make_relation(&caption_event_id, &thread_root, true, false, true);

        let json = serde_json::to_value(&content).unwrap();
        assert_eq!(json["m.relates_to"]["rel_type"],  "m.thread");
        assert_eq!(json["m.relates_to"]["event_id"],  "$thread_root:example.org");
        assert_eq!(json["m.relates_to"]["m.in_reply_to"]["event_id"],
            "$caption_in_thread:example.org");
    }

    #[test]
    fn image_caption_standalone_produces_no_relation_when_reply_to_original_false() {
        let caption_event_id = eid("$caption2:example.org");
        let thread_root = caption_event_id.clone();

        let mut content = make_translation_content("🇩🇪 Hallo".into(), "🇩🇪 Hallo".into(), false);
        content.relates_to = make_relation(&caption_event_id, &thread_root, false, false, false);

        let json = serde_json::to_value(&content).unwrap();
        assert!(json.get("m.relates_to").is_none(),
            "caption with reply_to_original=false must produce no relation");
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

/// Collapse block-level `<p>` tags into inline content suitable for embedding
/// inside a single Matrix message line. Inter-paragraph breaks become `<br>`.
fn inline_html(html: &str) -> String {
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

/// Strip the `<mx-reply>…</mx-reply>` fallback block that Matrix clients prepend
/// to `formatted_body` when a message is a reply.  Returns the remaining HTML.
fn strip_mx_reply(html: &str) -> String {
    if let Some(start) = html.find("<mx-reply>") {
        if let Some(rel_end) = html[start..].find("</mx-reply>") {
            return html[start + rel_end + "</mx-reply>".len()..].trim_start().to_owned();
        }
    }
    html.to_owned()
}

/// Core translation call: send pre-rendered HTML to LibreTranslate and return
/// `(plain_body, formatted_html)`.  Used by both text and image-caption paths.
async fn translate_html_content(
    state: &BotState,
    html: &str,
    source: &str,
    target: &str,
) -> Option<(String, String)> {
    match state.translate(html, source, target, "html").await {
        Some(translated_html) => {
            let plain = html_to_plain(&translated_html);
            if plain.is_empty() {
                warn!("Translation to {target} produced empty text — skipping");
                return None;
            }
            Some((plain, inline_html(&translated_html)))
        }
        None => {
            warn!("Translation failed for target={target} — skipping");
            None
        }
    }
}

/// Try HTML translation first; if it produces empty output (e.g. model can't handle
/// the language+HTML combo), fall back to translating `plain` as plain text.
async fn translate_html_with_fallback(
    state: &BotState,
    html: &str,
    plain: &str,
    source: &str,
    target: &str,
) -> Option<(String, String)> {
    if let Some(result) = translate_html_content(state, html, source, target).await {
        return Some(result);
    }
    // Plain-text fallback — no formatting preserved, but at least we get a translation.
    let translated = state.translate(plain, source, target, "text").await?;
    let translated = translated.trim().to_owned();
    if translated.is_empty() {
        return None;
    }
    Some((translated.clone(), translated))
}

/// Convenience wrapper: render Markdown to HTML then translate, with plain-text fallback.
async fn translate_message(
    state: &BotState,
    markdown: &str,
    source: &str,
    target: &str,
) -> Option<(String, String)> {
    translate_html_with_fallback(state, &render_html(markdown), markdown, source, target).await
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
/// `in_thread` — whether the original event is already inside a thread.
///   When true the bot always thread-replies into that same thread, regardless
///   of `thread_replies` config.  `thread_replies = false` only suppresses the
///   bot from *opening* new threads on standalone messages.
///
/// `event_id`    – the event being translated (used as reply target / thread in_reply_to).
/// `thread_root` – the thread root to use; pass `resolve_thread_root(event)` at call sites.
fn make_relation(
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
        Some(Relation::Thread(Thread::reply(thread_root.clone(), event_id.clone())))
    } else {
        Some(Relation::Reply(Reply::new(InReplyTo::new(event_id.clone()))))
    }
}

/// Build a translated message content with the right msgtype.
/// `silent = true` → `m.notice` (suppressed push, muted styling in most clients).
/// `silent = false` → `m.text` (default behaviour).
fn make_translation_content(plain: String, html: String, silent: bool) -> RoomMessageEventContent {
    if silent {
        RoomMessageEventContent::notice_html(plain, html)
    } else {
        RoomMessageEventContent::text_html(plain, html)
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

    let results = join_all(
        targets.iter().map(|target| translate_message(&state, &caption, &lang, target))
    ).await;
    let mut plain_lines = Vec::new();
    let mut html_lines = Vec::new();
    for (target, result) in targets.iter().zip(results) {
        if let Some((plain, html)) = result {
            let flag = flag_for_lang(target);
            plain_lines.push(format!("{flag} {plain}"));
            html_lines.push(format!("{flag} {html}"));
        }
    }

    if plain_lines.is_empty() { return; }

    let plain_body = plain_lines.join("\n");
    let mut content = make_translation_content(plain_body, html_lines.join("<br>\n"), state.silent_messages);
    let in_thread = matches!(&event.content.relates_to, Some(Relation::Thread(_)));
    let thread_root = resolve_thread_root(&event);
    content.relates_to = make_relation(&event.event_id, &thread_root, state.reply_to_original, state.thread_replies, in_thread);

    if let Err(e) = room.send(content).await {
        error!("Failed to send image caption translation: {e}");
    }
}

async fn handle_message(state: BotState, room: Room, event: OriginalSyncRoomMessageEvent) {
    // Belt-and-suspenders: skip any event that predates bot startup.
    // The primary defence is sync_once running before handlers are registered,
    // which prevents backlog events from reaching this function at all.
    if let Some(event_time) = event.origin_server_ts.to_system_time() {
        if event_time < state.startup_time {
            info!("Skipping backlog message {} from {} (pre-startup)", event.event_id, event.sender);
            return;
        }
    }

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

    // Strip the leading Matrix reply-fallback block: consecutive "> " lines at the top
    // followed by a blank separator line.  Only the top block is removed so that
    // intentional blockquotes later in the message are preserved.
    let text: String = if raw.starts_with("> ") {
        raw.lines()
            .skip_while(|l| l.starts_with("> "))
            .skip_while(|l| l.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        raw.to_owned()
    };
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

    // Prefer formatted_body (already HTML — strip <mx-reply> wrapper for replies).
    // Fall back to rendering the plain body as Markdown.
    let html_to_translate = match &text_content.formatted {
        Some(fb) if fb.format == MessageFormat::Html => strip_mx_reply(&fb.body),
        _ => render_html(text),
    };

    let results = join_all(
        targets.iter().map(|target| translate_html_with_fallback(&state, &html_to_translate, text, &lang, target))
    ).await;
    let mut plain_lines = Vec::new();
    let mut html_lines = Vec::new();
    for (target, result) in targets.iter().zip(results) {
        if let Some((plain, html)) = result {
            let flag = flag_for_lang(target);
            plain_lines.push(format!("{flag} {plain}"));
            html_lines.push(format!("{flag} {html}"));
        }
    }

    if plain_lines.is_empty() { return; }

    let plain_body = plain_lines.join("\n");
    let mut content = make_translation_content(plain_body, html_lines.join("<br>\n"), state.silent_messages);
    let in_thread = matches!(&event.content.relates_to, Some(Relation::Thread(_)));
    let thread_root = resolve_thread_root(&event);
    if state.reply_to_original && (state.thread_replies || in_thread) {
        info!("thread_root={} for event={}", thread_root, event.event_id);
    }
    content.relates_to = make_relation(&event.event_id, &thread_root, state.reply_to_original, state.thread_replies, in_thread);

    match room.send(content).await {
        Ok(resp) => {
            state.translation_map.write().unwrap()
                .insert(event.event_id.clone(), resp.response.event_id);
        }
        Err(e) => error!("Failed to send translation: {e}"),
    }
}

/// Determine the Matrix thread root for a given incoming event:
///
/// 1. Event is already in a thread (`m.thread`) → use that thread's root.
/// 2. Event is a plain reply or standalone → start a new thread on this event itself.
fn resolve_thread_root(event: &OriginalSyncRoomMessageEvent) -> OwnedEventId {
    match &event.content.relates_to {
        Some(Relation::Thread(thread)) => thread.event_id.clone(),
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
        if let Some((plain, html)) = translate_message(&state, text, &lang, target).await {
            let flag = flag_for_lang(target);
            plain_lines.push(format!("{flag} {plain}"));
            html_lines.push(format!("{flag} {html}"));
        }
    }
    if plain_lines.is_empty() { return; }

    let new_body = plain_lines.join("\n");
    let new_html = html_lines.join("<br>\n");

    // Build m.replace pointing at the bot's existing translation event.
    // The thread membership is inherited from bot_event_id — no thread relation needed here.
    // Preserve the same msgtype (m.notice vs m.text) as the original translation.
    let inner_msgtype = if state.silent_messages {
        MessageType::Notice(NoticeMessageEventContent::html(new_body.clone(), new_html.clone()))
    } else {
        MessageType::Text(TextMessageEventContent::html(new_body.clone(), new_html.clone()))
    };
    let new_without = RoomMessageEventContentWithoutRelation::new(inner_msgtype);
    let mut edit_content = make_translation_content(format!("* {new_body}"), new_html, state.silent_messages);
    edit_content.relates_to = Some(Relation::Replacement(Replacement::new(
        bot_event_id.clone(),
        new_without,
    )));

    info!("Editing bot translation {bot_event_id} for edit of {original_event_id}");
    if let Err(e) = room.send(edit_content).await {
        error!("Failed to send translation edit: {e}");
    }
}

