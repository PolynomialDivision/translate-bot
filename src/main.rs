use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

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
use futures_util::StreamExt;
use matrix_sdk::{
    Client, Room, RoomState, SessionMeta, SessionTokens,
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    encryption::verification::{
        SasState, Verification, VerificationRequest, VerificationRequestState,
    },
    ruma::{
        OwnedDeviceId, OwnedEventId, OwnedServerName, OwnedUserId, RoomOrAliasId,
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
            },
        },
    },
};
use matrix_sdk_crypto::CollectStrategy;
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
struct MatrixConfig {
    homeserver: String,
    user_id: String,
    access_token: String,
    device_id: String,
    // Security key from Element's "Set up Secure Backup" — used once to self-sign the bot's device
    recovery_key: Option<String>,
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

#[derive(Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum EncryptionStrategy {
    /// Share keys with all devices (no restriction).
    AllDevices,
    /// Only share keys with devices cross-signed by their owner (recommended).
    #[default]
    IdentityBased,
    /// Only share keys with devices that are explicitly trusted/verified.
    OnlyTrusted,
}

impl From<EncryptionStrategy> for CollectStrategy {
    fn from(s: EncryptionStrategy) -> Self {
        match s {
            EncryptionStrategy::AllDevices => CollectStrategy::AllDevices,
            EncryptionStrategy::IdentityBased => CollectStrategy::IdentityBasedStrategy,
            EncryptionStrategy::OnlyTrusted => CollectStrategy::OnlyTrustedDevices,
        }
    }
}

#[derive(Deserialize)]
struct SecurityConfig {
    #[serde(default)]
    admin_users: Vec<String>,
    #[serde(default)]
    allowed_inviters: Vec<String>,
    #[serde(default)]
    encryption_strategy: EncryptionStrategy,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self { admin_users: vec![], allowed_inviters: vec![], encryption_strategy: EncryptionStrategy::default() }
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
    fs::create_dir_all(&store_path).await?;

    let strategy: CollectStrategy = config.security.encryption_strategy.into();
    info!("Encryption strategy: {:?}", strategy);

    let client = Client::builder()
        .homeserver_url(&config.matrix.homeserver)
        .sqlite_store(&store_path, None)
        .with_room_key_recipient_strategy(strategy)
        .build()
        .await?;

    let user_id: OwnedUserId = config.matrix.user_id.parse()?;
    let device_id: OwnedDeviceId = OwnedDeviceId::from(config.matrix.device_id);

    client
        .restore_session(MatrixSession {
            meta: SessionMeta { user_id: user_id.clone(), device_id },
            tokens: SessionTokens {
                access_token: config.matrix.access_token,
                refresh_token: None,
            },
        })
        .await?;

    info!("Session restored as {}", user_id);

    if let Some(ref key) = config.matrix.recovery_key {
        info!("Recovering cross-signing keys from secure backup...");
        match client.encryption().recovery().recover(key).await {
            Ok(()) => info!("Cross-signing keys recovered"),
            Err(e) => warn!("Recovery failed: {e}"),
        }
    }
    bootstrap_cross_signing(&client, &user_id).await;

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
                            Err(ref e) if is_join_terminal(e) => {
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
                tokio::spawn(handle_verification_request(client, state, request));
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
                    tokio::spawn(handle_verification_request(client, state, request));
                    return;
                }

                if ev.sender == state.bot_user_id || room.state() != RoomState::Joined {
                    return;
                }

                tokio::spawn(handle_message(state, room, ev));
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

    client.sync(SyncSettings::default().filter(filter.into())).await?;

    Ok(())
}

fn strip_mx_reply(html: &str) -> &str {
    if let Some(end) = html.find("</mx-reply>") {
        html[end + "</mx-reply>".len()..].trim()
    } else {
        html.trim()
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

    // Extract HTML body if present, stripping the <mx-reply> fallback block
    let html_source = text_content
        .formatted
        .as_ref()
        .map(|f| strip_mx_reply(&f.body).to_owned());

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

        let plain = match state.translate(text, &lang, target, "text").await {
            Some(t) => format!("{flag} {t}"),
            None => {
                warn!("Translation to {target} failed");
                format!("{flag} [translation unavailable]")
            }
        };

        if let Some(ref html) = html_source {
            let html_translated = match state.translate(html, &lang, target, "html").await {
                Some(t) => format!("{flag} {t}"),
                None => plain.clone(),
            };
            html_lines.push(html_translated);
        }

        plain_lines.push(plain);
    }

    let plain_body = plain_lines.join("\n");

    let mut content = if !html_lines.is_empty() {
        RoomMessageEventContent::text_html(plain_body, html_lines.join("<br>\n"))
    } else {
        RoomMessageEventContent::text_plain(plain_body)
    };

    if state.thread_replies {
        let thread_root = resolve_thread_root(&event);
        info!("thread_root={} for event={}", thread_root, event.event_id);
        content.relates_to = Some(Relation::Thread(Thread::reply(
            thread_root.clone(),
            thread_root,
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
    for target in &targets {
        let flag = flag_for_lang(target);
        let translated = match state.translate(text, &lang, target, "text").await {
            Some(t) => format!("{flag} {t}"),
            None => {
                warn!("Translation to {target} failed for edit of {original_event_id}");
                format!("{flag} [translation unavailable]")
            }
        };
        plain_lines.push(translated);
    }

    let new_body = plain_lines.join("\n");

    // Build m.replace pointing at the bot's existing translation event.
    // The thread membership is inherited from bot_event_id — no thread relation needed here.
    let new_without = RoomMessageEventContentWithoutRelation::new(
        MessageType::Text(TextMessageEventContent::plain(new_body.clone())),
    );
    let mut edit_content = RoomMessageEventContent::text_plain(format!("* {new_body}"));
    edit_content.relates_to = Some(Relation::Replacement(Replacement::new(
        bot_event_id.clone(),
        new_without,
    )));

    info!("Editing bot translation {bot_event_id} for edit of {original_event_id}");
    if let Err(e) = room.send(edit_content).await {
        error!("Failed to send translation edit: {e}");
    }
}

async fn handle_verification_request(
    client: Client,
    state: BotState,
    request: VerificationRequest,
) {
    let user_id = request.other_user_id();

    // Check if this user already has a verified device
    let already_verified = client
        .encryption()
        .get_user_devices(user_id)
        .await
        .map(|devices| devices.devices().any(|d| d.is_verified()))
        .unwrap_or(false);

    if already_verified {
        // Allow only if an admin explicitly reset this user's trust
        let allowed = state.reset_allowed.lock().await.remove(user_id);
        if !allowed {
            warn!(
                "Rejecting verification from {} — already has a verified device",
                user_id
            );
            request.cancel().await.ok();
            return;
        }
        info!("Allowing re-verification for {} (trust was reset by admin)", user_id);
    }

    info!("Accepting verification from {}", user_id);
    if let Err(e) = request.accept().await {
        error!("Failed to accept verification request: {e}");
        return;
    }

    let mut stream = request.changes();
    while let Some(state) = stream.next().await {
        match state {
            VerificationRequestState::Transitioned { verification } => {
                if let Verification::SasV1(sas) = verification {
                    tokio::spawn(handle_sas(sas));
                    break;
                }
            }
            VerificationRequestState::Done | VerificationRequestState::Cancelled(_) => break,
            _ => {}
        }
    }
}

async fn handle_sas(sas: matrix_sdk::encryption::verification::SasVerification) {
    info!("SAS with {} {}", sas.other_device().user_id(), sas.other_device().device_id());

    if let Err(e) = sas.accept().await {
        error!("Failed to accept SAS: {e}");
        return;
    }

    let mut stream = sas.changes();
    while let Some(state) = stream.next().await {
        match state {
            SasState::KeysExchanged { .. } => {
                info!("Auto-confirming emojis");
                if let Err(e) = sas.confirm().await {
                    error!("SAS confirm failed: {e}");
                    break;
                }
            }
            SasState::Done { .. } => {
                info!(
                    "Verification done: {} {}",
                    sas.other_device().user_id(),
                    sas.other_device().device_id()
                );
                break;
            }
            SasState::Cancelled(info) => {
                warn!("Verification cancelled: {}", info.reason());
                break;
            }
            _ => {}
        }
    }
}

fn is_join_terminal(e: &matrix_sdk::Error) -> bool {
    let s = e.to_string();
    s.contains("No known servers")
        || s.contains("M_FORBIDDEN")
        || s.contains("M_UNKNOWN_TOKEN")
        || s.contains("M_GUEST_ACCESS_FORBIDDEN")
}

async fn bootstrap_cross_signing(client: &Client, user_id: &OwnedUserId) {
    if let Some(status) = client.encryption().cross_signing_status().await {
        if status.has_master && status.has_self_signing && status.has_user_signing {
            info!("Cross-signing already complete (keys present) — skipping bootstrap");
            return;
        }
    }
    match client.encryption().bootstrap_cross_signing(None).await {
        Ok(()) => info!("Cross-signing bootstrapped — bot device is now self-signed"),
        Err(e) => warn!("Cross-signing bootstrap failed for {user_id}: {e}"),
    }
}
