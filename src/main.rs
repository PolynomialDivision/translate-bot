use std::{
    collections::HashSet,
    path::PathBuf,
    sync::Arc,
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
    room::reply::{EnforceThread, Reply},
    ruma::{
        OwnedDeviceId, OwnedUserId,
        api::client::filter::FilterDefinition,
        events::{
            key::verification::request::ToDeviceKeyVerificationRequestEvent,
            room::{
                member::StrippedRoomMemberEvent,
                message::{
                    AddMentions, MessageType, OriginalSyncRoomMessageEvent,
                    RoomMessageEventContent, RoomMessageEventContentWithoutRelation,
                    TextMessageEventContent,
                },
            },
        },
    },
};
use matrix_sdk_base::crypto::CollectStrategy;
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
}

fn default_langs() -> Vec<String> {
    vec!["en".to_owned(), "de".to_owned()]
}

fn default_min_confidence() -> f64 {
    0.5
}

impl Default for TranslationConfig {
    fn default() -> Self {
        Self { langs: default_langs(), min_confidence: default_min_confidence() }
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
    http: reqwest::Client,
    bot_user_id: OwnedUserId,
    admin_users: HashSet<OwnedUserId>,
    allowed_inviters: HashSet<OwnedUserId>,
    // Users allowed to re-verify despite already having a verified device.
    // Populated by !reset-trust from an admin, cleared after use.
    reset_allowed: Arc<Mutex<HashSet<OwnedUserId>>>,
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
        warn!("No admin_users configured — !reset-trust command will not work");
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
        http: reqwest::Client::new(),
        bot_user_id: user_id,
        admin_users,
        allowed_inviters,
        reset_allowed: Arc::new(Mutex::new(HashSet::new())),
    };

    // Auto-join invited rooms (only from allowed_inviters)
    client.add_event_handler({
        let state = state.clone();
        move |ev: StrippedRoomMemberEvent, room: Room| {
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
                tokio::spawn(async move {
                    let mut delay = 2u64;
                    loop {
                        match room.join().await {
                            Ok(_) => {
                                info!("Joined {}", room.room_id());
                                break;
                            }
                            Err(err) => {
                                warn!("Join failed for {}: {err}; retry in {delay}s", room.room_id());
                                sleep(Duration::from_secs(delay)).await;
                                delay = (delay * 2).min(3600);
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

    let without_relation = if !html_lines.is_empty() {
        RoomMessageEventContentWithoutRelation::new(MessageType::Text(
            TextMessageEventContent::html(plain_body.clone(), html_lines.join("<br>\n")),
        ))
    } else {
        RoomMessageEventContentWithoutRelation::new(MessageType::Text(
            TextMessageEventContent::plain(plain_body.clone()),
        ))
    };

    let reply = Reply {
        event_id: event.event_id.clone(),
        enforce_thread: EnforceThread::Unthreaded,
        add_mentions: AddMentions::No,
    };

    let content = match room.make_reply_event(without_relation, reply).await {
        Ok(c) => c,
        Err(err) => {
            warn!("make_reply_event failed ({err}), sending without reply threading");
            if !html_lines.is_empty() {
                RoomMessageEventContent::text_html(plain_body, html_lines.join("<br>\n"))
            } else {
                RoomMessageEventContent::text_plain(plain_body)
            }
        }
    };

    if let Err(e) = room.send(content).await {
        error!("Failed to send translation: {e}");
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

async fn bootstrap_cross_signing(client: &Client, user_id: &OwnedUserId) {
    match client.encryption().bootstrap_cross_signing(None).await {
        Ok(()) => info!("Cross-signing bootstrapped — bot device is now self-signed"),
        Err(e) => warn!(
            "Cross-signing bootstrap failed for {user_id}: {e}"
        ),
    }
}
