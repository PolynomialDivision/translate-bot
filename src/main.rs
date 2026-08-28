#![allow(clippy::items_after_test_module)]

mod db;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant, SystemTime},
};

use mxbot_common::{
    config::{MatrixConfig, SecurityConfig},
    verify::{VerificationService, VerificationSettings},
};

use db::{Db, DbEventOutcome};

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

use anyhow::{Context, Result};
use futures_util::future::join_all;
use matrix_sdk::{
    config::SyncSettings,
    ruma::{
        api::client::filter::FilterDefinition,
        events::{
            relation::{InReplyTo, Replacement, Reply, Thread},
            room::{
                encrypted::OriginalSyncRoomEncryptedEvent,
                member::StrippedRoomMemberEvent,
                message::{
                    MessageFormat, MessageType, NoticeMessageEventContent,
                    OriginalSyncRoomMessageEvent, Relation, RoomMessageEventContent,
                    RoomMessageEventContentWithoutRelation, TextMessageEventContent,
                },
                redaction::OriginalSyncRoomRedactionEvent,
            },
        },
        OwnedEventId, OwnedServerName, OwnedUserId, RoomOrAliasId,
    },
    Client, Room, RoomState,
};
use pulldown_cmark::html::push_html;
use pulldown_cmark::{Options, Parser};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    sync::{Mutex, Notify, Semaphore},
    time::{sleep, timeout},
};
use tracing::{error, info, warn};

const MAX_TRANSLATION_ABSOLUTE_CHARS: usize = 1_000;
const MAX_TRANSLATION_EXPANSION_FACTOR: usize = 6;
const MAX_TRANSLATION_EXPANSION_SLACK: usize = 80;

fn parse_verify_device_arguments(
    arguments: &str,
) -> std::result::Result<(OwnedUserId, matrix_sdk::ruma::OwnedDeviceId), &'static str> {
    let mut parts = arguments.split_whitespace();
    let (Some(user), Some(device), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err("expected exactly a Matrix user ID and device ID");
    };
    let user_id = user.parse().map_err(|_| "invalid Matrix user ID")?;
    Ok((user_id, matrix_sdk::ruma::OwnedDeviceId::from(device)))
}

#[derive(Deserialize)]
struct Config {
    matrix: MatrixConfig,
    libretranslate: LibreTranslateConfig,
    #[serde(default)]
    translation: TranslationConfig,
    #[serde(default)]
    room_translations: HashMap<String, RoomTranslationConfig>,
    #[serde(default)]
    security: SecurityConfig,
}

#[derive(Deserialize)]
struct LibreTranslateConfig {
    url: String,
    api_key: Option<String>,
}

#[derive(Clone, Deserialize)]
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
    /// Maximum concurrent requests to the translation backend.
    #[serde(default = "default_backend_concurrency")]
    backend_concurrency: usize,
    /// Timeout for a single backend translation request.
    #[serde(default = "default_translation_request_timeout_secs")]
    request_timeout_secs: u64,
    /// Maximum attempts for a single target language.
    #[serde(default = "default_translation_max_attempts")]
    max_attempts: usize,
    /// Initial retry backoff in milliseconds. Backoff doubles after each retry.
    #[serde(default = "default_translation_retry_initial_backoff_ms")]
    retry_initial_backoff_ms: u64,
    /// Overall deadline for translating one Matrix event into all targets.
    #[serde(default = "default_translation_overall_timeout_secs")]
    overall_timeout_secs: u64,
    /// Maximum input length (message body or caption, in characters)
    /// submitted to the translation backend. The backend is a single,
    /// GPU-constrained, largely CPU-bound LLM shared across all rooms via a
    /// small concurrency semaphore (`backend_concurrency`) — one very long
    /// message would otherwise hold a slot for a disproportionate amount of
    /// time and delay everyone else's translations. Ordinary chat messages
    /// are nowhere near the default.
    #[serde(default = "default_max_input_chars")]
    max_input_chars: usize,
}

#[derive(Clone, Default, Deserialize)]
struct RoomTranslationConfig {
    langs: Option<Vec<String>>,
    min_confidence: Option<f64>,
    reply_to_original: Option<bool>,
    thread_replies: Option<bool>,
    silent_messages: Option<bool>,
}

#[derive(Clone)]
struct EffectiveTranslationConfig {
    langs: Vec<String>,
    min_confidence: f64,
    reply_to_original: bool,
    thread_replies: bool,
    silent_messages: bool,
}

fn default_langs() -> Vec<String> {
    vec!["en".to_owned(), "de".to_owned()]
}

fn default_min_confidence() -> f64 {
    0.5
}

fn default_true() -> bool {
    true
}

fn default_backend_concurrency() -> usize {
    4
}

fn default_translation_request_timeout_secs() -> u64 {
    60
}

fn default_translation_max_attempts() -> usize {
    3
}

fn default_translation_retry_initial_backoff_ms() -> u64 {
    250
}

fn default_translation_overall_timeout_secs() -> u64 {
    90
}

fn default_max_input_chars() -> usize {
    4_000
}

impl Default for TranslationConfig {
    fn default() -> Self {
        Self {
            langs: default_langs(),
            min_confidence: default_min_confidence(),
            reply_to_original: true,
            thread_replies: true,
            silent_messages: false,
            backend_concurrency: default_backend_concurrency(),
            request_timeout_secs: default_translation_request_timeout_secs(),
            max_attempts: default_translation_max_attempts(),
            retry_initial_backoff_ms: default_translation_retry_initial_backoff_ms(),
            overall_timeout_secs: default_translation_overall_timeout_secs(),
            max_input_chars: default_max_input_chars(),
        }
    }
}

fn effective_translation_config(
    default: &TranslationConfig,
    room_translations: &HashMap<String, RoomTranslationConfig>,
    room_id: &str,
) -> EffectiveTranslationConfig {
    let room = room_translations.get(room_id);
    EffectiveTranslationConfig {
        langs: room
            .and_then(|cfg| cfg.langs.clone())
            .unwrap_or_else(|| default.langs.clone()),
        min_confidence: room
            .and_then(|cfg| cfg.min_confidence)
            .unwrap_or(default.min_confidence),
        reply_to_original: room
            .and_then(|cfg| cfg.reply_to_original)
            .unwrap_or(default.reply_to_original),
        thread_replies: room
            .and_then(|cfg| cfg.thread_replies)
            .unwrap_or(default.thread_replies),
        silent_messages: room
            .and_then(|cfg| cfg.silent_messages)
            .unwrap_or(default.silent_messages),
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum TranslationErrorKind {
    Timeout,
    Request,
    HttpStatus(u16),
    BackendBusy,
    ResponseParse,
    Empty,
    Rejected,
    OverallTimeout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TranslationError {
    kind: TranslationErrorKind,
    detail: String,
}

impl TranslationError {
    fn new(kind: TranslationErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    fn is_transient(&self) -> bool {
        match self.kind {
            TranslationErrorKind::Timeout
            | TranslationErrorKind::Request
            | TranslationErrorKind::BackendBusy => true,
            TranslationErrorKind::HttpStatus(status) => status == 429 || status >= 500,
            TranslationErrorKind::ResponseParse
            | TranslationErrorKind::Empty
            | TranslationErrorKind::Rejected
            | TranslationErrorKind::OverallTimeout => false,
        }
    }

    fn class(&self) -> &'static str {
        match self.kind {
            TranslationErrorKind::Timeout => "timeout",
            TranslationErrorKind::Request => "backend_request_error",
            TranslationErrorKind::HttpStatus(_) => "backend_http_error",
            TranslationErrorKind::BackendBusy => "backend_busy",
            TranslationErrorKind::ResponseParse => "response_parse_error",
            TranslationErrorKind::Empty => "empty_response",
            TranslationErrorKind::Rejected => "validation_rejected",
            TranslationErrorKind::OverallTimeout => "overall_timeout",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TranslationRetryPolicy {
    request_timeout: Duration,
    max_attempts: usize,
    initial_backoff: Duration,
    overall_timeout: Duration,
}

impl TranslationRetryPolicy {
    fn from_config(config: &TranslationConfig) -> Self {
        Self {
            request_timeout: Duration::from_secs(config.request_timeout_secs.max(1)),
            max_attempts: config.max_attempts.max(1),
            initial_backoff: Duration::from_millis(config.retry_initial_backoff_ms),
            overall_timeout: Duration::from_secs(config.overall_timeout_secs.max(1)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TranslatedLine {
    target: String,
    plain: String,
    html: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TargetTranslationFailure {
    target: String,
    attempts: usize,
    error: TranslationError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TranslationBatchFailure {
    source: String,
    context: &'static str,
    failed_targets: Vec<TargetTranslationFailure>,
    suppressed: bool,
}

#[derive(Clone, Copy)]
enum TranslationInput<'a> {
    Text { plain: &'a str },
    Html { html: &'a str, plain: &'a str },
}

/// Why an event was not translated. The `&'static str` form is what actually
/// appears in logs/stats/diagnostics — kept as an enum so call sites can't
/// typo a reason string that then silently fails to aggregate correctly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SkipReason {
    Backlog,
    RoomNotJoined,
    UnsupportedMsgtype,
    NoCaption,
    EmptyText,
    InputTooLong,
    LangDetectFailed,
    BelowConfidence,
    LanguageNotConfigured,
    NoRemainingTargets,
    TranslationSuppressed,
    DecryptFailed,
    EditUnknownEvent,
    EditNoTranslatableText,
    AlreadyTranslated,
}

impl SkipReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Backlog => "backlog",
            Self::RoomNotJoined => "room_not_joined",
            Self::UnsupportedMsgtype => "unsupported_msgtype",
            Self::NoCaption => "no_caption",
            Self::EmptyText => "empty_text",
            Self::InputTooLong => "input_too_long",
            Self::LangDetectFailed => "lang_detect_failed",
            Self::BelowConfidence => "below_confidence",
            Self::LanguageNotConfigured => "language_not_configured",
            Self::NoRemainingTargets => "no_remaining_targets",
            Self::TranslationSuppressed => "translation_suppressed",
            Self::DecryptFailed => "decrypt_failed",
            Self::EditUnknownEvent => "edit_unknown_event",
            Self::EditNoTranslatableText => "edit_no_translatable_text",
            Self::AlreadyTranslated => "already_translated",
        }
    }
}

/// A snapshot of what happened to one Matrix event, kept in a bounded
/// ring buffer so `!translate debug <event-id>` can explain recent history
/// without needing a persistent store. Not a substitute for logs — just an
/// index into "what do I check first."
#[derive(Clone)]
struct EventOutcome {
    event_id: OwnedEventId,
    room_id: String,
    sender: Option<String>,
    msgtype: String,
    text_source: &'static str,
    decision: &'static str, // "translate" | "skip" | "error"
    reason: Option<&'static str>,
    source_lang: Option<String>,
    targets: Option<String>,
    matrix_send: Option<&'static str>, // "ok" | "failed"
    duration_ms: Option<u128>,
    at: SystemTime,
}

impl From<&EventOutcome> for DbEventOutcome {
    fn from(o: &EventOutcome) -> Self {
        DbEventOutcome {
            event_id: o.event_id.to_string(),
            room_id: o.room_id.clone(),
            sender: o.sender.clone(),
            msgtype: o.msgtype.clone(),
            text_source: o.text_source.to_owned(),
            decision: o.decision.to_owned(),
            reason: o.reason.map(str::to_owned),
            source_lang: o.source_lang.clone(),
            targets: o.targets.clone(),
            matrix_send: o.matrix_send.map(str::to_owned),
            duration_ms: o.duration_ms.map(|d| d as i64),
            at: o
                .at
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        }
    }
}

const EVENT_OUTCOME_HISTORY: usize = 300;

/// Process-lifetime counters and recent-event history. Reset on restart —
/// this is in-memory diagnostics for "what's happening right now", not a
/// durable audit log (that's what stdout/docker logs are for).
struct Stats {
    started_at: SystemTime,
    events_seen: AtomicU64,
    translated: AtomicU64,
    decrypt_failures: AtomicU64,
    translation_api_failures: AtomicU64,
    matrix_send_failures: AtomicU64,
    retries: AtomicU64,
    retries_succeeded: AtomicU64,
    sync_reconnects: AtomicU64,
    skipped: RwLock<HashMap<&'static str, u64>>,
    recent: RwLock<VecDeque<EventOutcome>>,
    db: Db,
}

impl Stats {
    fn new(db: Db) -> Self {
        Self {
            started_at: SystemTime::now(),
            events_seen: AtomicU64::new(0),
            translated: AtomicU64::new(0),
            decrypt_failures: AtomicU64::new(0),
            translation_api_failures: AtomicU64::new(0),
            matrix_send_failures: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            retries_succeeded: AtomicU64::new(0),
            sync_reconnects: AtomicU64::new(0),
            skipped: RwLock::new(HashMap::new()),
            recent: RwLock::new(VecDeque::new()),
            db,
        }
    }

    fn record_skip(&self, reason: SkipReason) {
        if let Ok(mut map) = self.skipped.write() {
            *map.entry(reason.as_str()).or_insert(0) += 1;
        }
    }

    /// Pushes into the in-memory ring buffer (fast path for the current
    /// process's lifetime) and, best-effort, persists the same outcome to
    /// the DB in the background so `!translate debug` can still find recent
    /// failures after a restart. The DB write is fire-and-forget — this is
    /// diagnostics, not the correctness-critical dedup path (that's
    /// `Db::record_translation`, which callers await directly).
    fn push_outcome(&self, outcome: EventOutcome) {
        if let Ok(mut recent) = self.recent.write() {
            recent.push_back(outcome.clone());
            while recent.len() > EVENT_OUTCOME_HISTORY {
                recent.pop_front();
            }
        }
        let db = self.db.clone();
        let db_outcome = DbEventOutcome::from(&outcome);
        tokio::spawn(async move {
            if let Err(e) = db.push_outcome(db_outcome).await {
                warn!("Failed to persist event outcome to DB: {e}");
            }
        });
    }

    fn find_outcome_in_memory(&self, event_id: &str) -> Option<EventOutcome> {
        let recent = self.recent.read().ok()?;
        recent
            .iter()
            .rev()
            .find(|o| o.event_id.as_str() == event_id)
            .cloned()
    }

    /// Checks the in-memory ring buffer first, then falls back to the DB —
    /// covers both "recent in this process" and "from before the last
    /// restart" lookups.
    async fn find_outcome(&self, event_id: &str) -> Option<DbEventOutcome> {
        if let Some(o) = self.find_outcome_in_memory(event_id) {
            return Some(DbEventOutcome::from(&o));
        }
        match self.db.find_outcome(event_id).await {
            Ok(found) => found,
            Err(e) => {
                warn!("DB lookup failed for !translate debug {event_id}: {e}");
                None
            }
        }
    }

    fn skipped_snapshot(&self) -> Vec<(&'static str, u64)> {
        let mut v: Vec<_> = self
            .skipped
            .read()
            .map(|map| map.iter().map(|(&k, &v)| (k, v)).collect())
            .unwrap_or_default();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        v
    }
}

#[derive(Clone)]
struct BotState {
    lt_url: String,
    lt_api_key: Option<String>,
    translation: TranslationConfig,
    room_translations: HashMap<String, RoomTranslationConfig>,
    http: reqwest::Client,
    bot_user_id: OwnedUserId,
    admin_users: HashSet<OwnedUserId>,
    allowed_inviters: HashSet<OwnedUserId>,
    verification: VerificationService,
    startup_time: SystemTime,
    // Caps concurrent in-flight message handlers to bound task/connection growth.
    inflight: Arc<Semaphore>,
    // Caps requests to ltengine across detection and translation.
    lt_requests: Arc<Semaphore>,
    stats: Arc<Stats>,
    // Durable dedup (original event_id -> bot's translation event_id) and
    // best-effort event-outcome history — survives process restarts.
    // See db.rs. Cheap to clone (Arc<Mutex<Connection>> internally).
    db: Db,
    // Marks original event IDs whose translate-and-persist pipeline is
    // currently running, so a same-event edit/redaction that races in before
    // `db` has the row can wait for it instead of concluding there's nothing
    // to update/delete. std Mutex: only ever held for a map lookup/insert,
    // never across an await.
    pending_translations: PendingMap,
}

impl BotState {
    fn translation_for_room(&self, room_id: &str) -> EffectiveTranslationConfig {
        effective_translation_config(&self.translation, &self.room_translations, room_id)
    }

    fn retry_policy(&self) -> TranslationRetryPolicy {
        TranslationRetryPolicy::from_config(&self.translation)
    }

    async fn detect(&self, text: &str) -> Option<(String, f64)> {
        let _permit = self
            .lt_requests
            .acquire()
            .await
            .map_err(|e| warn!("ltengine request limiter closed: {e}"))
            .ok()?;
        let resp = self
            .http
            .post(format!("{}/detect", self.lt_url))
            .json(&DetectRequest {
                q: text,
                api_key: self.lt_api_key.as_deref(),
            })
            .send()
            .await
            .map_err(|e| warn!("ltengine /detect request error: {e}"))
            .ok()?
            .error_for_status()
            .map_err(|e| warn!("ltengine /detect returned error status: {e}"))
            .ok()?;
        let results: Vec<DetectResult> = resp
            .json()
            .await
            .map_err(|e| warn!("ltengine /detect response parse error: {e}"))
            .ok()?;
        results
            .into_iter()
            .max_by(|a, b| {
                a.confidence
                    .partial_cmp(&b.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|r| (r.language, normalize_confidence(r.confidence)))
    }

    async fn translate(
        &self,
        text: &str,
        source: &str,
        target: &str,
        format: &str,
    ) -> Result<String, TranslationError> {
        let _permit = self.lt_requests.acquire().await.map_err(|e| {
            warn!("ltengine request limiter closed: {e}");
            TranslationError::new(TranslationErrorKind::Request, e.to_string())
        })?;

        let request_timeout = self.retry_policy().request_timeout;
        let request = async {
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
                .map_err(|e| TranslationError::new(TranslationErrorKind::Request, e.to_string()))?;

            let status = resp.status();
            let body = resp
                .text()
                .await
                .map_err(|e| TranslationError::new(TranslationErrorKind::Request, e.to_string()))?;

            if !status.is_success() {
                let kind = if is_backend_busy_body(&body) {
                    TranslationErrorKind::BackendBusy
                } else {
                    TranslationErrorKind::HttpStatus(status.as_u16())
                };
                return Err(TranslationError::new(kind, response_excerpt(&body)));
            }

            match serde_json::from_str::<TranslateResponse>(&body) {
                Ok(result) => Ok(result.translated_text),
                Err(e) if is_backend_busy_body(&body) => Err(TranslationError::new(
                    TranslationErrorKind::BackendBusy,
                    response_excerpt(&body),
                )),
                // Unlike the error-status and backend-busy cases above, a
                // 2xx response that fails to parse as {"translatedText":...}
                // is exactly the shape most likely to contain the translated
                // user content itself (truncated/malformed JSON, but the
                // text is still in there) — log the parse error and size
                // only, never the body, so translated content never reaches
                // logs.
                Err(e) => Err(TranslationError::new(
                    TranslationErrorKind::ResponseParse,
                    format!("{e} (body_len={} bytes)", body.len()),
                )),
            }
        };

        with_translation_timeout(request_timeout, request).await
    }
}

async fn with_translation_timeout<T, Fut>(
    request_timeout: Duration,
    future: Fut,
) -> Result<T, TranslationError>
where
    Fut: Future<Output = Result<T, TranslationError>>,
{
    timeout(request_timeout, future).await.map_err(|_| {
        TranslationError::new(
            TranslationErrorKind::Timeout,
            format!("request exceeded {}s", request_timeout.as_secs()),
        )
    })?
}

fn is_backend_busy_body(body: &str) -> bool {
    body.to_lowercase().contains("server busy")
}

/// LibreTranslate-compatible `/detect` endpoints (including ltengine) report
/// confidence on a 0-100 scale, but `min_confidence` is documented and
/// configured on a 0.0-1.0 scale. Without this conversion, `min_confidence`
/// compares against the wrong scale and only ever rejects a confidence of
/// exactly 0 — the threshold silently stops filtering anything.
fn normalize_confidence(raw: f64) -> f64 {
    (raw / 100.0).clamp(0.0, 1.0)
}

fn response_excerpt(body: &str) -> String {
    const MAX: usize = 240;
    let mut out: String = body.chars().take(MAX).collect();
    if body.chars().count() > MAX {
        out.push_str("...");
    }
    out
}

async fn retry_translation_operation<T, Op, Fut>(
    target: &str,
    policy: &TranslationRetryPolicy,
    attempts_by_target: Arc<Mutex<HashMap<String, usize>>>,
    stats: &Stats,
    mut op: Op,
) -> Result<T, TargetTranslationFailure>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, TranslationError>>,
{
    let mut attempts = 0usize;
    let mut backoff = policy.initial_backoff;

    loop {
        attempts += 1;
        attempts_by_target
            .lock()
            .await
            .insert(target.to_owned(), attempts);

        match op().await {
            Ok(value) => {
                if attempts > 1 {
                    stats.retries_succeeded.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(value);
            }
            Err(error) if attempts < policy.max_attempts && error.is_transient() => {
                warn!(
                    "Translation target={target} failed transiently attempt={attempts}/{} class={} detail={} — retrying",
                    policy.max_attempts,
                    error.class(),
                    error.detail
                );
                stats.retries.fetch_add(1, Ordering::Relaxed);
                if !backoff.is_zero() {
                    sleep(backoff).await;
                    backoff = backoff.saturating_mul(2);
                }
            }
            Err(error) => {
                return Err(TargetTranslationFailure {
                    target: target.to_owned(),
                    attempts,
                    error,
                });
            }
        }
    }
}

async fn collect_all_or_nothing<Fut>(
    source: &str,
    target_langs: &[String],
    context: &'static str,
    overall_timeout: Duration,
    attempts_by_target: Arc<Mutex<HashMap<String, usize>>>,
    futures: Vec<Fut>,
) -> Result<Vec<TranslatedLine>, TranslationBatchFailure>
where
    Fut: Future<Output = Result<TranslatedLine, TargetTranslationFailure>>,
{
    match timeout(overall_timeout, join_all(futures)).await {
        Ok(results) => {
            let mut lines = Vec::new();
            let mut failures = Vec::new();
            for result in results {
                match result {
                    Ok(line) => lines.push(line),
                    Err(failure) => failures.push(failure),
                }
            }

            if failures.is_empty() {
                Ok(lines)
            } else {
                Err(TranslationBatchFailure {
                    source: source.to_owned(),
                    context,
                    failed_targets: failures,
                    suppressed: true,
                })
            }
        }
        Err(_) => {
            let attempts = attempts_by_target.lock().await.clone();
            let failed_targets = target_langs
                .iter()
                .map(|target| TargetTranslationFailure {
                    target: target.clone(),
                    attempts: attempts.get(target).copied().unwrap_or(0),
                    error: TranslationError::new(
                        TranslationErrorKind::OverallTimeout,
                        format!(
                            "overall deadline exceeded after {}s",
                            overall_timeout.as_secs()
                        ),
                    ),
                })
                .collect();

            Err(TranslationBatchFailure {
                source: source.to_owned(),
                context,
                failed_targets,
                suppressed: true,
            })
        }
    }
}

fn log_suppressed_translation(
    state: &BotState,
    failure: &TranslationBatchFailure,
    room_id: &str,
    event_id: &OwnedEventId,
) {
    let failed = failure
        .failed_targets
        .iter()
        .map(|failure| {
            format!(
                "{} attempts={} class={} detail={}",
                failure.target,
                failure.attempts,
                failure.error.class(),
                failure.error.detail
            )
        })
        .collect::<Vec<_>>()
        .join("; ");

    warn!(
        "Translation suppressed for {event_id} in {room_id}: context={} source_lang={} \
         failed_targets=[{}] suppressed={}",
        failure.context, failure.source, failed, failure.suppressed
    );

    state
        .stats
        .translation_api_failures
        .fetch_add(failure.failed_targets.len() as u64, Ordering::Relaxed);
    state.stats.record_skip(SkipReason::TranslationSuppressed);
    state.stats.push_outcome(EventOutcome {
        event_id: event_id.clone(),
        room_id: room_id.to_owned(),
        sender: None,
        msgtype: failure.context.to_owned(),
        text_source: failure.context,
        decision: "error",
        reason: Some(SkipReason::TranslationSuppressed.as_str()),
        source_lang: Some(failure.source.clone()),
        targets: Some(
            failure
                .failed_targets
                .iter()
                .map(|f| f.target.clone())
                .collect::<Vec<_>>()
                .join(","),
        ),
        matrix_send: None,
        duration_ms: None,
        at: SystemTime::now(),
    });
    info!(
        event = %event_id,
        room = room_id,
        msgtype = failure.context,
        text_source = failure.context,
        decision = "error",
        reason = "translation_suppressed",
        source = %failure.source,
        "event processed"
    );
}

fn build_translation_bodies(lines: &[TranslatedLine]) -> (String, String) {
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

#[tokio::main]
async fn main() -> Result<()> {
    // matrix-sdk instruments its own sync/event-handler internals at INFO and
    // captures large Debug-formatted fields (the entire SyncSettings /
    // FilterDefinition) on every span — left unfiltered, that dwarfs our own
    // logs and burns through the container's log rotation budget within
    // hours, evicting the history needed to debug past events. RUST_LOG, if
    // set, still overrides this entirely.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(
            |_| {
                tracing_subscriber::EnvFilter::new(
                    "info,matrix_sdk=warn,matrix_sdk_crypto=warn,matrix_sdk_base=warn,matrix_sdk_ui=warn",
                )
            },
        ))
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_owned());
    let config_str = fs::read_to_string(&config_path)
        .await
        .unwrap_or_else(|_| std::fs::read_to_string("config.toml").expect("config.toml not found"));
    let config: Config = toml::from_str(&config_str)?;

    let store_path =
        PathBuf::from(std::env::var("STORE_PATH").unwrap_or_else(|_| "store".to_owned()));
    fs::create_dir_all(&store_path).await?;
    // Sibling file next to matrix-sdk's own store (matrix-sdk-*.sqlite3) —
    // same directory, same volume, distinct name, no shared schema. Holds
    // durable dedup (translations) and best-effort event-outcome history,
    // both of which need to survive process/container restarts.
    let db = Db::open(&store_path.join("translate-bot.sqlite3"))
        .context("Failed to open translate-bot database")?;
    let (client, user_id) = mxbot_common::session::build_and_restore(
        &config.matrix,
        &store_path,
        config.security.encryption_strategy.into(),
    )
    .await?;

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

    let mut verification_users: HashSet<OwnedUserId> = config
        .security
        .verification
        .allowed_users
        .iter()
        .filter_map(|user| user.parse().ok())
        .collect();
    if verification_users.is_empty() {
        verification_users.clone_from(&allowed_inviters);
    }

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

    if verification_users.is_empty() {
        warn!("No verification users configured — verification requests will require an administrative grant");
    } else {
        info!(
            "Users allowed to verify with the bot: {:?}",
            verification_users
        );
    }

    let backend_concurrency = config.translation.backend_concurrency.max(1);

    let verification = VerificationService::allowlisted_tofu(
        client.clone(),
        verification_users,
        VerificationSettings {
            flow_timeout: Duration::from_secs(
                config.security.verification.flow_timeout_secs.max(1),
            ),
            grant_ttl: Duration::from_secs(config.security.verification.grant_ttl_secs.max(1)),
            max_concurrent: config.security.verification.max_concurrent.max(1),
            allow_users_from_joined_rooms: config
                .security
                .verification
                .allow_users_from_joined_rooms,
        },
    );
    verification.install_handlers();

    let mut state = BotState {
        lt_url: config.libretranslate.url.trim_end_matches('/').to_owned(),
        lt_api_key: config.libretranslate.api_key,
        translation: config.translation,
        room_translations: config.room_translations,
        // Placeholder — the real cutoff is set below, once handlers are about
        // to be registered. It must not be captured this early: everything
        // above (session restore, the ltengine probe, sync_once itself) can
        // take several seconds, and a live message sent during that window
        // would otherwise be misclassified as backlog and silently dropped.
        startup_time: SystemTime::now(),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .expect("failed to build HTTP client"),
        bot_user_id: user_id,
        admin_users,
        allowed_inviters,
        verification,
        inflight: Arc::new(Semaphore::new(8)),
        lt_requests: Arc::new(Semaphore::new(backend_concurrency)),
        stats: Arc::new(Stats::new(db.clone())),
        db,
        pending_translations: Arc::new(std::sync::Mutex::new(HashMap::new())),
    };

    // Probe ltengine reachability at startup so failures are visible in logs.
    info!("ltengine URL: {}", state.lt_url);
    match state
        .http
        .get(format!("{}/languages", state.lt_url))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => info!("ltengine reachable at startup"),
        Ok(resp) => warn!(
            "ltengine returned {} at startup — translations may fail",
            resp.status()
        ),
        Err(e) => {
            warn!("ltengine not reachable at startup: {e} — translations will fail until it is up")
        }
    }

    // Advance the sync token past ordinary workload events from downtime before
    // registering those handlers. Verification handlers are already installed
    // so pending to-device and in-room verification requests are not discarded.
    let filter = FilterDefinition::with_lazy_loading();
    info!("Starting sync...");
    client
        .sync_once(SyncSettings::default().filter(filter.clone().into()))
        .await?;

    // Drain pending invites from prior sessions (StrippedRoomMemberEvent only fires for new
    // invites, not ones already persisted in the SQLite store).
    let invited = client.invited_rooms();
    if !invited.is_empty() {
        info!(
            "Pending invite(s) found after initial sync — joining {} room(s)",
            invited.len()
        );
        for room in invited {
            let room_id = room.room_id().to_owned();
            let inviter = match room.invite_details().await {
                Ok(details) => details.inviter_id,
                Err(error) => {
                    warn!("Could not determine inviter for pending invite {room_id}: {error}");
                    room.leave().await.ok();
                    continue;
                }
            };
            if !state.allowed_inviters.is_empty() && !state.allowed_inviters.contains(&inviter) {
                warn!("Rejecting pending invite from {inviter} to {room_id}");
                room.leave().await.ok();
                continue;
            }
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

    // Recompute the backlog cutoff now, right before handlers that act on it
    // are registered. sync_once (the primary backlog defence) has already
    // drained pre-restart history above without any handlers installed to
    // react to it; this belt-and-suspenders check in handle_message should
    // therefore only ever catch messages older than *this* point, not
    // messages merely older than process start.
    state.startup_time = SystemTime::now();

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

    // In-room messages: admin commands and translation. Verification requests
    // are consumed by mxbot-common's transport-independent handlers.
    client.add_event_handler({
        let state = state.clone();
        move |ev: OriginalSyncRoomMessageEvent, room: Room| {
            let state = state.clone();
            async move {
                info!(
                    "Received room message {} from {} in {}",
                    ev.event_id,
                    ev.sender,
                    room.room_id()
                );

                if let MessageType::VerificationRequest(_) = &ev.content.msgtype {
                    return;
                }

                if ev.sender == state.bot_user_id {
                    return;
                }

                state.stats.events_seen.fetch_add(1, Ordering::Relaxed);

                if room.state() != RoomState::Joined {
                    warn!(
                        "Ignoring {} in {}: room state is {:?}, not Joined",
                        ev.event_id,
                        room.room_id(),
                        room.state()
                    );
                    record_skip(
                        &state,
                        &ev.event_id,
                        room.room_id().as_str(),
                        Some(ev.sender.as_str()),
                        ev.content.msgtype.msgtype(),
                        "n/a",
                        SkipReason::RoomNotJoined,
                    );
                    return;
                }

                let inflight = Arc::clone(&state.inflight);
                tokio::spawn(async move {
                    // Acquire a slot before starting — bounds concurrent HTTP connections
                    // to ltengine and prevents unbounded task accumulation when it is slow.
                    let _permit = inflight.acquire_owned().await;
                    handle_message(state, room, ev).await;
                });
            }
        }
    });

    // Events matrix-sdk-crypto could not decrypt stay shaped as m.room.encrypted
    // and are dispatched here instead of to the OriginalSyncRoomMessageEvent
    // handler above (which only ever sees plaintext — the SDK recasts an event
    // to its plaintext type before dispatch, precisely when decryption
    // succeeded). Without this handler, undecryptable events were completely
    // invisible: no log, no counter, nothing. Known limitation: matrix-sdk can
    // later re-decrypt an event once a key arrives (see its `redecryptor`),
    // but that does not re-invoke event handlers, so a late key does not
    // retroactively translate the message — the room key needs to already be
    // available at sync time.
    client.add_event_handler({
        let state = state.clone();
        move |ev: OriginalSyncRoomEncryptedEvent, room: Room| {
            let state = state.clone();
            async move {
                if ev.sender == state.bot_user_id {
                    return;
                }
                warn!(
                    "Unable to decrypt {} from {} in {} — translation skipped",
                    ev.event_id,
                    ev.sender,
                    room.room_id()
                );
                state.stats.decrypt_failures.fetch_add(1, Ordering::Relaxed);
                record_skip(
                    &state,
                    &ev.event_id,
                    room.room_id().as_str(),
                    Some(ev.sender.as_str()),
                    "m.room.encrypted",
                    "n/a",
                    SkipReason::DecryptFailed,
                );
            }
        }
    });

    // Redactions: if a user deletes their original message, delete the bot's
    // translation too — including thread replies that would otherwise be orphaned.
    // Spawned (with the same inflight permit as regular messages) rather than
    // run inline: `handle_redaction` can wait up to `PENDING_TRANSLATION_WAIT`
    // for a same-event translation still in flight, and that must not block
    // the sync loop from dispatching the rest of this (or the next) batch.
    client.add_event_handler({
        let state = state.clone();
        move |ev: OriginalSyncRoomRedactionEvent, room: Room| {
            let state = state.clone();
            async move {
                // `redacts` can be None in some room versions / federation edge cases.
                let Some(redacted_id) = ev.redacts.clone() else {
                    warn!("Redaction event has no `redacts` field — ignoring");
                    return;
                };
                let inflight = Arc::clone(&state.inflight);
                tokio::spawn(async move {
                    let _permit = inflight.acquire_owned().await;
                    handle_redaction(state, room, redacted_id).await;
                });
            }
        }
    });

    // client.sync() already retries transient network errors internally
    // without returning; reaching the bottom of this loop means it gave up
    // entirely (e.g. the homeserver is down for an extended period), so this
    // outer retry backs off exponentially instead of hammering it every 5s —
    // capped, and reset once a session has clearly been healthy again.
    let mut sync_retry_backoff = SYNC_RETRY_INITIAL;
    loop {
        let attempt_started = Instant::now();
        match client
            .sync(SyncSettings::default().filter(filter.clone().into()))
            .await
        {
            Ok(()) => warn!("Sync loop exited cleanly — reconnecting"),
            Err(e) => warn!("Sync loop error: {e} — reconnecting in {sync_retry_backoff:?}"),
        }
        state.stats.sync_reconnects.fetch_add(1, Ordering::Relaxed);
        let healthy = attempt_started.elapsed() >= SYNC_HEALTHY_AFTER;
        let delay = sync_retry_delay(sync_retry_backoff, healthy);
        sleep(delay).await;
        sync_retry_backoff = (delay * 2).min(SYNC_RETRY_MAX);
    }
}

const SYNC_RETRY_INITIAL: Duration = Duration::from_secs(5);
const SYNC_RETRY_MAX: Duration = Duration::from_secs(300);
const SYNC_HEALTHY_AFTER: Duration = Duration::from_secs(60);

/// Backoff for the outer sync-reconnect loop: reset to the floor after a
/// session that ran long enough to count as healthy, otherwise keep the
/// current backoff (the caller doubles it, capped, for the round after).
fn sync_retry_delay(previous_backoff: Duration, attempt_was_healthy: bool) -> Duration {
    if attempt_was_healthy {
        SYNC_RETRY_INITIAL
    } else {
        previous_backoff
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::{BoxFuture, FutureExt};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_policy() -> TranslationRetryPolicy {
        TranslationRetryPolicy {
            request_timeout: Duration::from_millis(20),
            max_attempts: 3,
            initial_backoff: Duration::ZERO,
            overall_timeout: Duration::from_millis(100),
        }
    }

    fn test_line(target: &str) -> TranslatedLine {
        TranslatedLine {
            target: target.to_owned(),
            plain: format!("{target} plain"),
            html: format!("{target} html"),
        }
    }

    fn test_failure(
        target: &str,
        attempts: usize,
        kind: TranslationErrorKind,
    ) -> TargetTranslationFailure {
        TargetTranslationFailure {
            target: target.to_owned(),
            attempts,
            error: TranslationError::new(kind, "test failure"),
        }
    }

    #[tokio::test]
    async fn all_targets_succeed_first_attempt() {
        let targets = vec!["en".to_owned(), "uk".to_owned()];
        let attempts = Arc::new(Mutex::new(HashMap::new()));
        let futures: Vec<BoxFuture<'static, Result<TranslatedLine, TargetTranslationFailure>>> =
            targets
                .iter()
                .map(|target| {
                    let target = target.clone();
                    async move { Ok(test_line(&target)) }.boxed()
                })
                .collect::<Vec<_>>();

        let result = collect_all_or_nothing(
            "de",
            &targets,
            "message",
            Duration::from_millis(100),
            attempts,
            futures,
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].target, "en");
        assert_eq!(result[1].target, "uk");
    }

    #[tokio::test]
    async fn transient_failure_succeeds_on_retry() {
        let policy = test_policy();
        let attempts_by_target = Arc::new(Mutex::new(HashMap::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_op = Arc::clone(&calls);

        let stats = Stats::new(Db::open_in_memory().unwrap());
        let line =
            retry_translation_operation("en", &policy, attempts_by_target, &stats, move || {
                let calls_for_op = Arc::clone(&calls_for_op);
                async move {
                    let call = calls_for_op.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        Err(TranslationError::new(
                            TranslationErrorKind::BackendBusy,
                            "Server busy, please try again later",
                        ))
                    } else {
                        Ok(test_line("en"))
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(line.target, "en");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(stats.retries.load(Ordering::Relaxed), 1);
        assert_eq!(stats.retries_succeeded.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn continued_target_failure_suppresses_partial_batch() {
        let targets = vec!["en".to_owned(), "uk".to_owned()];
        let attempts = Arc::new(Mutex::new(HashMap::new()));
        let futures: Vec<BoxFuture<'static, Result<TranslatedLine, TargetTranslationFailure>>> = vec![
            async { Err(test_failure("en", 3, TranslationErrorKind::BackendBusy)) }.boxed(),
            async { Ok(test_line("uk")) }.boxed(),
        ];

        let failure = collect_all_or_nothing(
            "de",
            &targets,
            "message",
            Duration::from_millis(100),
            attempts,
            futures,
        )
        .await
        .unwrap_err();

        assert!(failure.suppressed);
        assert_eq!(failure.source, "de");
        assert_eq!(failure.failed_targets.len(), 1);
        assert_eq!(failure.failed_targets[0].target, "en");
    }

    #[tokio::test]
    async fn per_request_timeout_is_classified() {
        let result = with_translation_timeout(Duration::from_millis(5), async {
            sleep(Duration::from_millis(25)).await;
            Ok::<_, TranslationError>(test_line("en"))
        })
        .await;

        let error = result.unwrap_err();
        assert_eq!(error.kind, TranslationErrorKind::Timeout);
        assert!(error.is_transient());
    }

    #[tokio::test]
    async fn overall_deadline_suppresses_partial_batch() {
        let targets = vec!["en".to_owned(), "uk".to_owned()];
        let attempts = Arc::new(Mutex::new(HashMap::new()));
        attempts.lock().await.insert("en".to_owned(), 1);
        let futures: Vec<BoxFuture<'static, Result<TranslatedLine, TargetTranslationFailure>>> = vec![
            async {
                sleep(Duration::from_millis(50)).await;
                Ok(test_line("en"))
            }
            .boxed(),
            async { Ok(test_line("uk")) }.boxed(),
        ];

        let failure = collect_all_or_nothing(
            "de",
            &targets,
            "message",
            Duration::from_millis(10),
            attempts,
            futures,
        )
        .await
        .unwrap_err();

        assert!(failure.suppressed);
        assert!(failure
            .failed_targets
            .iter()
            .all(|target| target.error.kind == TranslationErrorKind::OverallTimeout));
    }

    #[tokio::test]
    async fn permanent_failure_is_not_retried_excessively() {
        let policy = test_policy();
        let attempts_by_target = Arc::new(Mutex::new(HashMap::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_op = Arc::clone(&calls);

        let stats = Stats::new(Db::open_in_memory().unwrap());
        let failure =
            retry_translation_operation("en", &policy, attempts_by_target, &stats, move || {
                let calls_for_op = Arc::clone(&calls_for_op);
                async move {
                    calls_for_op.fetch_add(1, Ordering::SeqCst);
                    Err::<TranslatedLine, _>(TranslationError::new(
                        TranslationErrorKind::ResponseParse,
                        "malformed success response",
                    ))
                }
            })
            .await
            .unwrap_err();

        assert_eq!(failure.attempts, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Not transient — must not count as a retry attempt.
        assert_eq!(stats.retries.load(Ordering::Relaxed), 0);
        assert_eq!(stats.retries_succeeded.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn retries_return_single_batch_without_duplicates() {
        let policy = test_policy();
        let attempts_by_target = Arc::new(Mutex::new(HashMap::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_op = Arc::clone(&calls);
        let target = "en".to_owned();

        let stats = Stats::new(Db::open_in_memory().unwrap());
        let retried =
            retry_translation_operation("en", &policy, attempts_by_target, &stats, move || {
                let calls_for_op = Arc::clone(&calls_for_op);
                async move {
                    let call = calls_for_op.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        Err(TranslationError::new(
                            TranslationErrorKind::Timeout,
                            "timed out",
                        ))
                    } else {
                        Ok(test_line("en"))
                    }
                }
            });
        let futures: Vec<BoxFuture<'_, Result<TranslatedLine, TargetTranslationFailure>>> =
            vec![retried.boxed()];
        let targets = vec![target];
        let lines = collect_all_or_nothing(
            "de",
            &targets,
            "message",
            Duration::from_millis(100),
            Arc::new(Mutex::new(HashMap::new())),
            futures,
        )
        .await
        .unwrap();

        assert_eq!(lines.len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn formatting_is_shared_for_message_caption_and_edit_paths() {
        let lines = vec![test_line("en"), test_line("uk")];
        let (plain, html) = build_translation_bodies(&lines);

        assert_eq!(plain, "🇬🇧 en plain\n🇺🇦 uk plain");
        assert_eq!(html, "🇬🇧 en html<br>\n🇺🇦 uk html");
    }

    #[test]
    fn batch_failure_status_records_suppressed_partial_translation() {
        let failure = TranslationBatchFailure {
            source: "de".to_owned(),
            context: "message",
            failed_targets: vec![test_failure("en", 3, TranslationErrorKind::BackendBusy)],
            suppressed: true,
        };

        assert!(failure.suppressed);
        assert_eq!(failure.failed_targets[0].attempts, 3);
        assert_eq!(failure.failed_targets[0].error.class(), "backend_busy");
    }

    // ── normalize_confidence tests ────────────────────────────────────────────
    //
    // LibreTranslate-compatible /detect endpoints (including ltengine, confirmed
    // by direct query: {"confidence":33,"language":"de"}) report confidence on a
    // 0-100 scale. min_confidence is documented and configured on a 0.0-1.0
    // scale. Without normalizing at the API boundary, `confidence < min_confidence`
    // (e.g. 33.0 < 0.5) is true for essentially every non-zero detection, so the
    // threshold never rejects a low-confidence, likely-wrong language guess.

    #[test]
    fn normalize_confidence_converts_percent_to_fraction() {
        assert_eq!(normalize_confidence(33.0), 0.33);
        assert_eq!(normalize_confidence(100.0), 1.0);
        assert_eq!(normalize_confidence(0.0), 0.0);
        assert_eq!(normalize_confidence(7.0), 0.07);
    }

    #[test]
    fn normalize_confidence_clamps_out_of_range_values() {
        // Defensive: never let a backend quirk produce an out-of-bounds fraction.
        assert_eq!(normalize_confidence(150.0), 1.0);
        assert_eq!(normalize_confidence(-5.0), 0.0);
    }

    #[test]
    fn normalize_confidence_low_value_correctly_fails_default_threshold() {
        // Reproduces the observed bug scenario: a short/ambiguous message
        // misdetected with low real-world confidence must fail the default
        // min_confidence=0.5 threshold once normalized — pre-fix, 7.0 (raw)
        // would have incorrectly cleared a 0.5 threshold.
        let default = TranslationConfig::default();
        assert!(normalize_confidence(7.0) < default.min_confidence);
    }

    // ── resolve_translation_targets tests ─────────────────────────────────────

    fn test_translation_config() -> EffectiveTranslationConfig {
        EffectiveTranslationConfig {
            langs: vec!["en".to_owned(), "de".to_owned(), "uk".to_owned()],
            min_confidence: 0.5,
            reply_to_original: true,
            thread_replies: true,
            silent_messages: false,
        }
    }

    #[test]
    fn resolve_translation_targets_below_confidence_is_skipped() {
        let cfg = test_translation_config();
        let result = resolve_translation_targets(
            "message",
            "!room:example.org",
            &eid("$ev:example.org"),
            "de",
            0.07,
            &cfg,
        );
        assert_eq!(result, Err(SkipReason::BelowConfidence));
    }

    #[test]
    fn resolve_translation_targets_language_not_configured_is_skipped() {
        let cfg = test_translation_config();
        let result = resolve_translation_targets(
            "message",
            "!room:example.org",
            &eid("$ev:example.org"),
            "id",
            0.99,
            &cfg,
        );
        assert_eq!(result, Err(SkipReason::LanguageNotConfigured));
    }

    #[test]
    fn resolve_translation_targets_no_remaining_targets_is_skipped() {
        let mut cfg = test_translation_config();
        cfg.langs = vec!["de".to_owned()];
        let result = resolve_translation_targets(
            "message",
            "!room:example.org",
            &eid("$ev:example.org"),
            "de",
            0.99,
            &cfg,
        );
        assert_eq!(result, Err(SkipReason::NoRemainingTargets));
    }

    #[test]
    fn resolve_translation_targets_returns_remaining_langs_on_success() {
        let cfg = test_translation_config();
        let result = resolve_translation_targets(
            "message",
            "!room:example.org",
            &eid("$ev:example.org"),
            "de",
            0.99,
            &cfg,
        );
        assert_eq!(result, Ok(vec!["en".to_owned(), "uk".to_owned()]));
    }

    #[test]
    fn error_classification_retries_only_transient_failures() {
        assert!(TranslationError::new(TranslationErrorKind::Timeout, "timeout").is_transient());
        assert!(TranslationError::new(TranslationErrorKind::Request, "connection").is_transient());
        assert!(TranslationError::new(TranslationErrorKind::BackendBusy, "busy").is_transient());
        assert!(
            TranslationError::new(TranslationErrorKind::HttpStatus(429), "rate").is_transient()
        );
        assert!(
            TranslationError::new(TranslationErrorKind::HttpStatus(503), "down").is_transient()
        );

        assert!(
            !TranslationError::new(TranslationErrorKind::HttpStatus(400), "bad").is_transient()
        );
        assert!(
            !TranslationError::new(TranslationErrorKind::ResponseParse, "bad json").is_transient()
        );
        assert!(!TranslationError::new(TranslationErrorKind::Rejected, "guard").is_transient());
    }

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

    #[test]
    fn rejects_verbose_reasoning_translation() {
        let translated = r#"4. **Step 1: Identify the source language and target language.**
* Source: English
* Target: German
5. **Step 2: Analyze the source text.**
* Text: `hallo`
6. **Step 3: Translate the content.**
* "hallo" in English is "hallo" in German.
7. **Step 4: Re-insert HTML tags.**
8. **Step 5: Final Review.**"#;

        assert_eq!(
            translation_rejection_reason("hallo", translated),
            Some("translation expanded far beyond source length")
        );
    }

    #[test]
    fn rejects_repeated_phrase_loop() {
        let translated =
            "name more chat more chat more chat more chat more chat more chat more chat";

        assert_eq!(
            translation_rejection_reason("mehfrach chat", translated),
            Some("translation contains repeated phrase loop")
        );
    }

    #[test]
    fn accepts_reasonable_short_translation() {
        assert_eq!(translation_rejection_reason("hallo", "привіт"), None);
    }

    #[test]
    fn accepts_normal_text_with_reasoning_words() {
        let source = "Step 1: choose the source language and target language.";
        let translated = "Schritt 1: Waehlen Sie die Ausgangssprache und die Zielsprache.";

        assert_eq!(translation_rejection_reason(source, translated), None);
    }

    #[test]
    fn rejects_backend_introduced_model_control_token() {
        assert_eq!(
            translation_rejection_reason("Hoffausgang Garten.", "<|channel>Вихід у сад."),
            Some("translation contains backend control token")
        );
    }

    #[test]
    fn accepts_model_control_token_when_it_was_in_source() {
        assert_eq!(
            translation_rejection_reason("literal <|channel> token", "literales <|channel> Token"),
            None
        );
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
        assert_eq!(strip_reply_fallback(raw), raw);
    }

    #[test]
    fn blockquote_strip_passthrough_when_no_leading_quote() {
        let raw = "Normal message without quotes";
        assert_eq!(strip_reply_fallback(raw), raw);
    }

    #[test]
    fn blockquote_strip_removes_leading_reply_fallback() {
        let raw = "> Alice: original message\n\nActual reply text";
        assert_eq!(strip_reply_fallback(raw), "Actual reply text");
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

    // ── media_caption_kind tests ─────────────────────────────────────────────
    // Covers the event that prompted this: an m.image with a real caption
    // ($6qLBtCRwYLUXpbr2zsyC-tAoaHSZUzz7kdksYQc09Ig, body "Hat jemand
    // Interesse an einer Couch?", filename "1000006546.jpg") turned out not
    // to be a msgtype-matching bug — these tests pin down the matching logic
    // regardless.

    use matrix_sdk::ruma::{
        events::room::{
            message::{
                AudioMessageEventContent, EmoteMessageEventContent, FileMessageEventContent,
                ImageMessageEventContent, LocationMessageEventContent, VideoMessageEventContent,
            },
            EncryptedFile, EncryptedFileHashes, EncryptedFileInfo, V2EncryptedFileInfo,
        },
        OwnedMxcUri,
    };

    fn mxc() -> OwnedMxcUri {
        "mxc://example.org/abc123".into()
    }

    fn dummy_encrypted_file() -> EncryptedFile {
        EncryptedFile::new(
            mxc(),
            EncryptedFileInfo::V2(V2EncryptedFileInfo::encode([0u8; 32], [0u8; 16])),
            EncryptedFileHashes::with_sha256([0u8; 32]),
        )
    }

    #[test]
    fn media_caption_kind_text_is_not_media() {
        let msgtype = MessageType::Text(TextMessageEventContent::plain("hello"));
        assert_eq!(media_caption_kind(&msgtype), None);
    }

    #[test]
    fn media_caption_kind_image_with_real_caption() {
        let mut content =
            ImageMessageEventContent::plain("Hat jemand Interesse an einer Couch?".into(), mxc());
        content.filename = Some("1000006546.jpg".into());
        let msgtype = MessageType::Image(content);
        assert_eq!(
            media_caption_kind(&msgtype),
            Some(("image", Some("Hat jemand Interesse an einer Couch?")))
        );
    }

    #[test]
    fn media_caption_kind_image_filename_equal_to_body_has_no_caption() {
        let mut content = ImageMessageEventContent::plain("1000006546.jpg".into(), mxc());
        content.filename = Some("1000006546.jpg".into());
        let msgtype = MessageType::Image(content);
        assert_eq!(media_caption_kind(&msgtype), Some(("image", None)));
    }

    #[test]
    fn media_caption_kind_image_without_filename_field_has_no_caption() {
        // Older/other clients may omit `filename` entirely and just send the
        // filename as `body` — caption() requires filename to be set to
        // recognize body as a distinct caption, per the Matrix spec.
        let content = ImageMessageEventContent::plain("1000006546.jpg".into(), mxc());
        let msgtype = MessageType::Image(content);
        assert_eq!(media_caption_kind(&msgtype), Some(("image", None)));
    }

    #[test]
    fn media_caption_kind_encrypted_image_caption_still_available() {
        // The media file being end-to-end encrypted (MediaSource::Encrypted)
        // is orthogonal to whether a plaintext caption is present — by the
        // time the bot's handler sees the event, room-level E2EE has already
        // been decrypted by matrix-sdk-crypto, and caption() only looks at
        // `body`/`filename`, never `source`.
        let mut content = ImageMessageEventContent::encrypted(
            "Hat jemand Interesse an einer Couch?".into(),
            dummy_encrypted_file(),
        );
        content.filename = Some("1000006546.jpg".into());
        let msgtype = MessageType::Image(content);
        assert_eq!(
            media_caption_kind(&msgtype),
            Some(("image", Some("Hat jemand Interesse an einer Couch?")))
        );
    }

    #[test]
    fn media_caption_kind_file_with_caption() {
        let mut content = FileMessageEventContent::plain("Rechnung anbei".into(), mxc());
        content.filename = Some("invoice.pdf".into());
        let msgtype = MessageType::File(content);
        assert_eq!(
            media_caption_kind(&msgtype),
            Some(("file", Some("Rechnung anbei")))
        );
    }

    #[test]
    fn media_caption_kind_video_with_caption() {
        let mut content = VideoMessageEventContent::plain("Schau mal!".into(), mxc());
        content.filename = Some("clip.mp4".into());
        let msgtype = MessageType::Video(content);
        assert_eq!(
            media_caption_kind(&msgtype),
            Some(("video", Some("Schau mal!")))
        );
    }

    #[test]
    fn media_caption_kind_audio_with_caption() {
        let mut content = AudioMessageEventContent::plain("Hör dir das an".into(), mxc());
        content.filename = Some("voice.ogg".into());
        let msgtype = MessageType::Audio(content);
        assert_eq!(
            media_caption_kind(&msgtype),
            Some(("audio", Some("Hör dir das an")))
        );
    }

    #[test]
    fn media_caption_kind_unsupported_type_is_ignored() {
        let msgtype = MessageType::Location(LocationMessageEventContent::new(
            "shared a location".into(),
            "geo:0,0".into(),
        ));
        assert_eq!(media_caption_kind(&msgtype), None);
    }

    // ── extract_edit_text tests ──────────────────────────────────────────────
    // Edits of captioned media arrive with the same media msgtype as the
    // original (not m.text) — these pin down that handle_edit can extract
    // text from that shape too, not just m.text/m.emote.

    #[test]
    fn extract_edit_text_from_text_strips_reply_fallback() {
        let msgtype = MessageType::Text(TextMessageEventContent::plain(
            "> Alice: original\n\nEdited reply",
        ));
        assert_eq!(extract_edit_text(&msgtype), Some("Edited reply".to_owned()));
    }

    #[test]
    fn extract_edit_text_from_emote() {
        let msgtype = MessageType::Emote(EmoteMessageEventContent::plain("waves hello"));
        assert_eq!(extract_edit_text(&msgtype), Some("waves hello".to_owned()));
    }

    #[test]
    fn extract_edit_text_from_edited_image_caption() {
        let mut content = ImageMessageEventContent::plain("Edited caption".into(), mxc());
        content.filename = Some("1000006546.jpg".into());
        let msgtype = MessageType::Image(content);
        assert_eq!(
            extract_edit_text(&msgtype),
            Some("Edited caption".to_owned())
        );
    }

    #[test]
    fn extract_edit_text_from_edited_filename_only_caption_is_none() {
        let mut content = ImageMessageEventContent::plain("1000006546.jpg".into(), mxc());
        content.filename = Some("1000006546.jpg".into());
        let msgtype = MessageType::Image(content);
        assert_eq!(extract_edit_text(&msgtype), None);
    }

    #[test]
    fn extract_edit_text_from_unsupported_type_is_none() {
        let msgtype = MessageType::Location(LocationMessageEventContent::new(
            "shared a location".into(),
            "geo:0,0".into(),
        ));
        assert_eq!(extract_edit_text(&msgtype), None);
    }

    #[test]
    fn extract_edit_text_empty_after_strip_is_none() {
        let msgtype = MessageType::Text(TextMessageEventContent::plain("   "));
        assert_eq!(extract_edit_text(&msgtype), None);
    }

    // ── Stats tests ───────────────────────────────────────────────────────────

    #[test]
    fn stats_record_skip_increments_reason_counter() {
        let stats = Stats::new(Db::open_in_memory().unwrap());
        stats.record_skip(SkipReason::BelowConfidence);
        stats.record_skip(SkipReason::BelowConfidence);
        stats.record_skip(SkipReason::Backlog);
        let snapshot = stats.skipped_snapshot();
        assert_eq!(
            snapshot
                .iter()
                .find(|(reason, _)| *reason == "below_confidence")
                .map(|(_, count)| *count),
            Some(2)
        );
        assert_eq!(
            snapshot
                .iter()
                .find(|(reason, _)| *reason == "backlog")
                .map(|(_, count)| *count),
            Some(1)
        );
    }

    fn test_outcome(event_id: &str) -> EventOutcome {
        EventOutcome {
            event_id: eid(event_id),
            room_id: "!room:example.org".to_owned(),
            sender: None,
            msgtype: "m.text".to_owned(),
            text_source: "body",
            decision: "skip",
            reason: Some("below_confidence"),
            source_lang: None,
            targets: None,
            matrix_send: None,
            duration_ms: None,
            at: SystemTime::now(),
        }
    }

    #[tokio::test]
    async fn stats_ring_buffer_evicts_oldest_beyond_capacity() {
        // Tests the in-memory fast path specifically (find_outcome_in_memory);
        // push_outcome also fires a best-effort DB write, covered separately
        // by db::tests.
        let stats = Stats::new(Db::open_in_memory().unwrap());
        for i in 0..(EVENT_OUTCOME_HISTORY + 5) {
            stats.push_outcome(test_outcome(&format!("$ev{i}:example.org")));
        }
        assert!(stats.find_outcome_in_memory("$ev0:example.org").is_none());
        assert!(stats.find_outcome_in_memory("$ev4:example.org").is_none());
        assert!(stats
            .find_outcome_in_memory(&format!("$ev{}:example.org", EVENT_OUTCOME_HISTORY + 4))
            .is_some());
    }

    #[tokio::test]
    async fn stats_find_outcome_returns_most_recent_match() {
        let stats = Stats::new(Db::open_in_memory().unwrap());
        let mut first = test_outcome("$dup:example.org");
        first.decision = "skip";
        stats.push_outcome(first);
        let mut second = test_outcome("$dup:example.org");
        second.decision = "translate";
        stats.push_outcome(second);

        let found = stats.find_outcome_in_memory("$dup:example.org").unwrap();
        assert_eq!(found.decision, "translate");
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
        assert!(
            make_relation(&id, &id, false, true, false).is_none(),
            "thread_replies=true must not override reply_to_original=false"
        );
    }

    #[test]
    fn relation_reply_when_reply_true_thread_false_not_in_thread() {
        let id = eid("$ev3:example.org");
        assert!(matches!(
            make_relation(&id, &id, true, false, false),
            Some(Relation::Reply(_))
        ));
    }

    #[test]
    fn relation_thread_when_reply_true_thread_true() {
        let id = eid("$ev4:example.org");
        assert!(matches!(
            make_relation(&id, &id, true, true, false),
            Some(Relation::Thread(_))
        ));
    }

    #[test]
    fn relation_thread_when_in_thread_even_if_thread_replies_false() {
        // Key regression test: thread_replies=false must NOT pull the bot out of
        // an existing thread.  If the original message is already in a thread,
        // the translation must always be a thread reply into that same thread.
        let event_id = eid("$reply_in_thread:example.org");
        let root_id = eid("$root:example.org");
        let Some(Relation::Thread(t)) = make_relation(&event_id, &root_id, true, false, true)
        else {
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
        let event_id = eid("$reply:example.org");
        let root_id = eid("$root:example.org");
        let Some(Relation::Thread(t)) = make_relation(&event_id, &root_id, true, true, false)
        else {
            panic!("expected Thread");
        };
        assert_eq!(t.event_id, root_id, "thread root must be root_id");
        // in_reply_to inside the thread must point to the translated event itself
        assert_eq!(t.in_reply_to.as_ref().map(|r| &r.event_id), Some(&event_id));
    }

    #[test]
    fn default_translation_config() {
        let cfg = TranslationConfig::default();
        assert!(
            cfg.reply_to_original,
            "default: reply_to_original must be true"
        );
        assert!(cfg.thread_replies, "default: thread_replies must be true");
        assert!(
            !cfg.silent_messages,
            "default: silent_messages must be false"
        );
        assert_eq!(
            cfg.backend_concurrency, 4,
            "default: backend_concurrency must be 4"
        );
        assert_eq!(cfg.request_timeout_secs, 60);
        assert_eq!(cfg.max_attempts, 3);
        assert_eq!(cfg.retry_initial_backoff_ms, 250);
        assert_eq!(cfg.overall_timeout_secs, 90);
    }

    #[test]
    fn room_translation_override_changes_only_langs() {
        let mut rooms = HashMap::new();
        rooms.insert(
            "!room:example.org".to_owned(),
            RoomTranslationConfig {
                langs: Some(vec!["de".to_owned(), "uk".to_owned()]),
                ..Default::default()
            },
        );

        let default = TranslationConfig {
            langs: vec!["en".to_owned(), "de".to_owned(), "uk".to_owned()],
            min_confidence: 0.8,
            reply_to_original: false,
            thread_replies: false,
            silent_messages: true,
            backend_concurrency: 2,
            request_timeout_secs: 60,
            max_attempts: 3,
            retry_initial_backoff_ms: 250,
            overall_timeout_secs: 90,
            max_input_chars: 4_000,
        };

        let effective = effective_translation_config(&default, &rooms, "!room:example.org");
        assert_eq!(effective.langs, vec!["de", "uk"]);
        assert_eq!(effective.min_confidence, 0.8);
        assert!(!effective.reply_to_original);
        assert!(!effective.thread_replies);
        assert!(effective.silent_messages);
    }

    #[test]
    fn room_translation_unknown_room_uses_default() {
        let mut rooms = HashMap::new();
        rooms.insert(
            "!room:example.org".to_owned(),
            RoomTranslationConfig {
                langs: Some(vec!["de".to_owned(), "uk".to_owned()]),
                ..Default::default()
            },
        );

        let default = TranslationConfig::default();
        let effective = effective_translation_config(&default, &rooms, "!other:example.org");

        assert_eq!(effective.langs, default.langs);
        assert_eq!(effective.min_confidence, default.min_confidence);
        assert_eq!(effective.reply_to_original, default.reply_to_original);
        assert_eq!(effective.thread_replies, default.thread_replies);
        assert_eq!(effective.silent_messages, default.silent_messages);
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
        assert!(
            cfg.reply_to_original,
            "absent key must default to true via default_true()"
        );
    }

    #[test]
    fn serde_room_translation_overrides_parse_from_config() {
        let toml = r#"
[matrix]
homeserver = "https://matrix.org"
user_id = "@bot:example.org"
access_token = "token"
device_id = "DEVICE"

[libretranslate]
url = "http://localhost:5000"

[translation]
langs = ["en", "de", "uk"]
silent_messages = true

[room_translations."!room:example.org"]
langs = ["de", "uk"]
min_confidence = 0.7
"#;

        let cfg: Config = toml::from_str(toml).unwrap();
        let effective = effective_translation_config(
            &cfg.translation,
            &cfg.room_translations,
            "!room:example.org",
        );

        assert_eq!(effective.langs, vec!["de", "uk"]);
        assert_eq!(effective.min_confidence, 0.7);
        assert!(effective.silent_messages);
    }

    #[test]
    fn serde_entire_translation_section_missing_defaults_to_true() {
        // Simulates a config.toml with no [translation] section at all.
        // Config has #[serde(default)] on the translation field, which calls
        // TranslationConfig::default() → reply_to_original = true.
        let cfg = TranslationConfig::default();
        assert!(
            cfg.reply_to_original,
            "TranslationConfig::default() must set reply_to_original = true"
        );
    }

    #[test]
    fn serde_verification_policy_parses_and_has_bounded_defaults() {
        let security: SecurityConfig = toml::from_str(
            r#"
allowed_inviters = ["@inviter:example.org"]

[verification]
allowed_users = ["@alice:example.org"]
flow_timeout_secs = 120
grant_ttl_secs = 300
max_concurrent = 4
"#,
        )
        .unwrap();

        assert_eq!(security.verification.allowed_users, ["@alice:example.org"]);
        assert_eq!(security.verification.flow_timeout_secs, 120);
        assert_eq!(security.verification.grant_ttl_secs, 300);
        assert_eq!(security.verification.max_concurrent, 4);

        let defaults: SecurityConfig = toml::from_str("").unwrap();
        assert_eq!(defaults.verification.flow_timeout_secs, 300);
        assert_eq!(defaults.verification.grant_ttl_secs, 600);
        assert_eq!(defaults.verification.max_concurrent, 8);
    }

    #[test]
    fn verify_device_command_arguments_are_strict() {
        let (user, device) = parse_verify_device_arguments("@alice:example.org DEVICE").unwrap();
        assert_eq!(user.as_str(), "@alice:example.org");
        assert_eq!(device.as_str(), "DEVICE");
        assert!(parse_verify_device_arguments("@alice:example.org").is_err());
        assert!(parse_verify_device_arguments("not-a-user DEVICE").is_err());
        assert!(parse_verify_device_arguments("@alice:example.org DEVICE extra").is_err());
    }

    // ── Exact serialized Matrix event JSON ────────────────────────────────────
    //
    // These tests serialize actual RoomMessageEventContent values and assert
    // the exact JSON shape that the Matrix homeserver will receive and forward
    // to other clients.  This is the ground truth for client compatibility.

    fn content_with_relation(reply_to_original: bool, thread_replies: bool) -> serde_json::Value {
        let event_id = eid("$original:example.org");
        let thread_root = eid("$root:example.org");
        let mut content = make_translation_content("🇬🇧 Hello".into(), "🇬🇧 Hello".into(), false);
        content.relates_to = make_relation(
            &event_id,
            &thread_root,
            reply_to_original,
            thread_replies,
            false,
        );
        serde_json::to_value(&content).unwrap()
    }

    #[test]
    fn json_standalone_has_no_relates_to() {
        // reply_to_original=false → no relation field at all.
        // Expected JSON:
        //   { "msgtype": "m.text", "body": "...", "format": "...", "formatted_body": "..." }
        let json = content_with_relation(false, false);
        assert!(
            json.get("m.relates_to").is_none(),
            "standalone must have no m.relates_to field; got: {json}"
        );
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
        let rel = &json["m.relates_to"];
        assert!(!rel.is_null(), "m.relates_to must be present");
        assert!(
            rel.get("rel_type").is_none(),
            "plain reply must not have rel_type; got: {rel}"
        );
        assert_eq!(
            rel["m.in_reply_to"]["event_id"], "$original:example.org",
            "in_reply_to must point to $original:example.org"
        );
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
        let rel = &json["m.relates_to"];
        assert_eq!(rel["rel_type"], "m.thread", "rel_type must be m.thread");
        assert_eq!(
            rel["event_id"], "$root:example.org",
            "event_id must be thread root"
        );
        assert_eq!(
            rel["m.in_reply_to"]["event_id"], "$original:example.org",
            "in_reply_to inside thread must point to translated event"
        );
        assert!(
            rel.get("is_falling_back").is_none(),
            "is_falling_back=false is omitted by ruma (absent == false per Matrix spec); \
             if it appears it means ruma set it to true unexpectedly: {rel}"
        );
    }

    // ── Image caption path integration ───────────────────────────────────────
    //
    // handle_media_caption() builds its relation with exactly:
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
        assert!(
            json["m.relates_to"].get("rel_type").is_none(),
            "caption reply must not have rel_type"
        );
        assert_eq!(
            json["m.relates_to"]["m.in_reply_to"]["event_id"],
            "$caption:example.org"
        );
    }

    // Case B: caption event is already inside a thread.
    // resolve_thread_root returns thread.event_id (the root), NOT event_id.
    // Even with thread_replies=false the bot must stay in the thread (in_thread=true).

    #[test]
    fn image_caption_threaded_event_thread_relation() {
        let thread_root_id = eid("$thread_root:example.org");
        let caption_event_id = eid("$caption_in_thread:example.org");
        // resolve_thread_root would return thread_root_id in this case
        let thread_root = thread_root_id.clone();

        let mut content = make_translation_content("🇩🇪 Hallo".into(), "🇩🇪 Hallo".into(), false);
        // in_thread=true mirrors what handle_media_caption now passes
        content.relates_to = make_relation(&caption_event_id, &thread_root, true, false, true);

        let json = serde_json::to_value(&content).unwrap();
        assert_eq!(json["m.relates_to"]["rel_type"], "m.thread");
        assert_eq!(json["m.relates_to"]["event_id"], "$thread_root:example.org");
        assert_eq!(
            json["m.relates_to"]["m.in_reply_to"]["event_id"],
            "$caption_in_thread:example.org"
        );
    }

    #[test]
    fn image_caption_standalone_produces_no_relation_when_reply_to_original_false() {
        let caption_event_id = eid("$caption2:example.org");
        let thread_root = caption_event_id.clone();

        let mut content = make_translation_content("🇩🇪 Hallo".into(), "🇩🇪 Hallo".into(), false);
        content.relates_to = make_relation(&caption_event_id, &thread_root, false, false, false);

        let json = serde_json::to_value(&content).unwrap();
        assert!(
            json.get("m.relates_to").is_none(),
            "caption with reply_to_original=false must produce no relation"
        );
    }

    // ── sync_retry_delay ─────────────────────────────────────────────────

    #[test]
    fn sync_retry_delay_holds_backoff_when_unhealthy() {
        assert_eq!(
            sync_retry_delay(Duration::from_secs(80), false),
            Duration::from_secs(80)
        );
    }

    #[test]
    fn sync_retry_delay_resets_to_floor_when_healthy() {
        assert_eq!(
            sync_retry_delay(Duration::from_secs(300), true),
            SYNC_RETRY_INITIAL
        );
    }

    #[test]
    fn sync_retry_backoff_doubles_and_caps() {
        let mut backoff = SYNC_RETRY_INITIAL;
        for _ in 0..20 {
            let delay = sync_retry_delay(backoff, false);
            backoff = (delay * 2).min(SYNC_RETRY_MAX);
        }
        assert_eq!(backoff, SYNC_RETRY_MAX);
    }

    // ── lookup_translation_awaiting_pending / mark_pending ──────────────────
    // These cover the edit/redaction race: an edit or redaction can be
    // dispatched (as its own concurrent task) before the original message's
    // own translate-and-persist pipeline has written its DB row yet.

    fn test_pending() -> PendingMap {
        Arc::new(std::sync::Mutex::new(HashMap::new()))
    }

    #[tokio::test]
    async fn pending_guard_removes_entry_on_drop_and_wakes_a_waiter() {
        let pending = test_pending();
        let event_id = eid("$orig:example.org");
        assert!(!pending.lock().unwrap().contains_key(event_id.as_str()));
        {
            let _guard = mark_pending(&pending, &event_id);
            assert!(pending.lock().unwrap().contains_key(event_id.as_str()));
        }
        assert!(!pending.lock().unwrap().contains_key(event_id.as_str()));
    }

    #[tokio::test]
    async fn lookup_translation_awaiting_pending_hits_immediately_when_already_recorded() {
        let db = Db::open_in_memory().unwrap();
        let pending = test_pending();
        db.record_translation("$orig:example.org", "$bot:example.org", "!room:example.org")
            .await
            .unwrap();

        let started = Instant::now();
        let found = lookup_translation_awaiting_pending(&db, &pending, "$orig:example.org")
            .await
            .unwrap();
        assert_eq!(found, Some("$bot:example.org".to_owned()));
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn lookup_translation_awaiting_pending_returns_none_fast_with_no_pending_and_no_row() {
        // Case 6: a genuinely unmapped event (e.g. never translated, or old
        // enough to have been pruned) must not hang or crash.
        let db = Db::open_in_memory().unwrap();
        let pending = test_pending();

        let started = Instant::now();
        let found = lookup_translation_awaiting_pending(&db, &pending, "$never-seen:example.org")
            .await
            .unwrap();
        assert_eq!(found, None);
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn lookup_translation_awaiting_pending_waits_for_in_flight_translation_to_land() {
        // Simulates an edit/redaction racing in while the original message's
        // handler is still between "detect/translate" and "record_translation".
        let db = Db::open_in_memory().unwrap();
        let pending = test_pending();
        let original = "$orig:example.org";
        let guard = mark_pending(&pending, &eid(original));

        let db2 = db.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            db2.record_translation(original, "$bot:example.org", "!room:example.org")
                .await
                .unwrap();
            drop(guard); // wakes the waiter via notify_one
        });

        let started = Instant::now();
        let found = lookup_translation_awaiting_pending(&db, &pending, original)
            .await
            .unwrap();
        assert_eq!(found, Some("$bot:example.org".to_owned()));
        assert!(started.elapsed() >= Duration::from_millis(25));
        assert!(started.elapsed() < PENDING_TRANSLATION_WAIT);
    }

    #[tokio::test]
    async fn lookup_translation_awaiting_pending_returns_none_promptly_when_original_was_skipped() {
        // The original message finishes without ever calling
        // record_translation (e.g. below_confidence) — the waiter must be
        // woken immediately by the guard's drop, not sit out the full
        // PENDING_TRANSLATION_WAIT before concluding there's no mapping.
        let db = Db::open_in_memory().unwrap();
        let pending = test_pending();
        let original = "$orig:example.org";
        let guard = mark_pending(&pending, &eid(original));

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(guard);
        });

        let started = Instant::now();
        let found = lookup_translation_awaiting_pending(&db, &pending, original)
            .await
            .unwrap();
        assert_eq!(found, None);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn lookup_translation_awaiting_pending_unrelated_event_is_unaffected() {
        // Reply/thread relations (or any other event) must never be confused
        // with the event actually being awaited.
        let db = Db::open_in_memory().unwrap();
        let pending = test_pending();
        let _guard = mark_pending(&pending, &eid("$unrelated:example.org"));

        let started = Instant::now();
        let found = lookup_translation_awaiting_pending(&db, &pending, "$orig:example.org")
            .await
            .unwrap();
        assert_eq!(found, None);
        assert!(started.elapsed() < Duration::from_millis(500));
    }
}

/// Parse `markdown` and return the indices + full text of every `Event::Text`
/// node that sits outside a code block.  These are the nodes that should be
/// translated; all other events (code blocks, inline code, HTML, URLs) are
/// left untouched.
#[allow(dead_code)]
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

fn translation_rejection_reason(
    source_plain: &str,
    translated_plain: &str,
) -> Option<&'static str> {
    let source_chars = source_plain.chars().count().max(1);
    let translated_chars = translated_plain.chars().count();
    let max_relative = source_chars
        .saturating_mul(MAX_TRANSLATION_EXPANSION_FACTOR)
        .saturating_add(MAX_TRANSLATION_EXPANSION_SLACK);
    let max_allowed = max_relative.min(MAX_TRANSLATION_ABSOLUTE_CHARS);

    if translated_chars > max_allowed {
        return Some("translation expanded far beyond source length");
    }

    if contains_introduced_model_control_token(source_plain, translated_plain) {
        return Some("translation contains backend control token");
    }

    let lower = translated_plain.to_lowercase();
    if has_repeated_ngram_loop(&lower, 2, 4) || has_repeated_ngram_loop(&lower, 3, 3) {
        return Some("translation contains repeated phrase loop");
    }

    None
}

fn contains_introduced_model_control_token(source: &str, translated: &str) -> bool {
    let mut rest = translated;
    while let Some(start) = rest.find("<|") {
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find('>') else {
            break;
        };
        let token = &rest[start..start + 2 + end + 1];
        let body = &after_start[..end];
        let valid_control_shape = !body.is_empty()
            && body
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '|'));
        if valid_control_shape && !source.contains(token) {
            return true;
        }
        rest = &after_start[end + 1..];
    }

    false
}

fn has_repeated_ngram_loop(text: &str, ngram_len: usize, min_repeats: usize) -> bool {
    let words: Vec<&str> = text
        .split_whitespace()
        .map(|word| word.trim_matches(|c: char| !c.is_alphanumeric()))
        .filter(|word| !word.is_empty())
        .collect();

    if words.len() < ngram_len * min_repeats {
        return false;
    }

    for start in 0..=words.len() - (ngram_len * min_repeats) {
        let ngram = &words[start..start + ngram_len];
        let mut repeats = 1;
        while start + ((repeats + 1) * ngram_len) <= words.len() {
            let next_start = start + repeats * ngram_len;
            if words[next_start..next_start + ngram_len] != *ngram {
                break;
            }
            repeats += 1;
            if repeats >= min_repeats {
                return true;
            }
        }
    }

    false
}

/// Strip the `<mx-reply>…</mx-reply>` fallback block that Matrix clients prepend
/// to `formatted_body` when a message is a reply.  Returns the remaining HTML.
fn strip_mx_reply(html: &str) -> String {
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
fn strip_reply_fallback(raw: &str) -> String {
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

fn build_text_line(
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

fn build_html_line(
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

async fn translate_text_target_with_retry(
    state: &BotState,
    plain: &str,
    source: &str,
    target: &str,
    attempts_by_target: Arc<Mutex<HashMap<String, usize>>>,
) -> Result<TranslatedLine, TargetTranslationFailure> {
    let policy = state.retry_policy();
    retry_translation_operation(
        target,
        &policy,
        attempts_by_target,
        &state.stats,
        || async {
            let translated = state.translate(plain, source, target, "text").await?;
            build_text_line(target, plain, translated)
        },
    )
    .await
}

async fn translate_html_target_with_retry(
    state: &BotState,
    html: &str,
    plain: &str,
    source: &str,
    target: &str,
    attempts_by_target: Arc<Mutex<HashMap<String, usize>>>,
) -> Result<TranslatedLine, TargetTranslationFailure> {
    let policy = state.retry_policy();
    let source_plain = html_to_plain(html);
    let html_result = retry_translation_operation(
        target,
        &policy,
        attempts_by_target.clone(),
        &state.stats,
        || async {
            let translated_html = state.translate(html, source, target, "html").await?;
            build_html_line(target, &source_plain, translated_html)
        },
    )
    .await;

    match html_result {
        Ok(line) => Ok(line),
        Err(html_failure) => {
            warn!(
                "HTML translation target={} failed after {} attempts class={} detail={} — falling back to text",
                html_failure.target,
                html_failure.attempts,
                html_failure.error.class(),
                html_failure.error.detail
            );
            translate_text_target_with_retry(state, plain, source, target, attempts_by_target).await
        }
    }
}

/// Decide whether a detected message should be translated and, if so, into
/// which target languages. Every rejection path logs its specific reason —
/// without this, a message skipped here leaves no trace in the logs, making
/// it indistinguishable from a message that was never received at all.
fn resolve_translation_targets(
    context: &str,
    room_id: &str,
    event_id: &OwnedEventId,
    lang: &str,
    confidence: f64,
    translation: &EffectiveTranslationConfig,
) -> Result<Vec<String>, SkipReason> {
    if confidence < translation.min_confidence {
        info!(
            "Skipping {context} {event_id} in {room_id}: detected lang={lang} confidence={confidence:.2} \
             below min_confidence={:.2}",
            translation.min_confidence
        );
        return Err(SkipReason::BelowConfidence);
    }

    if !translation.langs.iter().any(|l| l == lang) {
        info!(
            "Skipping {context} {event_id} in {room_id}: detected lang={lang} not in configured \
             langs={:?}",
            translation.langs
        );
        return Err(SkipReason::LanguageNotConfigured);
    }

    let targets: Vec<String> = translation
        .langs
        .iter()
        .filter(|t| t.as_str() != lang)
        .cloned()
        .collect();
    if targets.is_empty() {
        info!(
            "Skipping {context} {event_id} in {room_id}: no target languages remain after excluding \
             source lang={lang} (configured langs={:?})",
            translation.langs
        );
        return Err(SkipReason::NoRemainingTargets);
    }

    Ok(targets)
}

async fn translate_all_targets(
    state: &BotState,
    input: TranslationInput<'_>,
    source: &str,
    target_langs: Vec<String>,
    context: &'static str,
    room_id: &str,
    event_id: &OwnedEventId,
) -> Result<Vec<TranslatedLine>, TranslationBatchFailure> {
    let policy = state.retry_policy();
    let attempts_by_target = Arc::new(Mutex::new(HashMap::new()));
    let futures = target_langs
        .iter()
        .map(|target| {
            let attempts_by_target = Arc::clone(&attempts_by_target);
            async move {
                match input {
                    TranslationInput::Text { plain } => {
                        translate_text_target_with_retry(
                            state,
                            plain,
                            source,
                            target,
                            attempts_by_target,
                        )
                        .await
                    }
                    TranslationInput::Html { html, plain } => {
                        translate_html_target_with_retry(
                            state,
                            html,
                            plain,
                            source,
                            target,
                            attempts_by_target,
                        )
                        .await
                    }
                }
            }
        })
        .collect::<Vec<_>>();

    let result = collect_all_or_nothing(
        source,
        &target_langs,
        context,
        policy.overall_timeout,
        attempts_by_target,
        futures,
    )
    .await;

    if let Err(ref failure) = result {
        log_suppressed_translation(state, failure, room_id, event_id);
    }

    result
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

/// Sends a translation with a few retries on transient failure (brief
/// homeserver hiccup, 429 rate limit, ...), mirroring the retry/backoff
/// policy already used for the translation backend itself. Without this, a
/// translation that succeeded but hit a transient send error was simply
/// lost — silently, with no retry — even though the expensive part (getting
/// the translation) had already succeeded. Uses the same generic backoff
/// helper the rest of the mxbot fleet uses for its join-room retry loop.
const MATRIX_SEND_MAX_ATTEMPTS: u32 = 4;
const MATRIX_SEND_INITIAL_BACKOFF: Duration = Duration::from_secs(2);
const MATRIX_SEND_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Classifies a Matrix send failure as retryable or permanent, and extracts
/// any server-suggested retry delay (`M_LIMIT_EXCEEDED`'s `retry_after`).
/// Mirrors `TranslationError::is_transient()`'s split for the backend: a
/// clearly permanent error (forbidden, not-in-room, malformed request, ...)
/// is not worth retrying for ~30s before failing anyway, and a 429 should
/// wait exactly as long as the server asked rather than guessing.
fn classify_send_error(error: &matrix_sdk::Error) -> (bool, Option<Duration>) {
    let matrix_sdk::Error::Http(http_error) = error else {
        // Non-HTTP errors (serialization, crypto/store errors, ...) aren't
        // known to be transient — don't retry them.
        return (false, None);
    };
    let Some(api_error) = http_error.as_client_api_error() else {
        // No parseable Matrix error body (e.g. a raw connection/timeout
        // error that never reached the server) — treat as transient.
        return (true, None);
    };
    let retry_after = api_error.error_kind().and_then(|kind| match kind {
        matrix_sdk::ruma::api::error::ErrorKind::LimitExceeded(data) => {
            data.retry_after.as_ref().and_then(|r| match r {
                matrix_sdk::ruma::api::error::RetryAfter::Delay(d) => Some(*d),
                matrix_sdk::ruma::api::error::RetryAfter::DateTime(t) => {
                    t.duration_since(SystemTime::now()).ok()
                }
            })
        }
        _ => None,
    });
    let status = api_error.status_code.as_u16();
    (status == 429 || status >= 500, retry_after)
}

/// Sends a translation with a few retries on transient failure (brief
/// homeserver hiccup, 429 rate limit, 5xx), mirroring the retry/backoff
/// policy already used for the translation backend itself. Without this, a
/// translation that succeeded but hit a transient send error was simply
/// lost — silently, with no retry — even though the expensive part (getting
/// the translation) had already succeeded. Bounded to
/// `MATRIX_SEND_MAX_ATTEMPTS`: a homeserver outage delays in-flight sends,
/// it does not queue an unbounded, ever-growing backlog of retries, since
/// each call is one bounded loop awaited directly by its caller (itself
/// already serialized behind the `inflight`/`lt_requests` semaphores).
///
/// All attempts reuse the *same* transaction ID (fixed once, before the
/// loop): the Matrix C-S API guarantees `PUT .../send/{eventType}/{txnId}` is
/// idempotent per-txn-id, so if an earlier attempt's request actually reached
/// the homeserver and was processed — just the *response* was lost to a
/// timeout or connection reset, which `classify_send_error` cannot tell apart
/// from a request that never arrived — a retry is answered with the original
/// event instead of creating a duplicate translation message.
async fn send_with_retry(
    room: &Room,
    event_id: &OwnedEventId,
    content: RoomMessageEventContent,
) -> Result<OwnedEventId, matrix_sdk::Error> {
    let mut backoff = MATRIX_SEND_INITIAL_BACKOFF;
    let txn_id = matrix_sdk::ruma::TransactionId::new();
    for attempt in 1..=MATRIX_SEND_MAX_ATTEMPTS {
        match room.send(content.clone()).with_transaction_id(txn_id.clone()).await {
            Ok(resp) => return Ok(resp.response.event_id),
            Err(e) => {
                let (retryable, retry_after) = classify_send_error(&e);
                if !retryable || attempt == MATRIX_SEND_MAX_ATTEMPTS {
                    return Err(e);
                }
                let delay = retry_after.unwrap_or(backoff).min(MATRIX_SEND_MAX_BACKOFF);
                warn!(
                    "Matrix send for {event_id} failed transiently \
                     attempt={attempt}/{MATRIX_SEND_MAX_ATTEMPTS}: {e} — retrying in {delay:?}"
                );
                sleep(delay).await;
                backoff = backoff.saturating_mul(2).min(MATRIX_SEND_MAX_BACKOFF);
            }
        }
    }
    unreachable!("loop above always returns on its final iteration")
}

/// How long an edit or redaction will wait for a same-event translation that
/// is still in flight (detection + translation + Matrix send can take a few
/// seconds, longer under retries) before giving up and treating the event as
/// having no mapping.
const PENDING_TRANSLATION_WAIT: Duration = Duration::from_secs(30);

/// RAII marker that `event_id`'s translate-and-persist pipeline is running.
/// A same-event edit or redaction that arrives before this finishes — a real
/// race, since edits/redactions can land within milliseconds of the original
/// — can wait on it via [`lookup_translation_awaiting_pending`] instead of
/// concluding there is no translation to update/delete. Dropped on every
/// exit path (success, skip, or early return) and wakes at most one waiter
/// via `Notify::notify_one`, which stores its permit if nobody is waiting
/// yet, so the wake can never be lost regardless of interleaving.
type PendingMap = Arc<std::sync::Mutex<HashMap<String, Arc<Notify>>>>;

struct PendingGuard {
    pending: PendingMap,
    key: String,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let notify = self.pending.lock().unwrap().remove(&self.key);
        if let Some(notify) = notify {
            notify.notify_one();
        }
    }
}

fn mark_pending(pending: &PendingMap, event_id: &OwnedEventId) -> PendingGuard {
    let key = event_id.as_str().to_owned();
    pending
        .lock()
        .unwrap()
        .insert(key.clone(), Arc::new(Notify::new()));
    PendingGuard {
        pending: pending.clone(),
        key,
    }
}

/// Looks up the bot's translation for `original_event_id`, waiting briefly
/// for an in-flight translation of the same event to finish first if one is
/// running. Without this, an edit or redaction that races ahead of the
/// original message's own translate-and-persist pipeline (both are dispatched
/// as independent concurrent tasks) would find no row yet and wrongly
/// conclude there is nothing to update/delete.
async fn lookup_translation_awaiting_pending(
    db: &Db,
    pending: &PendingMap,
    original_event_id: &str,
) -> Result<Option<String>> {
    // Snapshot the in-flight handle (if any) *before* re-checking the DB, so
    // a task that finishes between our first check and this one can't race
    // us out of seeing its notification — notify_one() stores its permit if
    // it fires before anyone is waiting yet.
    let notify = pending.lock().unwrap().get(original_event_id).cloned();

    if let Some(found) = db.lookup_translation(original_event_id).await? {
        return Ok(Some(found));
    }
    let Some(notify) = notify else {
        return Ok(None);
    };
    info!(
        "Edit/redaction for {original_event_id} arrived while its translation was still in \
         flight — waiting up to {PENDING_TRANSLATION_WAIT:?}"
    );
    let _ = timeout(PENDING_TRANSLATION_WAIT, notify.notified()).await;
    db.lookup_translation(original_event_id).await
}

/// Durably records that `original_event_id` was translated into
/// `bot_event_id`, so a later edit or redaction — even after a restart —
/// can find and update/remove the bot's translation. Awaited synchronously
/// (not fire-and-forget): this is the correctness-critical dedup path, so
/// the write must complete before the caller considers the event handled.
async fn record_translation(
    state: &BotState,
    original_event_id: &OwnedEventId,
    bot_event_id: &OwnedEventId,
    room_id: &str,
) {
    if let Err(e) = state
        .db
        .record_translation(original_event_id.as_str(), bot_event_id.as_str(), room_id)
        .await
    {
        warn!("Failed to persist translation for {original_event_id}: {e}");
    }
}

/// Structured "event skipped" log line plus stats/ring-buffer bookkeeping.
/// The single place that decides what a skip decision looks like, so every
/// skip path in the pipeline stays consistent instead of drifting into its
/// own ad-hoc log format.
fn record_skip(
    state: &BotState,
    event_id: &OwnedEventId,
    room_id: &str,
    sender: Option<&str>,
    msgtype: &str,
    text_source: &'static str,
    reason: SkipReason,
) {
    state.stats.record_skip(reason);
    state.stats.push_outcome(EventOutcome {
        event_id: event_id.clone(),
        room_id: room_id.to_owned(),
        sender: sender.map(str::to_owned),
        msgtype: msgtype.to_owned(),
        text_source,
        decision: "skip",
        reason: Some(reason.as_str()),
        source_lang: None,
        targets: None,
        matrix_send: None,
        duration_ms: None,
        at: SystemTime::now(),
    });
    info!(
        event = %event_id,
        room = room_id,
        sender = sender.unwrap_or("-"),
        msgtype = msgtype,
        text_source = text_source,
        decision = "skip",
        reason = reason.as_str(),
        "event skipped"
    );
}

/// Structured "event processed" log line plus stats/ring-buffer bookkeeping
/// for a completed translate-and-send attempt (`matrix_send_ok` distinguishes
/// a translation that was produced but failed to deliver).
#[allow(clippy::too_many_arguments)]
fn record_translated(
    state: &BotState,
    event_id: &OwnedEventId,
    room_id: &str,
    sender: &str,
    msgtype: &str,
    text_source: &'static str,
    source_lang: &str,
    targets: &[String],
    matrix_send_ok: bool,
    duration: Duration,
) {
    state.stats.translated.fetch_add(1, Ordering::Relaxed);
    let matrix_send = if matrix_send_ok { "ok" } else { "failed" };
    if !matrix_send_ok {
        state
            .stats
            .matrix_send_failures
            .fetch_add(1, Ordering::Relaxed);
    }
    let targets_joined = targets.join(",");
    state.stats.push_outcome(EventOutcome {
        event_id: event_id.clone(),
        room_id: room_id.to_owned(),
        sender: Some(sender.to_owned()),
        msgtype: msgtype.to_owned(),
        text_source,
        decision: "translate",
        reason: None,
        source_lang: Some(source_lang.to_owned()),
        targets: Some(targets_joined.clone()),
        matrix_send: Some(matrix_send),
        duration_ms: Some(duration.as_millis()),
        at: SystemTime::now(),
    });
    info!(
        event = %event_id,
        room = room_id,
        sender = sender,
        msgtype = msgtype,
        text_source = text_source,
        decision = "translate",
        source = source_lang,
        targets = %targets_joined,
        matrix_send = matrix_send,
        duration_ms = duration.as_millis() as u64,
        "event processed"
    );
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

/// Handles a caption attached to a media message (`m.image`, `m.video`,
/// `m.audio`, `m.file`). `kind` is the short label used in logs and as the
/// translation-target-resolution context (e.g. `"image"`, `"video"`).
async fn handle_media_caption(
    state: BotState,
    room: Room,
    event: OriginalSyncRoomMessageEvent,
    caption: String,
    kind: &'static str,
) {
    let started = Instant::now();
    let room_id = room.room_id().as_str().to_owned();
    let sender = event.sender.as_str().to_owned();

    let Some((lang, confidence)) = state.detect(&caption).await else {
        warn!(
            "Language detection failed for {kind} caption {} in {} ({})",
            event.event_id, room_id, event.sender
        );
        record_skip(
            &state,
            &event.event_id,
            &room_id,
            Some(&sender),
            kind,
            "caption",
            SkipReason::LangDetectFailed,
        );
        return;
    };

    info!(
        "{kind} caption {} lang={lang} conf={confidence:.2} sender={} room={}",
        event.event_id, event.sender, room_id
    );

    let translation = state.translation_for_room(&room_id);

    let targets = match resolve_translation_targets(
        kind,
        &room_id,
        &event.event_id,
        &lang,
        confidence,
        &translation,
    ) {
        Ok(targets) => targets,
        Err(reason) => {
            record_skip(
                &state,
                &event.event_id,
                &room_id,
                Some(&sender),
                kind,
                "caption",
                reason,
            );
            return;
        }
    };

    let lines = match translate_all_targets(
        &state,
        TranslationInput::Text { plain: &caption },
        &lang,
        targets.clone(),
        kind,
        &room_id,
        &event.event_id,
    )
    .await
    {
        Ok(lines) => lines,
        Err(_) => return,
    };
    let (plain_body, html_body) = build_translation_bodies(&lines);
    let mut content = make_translation_content(plain_body, html_body, translation.silent_messages);
    let in_thread = matches!(&event.content.relates_to, Some(Relation::Thread(_)));
    let thread_root = resolve_thread_root(&event);
    content.relates_to = make_relation(
        &event.event_id,
        &thread_root,
        translation.reply_to_original,
        translation.thread_replies,
        in_thread,
    );

    let send_result = send_with_retry(&room, &event.event_id, content).await;
    record_translated(
        &state,
        &event.event_id,
        &room_id,
        &sender,
        kind,
        "caption",
        &lang,
        &targets,
        send_result.is_ok(),
        started.elapsed(),
    );
    match send_result {
        Ok(bot_event_id) => {
            record_translation(&state, &event.event_id, &bot_event_id, &room_id).await
        }
        Err(e) => error!("Failed to send {kind} caption translation: {e}"),
    }
}

/// Returns `Some((kind, caption))` for a msgtype that has a caption slot
/// (`m.image`, `m.video`, `m.audio`, `m.file`) — `caption` is `None` when the
/// slot is present but unused (no `filename` set, or `body == filename`, per
/// the Matrix media-caption spec: https://spec.matrix.org/v1.18/client-server-api/#media-captions).
/// Returns `None` for msgtypes that have no caption slot at all (`m.text`,
/// `m.location`, `m.emote`, etc.) — those are handled elsewhere or ignored.
fn media_caption_kind(msgtype: &MessageType) -> Option<(&'static str, Option<&str>)> {
    match msgtype {
        MessageType::Image(m) => Some(("image", m.caption())),
        MessageType::Video(m) => Some(("video", m.caption())),
        MessageType::Audio(m) => Some(("audio", m.caption())),
        MessageType::File(m) => Some(("file", m.caption())),
        _ => None,
    }
}

fn translate_version_report() -> String {
    format!(
        "translate-bot v{} ({})",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH")
    )
}

async fn translate_status_report(state: &BotState) -> String {
    let ltengine = match state
        .http
        .get(format!("{}/languages", state.lt_url))
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => "reachable".to_owned(),
        Ok(resp) => format!("HTTP {}", resp.status()),
        Err(e) => format!("unreachable: {e}"),
    };
    let uptime = state
        .stats
        .started_at
        .elapsed()
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let t = &state.translation;
    format!(
        "{} uptime={uptime}s ltengine={ltengine} sync_reconnects={}\n\
         langs={:?} min_confidence={:.2} backend_concurrency={} \
         request_timeout_secs={} overall_timeout_secs={} max_attempts={}\n\
         events_seen={} translated={} decrypt_failures={} matrix_send_failures={}",
        translate_version_report(),
        state.stats.sync_reconnects.load(Ordering::Relaxed),
        t.langs,
        t.min_confidence,
        t.backend_concurrency,
        t.request_timeout_secs,
        t.overall_timeout_secs,
        t.max_attempts,
        state.stats.events_seen.load(Ordering::Relaxed),
        state.stats.translated.load(Ordering::Relaxed),
        state.stats.decrypt_failures.load(Ordering::Relaxed),
        state.stats.matrix_send_failures.load(Ordering::Relaxed),
    )
}

fn translate_stats_report(state: &BotState) -> String {
    let s = &state.stats;
    let skipped = s.skipped_snapshot();
    let skipped_str = if skipped.is_empty() {
        "none".to_owned()
    } else {
        skipped
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "events_seen={} translated={} decrypt_failures={} translation_api_failures={} \
         matrix_send_failures={} retries={} retries_succeeded={}\nskipped: {skipped_str}",
        s.events_seen.load(Ordering::Relaxed),
        s.translated.load(Ordering::Relaxed),
        s.decrypt_failures.load(Ordering::Relaxed),
        s.translation_api_failures.load(Ordering::Relaxed),
        s.matrix_send_failures.load(Ordering::Relaxed),
        s.retries.load(Ordering::Relaxed),
        s.retries_succeeded.load(Ordering::Relaxed),
    )
}

async fn translate_debug_report(state: &BotState, arg: &str) -> String {
    if arg.is_empty() {
        return "Usage: !translate debug <event-id>".to_owned();
    }
    match state.stats.find_outcome(arg).await {
        Some(o) => {
            let ago = (db::now_secs() - o.at).max(0);
            format!(
                "event={} room={} sender={} msgtype={} text_source={} decision={} reason={} \
                 source={} targets={} matrix_send={} duration_ms={} ({ago}s ago)",
                o.event_id,
                o.room_id,
                o.sender.as_deref().unwrap_or("-"),
                o.msgtype,
                o.text_source,
                o.decision,
                o.reason.as_deref().unwrap_or("-"),
                o.source_lang.as_deref().unwrap_or("-"),
                o.targets.as_deref().unwrap_or("-"),
                o.matrix_send.as_deref().unwrap_or("-"),
                o.duration_ms
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".to_owned()),
            )
        }
        None => format!(
            "No record for {arg} — either it wasn't processed by this pipeline, or it's \
             older than the retained history (in-memory: last {EVENT_OUTCOME_HISTORY} \
             events this process has seen; DB: last {} events across restarts).",
            db::EVENT_OUTCOMES_RETENTION
        ),
    }
}

/// Re-processes a specific event through the normal translate-and-send
/// pipeline. Refuses if `translation_map` already has an entry for it (the
/// invariant this exists to serve: retrying must never create a duplicate
/// translation — if one already exists, editing the original message is the
/// correct way to update it). Also picks up events that couldn't be
/// decrypted at sync time but can be decrypted now, since `Room::event`
/// re-attempts decryption with whatever keys are currently available.
async fn translate_retry_command(state: &BotState, room: &Room, arg: &str) -> String {
    if arg.is_empty() {
        return "Usage: !translate retry <event-id>".to_owned();
    }
    let event_id = match matrix_sdk::ruma::EventId::parse(arg) {
        Ok(id) => id,
        Err(e) => return format!("'{arg}' is not a valid event ID: {e}"),
    };

    let already_translated = match state.db.lookup_translation(event_id.as_str()).await {
        Ok(found) => found,
        Err(e) => return format!("Could not check existing translations for {event_id}: {e}"),
    };
    if let Some(bot_event_id) = already_translated {
        record_skip(
            state,
            &event_id,
            room.room_id().as_str(),
            None,
            "n/a",
            "n/a",
            SkipReason::AlreadyTranslated,
        );
        return format!(
            "{event_id} was already translated as {bot_event_id} — edit the original \
             message if you want to force a re-translation."
        );
    }

    let timeline_event = match room.event(&event_id, None).await {
        Ok(ev) => ev,
        Err(e) => return format!("Could not fetch {event_id}: {e}"),
    };
    if timeline_event.kind.is_utd() {
        return format!("{event_id} is still not decryptable — no key available for it.");
    }
    let parsed: OriginalSyncRoomMessageEvent = match timeline_event
        .kind
        .raw()
        .cast_ref_unchecked::<OriginalSyncRoomMessageEvent>()
        .deserialize()
    {
        Ok(ev) => ev,
        Err(e) => return format!("{event_id} is not a room message: {e}"),
    };

    // Boxed: handle_message_core can (transitively, via !translate retry)
    // call back into itself, and an async fn can't recurse without indirection.
    Box::pin(handle_message_core(state.clone(), room.clone(), parsed)).await;
    format!("Retried {event_id} — run `!translate debug {event_id}` for the outcome.")
}

async fn handle_message(state: BotState, room: Room, event: OriginalSyncRoomMessageEvent) {
    // Belt-and-suspenders: skip any event that predates the cutoff captured
    // just before handlers were registered (see `main`). The primary defence
    // is sync_once running before handlers are registered at all, which
    // prevents backlog events from reaching this function in the first
    // place; this check only catches whatever slips through that.
    //
    // This check lives here rather than in handle_message_core so that
    // `!translate retry` — which necessarily reprocesses an old event — can
    // call handle_message_core directly and bypass it.
    if let Some(event_time) = event.origin_server_ts.to_system_time() {
        if event_time < state.startup_time {
            info!(
                "Skipping backlog message {} from {} (pre-startup)",
                event.event_id, event.sender
            );
            record_skip(
                &state,
                &event.event_id,
                room.room_id().as_str(),
                Some(event.sender.as_str()),
                event.content.msgtype.msgtype(),
                "n/a",
                SkipReason::Backlog,
            );
            return;
        }
    }

    handle_message_core(state, room, event).await;
}

async fn handle_message_core(state: BotState, room: Room, event: OriginalSyncRoomMessageEvent) {
    let started = Instant::now();
    let room_id = room.room_id().as_str().to_owned();
    let sender = event.sender.as_str().to_owned();

    // Edits arrive as m.replace — route them to handle_edit and stop.
    // Must be checked BEFORE the msgtype guard: the top-level body of an edit
    // is only a fallback ("* new text") for old clients and must NOT be translated.
    if let Some(Relation::Replacement(ref replacement)) = event.content.relates_to {
        let original_event_id = replacement.event_id.clone();
        let new_content = replacement.new_content.clone();
        handle_edit(state, room, original_event_id, new_content).await;
        return;
    }

    // Idempotency guard: if this event_id was already translated, don't do
    // it again. Normally unreachable (each event is only ever dispatched
    // once by matrix-sdk), but it's a cheap, indexed lookup that closes off
    // a real failure mode — a process restart between sending the
    // translation and the SDK's since-token being persisted, or `!translate
    // retry` racing a live delivery of the same event — from ever producing
    // a second translation of the same message.
    match state.db.lookup_translation(event.event_id.as_str()).await {
        Ok(Some(existing)) => {
            info!(
                "{} was already translated as {existing} — skipping duplicate delivery",
                event.event_id
            );
            record_skip(
                &state,
                &event.event_id,
                &room_id,
                Some(&sender),
                event.content.msgtype.msgtype(),
                "n/a",
                SkipReason::AlreadyTranslated,
            );
            return;
        }
        Ok(None) => {}
        Err(e) => warn!("Failed to check existing translation for {}: {e}", event.event_id),
    }

    // From here on this event might end up translated — mark it "pending" so
    // a same-event edit/redaction that races in before we're done (or before
    // we decide to skip) waits for us instead of concluding there's no
    // mapping. Held for the rest of this function, across every await below.
    let _pending_guard = mark_pending(&state.pending_translations, &event.event_id);

    // Media messages can carry a user-written caption in `body`. Per the
    // Matrix media-caption spec, `caption()` only returns it when `body`
    // differs from `filename` — a bare filename (e.g. "1000006546.jpg") is
    // never mistaken for a caption.
    if let Some((kind, caption)) = media_caption_kind(&event.content.msgtype) {
        let caption = caption
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        match caption {
            Some(caption) if caption.chars().count() > state.translation.max_input_chars => {
                record_skip(
                    &state,
                    &event.event_id,
                    &room_id,
                    Some(&sender),
                    kind,
                    "caption",
                    SkipReason::InputTooLong,
                )
            }
            Some(caption) => {
                info!("Translating caption from m.{kind} event {}", event.event_id);
                handle_media_caption(state, room, event, caption, kind).await;
            }
            None => record_skip(
                &state,
                &event.event_id,
                &room_id,
                Some(&sender),
                kind,
                "caption",
                SkipReason::NoCaption,
            ),
        }
        return;
    }

    // m.text and m.emote carry the same body/formatted shape and are both
    // genuine user-written text; other msgtypes (m.notice, m.location, ...)
    // are not translated. m.notice in particular is left alone deliberately:
    // it's the shape bots/servers use for their own chatter (including this
    // bot's own translations), so translating it risks feedback loops with
    // other notice-emitting bots in shared rooms.
    let (raw, formatted) = match &event.content.msgtype {
        MessageType::Text(t) => (t.body.as_str(), t.formatted.as_ref()),
        MessageType::Emote(e) => (e.body.as_str(), e.formatted.as_ref()),
        other => {
            record_skip(
                &state,
                &event.event_id,
                &room_id,
                Some(&sender),
                other.msgtype(),
                "body",
                SkipReason::UnsupportedMsgtype,
            );
            return;
        }
    };
    let msgtype_label = event.content.msgtype.msgtype();
    let is_command_eligible = matches!(event.content.msgtype, MessageType::Text(_));
    let raw = raw.trim();

    // Admin commands only apply to genuine m.text — not m.emote.
    if is_command_eligible {
        // Admin command: initiate a to-device verification without requiring the
        // target user to have permission to send room messages.
        if let Some(arguments) = raw.strip_prefix("!verify-device ") {
            if !state.admin_users.contains(&event.sender) {
                warn!("!verify-device from non-admin {} — ignored", event.sender);
                return;
            }

            let (target_user, target_device) = match parse_verify_device_arguments(arguments) {
                Ok(target) => target,
                Err(error) => {
                    warn!("!verify-device: {error}");
                    return;
                }
            };
            state
                .verification
                .grant_device(target_user.clone(), target_device.clone())
                .await;
            match state
                .verification
                .request_device_verification(&target_user, &target_device)
                .await
            {
                Ok(()) => info!(
                    "Started administrator-approved to-device verification for {} {}",
                    target_user, target_device
                ),
                Err(error) => warn!(
                    "Could not start to-device verification for {} {}: {}",
                    target_user, target_device, error
                ),
            }
            return;
        }

        // Admin command: !reset-trust @user:server
        if let Some(target) = raw.strip_prefix("!reset-trust ") {
            if state.admin_users.contains(&event.sender) {
                match target.trim().parse::<OwnedUserId>() {
                    Ok(target_user) => {
                        state.verification.grant_user(target_user.clone()).await;
                        info!(
                            "Trust reset allowed for {} (by {})",
                            target_user, event.sender
                        );
                    }
                    Err(_) => warn!("!reset-trust: invalid user ID '{}'", target.trim()),
                }
            } else {
                warn!("!reset-trust from non-admin {} — ignored", event.sender);
            }
            return;
        }

        // Admin command family: !translate status | stats | debug <event-id>
        // | retry <event-id> | version
        if raw == "!translate" || raw.starts_with("!translate ") {
            if !state.admin_users.contains(&event.sender) {
                warn!("!translate from non-admin {} — ignored", event.sender);
                return;
            }
            let rest = raw["!translate".len()..].trim();
            let (subcommand, arg) = rest
                .split_once(char::is_whitespace)
                .map(|(a, b)| (a, b.trim()))
                .unwrap_or((rest, ""));
            let reply = match subcommand {
                "status" => translate_status_report(&state).await,
                "stats" => translate_stats_report(&state),
                "debug" => translate_debug_report(&state, arg).await,
                "version" => translate_version_report(),
                "retry" => translate_retry_command(&state, &room, arg).await,
                _ => "Usage: !translate <status|stats|debug <event-id>|retry <event-id>|version>"
                    .to_owned(),
            };
            if let Err(e) = room
                .send(RoomMessageEventContent::notice_plain(reply))
                .await
            {
                warn!("Failed to send !translate reply: {e}");
            }
            return;
        }
    }

    let text = strip_reply_fallback(raw);
    let text = text.trim();
    if text.is_empty() {
        record_skip(
            &state,
            &event.event_id,
            &room_id,
            Some(&sender),
            msgtype_label,
            "body",
            SkipReason::EmptyText,
        );
        return;
    }
    if text.chars().count() > state.translation.max_input_chars {
        record_skip(
            &state,
            &event.event_id,
            &room_id,
            Some(&sender),
            msgtype_label,
            "body",
            SkipReason::InputTooLong,
        );
        return;
    }

    let Some((lang, confidence)) = state.detect(text).await else {
        warn!(
            "Language detection failed for {} in {} ({})",
            event.event_id, room_id, event.sender
        );
        record_skip(
            &state,
            &event.event_id,
            &room_id,
            Some(&sender),
            msgtype_label,
            "body",
            SkipReason::LangDetectFailed,
        );
        return;
    };

    info!(
        "{} lang={lang} conf={confidence:.2} sender={} room={}",
        event.event_id, event.sender, room_id
    );

    let translation = state.translation_for_room(&room_id);

    let targets = match resolve_translation_targets(
        "message",
        &room_id,
        &event.event_id,
        &lang,
        confidence,
        &translation,
    ) {
        Ok(targets) => targets,
        Err(reason) => {
            record_skip(
                &state,
                &event.event_id,
                &room_id,
                Some(&sender),
                msgtype_label,
                "body",
                reason,
            );
            return;
        }
    };

    let html_to_translate = match formatted {
        Some(fb) if fb.format == MessageFormat::Html => Some(strip_mx_reply(&fb.body)),
        _ => None,
    };
    let input = match html_to_translate.as_deref() {
        Some(html) => TranslationInput::Html { html, plain: text },
        None => TranslationInput::Text { plain: text },
    };
    let lines = match translate_all_targets(
        &state,
        input,
        &lang,
        targets.clone(),
        "message",
        &room_id,
        &event.event_id,
    )
    .await
    {
        Ok(lines) => lines,
        Err(_) => return,
    };
    let (plain_body, html_body) = build_translation_bodies(&lines);
    let mut content = make_translation_content(plain_body, html_body, translation.silent_messages);
    let in_thread = matches!(&event.content.relates_to, Some(Relation::Thread(_)));
    let thread_root = resolve_thread_root(&event);
    if translation.reply_to_original && (translation.thread_replies || in_thread) {
        info!("thread_root={} for event={}", thread_root, event.event_id);
    }
    content.relates_to = make_relation(
        &event.event_id,
        &thread_root,
        translation.reply_to_original,
        translation.thread_replies,
        in_thread,
    );

    let send_result = send_with_retry(&room, &event.event_id, content).await;
    record_translated(
        &state,
        &event.event_id,
        &room_id,
        &sender,
        msgtype_label,
        "body",
        &lang,
        &targets,
        send_result.is_ok(),
        started.elapsed(),
    );
    match send_result {
        Ok(bot_event_id) => {
            record_translation(&state, &event.event_id, &bot_event_id, &room_id).await
        }
        Err(e) => error!("Failed to send translation: {e}"),
    }
}

/// Called when a redaction event is received. If `redacted_id` is an original
/// message the bot translated, redacts the bot's translation too and removes
/// the now-stale mapping (so a later redaction of the same event, e.g. a
/// federation replay, is a no-op rather than a repeat redact attempt).
async fn handle_redaction(state: BotState, room: Room, redacted_id: OwnedEventId) {
    let bot_event_id = match lookup_translation_awaiting_pending(
        &state.db,
        &state.pending_translations,
        redacted_id.as_str(),
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            warn!("Failed to look up translation for redacted {redacted_id}: {e}");
            return;
        }
    };

    let Some(bot_event_id) = bot_event_id else {
        return;
    };
    let Ok(bot_event_id) = matrix_sdk::ruma::EventId::parse(&bot_event_id) else {
        warn!("Stored bot_event_id '{bot_event_id}' for {redacted_id} is not a valid event ID");
        return;
    };

    info!(
        "Original message {redacted_id} was redacted — \
         redacting bot translation {bot_event_id}"
    );

    if let Err(e) = room.redact(&bot_event_id, None, None).await {
        warn!("Failed to redact translation {bot_event_id}: {e}");
        return;
    }

    // Remove from DB so we don't attempt a double-redact.
    if let Err(e) = state.db.remove_translation(redacted_id.as_str()).await {
        warn!("Failed to remove translation record for {redacted_id}: {e}");
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

/// Extracts the translatable text from an edit's new content: the body
/// (reply-fallback stripped) for `m.text`/`m.emote`, or the caption for
/// caption-bearing media types (a caption can itself be edited — the new
/// content then carries the same media msgtype as the original). Returns
/// `None` when there is nothing translatable (unsupported msgtype, a
/// filename-only caption, or empty text).
fn extract_edit_text(msgtype: &MessageType) -> Option<String> {
    let raw = match msgtype {
        MessageType::Text(t) => t.body.as_str(),
        MessageType::Emote(e) => e.body.as_str(),
        other => {
            let (_, caption) = media_caption_kind(other)?;
            return caption
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
        }
    };
    let text = strip_reply_fallback(raw.trim());
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_owned())
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
    let started = Instant::now();
    let room_id = room.room_id().as_str().to_owned();
    let msgtype_label = new_content.msgtype.msgtype();

    // Look up whether we have a translation for this event — waiting briefly
    // if the original message's own translation is still in flight (see
    // `lookup_translation_awaiting_pending`).
    let bot_event_id = match lookup_translation_awaiting_pending(
        &state.db,
        &state.pending_translations,
        original_event_id.as_str(),
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            warn!("Failed to look up translation for edit of {original_event_id}: {e}");
            return;
        }
    };
    let bot_event_id = bot_event_id.and_then(|id| matrix_sdk::ruma::EventId::parse(id).ok());

    let Some(bot_event_id) = bot_event_id else {
        info!("Edit for unknown event {original_event_id} — no cached translation, ignoring");
        record_skip(
            &state,
            &original_event_id,
            &room_id,
            None,
            msgtype_label,
            "edit",
            SkipReason::EditUnknownEvent,
        );
        return;
    };

    // Use ONLY m.new_content as the source of truth (full replacement, not a diff).
    let Some(text) = extract_edit_text(&new_content.msgtype) else {
        info!(
            "Skipping edit of {original_event_id}: no translatable text in new content (msgtype={})",
            msgtype_label
        );
        record_skip(
            &state,
            &original_event_id,
            &room_id,
            None,
            msgtype_label,
            "edit",
            SkipReason::EditNoTranslatableText,
        );
        return;
    };
    if text.chars().count() > state.translation.max_input_chars {
        info!(
            "Skipping edit of {original_event_id}: input is {} chars, exceeds max_input_chars={}",
            text.chars().count(),
            state.translation.max_input_chars
        );
        record_skip(
            &state,
            &original_event_id,
            &room_id,
            None,
            msgtype_label,
            "edit",
            SkipReason::InputTooLong,
        );
        return;
    }
    let text = text.as_str();

    let Some((lang, confidence)) = state.detect(text).await else {
        warn!("Language detection failed for edit of {original_event_id} in {room_id}");
        record_skip(
            &state,
            &original_event_id,
            &room_id,
            None,
            msgtype_label,
            "edit",
            SkipReason::LangDetectFailed,
        );
        return;
    };

    let translation = state.translation_for_room(&room_id);

    let targets = match resolve_translation_targets(
        "edit",
        &room_id,
        &original_event_id,
        &lang,
        confidence,
        &translation,
    ) {
        Ok(targets) => targets,
        Err(reason) => {
            record_skip(
                &state,
                &original_event_id,
                &room_id,
                None,
                msgtype_label,
                "edit",
                reason,
            );
            return;
        }
    };

    let lines = match translate_all_targets(
        &state,
        TranslationInput::Text { plain: text },
        &lang,
        targets.clone(),
        "edit",
        &room_id,
        &original_event_id,
    )
    .await
    {
        Ok(lines) => lines,
        Err(_) => return,
    };
    let (new_body, new_html) = build_translation_bodies(&lines);

    // Build m.replace pointing at the bot's existing translation event.
    // The thread membership is inherited from bot_event_id — no thread relation needed here.
    // Preserve the same msgtype (m.notice vs m.text) as the original translation.
    let inner_msgtype = if translation.silent_messages {
        MessageType::Notice(NoticeMessageEventContent::html(
            new_body.clone(),
            new_html.clone(),
        ))
    } else {
        MessageType::Text(TextMessageEventContent::html(
            new_body.clone(),
            new_html.clone(),
        ))
    };
    let new_without = RoomMessageEventContentWithoutRelation::new(inner_msgtype);
    let mut edit_content = make_translation_content(
        format!("* {new_body}"),
        new_html,
        translation.silent_messages,
    );
    edit_content.relates_to = Some(Relation::Replacement(Replacement::new(
        bot_event_id.clone(),
        new_without,
    )));

    info!("Editing bot translation {bot_event_id} for edit of {original_event_id}");
    let send_result = send_with_retry(&room, &original_event_id, edit_content).await;
    record_translated(
        &state,
        &original_event_id,
        &room_id,
        "-",
        msgtype_label,
        "edit",
        &lang,
        &targets,
        send_result.is_ok(),
        started.elapsed(),
    );
    if let Err(e) = send_result {
        error!("Failed to send translation edit: {e}");
    }
}
