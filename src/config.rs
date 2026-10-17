//! Configuration: `[libretranslate]`, `[translation]` and per-room overrides.

use super::*;

/// Upper bound for any translation, independent of the source. Must leave
/// room for legitimate translations of the longest accepted input
/// (`max_input_chars`), which can be longer than their source; runaway
/// output is caught by the relative limit and the loop checks below.
pub(crate) const MAX_TRANSLATION_ABSOLUTE_CHARS: usize = 3 * DEFAULT_MAX_INPUT_CHARS;
pub(crate) const MAX_TRANSLATION_EXPANSION_FACTOR: usize = 6;
pub(crate) const MAX_TRANSLATION_EXPANSION_SLACK: usize = 80;

#[derive(Deserialize)]
pub(crate) struct Config {
    pub(crate) matrix: MatrixConfig,
    pub(crate) libretranslate: LibreTranslateConfig,
    #[serde(default)]
    pub(crate) translation: TranslationConfig,
    #[serde(default)]
    pub(crate) room_translations: HashMap<String, RoomTranslationConfig>,
    #[serde(default)]
    pub(crate) security: SecurityConfig,
}

#[derive(Deserialize)]
pub(crate) struct LibreTranslateConfig {
    pub(crate) url: String,
    pub(crate) api_key: Option<String>,
}

#[derive(Clone, Deserialize)]
pub(crate) struct TranslationConfig {
    #[serde(default = "default_langs")]
    pub(crate) langs: Vec<String>,
    #[serde(default = "default_min_confidence")]
    pub(crate) min_confidence: f64,
    /// Whether translations reference the original message at all (default: true).
    /// When false, translations are posted as standalone messages regardless of thread_replies.
    #[serde(default = "default_true")]
    pub(crate) reply_to_original: bool,
    /// When reply_to_original = true: use m.thread instead of m.in_reply_to (default: true).
    /// When reply_to_original = false: has no effect.
    #[serde(default = "default_true")]
    pub(crate) thread_replies: bool,
    /// Send translations as `m.notice` (non-notifying) instead of `m.text` (default: false).
    /// Most Matrix clients render notices with muted styling and suppress push notifications.
    #[serde(default)]
    pub(crate) silent_messages: bool,
    /// Maximum concurrent requests to the translation backend.
    #[serde(default = "default_backend_concurrency")]
    pub(crate) backend_concurrency: usize,
    /// Timeout for a single backend translation request.
    #[serde(default = "default_translation_request_timeout_secs")]
    pub(crate) request_timeout_secs: u64,
    /// Maximum attempts for a single target language.
    #[serde(default = "default_translation_max_attempts")]
    pub(crate) max_attempts: usize,
    /// Initial retry backoff in milliseconds. Backoff doubles after each retry.
    #[serde(default = "default_translation_retry_initial_backoff_ms")]
    pub(crate) retry_initial_backoff_ms: u64,
    /// Overall deadline for translating one Matrix event into all targets.
    #[serde(default = "default_translation_overall_timeout_secs")]
    pub(crate) overall_timeout_secs: u64,
    /// Maximum input length (message body or caption, in characters)
    /// submitted to the translation backend. The backend is a single,
    /// GPU-constrained, largely CPU-bound LLM shared across all rooms via a
    /// small concurrency semaphore (`backend_concurrency`): one very long
    /// message would otherwise hold a slot for a disproportionate amount of
    /// time and delay everyone else's translations. Ordinary chat messages
    /// are nowhere near the default.
    #[serde(default = "default_max_input_chars")]
    pub(crate) max_input_chars: usize,
    /// Senders whose messages are never translated (exact Matrix IDs, e.g.
    /// other bots that post in several languages already). Their edits and
    /// captions are skipped too; a room's `ignore_users` adds to these.
    #[serde(default)]
    pub(crate) ignore_users: Vec<String>,
}

#[derive(Clone, Default, Deserialize)]
pub(crate) struct RoomTranslationConfig {
    pub(crate) langs: Option<Vec<String>>,
    pub(crate) min_confidence: Option<f64>,
    pub(crate) reply_to_original: Option<bool>,
    pub(crate) thread_replies: Option<bool>,
    pub(crate) silent_messages: Option<bool>,
    /// Added to `[translation] ignore_users` in this room.
    #[serde(default)]
    pub(crate) ignore_users: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct EffectiveTranslationConfig {
    pub(crate) langs: Vec<String>,
    pub(crate) min_confidence: f64,
    pub(crate) reply_to_original: bool,
    pub(crate) thread_replies: bool,
    pub(crate) silent_messages: bool,
}

pub(crate) fn default_langs() -> Vec<String> {
    vec!["en".to_owned(), "de".to_owned()]
}

pub(crate) fn default_min_confidence() -> f64 {
    0.5
}

pub(crate) fn default_true() -> bool {
    true
}

pub(crate) fn default_backend_concurrency() -> usize {
    4
}

pub(crate) fn default_translation_request_timeout_secs() -> u64 {
    60
}

pub(crate) fn default_translation_max_attempts() -> usize {
    3
}

pub(crate) fn default_translation_retry_initial_backoff_ms() -> u64 {
    250
}

pub(crate) fn default_translation_overall_timeout_secs() -> u64 {
    90
}

pub(crate) const DEFAULT_MAX_INPUT_CHARS: usize = 4_000;

pub(crate) fn default_max_input_chars() -> usize {
    DEFAULT_MAX_INPUT_CHARS
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
            ignore_users: Vec::new(),
        }
    }
}

/// Whether `sender`'s messages in `room_id` are never translated.
pub(crate) fn is_ignored_sender(
    default: &TranslationConfig,
    room_translations: &HashMap<String, RoomTranslationConfig>,
    room_id: &str,
    sender: &str,
) -> bool {
    default.ignore_users.iter().any(|u| u == sender)
        || room_translations
            .get(room_id)
            .is_some_and(|room| room.ignore_users.iter().any(|u| u == sender))
}

/// Every `ignore_users` entry must be a Matrix user ID: a typo would
/// otherwise silently ignore nobody.
pub(crate) fn check_ignore_users(
    default: &TranslationConfig,
    room_translations: &HashMap<String, RoomTranslationConfig>,
) -> Result<()> {
    let rooms = room_translations
        .iter()
        .map(|(room, cfg)| (format!("[room_translations.\"{room}\"]"), &cfg.ignore_users));
    for (section, users) in
        std::iter::once(("[translation]".to_owned(), &default.ignore_users)).chain(rooms)
    {
        for user in users {
            if UserId::parse(user.as_str()).is_err() {
                anyhow::bail!(
                    "{section} ignore_users: {user:?} is not a Matrix user ID (@name:server)"
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn effective_translation_config(
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
