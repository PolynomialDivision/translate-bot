//! Counters, per-event outcome history and skip/translate bookkeeping.

use super::*;

/// Why an event was not translated. The `&'static str` form is what actually
/// appears in logs/stats/diagnostics — kept as an enum so call sites can't
/// typo a reason string that then silently fails to aggregate correctly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SkipReason {
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
    pub(crate) fn as_str(self) -> &'static str {
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
pub(crate) struct EventOutcome {
    pub(crate) event_id: OwnedEventId,
    pub(crate) room_id: String,
    pub(crate) sender: Option<String>,
    pub(crate) msgtype: String,
    pub(crate) text_source: &'static str,
    pub(crate) decision: &'static str, // "translate" | "skip" | "error"
    pub(crate) reason: Option<&'static str>,
    pub(crate) source_lang: Option<String>,
    pub(crate) targets: Option<String>,
    pub(crate) matrix_send: Option<&'static str>, // "ok" | "failed"
    pub(crate) duration_ms: Option<u128>,
    pub(crate) at: SystemTime,
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

pub(crate) const EVENT_OUTCOME_HISTORY: usize = 300;

/// Process-lifetime counters and recent-event history. Reset on restart —
/// this is in-memory diagnostics for "what's happening right now", not a
/// durable audit log (that's what stdout/docker logs are for).
pub(crate) struct Stats {
    pub(crate) started_at: SystemTime,
    pub(crate) events_seen: AtomicU64,
    pub(crate) translated: AtomicU64,
    pub(crate) decrypt_failures: AtomicU64,
    pub(crate) translation_api_failures: AtomicU64,
    pub(crate) matrix_send_failures: AtomicU64,
    pub(crate) retries: AtomicU64,
    pub(crate) retries_succeeded: AtomicU64,
    pub(crate) sync_reconnects: AtomicU64,
    pub(crate) skipped: RwLock<HashMap<&'static str, u64>>,
    pub(crate) recent: RwLock<VecDeque<EventOutcome>>,
    pub(crate) db: Db,
}

impl Stats {
    pub(crate) fn new(db: Db) -> Self {
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

    pub(crate) fn record_skip(&self, reason: SkipReason) {
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
    pub(crate) fn push_outcome(&self, outcome: EventOutcome) {
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

    pub(crate) fn find_outcome_in_memory(&self, event_id: &str) -> Option<EventOutcome> {
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
    pub(crate) async fn find_outcome(&self, event_id: &str) -> Option<DbEventOutcome> {
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

    pub(crate) fn skipped_snapshot(&self) -> Vec<(&'static str, u64)> {
        let mut v: Vec<_> = self
            .skipped
            .read()
            .map(|map| map.iter().map(|(&k, &v)| (k, v)).collect())
            .unwrap_or_default();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        v
    }
}

/// Durably records that `original_event_id` was translated into
/// `bot_event_id`, so a later edit or redaction — even after a restart —
/// can find and update/remove the bot's translation. Awaited synchronously
/// (not fire-and-forget): this is the correctness-critical dedup path, so
/// the write must complete before the caller considers the event handled.
pub(crate) async fn record_translation(
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
pub(crate) fn record_skip(
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
    debug!(sender = sender.unwrap_or("-"), event = %event_id, "event sender");
    info!(
        event = %event_id,
        room = room_id,
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
pub(crate) fn record_translated(
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
    debug!(sender = sender, event = %event_id, "event sender");
    info!(
        event = %event_id,
        room = room_id,
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
