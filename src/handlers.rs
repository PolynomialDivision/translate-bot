//! Matrix event handling: new messages, captions, edits and redactions.

use super::*;

/// How long an edit or redaction will wait for a same-event translation that
/// is still in flight (detection + translation + Matrix send can take a few
/// seconds, longer under retries) before giving up and treating the event as
/// having no mapping.
pub(crate) const PENDING_TRANSLATION_WAIT: Duration = Duration::from_secs(30);

/// RAII marker that `event_id`'s translate-and-persist pipeline is running.
/// A same-event edit or redaction that arrives before this finishes (a real
/// race, since edits/redactions can land within milliseconds of the original)
/// can wait on it via [`lookup_translation_awaiting_pending`] instead of
/// concluding there is no translation to update/delete. Dropped on every
/// exit path (success, skip, or early return) and wakes at most one waiter
/// via `Notify::notify_one`, which stores its permit if nobody is waiting
/// yet, so the wake can never be lost regardless of interleaving.
pub(crate) type PendingMap = Arc<std::sync::Mutex<HashMap<String, Arc<Notify>>>>;

pub(crate) struct PendingGuard {
    pub(crate) pending: PendingMap,
    pub(crate) key: String,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let notify = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
        if let Some(notify) = notify {
            notify.notify_one();
        }
    }
}

pub(crate) fn mark_pending(pending: &PendingMap, event_id: &OwnedEventId) -> PendingGuard {
    let key = event_id.as_str().to_owned();
    pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
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
pub(crate) async fn lookup_translation_awaiting_pending(
    db: &Db,
    pending: &PendingMap,
    original_event_id: &str,
) -> Result<Option<String>> {
    // Snapshot the in-flight handle (if any) *before* re-checking the DB, so
    // a task that finishes between our first check and this one can't race
    // us out of seeing its notification: notify_one() stores its permit if
    // it fires before anyone is waiting yet.
    let notify = pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(original_event_id)
        .cloned();

    if let Some(found) = db.lookup_translation(original_event_id).await? {
        return Ok(Some(found));
    }
    let Some(notify) = notify else {
        return Ok(None);
    };
    info!(
        "Edit/redaction for {original_event_id} arrived while its translation was still in \
         flight: waiting up to {PENDING_TRANSLATION_WAIT:?}"
    );
    let _ = timeout(PENDING_TRANSLATION_WAIT, notify.notified()).await;
    db.lookup_translation(original_event_id).await
}

/// How handling one event ended, for the late retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CoreOutcome {
    Translated,
    Skipped,
    /// `retry_later`: every target failed transiently or by the overall deadline.
    Failed {
        retry_later: bool,
    },
}

/// Returns `Some((kind, caption))` for a msgtype that has a caption slot
/// (`m.image`, `m.video`, `m.audio`, `m.file`): `caption` is `None` when the
/// slot is present but unused (no `filename` set, or `body == filename`, per
/// the Matrix media-caption spec: https://spec.matrix.org/v1.18/client-server-api/#media-captions).
/// Returns `None` for msgtypes that have no caption slot at all (`m.text`,
/// `m.location`, `m.emote`, etc.). Those are handled elsewhere or ignored.
pub(crate) fn media_caption_kind(msgtype: &MessageType) -> Option<(&'static str, Option<&str>)> {
    match msgtype {
        MessageType::Image(m) => Some(("image", m.caption())),
        MessageType::Video(m) => Some(("video", m.caption())),
        MessageType::Audio(m) => Some(("audio", m.caption())),
        MessageType::File(m) => Some(("file", m.caption())),
        _ => None,
    }
}

fn formatted_caption(msgtype: &MessageType) -> Option<&FormattedBody> {
    match msgtype {
        MessageType::Image(m) => m.formatted_caption(),
        MessageType::Video(m) => m.formatted_caption(),
        MessageType::Audio(m) => m.formatted_caption(),
        MessageType::File(m) => m.formatted_caption(),
        _ => None,
    }
}

fn html_body(formatted: Option<&FormattedBody>) -> Option<String> {
    formatted
        .filter(|f| f.format == MessageFormat::Html)
        .map(|f| f.body.clone())
}

/// The translatable text of a message, a caption or an edit's new content: the body of `m.text`
/// and `m.emote` without the reply fallback, or a media caption, each with its HTML. m.notice
/// and other msgtypes are not translated: notices are what bots (this one included) post, and
/// translating them risks loops between bots in shared rooms.
pub(crate) fn source_content(msgtype: &MessageType) -> Result<SourceContent, SkipReason> {
    let (body, formatted) = match msgtype {
        MessageType::Text(t) => (t.body.as_str(), t.formatted.as_ref()),
        MessageType::Emote(e) => (e.body.as_str(), e.formatted.as_ref()),
        other => {
            let (_, caption) = media_caption_kind(other).ok_or(SkipReason::UnsupportedMsgtype)?;
            // `caption()` is only set when `body` differs from `filename`: a bare file name
            // ("1000006546.jpg") is never taken for a caption.
            let caption = caption
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or(SkipReason::NoCaption)?;
            return Ok(SourceContent {
                plain: caption.to_owned(),
                html: html_body(formatted_caption(other)),
            });
        }
    };
    let plain = strip_reply_fallback(body.trim()).trim();
    if plain.is_empty() {
        return Err(SkipReason::EmptyText);
    }
    Ok(SourceContent {
        plain: plain.to_owned(),
        html: html_body(formatted),
    })
}

/// A new message, as delivered by sync: skipped when it predates the start-up cut-off, retried
/// later when every target failed for a passing reason.
pub(crate) async fn handle_message(
    state: BotState,
    room: Room,
    event: OriginalSyncRoomMessageEvent,
) {
    // Belt-and-suspenders: skip any event that predates the cutoff captured
    // just before handlers were registered (see `main`). The primary defence
    // is sync_once running before handlers are registered at all, which
    // prevents backlog events from reaching this function in the first
    // place; this check only catches whatever slips through that.
    //
    // This check lives here rather than in handle_message_core so that
    // `!translate retry` and the late retries (which reprocess an old event)
    // can call handle_message_core directly and bypass it.
    if let Some(event_time) = event.origin_server_ts.to_system_time() {
        if event_time < state.startup_time {
            info!("Skipping backlog message {} (pre-startup)", event.event_id);
            record_skip(
                &state.stats,
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

    let event_id = event.event_id.clone();
    let outcome = handle_message_core(state.clone(), room.clone(), event).await;
    if outcome == (CoreOutcome::Failed { retry_later: true }) {
        schedule_late_retries(&state, &room, event_id, &LATE_RETRIES_AFTER_FAILURE);
    }
}

pub(crate) async fn handle_message_core(
    state: BotState,
    room: Room,
    event: OriginalSyncRoomMessageEvent,
) -> CoreOutcome {
    let started = Instant::now();
    let room_id = room.room_id().as_str().to_owned();
    let sender = event.sender.as_str().to_owned();

    // Senders never translated ([translation] / per-room ignore_users): before
    // edits, captions and `!translate retry`, which all come through here.
    if is_ignored_sender(
        &state.translation,
        &state.room_translations,
        &room_id,
        &sender,
    ) {
        debug!(
            "Not translating {} from ignored sender {sender}",
            event.event_id
        );
        record_skip(
            &state.stats,
            &event.event_id,
            &room_id,
            Some(&sender),
            event.content.msgtype.msgtype(),
            "n/a",
            SkipReason::IgnoredSender,
        );
        return CoreOutcome::Skipped;
    }

    // Edits arrive as m.replace: route them to handle_edit and stop.
    // Must be checked BEFORE the msgtype guard: the top-level body of an edit
    // is only a fallback ("* new text") for old clients and must NOT be translated.
    if let Some(Relation::Replacement(ref replacement)) = event.content.relates_to {
        let original_event_id = replacement.event_id.clone();
        let new_content = replacement.new_content.clone();
        return handle_edit(state, room, &sender, original_event_id, new_content).await;
    }

    // Idempotency guard: if this event_id was already translated, don't do
    // it again. Normally unreachable (each event is only ever dispatched
    // once by matrix-sdk), but it's a cheap, indexed lookup that closes off
    // a real failure mode (a process restart between sending the
    // translation and the SDK's since-token being persisted, or `!translate
    // retry` racing a live delivery of the same event) from ever producing
    // a second translation of the same message.
    match state.db.lookup_translation(event.event_id.as_str()).await {
        Ok(Some(existing)) => {
            info!(
                "{} was already translated as {existing}: skipping duplicate delivery",
                event.event_id
            );
            record_skip(
                &state.stats,
                &event.event_id,
                &room_id,
                Some(&sender),
                event.content.msgtype.msgtype(),
                "n/a",
                SkipReason::AlreadyTranslated,
            );
            return CoreOutcome::Skipped;
        }
        Ok(None) => {}
        Err(e) => warn!(
            "Failed to check existing translation for {}: {e}",
            event.event_id
        ),
    }

    // From here on this event might end up translated: mark it "pending" so
    // a same-event edit/redaction that races in before we're done (or before
    // we decide to skip) waits for us instead of concluding there's no
    // mapping. Held for the rest of this function, across every await below.
    let _pending_guard = mark_pending(&state.pending_translations, &event.event_id);

    // Admin command family, in genuine m.text only (not m.emote): !translate status | stats |
    // debug <event-id> | retry <event-id> | version
    if let MessageType::Text(text) = &event.content.msgtype {
        let raw = text.body.trim();
        if raw == "!translate" || raw.starts_with("!translate ") {
            if !state.bot.is_admin(&event.sender) {
                warn!("!translate from non-admin {}, ignored", event.sender);
                return CoreOutcome::Skipped;
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
                _ => "🌍 !translate commands\n\
                      🩺 status · 📊 stats · 🏷️ version\n\
                      🔍 debug <event-id>\n\
                      🔁 retry <event-id>"
                    .to_owned(),
            };
            if let Err(e) =
                send_with_retry(&room, RoomMessageEventContent::notice_plain(reply)).await
            {
                warn!("Failed to send !translate reply: {e}");
            }
            return CoreOutcome::Skipped;
        }
    }

    let (context, msgtype_label, text_source) = match media_caption_kind(&event.content.msgtype) {
        Some((kind, _)) => (kind, kind, "caption"),
        None => ("message", event.content.msgtype.msgtype(), "body"),
    };
    let skip = |reason| {
        record_skip(
            &state.stats,
            &event.event_id,
            &room_id,
            Some(&sender),
            msgtype_label,
            text_source,
            reason,
        );
        CoreOutcome::Skipped
    };
    let source = match source_content(&event.content.msgtype) {
        Ok(source) => source,
        Err(reason) => return skip(reason),
    };
    if text_source == "caption" {
        info!(
            "Translating caption from m.{context} event {}",
            event.event_id
        );
    }

    let translation = state.translation_for_room(&room_id);
    let pipeline = state.pipeline(&translation, context, &room_id, &sender, &event.event_id);
    let result = match translate_source(&state, &pipeline, &source).await {
        Ok(result) => result,
        Err(NotTranslated::Skipped(reason)) => return skip(reason),
        Err(NotTranslated::Failed(failure)) => {
            return CoreOutcome::Failed {
                retry_later: should_retry_later(&failure),
            }
        }
    };

    let (plain_body, html_body) = build_translation_bodies(&result.lines);
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

    let send_result = send_with_retry(&room, content).await;
    record_translated(
        &state.stats,
        &event.event_id,
        &room_id,
        &sender,
        msgtype_label,
        text_source,
        &result.lang,
        &line_targets(&result.lines),
        send_result.is_ok(),
        started.elapsed(),
    );
    match send_result {
        Ok(bot_event_id) => {
            record_translation(&state, &event.event_id, &bot_event_id, &room_id).await;
            CoreOutcome::Translated
        }
        Err(e) => {
            error!("Failed to send {context} translation: {e}");
            CoreOutcome::Failed { retry_later: false }
        }
    }
}

pub(crate) fn line_targets(lines: &[TranslatedLine]) -> Vec<String> {
    lines.iter().map(|line| line.target.clone()).collect()
}

/// Called when a redaction event is received. If `redacted_id` is an original
/// message the bot translated, redacts the bot's translation too.
pub(crate) async fn handle_redaction(state: BotState, room: Room, redacted_id: OwnedEventId) {
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
        "Original message {redacted_id} was redacted: \
         redacting bot translation {bot_event_id}"
    );
    redact_translation(&state, &room, &redacted_id, &bot_event_id).await;
}

/// Redacts the bot's translation of `original_event_id` and removes the now-stale mapping (so a
/// later redaction of the same event, e.g. a federation replay, is a no-op rather than a repeat
/// redact attempt).
pub(crate) async fn redact_translation(
    state: &BotState,
    room: &Room,
    original_event_id: &OwnedEventId,
    bot_event_id: &OwnedEventId,
) {
    if let Err(e) = room.redact(bot_event_id, None, None).await {
        warn!("Failed to redact translation {bot_event_id}: {e}");
        return;
    }
    if let Err(e) = state
        .db
        .remove_translation(original_event_id.as_str())
        .await
    {
        warn!("Failed to remove translation record for {original_event_id}: {e}");
    }
}

/// Determine the Matrix thread root for a given incoming event:
///
/// 1. Event is already in a thread (`m.thread`) → use that thread's root.
/// 2. Event is a plain reply or standalone → start a new thread on this event itself.
pub(crate) fn resolve_thread_root(event: &OriginalSyncRoomMessageEvent) -> OwnedEventId {
    match &event.content.relates_to {
        Some(Relation::Thread(thread)) => thread.event_id.clone(),
        _ => event.event_id.clone(),
    }
}

/// Whether an edit that ends with `reason` leaves nothing to translate, so that the old
/// translation would contradict the edited message.
pub(crate) fn edit_leaves_nothing_to_translate(reason: SkipReason) -> bool {
    matches!(
        reason,
        SkipReason::EditNoTranslatableText
            | SkipReason::NoTranslatableText
            | SkipReason::NoRemainingTargets
            | SkipReason::TranslationUnchanged
    )
}

/// Called when a user edits a message the bot previously translated.
/// Re-translates the new content and edits the bot's existing translation
/// in-place using m.replace. No new message is sent, thread context is preserved.
/// An edit that leaves nothing to translate redacts the translation instead.
pub(crate) async fn handle_edit(
    state: BotState,
    room: Room,
    sender: &str,
    original_event_id: OwnedEventId,
    new_content: RoomMessageEventContentWithoutRelation,
) -> CoreOutcome {
    let started = Instant::now();
    let room_id = room.room_id().as_str().to_owned();
    let msgtype_label = new_content.msgtype.msgtype();

    // Look up whether we have a translation for this event: waiting briefly
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
            return CoreOutcome::Skipped;
        }
    };
    let bot_event_id = bot_event_id.and_then(|id| matrix_sdk::ruma::EventId::parse(id).ok());

    let skip = |reason| {
        record_skip(
            &state.stats,
            &original_event_id,
            &room_id,
            None,
            msgtype_label,
            "edit",
            reason,
        );
        CoreOutcome::Skipped
    };
    let Some(bot_event_id) = bot_event_id else {
        info!("Edit for unknown event {original_event_id}, no cached translation, ignoring");
        return skip(SkipReason::EditUnknownEvent);
    };

    // Use ONLY m.new_content as the source of truth (full replacement, not a diff).
    let translation = state.translation_for_room(&room_id);
    let pipeline = state.pipeline(&translation, "edit", &room_id, sender, &original_event_id);
    let result = match source_content(&new_content.msgtype) {
        Ok(source) => translate_source(&state, &pipeline, &source).await,
        Err(_) => Err(NotTranslated::Skipped(SkipReason::EditNoTranslatableText)),
    };
    let result = match result {
        Ok(result) => result,
        Err(NotTranslated::Skipped(reason)) => {
            let outcome = skip(reason);
            if edit_leaves_nothing_to_translate(reason) {
                info!(
                    "Edit of {original_event_id} leaves nothing to translate ({}): \
                     redacting translation {bot_event_id}",
                    reason.as_str()
                );
                redact_translation(&state, &room, &original_event_id, &bot_event_id).await;
            }
            return outcome;
        }
        // Transient or not, the old translation stays; an edit is not retried later, a newer
        // edit could have replaced it by then.
        Err(NotTranslated::Failed(_)) => return CoreOutcome::Failed { retry_later: false },
    };

    let edit_content = make_translation_edit(
        &result.lines,
        translation.silent_messages,
        bot_event_id.clone(),
    );
    info!("Editing bot translation {bot_event_id} for edit of {original_event_id}");
    let send_result = send_with_retry(&room, edit_content).await;
    record_translated(
        &state.stats,
        &original_event_id,
        &room_id,
        "-",
        msgtype_label,
        "edit",
        &result.lang,
        &line_targets(&result.lines),
        send_result.is_ok(),
        started.elapsed(),
    );
    match send_result {
        Ok(_) => CoreOutcome::Translated,
        Err(e) => {
            error!("Failed to send translation edit: {e}");
            CoreOutcome::Failed { retry_later: false }
        }
    }
}

/// Late retries of an event that could not be decrypted at sync time (its key often arrives
/// seconds later), counted from the failure.
pub(crate) const LATE_RETRIES_UNDECRYPTABLE: [Duration; 2] =
    [Duration::from_secs(30), Duration::from_secs(5 * 60)];
/// Late retries of a message whose every target failed transiently or by the overall deadline
/// (the backend restarting, say), counted from the failure.
pub(crate) const LATE_RETRIES_AFTER_FAILURE: [Duration; 2] =
    [Duration::from_secs(5 * 60), Duration::from_secs(20 * 60)];

/// Calls `attempt` at each of `delays` after now until one succeeds or a further try could not
/// end differently.
pub(crate) async fn run_late_retries<F, Fut>(delays: &[Duration], stats: &Stats, mut attempt: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Reprocessed>,
{
    let start = tokio::time::Instant::now();
    for delay in delays {
        tokio::time::sleep_until(start + *delay).await;
        stats.late_retries.fetch_add(1, Ordering::Relaxed);
        let outcome = attempt().await;
        if outcome.succeeded() {
            stats.late_retries_succeeded.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if !outcome.worth_another_try() {
            return;
        }
    }
}

/// Reprocesses `event_id` at `delays` in a task of its own, in memory only (gone after a
/// restart). Each attempt holds an `inflight` slot like a live message.
pub(crate) fn schedule_late_retries(
    state: &BotState,
    room: &Room,
    event_id: OwnedEventId,
    delays: &'static [Duration],
) {
    info!("Scheduling late retries of {event_id} after {delays:?}");
    let state = state.clone();
    let room = room.clone();
    tokio::spawn(async move {
        let stats = Arc::clone(&state.stats);
        run_late_retries(delays, &stats, || {
            let (state, room, event_id) = (state.clone(), room.clone(), event_id.clone());
            async move {
                let _permit = state.inflight.acquire().await;
                info!("Late retry of {event_id}");
                reprocess_event(&state, &room, &event_id).await
            }
        })
        .await;
    });
}
