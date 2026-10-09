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
        let notify = self.pending.lock().unwrap().remove(&self.key);
        if let Some(notify) = notify {
            notify.notify_one();
        }
    }
}

pub(crate) fn mark_pending(pending: &PendingMap, event_id: &OwnedEventId) -> PendingGuard {
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
pub(crate) async fn lookup_translation_awaiting_pending(
    db: &Db,
    pending: &PendingMap,
    original_event_id: &str,
) -> Result<Option<String>> {
    // Snapshot the in-flight handle (if any) *before* re-checking the DB, so
    // a task that finishes between our first check and this one can't race
    // us out of seeing its notification: notify_one() stores its permit if
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
         flight: waiting up to {PENDING_TRANSLATION_WAIT:?}"
    );
    let _ = timeout(PENDING_TRANSLATION_WAIT, notify.notified()).await;
    db.lookup_translation(original_event_id).await
}

/// Handles a caption attached to a media message (`m.image`, `m.video`,
/// `m.audio`, `m.file`). `kind` is the short label used in logs and as the
/// translation-target-resolution context (e.g. `"image"`, `"video"`).
pub(crate) async fn handle_media_caption(
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

    debug!(
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

    let send_result = send_with_retry(&room, content).await;
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
    // `!translate retry` (which necessarily reprocesses an old event) can
    // call handle_message_core directly and bypass it.
    if let Some(event_time) = event.origin_server_ts.to_system_time() {
        if event_time < state.startup_time {
            info!("Skipping backlog message {} (pre-startup)", event.event_id);
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

pub(crate) async fn handle_message_core(
    state: BotState,
    room: Room,
    event: OriginalSyncRoomMessageEvent,
) {
    let started = Instant::now();
    let room_id = room.room_id().as_str().to_owned();
    let sender = event.sender.as_str().to_owned();

    // Edits arrive as m.replace: route them to handle_edit and stop.
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

    // Media messages can carry a user-written caption in `body`. Per the
    // Matrix media-caption spec, `caption()` only returns it when `body`
    // differs from `filename`. A bare filename (e.g. "1000006546.jpg") is
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

    // Admin commands only apply to genuine m.text, not m.emote.
    if is_command_eligible {
        // Admin command family: !translate status | stats | debug <event-id>
        // | retry <event-id> | version
        if raw == "!translate" || raw.starts_with("!translate ") {
            if !state.bot.is_admin(&event.sender) {
                warn!("!translate from non-admin {}, ignored", event.sender);
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
                _ => "🌍 !translate commands\n\
                      🩺 status · 📊 stats · 🏷️ version\n\
                      🔍 debug <event-id>\n\
                      🔁 retry <event-id>"
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

    debug!(
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

    let send_result = send_with_retry(&room, content).await;
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
pub(crate) fn resolve_thread_root(event: &OriginalSyncRoomMessageEvent) -> OwnedEventId {
    match &event.content.relates_to {
        Some(Relation::Thread(thread)) => thread.event_id.clone(),
        _ => event.event_id.clone(),
    }
}

/// Extracts the translatable text from an edit's new content: the body
/// (reply-fallback stripped) for `m.text`/`m.emote`, or the caption for
/// caption-bearing media types (a caption can itself be edited: the new
/// content then carries the same media msgtype as the original). Returns
/// `None` when there is nothing translatable (unsupported msgtype, a
/// filename-only caption, or empty text).
pub(crate) fn extract_edit_text(msgtype: &MessageType) -> Option<String> {
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
/// in-place using m.replace. No new message is sent, thread context is preserved.
pub(crate) async fn handle_edit(
    state: BotState,
    room: Room,
    original_event_id: OwnedEventId,
    new_content: RoomMessageEventContentWithoutRelation,
) {
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
            return;
        }
    };
    let bot_event_id = bot_event_id.and_then(|id| matrix_sdk::ruma::EventId::parse(id).ok());

    let Some(bot_event_id) = bot_event_id else {
        info!("Edit for unknown event {original_event_id}, no cached translation, ignoring");
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
    // The thread membership is inherited from bot_event_id. No thread relation needed here.
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
    let send_result = send_with_retry(&room, edit_content).await;
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
