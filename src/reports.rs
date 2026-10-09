//! Admin `!translate` reports and the retry command.

use super::*;

pub(crate) fn translate_version_report() -> String {
    format!(
        "🌍 translate-bot v{} ({})",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH")
    )
}

pub(crate) async fn translate_status_report(state: &BotState) -> String {
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
        "{}\n\
         ⏱️ Uptime: {}h {:02}m\n\
         🔌 LTEngine: {ltengine}\n\
         🔁 Sync reconnects: {}\n\
         \n\
         ⚙️ Settings\n\
         🗣️ Languages: {}\n\
         🎯 Min confidence: {:.2}\n\
         🚦 Concurrency: {}\n\
         ⏳ Timeouts: {}s per request, {}s overall\n\
         🔂 Max attempts: {}\n\
         \n\
         📊 Counters\n\
         👀 Seen: {}\n\
         ✅ Translated: {}\n\
         🔒 Decrypt failures: {}\n\
         📤 Send failures: {}",
        translate_version_report(),
        uptime / 3600,
        (uptime % 3600) / 60,
        state.stats.sync_reconnects.load(Ordering::Relaxed),
        t.langs.join(", "),
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

pub(crate) fn translate_stats_report(state: &BotState) -> String {
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
        "📊 Translation stats\n\
         👀 Seen: {}\n\
         ✅ Translated: {}\n\
         🔒 Decrypt failures: {}\n\
         🌐 API failures: {}\n\
         📤 Send failures: {}\n\
         🔁 Retries: {} ({} succeeded)\n\
         ⏭️ Skipped: {skipped_str}",
        s.events_seen.load(Ordering::Relaxed),
        s.translated.load(Ordering::Relaxed),
        s.decrypt_failures.load(Ordering::Relaxed),
        s.translation_api_failures.load(Ordering::Relaxed),
        s.matrix_send_failures.load(Ordering::Relaxed),
        s.retries.load(Ordering::Relaxed),
        s.retries_succeeded.load(Ordering::Relaxed),
    )
}

pub(crate) async fn translate_debug_report(state: &BotState, arg: &str) -> String {
    if arg.is_empty() {
        return "💡 Usage: !translate debug <event-id>".to_owned();
    }
    match state.stats.find_outcome(arg).await {
        Some(o) => {
            let ago = (db::now_secs() - o.at).max(0);
            format!(
                "🔍 Event {}\n\
                 🏠 Room: {}\n\
                 👤 Sender: {}\n\
                 📨 Type: {} ({})\n\
                 ⚖️ Decision: {}\n\
                 💬 Reason: {}\n\
                 🗣️ Source: {}\n\
                 🎯 Targets: {}\n\
                 📤 Send: {}\n\
                 ⏱️ Took: {} ms, {ago}s ago",
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
            "🤷 No record for {arg}.\n\
             Either I never processed it, or it is older than what I keep \
             (last {EVENT_OUTCOME_HISTORY} events in memory, last {} in the database).",
            db::EVENT_OUTCOMES_RETENTION
        ),
    }
}

/// Re-processes a specific event through the normal translate-and-send
/// pipeline. Refuses if `translation_map` already has an entry for it (the
/// invariant this exists to serve: retrying must never create a duplicate
/// translation: if one already exists, editing the original message is the
/// correct way to update it). Also picks up events that couldn't be
/// decrypted at sync time but can be decrypted now, since `Room::event`
/// re-attempts decryption with whatever keys are currently available.
pub(crate) async fn translate_retry_command(state: &BotState, room: &Room, arg: &str) -> String {
    if arg.is_empty() {
        return "💡 Usage: !translate retry <event-id>".to_owned();
    }
    let event_id = match matrix_sdk::ruma::EventId::parse(arg) {
        Ok(id) => id,
        Err(e) => return format!("❌ '{arg}' is not a valid event ID: {e}"),
    };

    let already_translated = match state.db.lookup_translation(event_id.as_str()).await {
        Ok(found) => found,
        Err(e) => return format!("❌ Could not check existing translations for {event_id}: {e}"),
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
            "✅ {event_id} was already translated as {bot_event_id}.\n\
             ✏️ Edit the original message to force a new translation."
        );
    }

    let timeline_event = match room.event(&event_id, None).await {
        Ok(ev) => ev,
        Err(e) => return format!("❌ Could not fetch {event_id}: {e}"),
    };
    if timeline_event.kind.is_utd() {
        return format!("🔒 {event_id} is still not decryptable. I have no key for it.");
    }
    let parsed: OriginalSyncRoomMessageEvent = match timeline_event
        .kind
        .raw()
        .cast_ref_unchecked::<OriginalSyncRoomMessageEvent>()
        .deserialize()
    {
        Ok(ev) => ev,
        Err(e) => return format!("❌ {event_id} is not a room message: {e}"),
    };

    // Boxed: handle_message_core can (transitively, via !translate retry)
    // call back into itself, and an async fn can't recurse without indirection.
    Box::pin(handle_message_core(state.clone(), room.clone(), parsed)).await;
    format!("🔁 Retried {event_id}.\n🔍 Run !translate debug {event_id} for the outcome.")
}
