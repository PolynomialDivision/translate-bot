mod backend;
mod config;
mod db;
mod handlers;
mod pipeline;
mod render;
mod reports;
mod stats;
#[cfg(test)]
mod tests;

use backend::*;
use config::*;
use handlers::*;
use pipeline::*;
use render::*;
use reports::*;
use stats::*;

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant, SystemTime},
};

use mxbot_common::{
    admin::Dispatch,
    config::{MatrixConfig, SecurityConfig},
    matrix_sdk,
    send::send_with_retry,
    Bot,
};

use db::{Db, DbEventOutcome};

use anyhow::{Context, Result};
use futures_util::future::join_all;
use matrix_sdk::{
    deserialized_responses::EncryptionInfo,
    ruma::{
        events::{
            relation::{InReplyTo, Replacement, Reply, Thread},
            room::{
                encrypted::OriginalSyncRoomEncryptedEvent,
                message::{
                    MessageFormat, MessageType, NoticeMessageEventContent,
                    OriginalSyncRoomMessageEvent, Relation, RoomMessageEventContent,
                    RoomMessageEventContentWithoutRelation, TextMessageEventContent,
                },
                redaction::OriginalSyncRoomRedactionEvent,
            },
        },
        OwnedEventId,
    },
    Room, RoomState,
};
use pulldown_cmark::html::push_html;
use pulldown_cmark::{Options, Parser};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    sync::{Mutex, Notify, Semaphore},
    time::{sleep, timeout},
};
use tracing::{debug, error, info, warn};

#[derive(Clone)]
struct BotState {
    lt_url: String,
    lt_api_key: Option<String>,
    translation: TranslationConfig,
    room_translations: HashMap<String, RoomTranslationConfig>,
    http: reqwest::Client,
    bot: Bot,
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

#[tokio::main]
async fn main() -> Result<()> {
    mxbot_common::logging::init("translate_bot");

    let config: Config =
        mxbot_common::config::load_toml(&mxbot_common::config::config_path_from_args())?;

    let store_path = mxbot_common::config::store_path_from_env();
    fs::create_dir_all(&store_path).await?;
    // Sibling file next to matrix-sdk's own store (matrix-sdk-*.sqlite3) —
    // same directory, same volume, distinct name, no shared schema. Holds
    // durable dedup (translations) and best-effort event-outcome history,
    // both of which need to survive process/container restarts.
    let db = Db::open(&store_path.join("translate-bot.sqlite3"))
        .context("Failed to open translate-bot database")?;

    // Installs verification, invite and admin-console handling before the
    // first sync, so pending verification requests are not discarded.
    let bot = Bot::builder("translate-bot", env!("CARGO_PKG_VERSION"))
        .store_path(&store_path)
        .admin_help("!translate status | stats | debug <event-id> | retry <event-id> | version")
        .start(&config.matrix, &config.security)
        .await?;

    let backend_concurrency = config.translation.backend_concurrency.max(1);
    // Hard cap for any backend HTTP call; must never cut off a translation
    // that is still within the configured request_timeout_secs.
    let http_timeout = Duration::from_secs(config.translation.request_timeout_secs.max(300) + 30);

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
            .timeout(http_timeout)
            .build()
            .expect("failed to build HTTP client"),
        bot: bot.clone(),
        inflight: Arc::new(Semaphore::new(8)),
        lt_requests: Arc::new(Semaphore::new(backend_concurrency)),
        stats: Arc::new(Stats::new(db.clone())),
        db,
        pending_translations: Arc::new(std::sync::Mutex::new(HashMap::new())),
    };

    // Probe ltengine reachability at startup so failures are visible in logs.
    info!("ltengine URL: {}", state.lt_url);
    // ltengine serves /health/ready (200 once the model is loaded, 503 while
    // it is still downloading/loading). Other LibreTranslate backends don't
    // have it, so fall back to /languages on 404.
    let ready_probe = state
        .http
        .get(format!("{}/health/ready", state.lt_url))
        .timeout(Duration::from_secs(10))
        .send()
        .await;
    let probe = match ready_probe {
        Ok(resp) if resp.status() == reqwest::StatusCode::NOT_FOUND => {
            state
                .http
                .get(format!("{}/languages", state.lt_url))
                .timeout(Duration::from_secs(10))
                .send()
                .await
        }
        other => other,
    };
    match probe {
        Ok(resp) if resp.status().is_success() => info!("ltengine ready at startup"),
        Ok(resp) if resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE => warn!(
            "ltengine reachable but its model is still loading — translations will be retried until it is ready"
        ),
        Ok(resp) => warn!(
            "ltengine returned {} at startup — translations may fail",
            resp.status()
        ),
        Err(e) => {
            warn!("ltengine not reachable at startup: {e} — translations will fail until it is up")
        }
    }

    // Advance the sync token past ordinary workload events from downtime before
    // registering those handlers (also joins invites received while offline).
    info!("Starting sync...");
    bot.initial_sync().await;

    // Recompute the backlog cutoff now, right before handlers that act on it
    // are registered. sync_once (the primary backlog defence) has already
    // drained pre-restart history above without any handlers installed to
    // react to it; this belt-and-suspenders check in handle_message should
    // therefore only ever catch messages older than *this* point, not
    // messages merely older than process start.
    state.startup_time = SystemTime::now();

    // In-room messages: admin commands and translation. Verification requests
    // are consumed by mxbot-common's transport-independent handlers.
    bot.client.add_event_handler({
        let state = state.clone();
        move |ev: OriginalSyncRoomMessageEvent, room: Room, encryption: Option<EncryptionInfo>| {
            let state = state.clone();
            async move {
                debug!(
                    "Received room message {} from {} in {}",
                    ev.event_id,
                    ev.sender,
                    room.room_id()
                );

                if let MessageType::VerificationRequest(_) = &ev.content.msgtype {
                    return;
                }

                if ev.sender == state.bot.user_id {
                    return;
                }

                // Shared admin console (verification, settings, status).
                if room.state() == RoomState::Joined
                    && state
                        .bot
                        .admin
                        .handle(&room, &ev, encryption.as_ref())
                        .await
                        == Dispatch::Handled
                {
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
    bot.client.add_event_handler({
        let state = state.clone();
        move |ev: OriginalSyncRoomEncryptedEvent, room: Room| {
            let state = state.clone();
            async move {
                if ev.sender == state.bot.user_id {
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
    bot.client.add_event_handler({
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

    let stats = Arc::clone(&state.stats);
    bot.run_with(move || {
        stats.sync_reconnects.fetch_add(1, Ordering::Relaxed);
    })
    .await
}
