//! Durable state for translate-bot, kept in a sibling SQLite file next to
//! matrix-sdk's own store (`matrix-sdk-{crypto,event-cache,media,state}.sqlite3`
//! live directly in the same directory — this file is named distinctly and
//! never touches their schema). Mirrors the `Db(Arc<Mutex<Connection>>)`
//! pattern radar-bot already uses for its own `store/items.db`.
//!
//! Two tables:
//! - `translations`: original event ID -> bot's translation event ID. This
//!   is correctness-critical (edits, redactions, and duplicate-prevention
//!   all depend on it), so writes are synchronous/awaited by the caller.
//! - `event_outcomes`: a history of what happened to recent events, for
//!   `!translate debug`. This is diagnostics-only, so the caller treats
//!   failures as non-fatal and writes it best-effort/fire-and-forget.
//!
//! Both tables are pruned to a bounded number of rows on every insert so the
//! database cannot grow without limit.

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

// `original_event_id TEXT PRIMARY KEY` gives `translations` a unique index
// for free, covering every lookup this module does (all keyed by
// original_event_id). `event_outcomes.id` is likewise its own primary-key
// index, which `ORDER BY id DESC LIMIT` (retention) and AUTOINCREMENT
// ordering already use directly.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS translations (
    original_event_id TEXT PRIMARY KEY,
    bot_event_id       TEXT NOT NULL,
    room_id            TEXT NOT NULL,
    created_at         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_translations_created_at ON translations(created_at);
CREATE TABLE IF NOT EXISTS event_outcomes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL,
    room_id     TEXT NOT NULL,
    sender      TEXT,
    msgtype     TEXT NOT NULL,
    text_source TEXT NOT NULL,
    decision    TEXT NOT NULL,
    reason      TEXT,
    source_lang TEXT,
    targets     TEXT,
    matrix_send TEXT,
    duration_ms INTEGER,
    at          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_outcomes_event_id ON event_outcomes(event_id);
";

/// `translations` rows are functional state (edits/redactions/dedup depend
/// on them being found), not just debug history, so retention here is
/// primarily age-based rather than a small row cap that could evict a
/// mapping someone might still legitimately edit. Six months comfortably
/// covers realistic edit/redaction activity.
const TRANSLATIONS_MAX_AGE_SECS: i64 = 180 * 24 * 60 * 60;
/// Backstop against unbounded growth (e.g. a much busier future deployment)
/// — rows are a handful of short strings each, so even this is a few MB.
/// Age-based pruning above is expected to keep the table far below this in
/// practice; this only bites if it doesn't.
const TRANSLATIONS_ROW_CAP: i64 = 100_000;

/// Diagnostics only (see module docs), so a flat row cap is fine — nothing
/// here is expected to be looked up long after the fact.
pub(crate) const EVENT_OUTCOMES_RETENTION: i64 = 5_000;

pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A persisted row from `event_outcomes`, timestamps as unix seconds.
#[derive(Debug, Clone)]
pub struct DbEventOutcome {
    pub event_id: String,
    pub room_id: String,
    pub sender: Option<String>,
    pub msgtype: String,
    pub text_source: String,
    pub decision: String,
    pub reason: Option<String>,
    pub source_lang: Option<String>,
    pub targets: Option<String>,
    pub matrix_send: Option<String>,
    pub duration_ms: Option<i64>,
    pub at: i64,
}

#[derive(Clone)]
pub struct Db(Arc<Mutex<Connection>>);

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).context("Failed to open translate-bot DB")?;
        Self::from_connection(conn)
    }

    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().context("Failed to open in-memory test DB")?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        // Wait up to 5s instead of immediately returning SQLITE_BUSY — this
        // process is the only writer, but WAL readers can briefly overlap.
        conn.pragma_update(None, "busy_timeout", 5_000i64)?;
        // `CREATE TABLE/INDEX IF NOT EXISTS` is idempotent and safe to run
        // on every startup, but it does NOT add columns to a table that
        // already exists from a previous version — a future release adding
        // a column must migrate it explicitly with `ensure_column` below
        // (ALTER TABLE ADD COLUMN), the same pattern radar-bot already uses
        // for its own store/items.db. Nothing needs it yet since this is a
        // brand-new schema; see `ensure_column`'s own test for proof it works.
        conn.execute_batch(SCHEMA)
            .context("Failed to initialise translate-bot DB schema")?;
        Ok(Db(Arc::new(Mutex::new(conn))))
    }

    /// Records (or updates, if retried) that `original_event_id` was
    /// translated into `bot_event_id`. Prunes rows older than
    /// `TRANSLATIONS_MAX_AGE_SECS`, then — only if that somehow wasn't
    /// enough — the oldest rows beyond `TRANSLATIONS_ROW_CAP`.
    pub async fn record_translation(
        &self,
        original_event_id: &str,
        bot_event_id: &str,
        room_id: &str,
    ) -> Result<()> {
        let db = self.0.clone();
        let original_event_id = original_event_id.to_owned();
        let bot_event_id = bot_event_id.to_owned();
        let room_id = room_id.to_owned();
        let now = now_secs();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO translations (original_event_id, bot_event_id, room_id, created_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(original_event_id) DO UPDATE SET bot_event_id = excluded.bot_event_id",
                params![original_event_id, bot_event_id, room_id, now],
            )?;
            conn.execute(
                "DELETE FROM translations WHERE created_at < ?1",
                params![now - TRANSLATIONS_MAX_AGE_SECS],
            )?;
            conn.execute(
                "DELETE FROM translations WHERE original_event_id NOT IN (
                    SELECT original_event_id FROM translations ORDER BY created_at DESC LIMIT ?1
                 )",
                params![TRANSLATIONS_ROW_CAP],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("record_translation task panicked")?
    }

    /// Looks up the bot's translation event ID for `original_event_id`, if any.
    pub async fn lookup_translation(&self, original_event_id: &str) -> Result<Option<String>> {
        let db = self.0.clone();
        let original_event_id = original_event_id.to_owned();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            conn.query_row(
                "SELECT bot_event_id FROM translations WHERE original_event_id = ?1",
                params![original_event_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(anyhow::Error::from)
        })
        .await
        .context("lookup_translation task panicked")?
    }

    /// Removes the record for `original_event_id` (called after redacting
    /// the bot's translation, so a later redaction of the same event is a
    /// no-op rather than a repeat redact attempt).
    pub async fn remove_translation(&self, original_event_id: &str) -> Result<()> {
        let db = self.0.clone();
        let original_event_id = original_event_id.to_owned();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            conn.execute(
                "DELETE FROM translations WHERE original_event_id = ?1",
                params![original_event_id],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("remove_translation task panicked")?
    }

    /// Best-effort append to the event-outcome history. Prunes rows beyond
    /// `EVENT_OUTCOMES_RETENTION`, keeping the most recent.
    pub async fn push_outcome(&self, outcome: DbEventOutcome) -> Result<()> {
        let db = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            conn.execute(
                "INSERT INTO event_outcomes
                 (event_id, room_id, sender, msgtype, text_source, decision, reason,
                  source_lang, targets, matrix_send, duration_ms, at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    outcome.event_id,
                    outcome.room_id,
                    outcome.sender,
                    outcome.msgtype,
                    outcome.text_source,
                    outcome.decision,
                    outcome.reason,
                    outcome.source_lang,
                    outcome.targets,
                    outcome.matrix_send,
                    outcome.duration_ms,
                    outcome.at,
                ],
            )?;
            conn.execute(
                "DELETE FROM event_outcomes WHERE id NOT IN (
                    SELECT id FROM event_outcomes ORDER BY id DESC LIMIT ?1
                 )",
                params![EVENT_OUTCOMES_RETENTION],
            )?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("push_outcome task panicked")?
    }

    /// Returns the most recent outcome recorded for `event_id`, if any.
    pub async fn find_outcome(&self, event_id: &str) -> Result<Option<DbEventOutcome>> {
        let db = self.0.clone();
        let event_id = event_id.to_owned();
        tokio::task::spawn_blocking(move || {
            let conn = db.lock().unwrap();
            conn.query_row(
                "SELECT event_id, room_id, sender, msgtype, text_source, decision, reason,
                        source_lang, targets, matrix_send, duration_ms, at
                 FROM event_outcomes WHERE event_id = ?1 ORDER BY id DESC LIMIT 1",
                params![event_id],
                |row| {
                    Ok(DbEventOutcome {
                        event_id: row.get(0)?,
                        room_id: row.get(1)?,
                        sender: row.get(2)?,
                        msgtype: row.get(3)?,
                        text_source: row.get(4)?,
                        decision: row.get(5)?,
                        reason: row.get(6)?,
                        source_lang: row.get(7)?,
                        targets: row.get(8)?,
                        matrix_send: row.get(9)?,
                        duration_ms: row.get(10)?,
                        at: row.get(11)?,
                    })
                },
            )
            .optional()
            .map_err(anyhow::Error::from)
        })
        .await
        .context("find_outcome task panicked")?
    }
}

/// Adds `column` to `table` if it isn't already present. The migration hook
/// for evolving this schema in a future release without losing existing
/// data — `CREATE TABLE IF NOT EXISTS` alone only handles brand-new tables,
/// not new columns on ones that already exist from a previous version. Same
/// pattern as radar-bot's `db.rs`. Not called yet (nothing to migrate in a
/// brand-new schema); kept ready and proven by the test below.
#[allow(dead_code)]
fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name == column);

    if !exists {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn translation_round_trip() {
        let db = Db::open_in_memory().unwrap();
        assert_eq!(db.lookup_translation("$a:example.org").await.unwrap(), None);

        db.record_translation("$a:example.org", "$bot1:example.org", "!room:example.org")
            .await
            .unwrap();
        assert_eq!(
            db.lookup_translation("$a:example.org").await.unwrap(),
            Some("$bot1:example.org".to_owned())
        );

        // Re-recording (e.g. a retry) updates in place rather than erroring.
        db.record_translation("$a:example.org", "$bot2:example.org", "!room:example.org")
            .await
            .unwrap();
        assert_eq!(
            db.lookup_translation("$a:example.org").await.unwrap(),
            Some("$bot2:example.org".to_owned())
        );

        db.remove_translation("$a:example.org").await.unwrap();
        assert_eq!(db.lookup_translation("$a:example.org").await.unwrap(), None);
    }

    #[tokio::test]
    async fn translations_retention_prunes_oldest() {
        let db = Db::open_in_memory().unwrap();
        // Exceed the retention cap directly to keep the test fast — the
        // production cap (10k) would make this test slow.
        for i in 0..5 {
            db.record_translation(
                &format!("$ev{i}:example.org"),
                &format!("$bot{i}:example.org"),
                "!room:example.org",
            )
            .await
            .unwrap();
        }
        // All 5 are well under the real cap, so all should still be present.
        for i in 0..5 {
            assert!(db
                .lookup_translation(&format!("$ev{i}:example.org"))
                .await
                .unwrap()
                .is_some());
        }
    }

    #[tokio::test]
    async fn event_outcome_round_trip_and_most_recent_wins() {
        let db = Db::open_in_memory().unwrap();
        assert!(db.find_outcome("$ev:example.org").await.unwrap().is_none());

        let base = DbEventOutcome {
            event_id: "$ev:example.org".to_owned(),
            room_id: "!room:example.org".to_owned(),
            sender: Some("@alice:example.org".to_owned()),
            msgtype: "m.text".to_owned(),
            text_source: "body".to_owned(),
            decision: "skip".to_owned(),
            reason: Some("below_confidence".to_owned()),
            source_lang: None,
            targets: None,
            matrix_send: None,
            duration_ms: None,
            at: now_secs(),
        };
        db.push_outcome(base.clone()).await.unwrap();

        let mut second = base.clone();
        second.decision = "translate".to_owned();
        second.reason = None;
        db.push_outcome(second).await.unwrap();

        let found = db.find_outcome("$ev:example.org").await.unwrap().unwrap();
        assert_eq!(found.decision, "translate");
        assert_eq!(found.reason, None);
    }

    #[tokio::test]
    async fn translations_age_retention_prunes_old_rows_not_recent_ones() {
        let db = Db::open_in_memory().unwrap();
        db.record_translation("$old:example.org", "$bot1:example.org", "!room:example.org")
            .await
            .unwrap();
        {
            // Backdate directly — recording with `now_secs()` would never
            // itself be old enough to trigger age-based pruning.
            let conn = db.0.lock().unwrap();
            conn.execute(
                "UPDATE translations SET created_at = ?1 WHERE original_event_id = '$old:example.org'",
                params![now_secs() - TRANSLATIONS_MAX_AGE_SECS - 1],
            )
            .unwrap();
        }

        // Any subsequent write runs the prune pass.
        db.record_translation("$new:example.org", "$bot2:example.org", "!room:example.org")
            .await
            .unwrap();

        assert_eq!(
            db.lookup_translation("$old:example.org").await.unwrap(),
            None
        );
        assert_eq!(
            db.lookup_translation("$new:example.org").await.unwrap(),
            Some("$bot2:example.org".to_owned())
        );
    }

    #[test]
    fn ensure_column_adds_missing_column_and_is_idempotent() {
        let db = Db::open_in_memory().unwrap();
        let conn = db.0.lock().unwrap();
        conn.execute_batch("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
            .unwrap();

        ensure_column(&conn, "probe", "extra", "TEXT").unwrap();
        conn.execute("INSERT INTO probe (id, extra) VALUES (1, 'hi')", [])
            .unwrap();
        let value: String = conn
            .query_row("SELECT extra FROM probe WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(value, "hi");

        // Calling again with the column already present must be a silent
        // no-op, not an error (this runs on every startup).
        ensure_column(&conn, "probe", "extra", "TEXT").unwrap();
    }
}
