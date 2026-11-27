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
    let line = retry_translation_operation("en", &policy, attempts_by_target, &stats, move || {
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
    // Not transient: must not count as a retry attempt.
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
fn translated_plain_text_is_escaped_in_the_html_body_only() {
    let source = "<b>fett</b> <a href=\"https://x.example\">Link</a> & Co";
    let translated = "<b>bold</b> <a href=\"https://x.example\">link</a> & Co".to_owned();
    let line = build_line("en", source, &translated, TextFormat::Text, &[]).unwrap();

    assert_eq!(line.plain, translated);
    assert_eq!(
        line.html,
        "&lt;b&gt;bold&lt;/b&gt; &lt;a href=&quot;https://x.example&quot;&gt;link&lt;/a&gt; &amp; Co"
    );

    let (plain, html) = build_translation_bodies(&[line]);
    assert_eq!(plain, format!("🇬🇧 {translated}"));
    assert!(!html.contains("<b>") && !html.contains("<a "));
    assert!(html.ends_with("&amp; Co"));
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
    // min_confidence=0.5 threshold once normalized: pre-fix, 7.0 (raw)
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
    assert!(TranslationError::new(TranslationErrorKind::HttpStatus(429), "rate").is_transient());
    assert!(TranslationError::new(TranslationErrorKind::HttpStatus(503), "down").is_transient());

    assert!(!TranslationError::new(TranslationErrorKind::HttpStatus(400), "bad").is_transient());
    assert!(!TranslationError::new(TranslationErrorKind::ResponseParse, "bad json").is_transient());
    assert!(!TranslationError::new(TranslationErrorKind::Rejected, "guard").is_transient());
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
fn html_to_plain_of_nested_formatting() {
    let plain = html_to_plain("<p>Hello <strong>world</strong></p>\n");
    assert_eq!(plain, "Hello world");
}

#[test]
fn html_to_plain_decodes_entities() {
    let plain = html_to_plain("<p>a &amp; b &lt; c &gt; d &#x27;e&#x27; &euro;</p>");
    assert_eq!(plain, "a & b < c > d 'e' €");
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
    let translated = "name more chat more chat more chat more chat more chat more chat more chat";

    assert_eq!(
        translation_rejection_reason("mehfrach chat", translated),
        Some("translation contains repeated phrase loop")
    );
}

#[test]
fn retry_after_header_is_parsed_and_capped() {
    let mut headers = reqwest::header::HeaderMap::new();
    assert_eq!(parse_retry_after(&headers), None);
    headers.insert(reqwest::header::RETRY_AFTER, "10".parse().unwrap());
    assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(10)));
    headers.insert(reqwest::header::RETRY_AFTER, "3600".parse().unwrap());
    assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(60)));
    headers.insert(reqwest::header::RETRY_AFTER, "soon".parse().unwrap());
    assert_eq!(parse_retry_after(&headers), None);
}

#[test]
fn accepts_long_translation_of_long_message() {
    // Regression: translations over 1000 characters were always rejected
    // although inputs up to max_input_chars (4000) are accepted.
    let source: String = (0..200).map(|i| format!("Wort{i} ")).collect();
    let translated: String = (0..200).map(|i| format!("Word{i}x ")).collect();
    assert!(translated.chars().count() > 1_000);
    assert_eq!(translation_rejection_reason(&source, &translated), None);
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

// ── reply fallback in HTML (removed by protect) ──────────────────────────

fn protected_html(html: &str) -> String {
    let source = SourceContent {
        plain: "-".to_owned(),
        html: Some(html.to_owned()),
    };
    protect(&source).0.html.unwrap()
}

#[test]
fn protect_removes_mx_reply_wrapper() {
    let html = "<mx-reply><blockquote>quoted</blockquote></mx-reply>Actual message";
    assert_eq!(protected_html(html), "Actual message");
}

#[test]
fn protect_passes_html_without_reply_through() {
    let html = "<p>Normal message</p>";
    assert_eq!(protected_html(html), html);
}

#[test]
fn protect_trims_leading_whitespace_after_mx_reply() {
    let html = "<mx-reply><blockquote>q</blockquote></mx-reply>\n\n<p>message</p>";
    assert_eq!(protected_html(html), "<p>message</p>");
}

// ── blockquote stripping tests ────────────────────────────────────────────

#[test]
fn blockquote_strip_removes_leading_fallback_block() {
    let raw = "> Alice said something\n> more quoted\n\nActual reply text\n> own quote";
    assert_eq!(strip_reply_fallback(raw), "Actual reply text\n> own quote");
    // Only a fallback, no text of its own: nothing left to translate.
    assert_eq!(strip_reply_fallback("> quoted\n>\n"), "");
}

#[test]
fn rendered_house_infra_config_parses() {
    // A copy of the config house-infra renders (fake secrets), as deployed.
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/house-infra-config.toml");
    let config: Config = mxbot_common::config::load_toml(std::path::Path::new(path))
        .expect("the config house-infra renders");
    check_ignore_users(&config.translation, &config.room_translations).unwrap();
    config.security.validate().unwrap();
    assert_eq!(config.translation.langs, ["en", "de", "uk"]);
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
// to be a msgtype-matching bug. These tests pin down the matching logic
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
    // filename as `body`: caption() requires filename to be set to
    // recognize body as a distinct caption, per the Matrix spec.
    let content = ImageMessageEventContent::plain("1000006546.jpg".into(), mxc());
    let msgtype = MessageType::Image(content);
    assert_eq!(media_caption_kind(&msgtype), Some(("image", None)));
}

#[test]
fn media_caption_kind_encrypted_image_caption_still_available() {
    // The media file being end-to-end encrypted (MediaSource::Encrypted)
    // is orthogonal to whether a plaintext caption is present: by the
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

// ── source_content of an edit's new content ──────────────────────────────
// Edits of captioned media arrive with the same media msgtype as the
// original (not m.text). These pin down that handle_edit can extract
// text from that shape too, not just m.text/m.emote.

fn extract_edit_text(msgtype: &MessageType) -> Option<String> {
    source_content(msgtype).ok().map(|source| source.plain)
}

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
    let Some(Relation::Thread(t)) = make_relation(&event_id, &root_id, true, false, true) else {
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
    let Some(Relation::Thread(t)) = make_relation(&event_id, &root_id, true, true, false) else {
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
        ignore_users: Vec::new(),
    };

    let effective = effective_translation_config(&default, &rooms, "!room:example.org");
    assert_eq!(effective.langs, vec!["de", "uk"]);
    assert_eq!(effective.min_confidence, 0.8);
    assert!(!effective.reply_to_original);
    assert!(!effective.thread_replies);
    assert!(effective.silent_messages);
}

#[test]
fn ignored_senders_are_global_or_per_room() {
    let default = TranslationConfig {
        ignore_users: vec!["@heating-bot:example.org".to_owned()],
        ..Default::default()
    };
    let mut rooms = HashMap::new();
    rooms.insert(
        "!room:example.org".to_owned(),
        RoomTranslationConfig {
            ignore_users: vec!["@quiz-bot:example.org".to_owned()],
            ..Default::default()
        },
    );
    let ignored = |room: &str, sender: &str| is_ignored_sender(&default, &rooms, room, sender);
    // Global: in every room.
    assert!(ignored("!room:example.org", "@heating-bot:example.org"));
    assert!(ignored("!other:example.org", "@heating-bot:example.org"));
    // Per room: only there, in addition to the global list.
    assert!(ignored("!room:example.org", "@quiz-bot:example.org"));
    assert!(!ignored("!other:example.org", "@quiz-bot:example.org"));
    // Exact IDs only.
    assert!(!ignored("!room:example.org", "@nick:example.org"));
    assert!(!ignored("!room:example.org", "@heating-bot:other.org"));
    check_ignore_users(&default, &rooms).unwrap();
}

#[test]
fn ignore_users_must_be_matrix_ids() {
    let default = TranslationConfig {
        ignore_users: vec!["heating-bot".to_owned()],
        ..Default::default()
    };
    assert!(check_ignore_users(&default, &HashMap::new()).is_err());
    let mut rooms = HashMap::new();
    rooms.insert(
        "!room:example.org".to_owned(),
        RoomTranslationConfig {
            ignore_users: vec!["@ok:example.org".to_owned(), "nope".to_owned()],
            ..Default::default()
        },
    );
    assert!(check_ignore_users(&TranslationConfig::default(), &rooms).is_err());
    // Parsed from TOML, global and per room.
    let parsed: Config = toml::from_str(
        "[matrix]\nhomeserver = \"https://m.x\"\nuser_id = \"@t:x\"\ndevice_id = \"D\"\n\
         [libretranslate]\nurl = \"http://lt\"\n\
         [translation]\nignore_users = [\"@heating-bot:x\"]\n\
         [room_translations.\"!r:x\"]\nignore_users = [\"@quiz-bot:x\"]\n",
    )
    .unwrap();
    assert_eq!(parsed.translation.ignore_users, ["@heating-bot:x"]);
    assert!(is_ignored_sender(
        &parsed.translation,
        &parsed.room_translations,
        "!r:x",
        "@quiz-bot:x"
    ));
    check_ignore_users(&parsed.translation, &parsed.room_translations).unwrap();
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
    // Key is absent: serde invokes the #[serde(default = "default_true")] path.
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
    // Note: no "rel_type" key: pure replies do not have rel_type per Matrix spec.
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
    // an absent is_falling_back is equivalent to false. A genuine thread
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
// A caption goes through handle_message_core like a message and gets its
// relation from the same calls:
//
//   let thread_root = resolve_thread_root(&event);
//   content.relates_to = make_relation(
//       &event.event_id, &thread_root,
//       state.reply_to_original, state.thread_replies, in_thread,
//   );
//
// The tests below reproduce that call sequence directly.
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
    // in_thread=true mirrors what handle_message_core passes for a threaded caption
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
    // record_translation (e.g. below_confidence). The waiter must be
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

#[test]
fn a_failed_translation_is_reported_without_text_or_detail() {
    let failed = |target: &str, kind| TargetTranslationFailure {
        target: target.to_owned(),
        attempts: 3,
        error: TranslationError::new(kind, "the message text and https://backend/?key=x"),
    };
    let failure = TranslationBatchFailure {
        source: "de".to_owned(),
        context: "message",
        failed_targets: vec![
            failed("en", TranslationErrorKind::Timeout),
            failed("fr", TranslationErrorKind::HttpStatus(503)),
            failed("ar", TranslationErrorKind::Timeout),
        ],
        suppressed: false,
    };
    assert_eq!(
        batch_failure_reason(&failure),
        "translation into en, fr, ar failed: backend_http_error, timeout"
    );
}

// ── Local language detection (B1) ─────────────────────────────────────────

fn room_langs() -> Vec<String> {
    vec!["en".to_owned(), "de".to_owned(), "uk".to_owned()]
}

const GERMAN_SHORT: &[&str] = &[
    "ok danke",
    "bis später",
    "guten morgen zusammen",
    "passt",
    "kommst du heute abend?",
    "ich bin gleich da",
    "danke für die info",
    "wer hat noch milch?",
    "das klingt gut",
    "ich komme etwas später",
    "hat jemand meinen schlüssel gesehen?",
    "die waschmaschine ist frei",
    "können wir morgen reden?",
    "alles klar, bis dann",
    "ich koche heute nudeln",
    "wo ist der staubsauger?",
    "schönes wochenende euch allen",
    "mir geht es gut",
];

const ENGLISH_SHORT: &[&str] = &[
    "sounds good",
    "dinner is ready",
    "see you later",
    "thanks a lot",
    "good morning everyone",
    "is anyone home?",
    "I will be late today",
    "where is the vacuum cleaner?",
    "the washing machine is free",
    "can we talk tomorrow?",
    "who has the keys?",
    "have a nice weekend",
    "I am cooking pasta tonight",
    "let me know",
    "that works for me",
    "the package arrived",
    "thank you so much",
    "what time is the meeting?",
];

const UKRAINIAN_SHORT: &[&str] = &[
    "добре",
    "вечеря готова",
    "дякую",
    "доброго ранку всім",
    "до зустрічі",
    "я буду пізніше",
    "хто має ключі?",
    "пральна машина вільна",
    "можемо поговорити завтра?",
    "гарних вихідних",
    "я сьогодні готую макарони",
    "де пилосос?",
    "все гаразд",
    "посилка прийшла",
    "дякую за інформацію",
    "я вже йду",
];

/// Short messages the allow-list detection gets wrong, all English and German taken for each
/// other: too few letters for trigrams to tell them apart.
const KNOWN_MISSES: &[(&str, &str)] = &[
    ("passt", "en"),
    ("sounds good", "de"),
    ("dinner is ready", "de"),
    ("let me know", "de"),
    ("thank you so much", "de"),
];

#[test]
fn short_messages_are_detected_among_the_room_languages() {
    let langs = room_langs();
    let mut right = 0;
    let mut total = 0;
    let mut misses = Vec::new();
    for (expected, list) in [
        ("de", GERMAN_SHORT),
        ("en", ENGLISH_SHORT),
        ("uk", UKRAINIAN_SHORT),
    ] {
        assert!(list.len() >= 15);
        for text in list {
            let prose = prose_for_detection(text);
            assert!(has_translatable_text(&prose), "{text}");
            let detection = detect(&prose, &langs).expect(text);
            assert!(langs.contains(&detection.lang), "{text}: {detection:?}");
            total += 1;
            if detection.lang == expected {
                right += 1;
            } else {
                misses.push((*text, detection.lang));
            }
        }
    }
    let known: Vec<(&str, String)> = KNOWN_MISSES
        .iter()
        .map(|(text, lang)| (*text, (*lang).to_owned()))
        .collect();
    assert_eq!(misses, known);
    assert!(right * 100 >= total * 85, "{right}/{total}");
}

#[test]
fn messages_without_letters_are_not_translatable() {
    for text in [
        "https://example.org/a/b",
        "👍",
        "42",
        "@anna:example.org",
        "`make`",
    ] {
        assert!(!has_translatable_text(&prose_for_detection(text)), "{text}");
    }
    // Two letters are text: detected, and among the room's languages.
    let prose = prose_for_detection("ok");
    assert!(has_translatable_text(&prose));
    assert!(room_langs().contains(&detect(&prose, &room_langs()).unwrap().lang));
}

#[test]
fn links_ids_and_code_are_left_out_of_detection() {
    let text = "Schau mal hier https://www.example.org/the-best-recipes/pumpkin-soup-with-ginger?ref=share \
                und hier https://example.org/news/weather-today, @anna:example.org kennt `make build` schon";
    let prose = prose_for_detection(text);
    assert_eq!(prose, "Schau mal hier und hier , kennt schon");
    assert_eq!(detect(&prose, &room_langs()).unwrap().lang, "de");
    assert_eq!(
        prose_for_detection(
            "siehe ```\nlet x = 1;\n``` und #room:example.org oder !abc:example.org"
        ),
        "siehe und oder"
    );
    // Not IDs: no server part.
    assert_eq!(prose_for_detection("!translate @ #1:"), "!translate @ #1:");
}

#[tokio::test]
async fn an_unreliable_message_takes_the_senders_last_reliable_language() {
    let memory = SenderLanguages::default();
    let langs = room_langs();
    let detect = |text: &'static str, sender: &'static str| {
        let memory = memory.clone();
        let langs = langs.clone();
        async move {
            detect_source_language(
                &memory,
                &langs,
                "!room:example.org",
                sender,
                text,
                |_| async { panic!("no backend detection for known languages") },
            )
            .await
        }
    };
    // "ok" alone is not reliable.
    assert!(!detect("ok", "@ben:example.org")
        .await
        .is_ok_and(|(_, c)| c > 0.9));
    let (lang, _) = detect(
        "Привіт усім, я сьогодні трохи запізнюся",
        "@anna:example.org",
    )
    .await
    .unwrap();
    assert_eq!(lang, "uk");
    assert_eq!(
        detect("ok", "@anna:example.org").await,
        Ok(("uk".to_owned(), 1.0))
    );
    // Per sender.
    assert_ne!(detect("ok", "@ben:example.org").await.unwrap().0, "uk");
    memory.lock().unwrap().insert(
        (
            "!room:example.org".to_owned(),
            "@ben:example.org".to_owned(),
        ),
        "de".to_owned(),
    );
    assert_eq!(
        detect("sounds good", "@ben:example.org").await,
        Ok(("de".to_owned(), 1.0))
    );
    assert_eq!(
        detect("https://example.org/a/b", "@anna:example.org").await,
        Err(SkipReason::NoTranslatableText)
    );
}

#[tokio::test]
async fn a_room_language_unknown_to_whatlang_asks_the_backend() {
    let memory = SenderLanguages::default();
    let langs = vec!["de".to_owned(), "eu".to_owned()];
    assert_eq!(unsupported_langs(&langs), ["eu"]);
    assert!(unsupported_langs(&room_langs()).is_empty());
    let asked = AtomicUsize::new(0);
    let detected = detect_source_language(
        &memory,
        &langs,
        "!room:example.org",
        "@anna:example.org",
        "Kaixo, zer moduz https://example.org/x",
        |prose| {
            asked.fetch_add(1, Ordering::SeqCst);
            assert_eq!(prose, "Kaixo, zer moduz");
            async { Some(("eu".to_owned(), 0.8)) }
        },
    )
    .await;
    assert_eq!(detected, Ok(("eu".to_owned(), 0.8)));
    assert_eq!(asked.load(Ordering::SeqCst), 1);
}

// ── The pipeline with a scripted translator (B2-B5) ──────────────────────

type Answer = dyn Fn(&str, &str, TextFormat) -> Result<String, TranslationError> + Send + Sync;

/// A backend answering with `answer(text, target, format)` and recording every request.
struct FakeTranslator {
    answer: Box<Answer>,
    calls: std::sync::Mutex<Vec<(String, String, TextFormat)>>,
}

impl FakeTranslator {
    fn new(
        answer: impl Fn(&str, &str, TextFormat) -> Result<String, TranslationError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            answer: Box::new(answer),
            calls: Default::default(),
        }
    }

    /// Prefixes the target language: a translation that differs from its source.
    fn tagging() -> Self {
        Self::new(|text, target, _| Ok(tag(text, target)))
    }

    fn calls(&self) -> Vec<(String, String, TextFormat)> {
        self.calls.lock().unwrap().clone()
    }
}

impl Translator for FakeTranslator {
    fn translate<'a>(
        &'a self,
        text: &'a str,
        _source: &'a str,
        target: &'a str,
        format: TextFormat,
    ) -> BoxFuture<'a, Result<String, TranslationError>> {
        self.calls
            .lock()
            .unwrap()
            .push((text.to_owned(), target.to_owned(), format));
        let answer = (self.answer)(text, target, format);
        async move { answer }.boxed()
    }

    fn detect<'a>(&'a self, _text: &'a str) -> BoxFuture<'a, Option<(String, f64)>> {
        async { None }.boxed()
    }
}

/// `text` marked as translated into `target`, inside a leading paragraph.
fn tag(text: &str, target: &str) -> String {
    match text.strip_prefix("<p>") {
        Some(rest) => format!("<p>[{target}] {rest}"),
        None => format!("[{target}] {text}"),
    }
}

struct Harness {
    stats: Stats,
    memory: SenderLanguages,
    translation: EffectiveTranslationConfig,
    event_id: OwnedEventId,
}

impl Harness {
    fn new() -> Self {
        let mut translation = test_translation_config();
        // As in production: detection inside the room's languages decides, not a threshold.
        translation.min_confidence = 0.0;
        Self {
            stats: Stats::new(Db::open_in_memory().unwrap()),
            memory: SenderLanguages::default(),
            translation,
            event_id: eid("$ev:example.org"),
        }
    }

    fn pipeline(&self, context: &'static str) -> Pipeline<'_> {
        Pipeline {
            translation: &self.translation,
            policy: test_policy(),
            max_input_chars: DEFAULT_MAX_INPUT_CHARS,
            stats: &self.stats,
            memory: &self.memory,
            context,
            room_id: "!room:example.org",
            sender: "@anna:example.org",
            event_id: &self.event_id,
        }
    }

    async fn run(
        &self,
        translator: &FakeTranslator,
        source: &SourceContent,
    ) -> Result<Translation, NotTranslated> {
        translate_source(translator, &self.pipeline("message"), source).await
    }
}

const GERMAN_SENTENCE: &str =
    "Das Abendessen ist heute um sieben Uhr fertig, bringt bitte Teller mit.";
const GERMAN_HTML: &str =
    "<p>Das Abendessen ist heute um <strong>sieben Uhr</strong> fertig, bringt bitte Teller mit.</p>";

fn formatted_text() -> TextMessageEventContent {
    TextMessageEventContent::html(GERMAN_SENTENCE, GERMAN_HTML)
}

fn formatted_image() -> MessageType {
    let mut image = ImageMessageEventContent::plain(GERMAN_SENTENCE.into(), mxc());
    image.filename = Some("dinner.jpg".into());
    image.formatted = Some(FormattedBody::html(GERMAN_HTML));
    MessageType::Image(image)
}

fn translated(result: Result<Translation, NotTranslated>) -> Translation {
    match result {
        Ok(translation) => translation,
        Err(e) => panic!("not translated: {e:?}"),
    }
}

#[tokio::test]
async fn message_caption_and_edit_give_the_same_lines() {
    let harness = Harness::new();
    let message = MessageType::Text(formatted_text());
    let edit = RoomMessageEventContentWithoutRelation::new(MessageType::Text(formatted_text()));
    let mut lines = Vec::new();
    for (context, msgtype) in [
        ("message", &message),
        ("image", &formatted_image()),
        ("edit", &edit.msgtype),
    ] {
        let source = source_content(msgtype).unwrap();
        assert_eq!(source.html.as_deref(), Some(GERMAN_HTML), "{context}");
        let translator = FakeTranslator::tagging();
        let result =
            translated(translate_source(&translator, &harness.pipeline(context), &source).await);
        assert_eq!(result.lang, "de");
        assert!(translator.calls().iter().all(|c| c.2 == TextFormat::Html));
        lines.push(result.lines);
    }
    assert_eq!(lines[0], lines[1]);
    assert_eq!(lines[0], lines[2]);
    assert_eq!(
        lines[0][0].html,
        "[en] Das Abendessen ist heute um <strong>sieben Uhr</strong> fertig, bringt bitte Teller mit."
    );
}

#[tokio::test]
async fn an_edit_and_a_caption_keep_their_formatting() {
    let harness = Harness::new();
    let edit = RoomMessageEventContentWithoutRelation::new(MessageType::Text(formatted_text()));
    let source = source_content(&edit.msgtype).unwrap();
    let result = translated(
        translate_source(
            &FakeTranslator::tagging(),
            &harness.pipeline("edit"),
            &source,
        )
        .await,
    );
    let content = make_translation_edit(&result.lines, false, eid("$bot:example.org"));
    let json = serde_json::to_value(&content).unwrap();
    assert_eq!(json["m.relates_to"]["rel_type"], "m.replace");
    assert_eq!(json["m.relates_to"]["event_id"], "$bot:example.org");
    let new_content = &json["m.new_content"];
    assert_eq!(new_content["format"], "org.matrix.custom.html");
    assert!(new_content["formatted_body"]
        .as_str()
        .unwrap()
        .contains("<strong>sieben Uhr</strong>"));
    assert!(new_content["body"]
        .as_str()
        .unwrap()
        .starts_with("🇬🇧 [en] Das"));
    assert!(json["body"].as_str().unwrap().starts_with("* 🇬🇧 [en] Das"));

    let source = source_content(&formatted_image()).unwrap();
    let result = translated(
        translate_source(
            &FakeTranslator::tagging(),
            &harness.pipeline("image"),
            &source,
        )
        .await,
    );
    let (plain, html) = build_translation_bodies(&result.lines);
    let json = serde_json::to_value(make_translation_content(plain, html, false)).unwrap();
    assert!(json["formatted_body"]
        .as_str()
        .unwrap()
        .contains("<strong>sieben Uhr</strong>"));
}

#[tokio::test]
async fn a_backend_answering_503_gets_max_attempts_requests_per_target_and_no_text_fallback() {
    let harness = Harness::new();
    let translator = FakeTranslator::new(|_, _, _| {
        Err(TranslationError::new(
            TranslationErrorKind::HttpStatus(503),
            "unavailable",
        ))
    });
    let source = source_content(&MessageType::Text(formatted_text())).unwrap();
    let Err(NotTranslated::Failed(failure)) = harness.run(&translator, &source).await else {
        panic!("expected a failed batch");
    };
    let calls = translator.calls();
    for target in ["en", "uk"] {
        let requests: Vec<_> = calls.iter().filter(|c| c.1 == target).collect();
        assert_eq!(requests.len(), test_policy().max_attempts, "{target}");
        assert!(requests.iter().all(|c| c.2 == TextFormat::Html), "{target}");
    }
    assert!(should_retry_later(&failure));
}

#[tokio::test]
async fn a_rejected_html_translation_falls_back_to_text() {
    let harness = Harness::new();
    let translator = FakeTranslator::new(|text, target, format| match format {
        TextFormat::Html => Err(TranslationError::new(
            TranslationErrorKind::HttpStatus(400),
            "bad request",
        )),
        TextFormat::Text => Ok(tag(text, target)),
    });
    let source = source_content(&MessageType::Text(formatted_text())).unwrap();
    let result = translated(harness.run(&translator, &source).await);
    assert_eq!(result.lines.len(), 2);
    let formats: Vec<TextFormat> = translator.calls().iter().map(|c| c.2).collect();
    assert_eq!(
        formats.iter().filter(|f| **f == TextFormat::Html).count(),
        2
    );
    assert_eq!(
        formats.iter().filter(|f| **f == TextFormat::Text).count(),
        2
    );
}

// ── Placeholders (B3) ────────────────────────────────────────────────────

const PILL: &str = "<a href=\"https://matrix.to/#/@anna:example.org\">Anna</a>";

/// (name, formatted_body, what the backend must not see)
const FIXTURES: &[(&str, &str, &[&str])] = &[
    (
        "pill",
        "Hallo <a href=\"https://matrix.to/#/@anna:example.org\">Anna</a>, kommst du heute?",
        &["matrix.to", "@anna"],
    ),
    (
        "two pills",
        "<a href=\"https://matrix.to/#/@anna:example.org\">Anna</a> und \
         <a href=\"https://matrix.to/#/@ben:example.org\">Ben</a>: das Essen ist fertig",
        &["matrix.to", "Ben"],
    ),
    (
        "bare URL",
        "Siehe <a href=\"https://example.org/a/b?c=d&amp;e=f\">https://example.org/a/b?c=d&amp;e=f</a> für Details",
        &["https://", "example.org"],
    ),
    (
        "link with text",
        "Hier ist das <a href=\"https://example.org/rezept\">Rezept</a> für heute",
        &["https://", "example.org"],
    ),
    (
        "inline code",
        "Bitte zuerst <code>make build</code> ausführen",
        &["make build"],
    ),
    (
        "code block with a comment",
        "<p>Probier das:</p>\n<pre><code class=\"language-sh\"># Dienst neu starten\nsystemctl restart example\n</code></pre>\n",
        &["Dienst", "systemctl"],
    ),
    (
        "custom emoji",
        "Gut gemacht <img data-mx-emoticon src=\"mxc://example.org/abc\" alt=\":party:\" title=\":party:\" height=\"32\">",
        &["mxc://", ":party:"],
    ),
    (
        "spoiler",
        "Das Ende: <span data-mx-spoiler>alle überleben</span>",
        &["überleben"],
    ),
    (
        "reply fallback",
        "<mx-reply><blockquote><a href=\"https://matrix.to/#/!room:example.org/$ev\">In reply to</a> \
         <a href=\"https://matrix.to/#/@anna:example.org\">@anna:example.org</a><br>Ursprünglicher Text</blockquote></mx-reply>\
         Die Antwort ist ja",
        &["Ursprünglicher", "matrix.to", "In reply to"],
    ),
    (
        "nested lists",
        "<ul><li>Erstens<ul><li>Innen eins</li><li>Innen zwei</li></ul></li><li>Zweitens</li></ul>",
        &[],
    ),
];

fn html_source(html: &str) -> SourceContent {
    SourceContent {
        plain: html_to_plain(html),
        html: Some(html.to_owned()),
    }
}

#[test]
fn an_identity_translation_restores_every_fixture() {
    for (name, html, hidden) in FIXTURES {
        let (protected, segments) = protect(&html_source(html));
        let sent_html = protected.html.as_deref().unwrap();
        for sent in [sent_html, protected.plain.as_str()] {
            for word in *hidden {
                assert!(!sent.contains(word), "{name}: {word} in {sent}");
            }
        }
        let restored = restore(sent_html, sent_html, TextFormat::Html, &segments).unwrap();
        let expected = outgoing_html(html);
        assert_eq!(restored.html, expected, "{name}");
        assert_eq!(restored.plain, html_to_plain(&expected), "{name}");
        assert!(!restored.html.contains("@@") && !restored.plain.contains("@@"));
    }
}

#[test]
fn plain_text_protects_links_ids_and_code() {
    let source = SourceContent {
        plain: "Schau https://example.org/a?b=c, @anna:example.org: `make build` und \
                ```\nlet x = 1;\n``` sowie @@7@@ und nochmal https://example.org/a?b=c."
            .to_owned(),
        html: None,
    };
    let (protected, segments) = protect(&source);
    assert_eq!(
        protected.plain,
        "Schau @@1@@, @@2@@: @@3@@ und @@4@@ sowie @@5@@ und nochmal @@1@@."
    );
    assert_eq!(segments.len(), 5);
    let restored = restore(
        &protected.plain,
        &protected.plain,
        TextFormat::Text,
        &segments,
    )
    .unwrap();
    assert_eq!(restored.plain, source.plain);
    assert_eq!(
        restored.html,
        html_escape(&source.plain).replace('\n', "<br>")
    );
}

#[test]
fn a_lost_or_repeated_placeholder_is_lost() {
    let (protected, segments) = protect(&html_source(FIXTURES[1].1));
    let sent = protected.html.unwrap();
    assert!(sent.contains("@@1@@") && sent.contains("@@2@@"));
    let dropped = sent.replace("@@2@@", "");
    assert_eq!(
        restore(&dropped, &sent, TextFormat::Html, &segments),
        Err(Lost)
    );
    let doubled = format!("{sent} @@1@@");
    assert_eq!(
        restore(&doubled, &sent, TextFormat::Html, &segments),
        Err(Lost)
    );
    let invented = format!("{sent} @@9@@");
    assert_eq!(
        restore(&invented, &sent, TextFormat::Html, &segments),
        Err(Lost)
    );
}

#[tokio::test]
async fn the_restored_pill_and_spoiler_are_the_originals() {
    let harness = Harness::new();
    let source = html_source(&format!(
        "Hallo {PILL}, das Ende des Buches: <span data-mx-spoiler>alle überleben</span>"
    ));
    let translator = FakeTranslator::tagging();
    let result = translated(harness.run(&translator, &source).await);
    for line in &result.lines {
        assert!(line.html.contains(PILL), "{}", line.html);
        assert!(line
            .html
            .contains("<span data-mx-spoiler=\"\">alle überleben</span>"));
        assert!(line.plain.contains("Anna") && line.plain.contains(SPOILER_PLAIN));
        assert!(!line.plain.contains("überleben"));
    }
    for (sent, _, _) in translator.calls() {
        assert!(
            !sent.contains("matrix.to") && !sent.contains("überleben"),
            "{sent}"
        );
    }
}

#[tokio::test]
async fn a_lost_placeholder_is_retried_as_plain_text_then_fails_the_target() {
    let harness = Harness::new();
    let source = html_source(&format!("Hallo {PILL}, kommst du heute Abend zum Essen?"));

    // Drops the placeholder in HTML only: the plain-text retry restores it.
    let translator = FakeTranslator::new(|text, target, format| {
        Ok(match format {
            TextFormat::Html => tag(&text.replace("@@1@@", ""), target),
            TextFormat::Text => tag(text, target),
        })
    });
    let result = translated(harness.run(&translator, &source).await);
    for target in ["en", "uk"] {
        let formats: Vec<_> = translator
            .calls()
            .into_iter()
            .filter(|c| c.1 == target)
            .map(|c| c.2)
            .collect();
        assert_eq!(formats, [TextFormat::Html, TextFormat::Text], "{target}");
    }
    for line in &result.lines {
        assert!(line.html.contains(PILL), "{}", line.html);
        assert!(!line.html.contains("@@") && !line.plain.contains("@@"));
    }

    // Drops it both times: the target fails, not retried later.
    let translator =
        FakeTranslator::new(|text, target, _| Ok(tag(&text.replace("@@1@@", ""), target)));
    let Err(NotTranslated::Failed(failure)) = harness.run(&translator, &source).await else {
        panic!("expected a failed batch");
    };
    assert!(failure
        .failed_targets
        .iter()
        .all(|f| f.error.class() == "validation_rejected"));
    assert_eq!(translator.calls().len(), 4);
    assert!(!should_retry_later(&failure));
}

#[test]
fn length_checks_count_a_placeholder_not_the_long_url() {
    let long_url = format!("https://example.org/{}", "a".repeat(400));
    let source = SourceContent {
        plain: format!("schau mal {long_url}"),
        html: None,
    };
    let (protected, segments) = protect(&source);
    assert_eq!(protected.plain, "schau mal @@1@@");
    // A short answer around the placeholder is accepted although the restored line is long.
    let line = build_line(
        "en",
        &protected.plain,
        "look @@1@@",
        TextFormat::Text,
        &segments,
    )
    .unwrap();
    assert_eq!(line.plain, format!("look {long_url}"));
    // A runaway answer is caught although the URL would have hidden it.
    let runaway = format!("look {} @@1@@", "again ".repeat(60));
    assert!(build_line(
        "en",
        &protected.plain,
        &runaway,
        TextFormat::Text,
        &segments
    )
    .is_err());
}

// ── Safe output (B4) ─────────────────────────────────────────────────────

#[test]
fn a_translation_and_an_edit_carry_empty_mentions() {
    let json = serde_json::to_value(make_translation_content(
        "🇬🇧 Hello @room".into(),
        "🇬🇧 Hello @room".into(),
        false,
    ))
    .unwrap();
    assert_eq!(json["m.mentions"], serde_json::json!({}));
    let edit = make_translation_edit(&[test_line("en")], true, eid("$bot:example.org"));
    let json = serde_json::to_value(edit).unwrap();
    assert_eq!(json["m.mentions"], serde_json::json!({}));
    assert_eq!(json["m.new_content"]["m.mentions"], serde_json::json!({}));
    assert_eq!(json["m.new_content"]["msgtype"], "m.notice");
}

#[tokio::test]
async fn markup_outside_the_matrix_subset_is_removed_from_the_output() {
    let harness = Harness::new();
    let translator = FakeTranslator::new(|text, target, _| {
        Ok(format!(
            "<script>alert(1)</script><mx-reply>quoted</mx-reply>\
             <b onclick=\"steal()\">[{target}]</b> {text}<style>p {{}}</style>"
        ))
    });
    let source = source_content(&MessageType::Text(formatted_text())).unwrap();
    let result = translated(harness.run(&translator, &source).await);
    for line in &result.lines {
        for gone in [
            "<script", "alert", "onclick", "mx-reply", "quoted", "<style",
        ] {
            assert!(!line.html.contains(gone), "{gone} in {}", line.html);
            assert!(!line.plain.contains(gone), "{gone} in {}", line.plain);
        }
        assert!(line.html.starts_with("<b>["));
    }
}

#[tokio::test]
async fn a_translation_equal_to_the_source_is_left_out() {
    let harness = Harness::new();
    let source = SourceContent {
        plain: "ok".to_owned(),
        html: None,
    };
    let echo = FakeTranslator::new(|text, _, _| Ok(format!(" {}! ", text.to_uppercase())));
    assert!(matches!(
        harness.run(&echo, &source).await,
        Err(NotTranslated::Skipped(SkipReason::TranslationUnchanged))
    ));

    // English taken for German (the sender's remembered language): only Ukrainian is new.
    harness.memory.lock().unwrap().insert(
        (
            "!room:example.org".to_owned(),
            "@anna:example.org".to_owned(),
        ),
        "de".to_owned(),
    );
    let source = SourceContent {
        plain: "dinner is ready".to_owned(),
        html: None,
    };
    let translator = FakeTranslator::new(|text, target, _| {
        Ok(match target {
            "en" => "Dinner is ready.".to_owned(),
            _ => tag(text, target),
        })
    });
    let result = translated(harness.run(&translator, &source).await);
    assert_eq!(result.lang, "de");
    assert_eq!(line_targets(&result.lines), ["uk"]);
}

#[test]
fn an_edit_with_nothing_left_to_translate_replaces_nothing_and_redacts() {
    for reason in [
        SkipReason::EditNoTranslatableText,
        SkipReason::NoTranslatableText,
        SkipReason::NoRemainingTargets,
        SkipReason::TranslationUnchanged,
    ] {
        assert!(edit_leaves_nothing_to_translate(reason), "{reason:?}");
    }
    for reason in [
        SkipReason::InputTooLong,
        SkipReason::LanguageNotConfigured,
        SkipReason::TranslationSuppressed,
    ] {
        assert!(!edit_leaves_nothing_to_translate(reason), "{reason:?}");
    }
}

// ── Late retries (B5) ────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn a_transient_failure_is_retried_at_five_minutes_and_not_again() {
    let harness = Harness::new();
    let source = source_content(&MessageType::Text(formatted_text())).unwrap();
    let down = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let translator = {
        let down = Arc::clone(&down);
        FakeTranslator::new(move |text, target, _| {
            if down.load(Ordering::SeqCst) {
                Err(TranslationError::new(
                    TranslationErrorKind::Request,
                    "refused",
                ))
            } else {
                Ok(tag(text, target))
            }
        })
    };
    let Err(NotTranslated::Failed(failure)) = harness.run(&translator, &source).await else {
        panic!("expected a failed batch");
    };
    assert!(should_retry_later(&failure));

    down.store(false, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let attempts = std::sync::Mutex::new(Vec::new());
    let (h, t, s) = (&harness, &translator, &source);
    run_late_retries(&LATE_RETRIES_AFTER_FAILURE, &harness.stats, || {
        attempts.lock().unwrap().push(started.elapsed());
        async move {
            match h.run(t, s).await {
                Ok(_) => Reprocessed::Ran(CoreOutcome::Translated),
                Err(e) => panic!("{e:?}"),
            }
        }
    })
    .await;
    assert_eq!(*attempts.lock().unwrap(), [Duration::from_secs(300)]);
    assert_eq!(harness.stats.late_retries.load(Ordering::Relaxed), 1);
    assert_eq!(
        harness.stats.late_retries_succeeded.load(Ordering::Relaxed),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn an_undecryptable_event_is_retried_twice_at_most() {
    let stats = Stats::new(Db::open_in_memory().unwrap());
    let started = tokio::time::Instant::now();
    let attempts = std::sync::Mutex::new(Vec::new());
    run_late_retries(&LATE_RETRIES_UNDECRYPTABLE, &stats, || {
        attempts.lock().unwrap().push(started.elapsed());
        async { Reprocessed::Undecryptable }
    })
    .await;
    assert_eq!(
        *attempts.lock().unwrap(),
        [Duration::from_secs(30), Duration::from_secs(300)]
    );
    assert_eq!(stats.late_retries_succeeded.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn an_event_translated_in_the_meantime_is_skipped_as_already_translated() {
    let db = Db::open_in_memory().unwrap();
    let stats = Stats::new(db.clone());
    let event_id = eid("$ev:example.org");
    db.record_translation(event_id.as_str(), "$bot:example.org", "!room:example.org")
        .await
        .unwrap();
    let ran = AtomicUsize::new(0);
    let (db_ref, stats_ref, event_ref, ran_ref) = (&db, &stats, &event_id, &ran);
    run_late_retries(&LATE_RETRIES_AFTER_FAILURE, &stats, move || {
        reprocess_unless_translated(
            db_ref,
            stats_ref,
            "!room:example.org",
            event_ref,
            async move {
                ran_ref.fetch_add(1, Ordering::SeqCst);
                Reprocessed::Ran(CoreOutcome::Translated)
            },
        )
    })
    .await;
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert_eq!(stats.late_retries.load(Ordering::Relaxed), 1);
    assert!(stats
        .skipped_snapshot()
        .contains(&("already_translated", 1)));
}

#[test]
fn only_a_batch_that_failed_for_passing_reasons_is_retried_later() {
    let batch = |kinds: &[TranslationErrorKind]| TranslationBatchFailure {
        source: "de".to_owned(),
        context: "message",
        failed_targets: kinds
            .iter()
            .map(|kind| test_failure("en", 3, kind.clone()))
            .collect(),
        suppressed: true,
    };
    use TranslationErrorKind::*;
    assert!(should_retry_later(&batch(&[Timeout, OverallTimeout])));
    assert!(should_retry_later(&batch(&[HttpStatus(503), BackendBusy])));
    assert!(!should_retry_later(&batch(&[Timeout, Rejected])));
    assert!(!should_retry_later(&batch(&[HttpStatus(400)])));
    assert!(!should_retry_later(&batch(&[Empty])));
    assert!(!should_retry_later(&batch(&[])));
}

// ── Real backend run (skipped unless TRANSLATE_BOT_TEST_BACKEND is set) ──

/// A LibreTranslate-compatible backend at `url`, through the bot's own request code.
struct LiveBackend {
    http: reqwest::Client,
    url: String,
}

impl Translator for LiveBackend {
    fn translate<'a>(
        &'a self,
        text: &'a str,
        source: &'a str,
        target: &'a str,
        format: TextFormat,
    ) -> BoxFuture<'a, Result<String, TranslationError>> {
        async move {
            let request = TranslateRequest {
                q: text,
                source,
                target,
                format: format.as_str(),
                api_key: None,
            };
            post_translate(&self.http, &self.url, &request, Duration::from_secs(600)).await
        }
        .boxed()
    }

    fn detect<'a>(&'a self, _text: &'a str) -> BoxFuture<'a, Option<(String, f64)>> {
        async { None }.boxed()
    }
}

/// Sends six generic messages through protect, the backend and restore into the other room
/// languages and prints each step. Run with
/// `TRANSLATE_BOT_TEST_BACKEND=http://host:port cargo test live_backend -- --nocapture`.
#[tokio::test]
async fn live_backend_round_trip() {
    let Ok(url) = std::env::var("TRANSLATE_BOT_TEST_BACKEND") else {
        eprintln!("skipped: TRANSLATE_BOT_TEST_BACKEND is not set");
        return;
    };
    let backend = LiveBackend {
        http: reqwest::Client::new(),
        url: url.trim_end_matches('/').to_owned(),
    };
    let plain = |text: &str| SourceContent {
        plain: text.to_owned(),
        html: None,
    };
    let messages = [
        (
            "pill",
            html_source(&format!("Hallo {PILL}, kommst du heute Abend zum Essen?")),
        ),
        (
            "link",
            plain(
                "Ich habe das Rezept gefunden: \
                 https://www.example.org/rezepte/2026/kuerbis-suppe-mit-ingwer?ref=share \
                 Wer kocht mit?",
            ),
        ),
        (
            "code",
            html_source(
                "Bitte vor dem Neustart <code>make build</code> ausführen und dann \
                 <em>neu starten</em>.",
            ),
        ),
        (
            "spoiler",
            html_source(
                "Achtung, das Ende des Films: <span data-mx-spoiler>der Hund überlebt</span>!",
            ),
        ),
        ("short German", plain("bis später, ich bring Brot mit")),
        ("short Ukrainian", plain("вечеря готова, приходьте")),
    ];
    let langs = vec!["de".to_owned(), "en".to_owned(), "uk".to_owned()];
    let memory = SenderLanguages::default();
    let (mut sent_count, mut back_first, mut back_after_retry) = (0, 0, 0);
    for (name, source) in &messages {
        let (protected, segments) = protect(source);
        let (lang, _) = detect_source_language(
            &memory,
            &langs,
            "!room:example.org",
            "@anna:example.org",
            &protected.plain,
            |_| async { None },
        )
        .await
        .unwrap();
        println!("### {name}\n");
        println!(
            "- source: `{}`",
            source.html.as_deref().unwrap_or(&source.plain)
        );
        println!("- detected: `{lang}`");
        let (sent, format) = match &protected.html {
            Some(html) => (html.as_str(), TextFormat::Html),
            None => (protected.plain.as_str(), TextFormat::Text),
        };
        println!("- sent ({}): `{sent}`", format.as_str());
        for target in langs.iter().filter(|l| **l != lang) {
            sent_count += 1;
            let started = Instant::now();
            let raw = backend
                .translate(sent, &lang, target, format)
                .await
                .unwrap();
            let took = started.elapsed().as_secs_f32();
            println!("- **{target}** ({took:.1} s) raw: `{raw}`");
            match restore(&raw, sent, format, &segments) {
                Ok(restored) => {
                    back_first += 1;
                    back_after_retry += 1;
                    println!("  - html: `{}`", restored.html);
                    println!("  - plain: `{}`", restored.plain);
                }
                Err(Lost) => {
                    println!("  - placeholder LOST");
                    if format == TextFormat::Html {
                        let raw = backend
                            .translate(&protected.plain, &lang, target, TextFormat::Text)
                            .await
                            .unwrap();
                        println!("  - plain-text retry raw: `{raw}`");
                        match restore(&raw, &protected.plain, TextFormat::Text, &segments) {
                            Ok(restored) => {
                                back_after_retry += 1;
                                println!("  - html: `{}`", restored.html);
                            }
                            Err(Lost) => println!("  - placeholder LOST again: target fails"),
                        }
                    }
                }
            }
        }
        println!();
    }
    println!(
        "Placeholders intact: {back_first}/{sent_count} on the first request, \
         {back_after_retry}/{sent_count} after the plain-text retry."
    );
}
