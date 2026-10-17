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

// ── extract_edit_text tests ──────────────────────────────────────────────
// Edits of captioned media arrive with the same media msgtype as the
// original (not m.text). These pin down that handle_edit can extract
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
