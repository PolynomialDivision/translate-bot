//! Translating one message into all target languages and vetting the output.

use super::*;

pub(crate) fn translation_rejection_reason(
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

pub(crate) fn contains_introduced_model_control_token(source: &str, translated: &str) -> bool {
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

pub(crate) fn has_repeated_ngram_loop(text: &str, ngram_len: usize, min_repeats: usize) -> bool {
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

/// Translates the protected source into `target`: as HTML when the source has formatting, then,
/// after a failure that is not transient (rejected, empty, a lost placeholder, 4xx), once more as
/// plain text without formatting.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn translate_target<T: Translator + ?Sized>(
    translator: &T,
    protected: &Protected,
    segments: &[Segment],
    source: &str,
    target: &str,
    policy: &TranslationRetryPolicy,
    attempts_by_target: Arc<Mutex<HashMap<String, usize>>>,
    stats: &Stats,
) -> Result<TranslatedLine, TargetTranslationFailure> {
    if let Some(html) = &protected.html {
        let html_result = retry_translation_operation(
            target,
            policy,
            attempts_by_target.clone(),
            stats,
            || async {
                let translated = translator
                    .translate(html, source, target, TextFormat::Html)
                    .await?;
                build_line(target, html, &translated, TextFormat::Html, segments)
            },
        )
        .await;
        match html_result {
            Ok(line) => return Ok(line),
            Err(failure) if failure.error.is_transient() => return Err(failure),
            Err(failure) => warn!(
                "HTML translation target={} failed after {} attempts class={} detail={}: falling back to text",
                failure.target,
                failure.attempts,
                failure.error.class(),
                failure.error.detail
            ),
        }
    }
    retry_translation_operation(target, policy, attempts_by_target, stats, || async {
        let translated = translator
            .translate(&protected.plain, source, target, TextFormat::Text)
            .await?;
        build_line(
            target,
            &protected.plain,
            &translated,
            TextFormat::Text,
            segments,
        )
    })
    .await
}

/// Decide whether a detected message should be translated and, if so, into
/// which target languages. Every rejection path logs its specific reason,
/// without this, a message skipped here leaves no trace in the logs, making
/// it indistinguishable from a message that was never received at all.
pub(crate) fn resolve_translation_targets(
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

/// What one run of the pipeline needs besides the source and the backend.
pub(crate) struct Pipeline<'a> {
    pub(crate) translation: &'a EffectiveTranslationConfig,
    pub(crate) policy: TranslationRetryPolicy,
    pub(crate) max_input_chars: usize,
    pub(crate) stats: &'a Stats,
    pub(crate) memory: &'a SenderLanguages,
    /// "message", the media kind of a caption, or "edit": in logs and outcome records.
    pub(crate) context: &'static str,
    pub(crate) room_id: &'a str,
    pub(crate) sender: &'a str,
    pub(crate) event_id: &'a OwnedEventId,
}

/// A message's translation: the detected source language and one line per target posted.
#[derive(Debug)]
pub(crate) struct Translation {
    pub(crate) lang: String,
    pub(crate) lines: Vec<TranslatedLine>,
}

#[derive(Debug)]
pub(crate) enum NotTranslated {
    Skipped(SkipReason),
    /// Already logged and counted.
    Failed(TranslationBatchFailure),
}

/// From a message's text to its translation lines, the same for messages, captions and edits:
/// protect, detect, choose targets, translate all, leave out what came back unchanged.
pub(crate) async fn translate_source<T: Translator + ?Sized>(
    translator: &T,
    pipeline: &Pipeline<'_>,
    source: &SourceContent,
) -> Result<Translation, NotTranslated> {
    let Pipeline {
        translation,
        context,
        room_id,
        event_id,
        ..
    } = *pipeline;
    if source.plain.chars().count() > pipeline.max_input_chars {
        return Err(NotTranslated::Skipped(SkipReason::InputTooLong));
    }
    let (protected, segments) = protect(source);

    let (lang, confidence) = detect_source_language(
        pipeline.memory,
        &translation.langs,
        room_id,
        pipeline.sender,
        &protected.plain,
        |prose| async move { translator.detect(&prose).await },
    )
    .await
    .map_err(|reason| {
        if reason == SkipReason::LangDetectFailed {
            warn!("Language detection failed for {context} {event_id} in {room_id}");
        }
        NotTranslated::Skipped(reason)
    })?;
    debug!("{context} {event_id} lang={lang} conf={confidence:.2} room={room_id}");

    let targets =
        resolve_translation_targets(context, room_id, event_id, &lang, confidence, translation)
            .map_err(NotTranslated::Skipped)?;

    let lines = translate_all_targets(translator, pipeline, &protected, &segments, &lang, targets)
        .await
        .map_err(NotTranslated::Failed)?;

    // What an identity translation would give: a line equal to it says nothing new.
    let unchanged = restore(
        &protected.plain,
        &protected.plain,
        TextFormat::Text,
        &segments,
    )
    .map(|r| r.plain)
    .unwrap_or_default();
    let lines: Vec<TranslatedLine> = lines
        .into_iter()
        .filter(|line| {
            let same = same_text(&line.plain, &unchanged);
            if same {
                info!(
                    "{context} {event_id} in {room_id}: the {} translation equals the source, left out",
                    line.target
                );
            }
            !same
        })
        .collect();
    if lines.is_empty() {
        return Err(NotTranslated::Skipped(SkipReason::TranslationUnchanged));
    }
    Ok(Translation { lang, lines })
}

/// Equal apart from case, whitespace and final punctuation.
pub(crate) fn same_text(a: &str, b: &str) -> bool {
    let normal = |s: &str| {
        s.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
            .trim_end_matches(|c: char| c.is_whitespace() || is_final_punctuation(c))
            .to_owned()
    };
    normal(a) == normal(b)
}

fn is_final_punctuation(c: char) -> bool {
    matches!(
        c,
        '.' | '!' | '?' | ',' | ';' | ':' | '…' | '。' | '！' | '？' | '¡' | '¿'
    )
}

/// Whether a failed batch is worth a late retry: every target failed transiently or by the
/// overall deadline, none was rejected.
pub(crate) fn should_retry_later(failure: &TranslationBatchFailure) -> bool {
    !failure.failed_targets.is_empty()
        && failure
            .failed_targets
            .iter()
            .all(|f| f.error.is_transient() || f.error.kind == TranslationErrorKind::OverallTimeout)
}

pub(crate) async fn translate_all_targets<T: Translator + ?Sized>(
    translator: &T,
    pipeline: &Pipeline<'_>,
    protected: &Protected,
    segments: &[Segment],
    source: &str,
    target_langs: Vec<String>,
) -> Result<Vec<TranslatedLine>, TranslationBatchFailure> {
    let policy = &pipeline.policy;
    let attempts_by_target = Arc::new(Mutex::new(HashMap::new()));
    let futures = target_langs
        .iter()
        .map(|target| {
            translate_target(
                translator,
                protected,
                segments,
                source,
                target,
                policy,
                Arc::clone(&attempts_by_target),
                pipeline.stats,
            )
        })
        .collect::<Vec<_>>();

    let result = collect_all_or_nothing(
        source,
        &target_langs,
        pipeline.context,
        policy.overall_timeout,
        attempts_by_target,
        futures,
    )
    .await;

    if let Err(ref failure) = result {
        log_suppressed_translation(pipeline.stats, failure, pipeline.room_id, pipeline.event_id);
    }
    // For the status file: each message's translation done or failed.
    let translations = mxbot_common::supervise::report("translations");
    match &result {
        Ok(_) => translations.ok(),
        Err(failure) => translations.failed(batch_failure_reason(failure)),
    }

    result
}

/// A failed translation for the status file: the target languages and error classes only,
/// never the text or the backend's detail.
pub(crate) fn batch_failure_reason(failure: &TranslationBatchFailure) -> String {
    let mut targets: Vec<&str> = failure
        .failed_targets
        .iter()
        .map(|f| f.target.as_str())
        .collect();
    targets.dedup();
    let mut classes: Vec<&str> = failure
        .failed_targets
        .iter()
        .map(|f| f.error.class())
        .collect();
    classes.sort_unstable();
    classes.dedup();
    format!(
        "translation into {} failed: {}",
        targets.join(", "),
        classes.join(", ")
    )
}
