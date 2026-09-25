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

pub(crate) async fn translate_text_target_with_retry(
    state: &BotState,
    plain: &str,
    source: &str,
    target: &str,
    attempts_by_target: Arc<Mutex<HashMap<String, usize>>>,
) -> Result<TranslatedLine, TargetTranslationFailure> {
    let policy = state.retry_policy();
    retry_translation_operation(
        target,
        &policy,
        attempts_by_target,
        &state.stats,
        || async {
            let translated = state.translate(plain, source, target, "text").await?;
            build_text_line(target, plain, translated)
        },
    )
    .await
}

pub(crate) async fn translate_html_target_with_retry(
    state: &BotState,
    html: &str,
    plain: &str,
    source: &str,
    target: &str,
    attempts_by_target: Arc<Mutex<HashMap<String, usize>>>,
) -> Result<TranslatedLine, TargetTranslationFailure> {
    let policy = state.retry_policy();
    let source_plain = html_to_plain(html);
    let html_result = retry_translation_operation(
        target,
        &policy,
        attempts_by_target.clone(),
        &state.stats,
        || async {
            let translated_html = state.translate(html, source, target, "html").await?;
            build_html_line(target, &source_plain, translated_html)
        },
    )
    .await;

    match html_result {
        Ok(line) => Ok(line),
        Err(html_failure) => {
            warn!(
                "HTML translation target={} failed after {} attempts class={} detail={} — falling back to text",
                html_failure.target,
                html_failure.attempts,
                html_failure.error.class(),
                html_failure.error.detail
            );
            translate_text_target_with_retry(state, plain, source, target, attempts_by_target).await
        }
    }
}

/// Decide whether a detected message should be translated and, if so, into
/// which target languages. Every rejection path logs its specific reason —
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

pub(crate) async fn translate_all_targets(
    state: &BotState,
    input: TranslationInput<'_>,
    source: &str,
    target_langs: Vec<String>,
    context: &'static str,
    room_id: &str,
    event_id: &OwnedEventId,
) -> Result<Vec<TranslatedLine>, TranslationBatchFailure> {
    let policy = state.retry_policy();
    let attempts_by_target = Arc::new(Mutex::new(HashMap::new()));
    let futures = target_langs
        .iter()
        .map(|target| {
            let attempts_by_target = Arc::clone(&attempts_by_target);
            async move {
                match input {
                    TranslationInput::Text { plain } => {
                        translate_text_target_with_retry(
                            state,
                            plain,
                            source,
                            target,
                            attempts_by_target,
                        )
                        .await
                    }
                    TranslationInput::Html { html, plain } => {
                        translate_html_target_with_retry(
                            state,
                            html,
                            plain,
                            source,
                            target,
                            attempts_by_target,
                        )
                        .await
                    }
                }
            }
        })
        .collect::<Vec<_>>();

    let result = collect_all_or_nothing(
        source,
        &target_langs,
        context,
        policy.overall_timeout,
        attempts_by_target,
        futures,
    )
    .await;

    if let Err(ref failure) = result {
        log_suppressed_translation(state, failure, room_id, event_id);
    }

    result
}
