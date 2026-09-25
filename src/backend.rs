//! LibreTranslate/ltengine client: detection, translation, timeouts and retries.

use super::*;

#[derive(Serialize)]
pub(crate) struct DetectRequest<'a> {
    pub(crate) q: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) api_key: Option<&'a str>,
}

#[derive(Deserialize)]
pub(crate) struct DetectResult {
    pub(crate) language: String,
    pub(crate) confidence: f64,
}

#[derive(Serialize)]
pub(crate) struct TranslateRequest<'a> {
    pub(crate) q: &'a str,
    pub(crate) source: &'a str,
    pub(crate) target: &'a str,
    pub(crate) format: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) api_key: Option<&'a str>,
}

#[derive(Deserialize)]
pub(crate) struct TranslateResponse {
    #[serde(rename = "translatedText")]
    pub(crate) translated_text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TranslationErrorKind {
    Timeout,
    Request,
    HttpStatus(u16),
    BackendBusy,
    ResponseParse,
    Empty,
    Rejected,
    OverallTimeout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TranslationError {
    pub(crate) kind: TranslationErrorKind,
    pub(crate) detail: String,
    /// Server-suggested delay before retrying (HTTP `Retry-After`).
    pub(crate) retry_after: Option<Duration>,
}

impl TranslationError {
    pub(crate) fn new(kind: TranslationErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
            retry_after: None,
        }
    }

    pub(crate) fn with_retry_after(mut self, retry_after: Option<Duration>) -> Self {
        self.retry_after = retry_after;
        self
    }

    pub(crate) fn is_transient(&self) -> bool {
        match self.kind {
            TranslationErrorKind::Timeout
            | TranslationErrorKind::Request
            | TranslationErrorKind::BackendBusy => true,
            TranslationErrorKind::HttpStatus(status) => status == 429 || status >= 500,
            TranslationErrorKind::ResponseParse
            | TranslationErrorKind::Empty
            | TranslationErrorKind::Rejected
            | TranslationErrorKind::OverallTimeout => false,
        }
    }

    pub(crate) fn class(&self) -> &'static str {
        match self.kind {
            TranslationErrorKind::Timeout => "timeout",
            TranslationErrorKind::Request => "backend_request_error",
            TranslationErrorKind::HttpStatus(_) => "backend_http_error",
            TranslationErrorKind::BackendBusy => "backend_busy",
            TranslationErrorKind::ResponseParse => "response_parse_error",
            TranslationErrorKind::Empty => "empty_response",
            TranslationErrorKind::Rejected => "validation_rejected",
            TranslationErrorKind::OverallTimeout => "overall_timeout",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TranslationRetryPolicy {
    pub(crate) request_timeout: Duration,
    pub(crate) max_attempts: usize,
    pub(crate) initial_backoff: Duration,
    pub(crate) overall_timeout: Duration,
}

impl TranslationRetryPolicy {
    pub(crate) fn from_config(config: &TranslationConfig) -> Self {
        Self {
            request_timeout: Duration::from_secs(config.request_timeout_secs.max(1)),
            max_attempts: config.max_attempts.max(1),
            initial_backoff: Duration::from_millis(config.retry_initial_backoff_ms),
            overall_timeout: Duration::from_secs(config.overall_timeout_secs.max(1)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TranslatedLine {
    pub(crate) target: String,
    pub(crate) plain: String,
    pub(crate) html: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetTranslationFailure {
    pub(crate) target: String,
    pub(crate) attempts: usize,
    pub(crate) error: TranslationError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TranslationBatchFailure {
    pub(crate) source: String,
    pub(crate) context: &'static str,
    pub(crate) failed_targets: Vec<TargetTranslationFailure>,
    pub(crate) suppressed: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum TranslationInput<'a> {
    Text { plain: &'a str },
    Html { html: &'a str, plain: &'a str },
}

impl BotState {
    pub(crate) fn translation_for_room(&self, room_id: &str) -> EffectiveTranslationConfig {
        effective_translation_config(&self.translation, &self.room_translations, room_id)
    }

    pub(crate) fn retry_policy(&self) -> TranslationRetryPolicy {
        TranslationRetryPolicy::from_config(&self.translation)
    }

    pub(crate) async fn detect(&self, text: &str) -> Option<(String, f64)> {
        let _permit = self
            .lt_requests
            .acquire()
            .await
            .map_err(|e| warn!("ltengine request limiter closed: {e}"))
            .ok()?;
        let resp = self
            .http
            .post(format!("{}/detect", self.lt_url))
            .json(&DetectRequest {
                q: text,
                api_key: self.lt_api_key.as_deref(),
            })
            .send()
            .await
            .map_err(|e| warn!("ltengine /detect request error: {e}"))
            .ok()?
            .error_for_status()
            .map_err(|e| warn!("ltengine /detect returned error status: {e}"))
            .ok()?;
        let results: Vec<DetectResult> = resp
            .json()
            .await
            .map_err(|e| warn!("ltengine /detect response parse error: {e}"))
            .ok()?;
        results
            .into_iter()
            .max_by(|a, b| {
                a.confidence
                    .partial_cmp(&b.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|r| (r.language, normalize_confidence(r.confidence)))
    }

    pub(crate) async fn translate(
        &self,
        text: &str,
        source: &str,
        target: &str,
        format: &str,
    ) -> Result<String, TranslationError> {
        let _permit = self.lt_requests.acquire().await.map_err(|e| {
            warn!("ltengine request limiter closed: {e}");
            TranslationError::new(TranslationErrorKind::Request, e.to_string())
        })?;

        let request_timeout = self.retry_policy().request_timeout;
        let request = async {
            let resp = self
                .http
                .post(format!("{}/translate", self.lt_url))
                .json(&TranslateRequest {
                    q: text,
                    source,
                    target,
                    format,
                    api_key: self.lt_api_key.as_deref(),
                })
                .send()
                .await
                .map_err(|e| TranslationError::new(TranslationErrorKind::Request, e.to_string()))?;

            let status = resp.status();
            let retry_after = parse_retry_after(resp.headers());
            let body = resp
                .text()
                .await
                .map_err(|e| TranslationError::new(TranslationErrorKind::Request, e.to_string()))?;

            if !status.is_success() {
                let kind = if is_backend_busy_body(&body) {
                    TranslationErrorKind::BackendBusy
                } else {
                    TranslationErrorKind::HttpStatus(status.as_u16())
                };
                return Err(TranslationError::new(kind, response_excerpt(&body))
                    .with_retry_after(retry_after));
            }

            match serde_json::from_str::<TranslateResponse>(&body) {
                Ok(result) => Ok(result.translated_text),
                Err(_) if is_backend_busy_body(&body) => Err(TranslationError::new(
                    TranslationErrorKind::BackendBusy,
                    response_excerpt(&body),
                )),
                // Unlike the error-status and backend-busy cases above, a
                // 2xx response that fails to parse as {"translatedText":...}
                // is exactly the shape most likely to contain the translated
                // user content itself (truncated/malformed JSON, but the
                // text is still in there) — log the parse error and size
                // only, never the body, so translated content never reaches
                // logs.
                Err(e) => Err(TranslationError::new(
                    TranslationErrorKind::ResponseParse,
                    format!("{e} (body_len={} bytes)", body.len()),
                )),
            }
        };

        with_translation_timeout(request_timeout, request).await
    }
}

pub(crate) async fn with_translation_timeout<T, Fut>(
    request_timeout: Duration,
    future: Fut,
) -> Result<T, TranslationError>
where
    Fut: Future<Output = Result<T, TranslationError>>,
{
    timeout(request_timeout, future).await.map_err(|_| {
        TranslationError::new(
            TranslationErrorKind::Timeout,
            format!("request exceeded {}s", request_timeout.as_secs()),
        )
    })?
}

/// `Retry-After` in seconds (ltengine sends it while loading or busy),
/// capped so a bad header cannot stall a translation for long.
pub(crate) fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let seconds: u64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(seconds.min(60)))
}

pub(crate) fn is_backend_busy_body(body: &str) -> bool {
    body.to_lowercase().contains("server busy")
}

/// LibreTranslate-compatible `/detect` endpoints (including ltengine) report
/// confidence on a 0-100 scale, but `min_confidence` is documented and
/// configured on a 0.0-1.0 scale. Without this conversion, `min_confidence`
/// compares against the wrong scale and only ever rejects a confidence of
/// exactly 0 — the threshold silently stops filtering anything.
pub(crate) fn normalize_confidence(raw: f64) -> f64 {
    (raw / 100.0).clamp(0.0, 1.0)
}

pub(crate) fn response_excerpt(body: &str) -> String {
    const MAX: usize = 240;
    let mut out: String = body.chars().take(MAX).collect();
    if body.chars().count() > MAX {
        out.push_str("...");
    }
    out
}

pub(crate) async fn retry_translation_operation<T, Op, Fut>(
    target: &str,
    policy: &TranslationRetryPolicy,
    attempts_by_target: Arc<Mutex<HashMap<String, usize>>>,
    stats: &Stats,
    mut op: Op,
) -> Result<T, TargetTranslationFailure>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, TranslationError>>,
{
    let mut attempts = 0usize;
    let mut backoff = policy.initial_backoff;

    loop {
        attempts += 1;
        attempts_by_target
            .lock()
            .await
            .insert(target.to_owned(), attempts);

        match op().await {
            Ok(value) => {
                if attempts > 1 {
                    stats.retries_succeeded.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(value);
            }
            Err(error) if attempts < policy.max_attempts && error.is_transient() => {
                warn!(
                    "Translation target={target} failed transiently attempt={attempts}/{} class={} detail={} — retrying",
                    policy.max_attempts,
                    error.class(),
                    error.detail
                );
                stats.retries.fetch_add(1, Ordering::Relaxed);
                // Honour the backend's Retry-After (e.g. ltengine loading its
                // model after a restart) instead of burning all attempts
                // within a second. The overall timeout still bounds this.
                let wait = error.retry_after.map_or(backoff, |ra| ra.max(backoff));
                if !wait.is_zero() {
                    sleep(wait).await;
                }
                backoff = backoff.saturating_mul(2);
            }
            Err(error) => {
                return Err(TargetTranslationFailure {
                    target: target.to_owned(),
                    attempts,
                    error,
                });
            }
        }
    }
}

pub(crate) async fn collect_all_or_nothing<Fut>(
    source: &str,
    target_langs: &[String],
    context: &'static str,
    overall_timeout: Duration,
    attempts_by_target: Arc<Mutex<HashMap<String, usize>>>,
    futures: Vec<Fut>,
) -> Result<Vec<TranslatedLine>, TranslationBatchFailure>
where
    Fut: Future<Output = Result<TranslatedLine, TargetTranslationFailure>>,
{
    match timeout(overall_timeout, join_all(futures)).await {
        Ok(results) => {
            let mut lines = Vec::new();
            let mut failures = Vec::new();
            for result in results {
                match result {
                    Ok(line) => lines.push(line),
                    Err(failure) => failures.push(failure),
                }
            }

            if failures.is_empty() {
                Ok(lines)
            } else {
                Err(TranslationBatchFailure {
                    source: source.to_owned(),
                    context,
                    failed_targets: failures,
                    suppressed: true,
                })
            }
        }
        Err(_) => {
            let attempts = attempts_by_target.lock().await.clone();
            let failed_targets = target_langs
                .iter()
                .map(|target| TargetTranslationFailure {
                    target: target.clone(),
                    attempts: attempts.get(target).copied().unwrap_or(0),
                    error: TranslationError::new(
                        TranslationErrorKind::OverallTimeout,
                        format!(
                            "overall deadline exceeded after {}s",
                            overall_timeout.as_secs()
                        ),
                    ),
                })
                .collect();

            Err(TranslationBatchFailure {
                source: source.to_owned(),
                context,
                failed_targets,
                suppressed: true,
            })
        }
    }
}

pub(crate) fn log_suppressed_translation(
    state: &BotState,
    failure: &TranslationBatchFailure,
    room_id: &str,
    event_id: &OwnedEventId,
) {
    let failed = failure
        .failed_targets
        .iter()
        .map(|failure| {
            format!(
                "{} attempts={} class={} detail={}",
                failure.target,
                failure.attempts,
                failure.error.class(),
                failure.error.detail
            )
        })
        .collect::<Vec<_>>()
        .join("; ");

    warn!(
        "Translation suppressed for {event_id} in {room_id}: context={} source_lang={} \
         failed_targets=[{}] suppressed={}",
        failure.context, failure.source, failed, failure.suppressed
    );

    state
        .stats
        .translation_api_failures
        .fetch_add(failure.failed_targets.len() as u64, Ordering::Relaxed);
    state.stats.record_skip(SkipReason::TranslationSuppressed);
    state.stats.push_outcome(EventOutcome {
        event_id: event_id.clone(),
        room_id: room_id.to_owned(),
        sender: None,
        msgtype: failure.context.to_owned(),
        text_source: failure.context,
        decision: "error",
        reason: Some(SkipReason::TranslationSuppressed.as_str()),
        source_lang: Some(failure.source.clone()),
        targets: Some(
            failure
                .failed_targets
                .iter()
                .map(|f| f.target.clone())
                .collect::<Vec<_>>()
                .join(","),
        ),
        matrix_send: None,
        duration_ms: None,
        at: SystemTime::now(),
    });
    info!(
        event = %event_id,
        room = room_id,
        msgtype = failure.context,
        text_source = failure.context,
        decision = "error",
        reason = "translation_suppressed",
        source = %failure.source,
        "event processed"
    );
}
