//! Language detection among a room's languages, on the part of a message that is prose.

use super::*;
use whatlang::{Detector, Lang};

/// Room language codes (LibreTranslate) and the whatlang language (ISO 639-3) for each.
const WHATLANG_CODES: &[(&str, &str)] = &[
    ("af", "afr"),
    ("ar", "ara"),
    ("az", "aze"),
    ("be", "bel"),
    ("bg", "bul"),
    ("bn", "ben"),
    ("ca", "cat"),
    ("cs", "ces"),
    ("da", "dan"),
    ("de", "deu"),
    ("el", "ell"),
    ("en", "eng"),
    ("eo", "epo"),
    ("es", "spa"),
    ("et", "est"),
    ("fa", "pes"),
    ("fi", "fin"),
    ("fr", "fra"),
    ("he", "heb"),
    ("hi", "hin"),
    ("hr", "hrv"),
    ("hu", "hun"),
    ("hy", "hye"),
    ("id", "ind"),
    ("it", "ita"),
    ("ja", "jpn"),
    ("ka", "kat"),
    ("ko", "kor"),
    ("la", "lat"),
    ("lt", "lit"),
    ("lv", "lav"),
    ("mk", "mkd"),
    ("nb", "nob"),
    ("nl", "nld"),
    ("pl", "pol"),
    ("pt", "por"),
    ("pb", "por"),
    ("pt-BR", "por"),
    ("ro", "ron"),
    ("ru", "rus"),
    ("sk", "slk"),
    ("sl", "slv"),
    ("sr", "srp"),
    ("sv", "swe"),
    ("th", "tha"),
    ("tl", "tgl"),
    ("tr", "tur"),
    ("uk", "ukr"),
    ("ur", "urd"),
    ("vi", "vie"),
    ("zh", "cmn"),
    ("zh-Hans", "cmn"),
    ("zt", "cmn"),
    ("zh-Hant", "cmn"),
];

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Detection {
    pub(crate) lang: String,
    /// 0.0-1.0, the scale of `min_confidence`.
    pub(crate) confidence: f64,
    pub(crate) reliable: bool,
}

/// The last reliably detected language per (room, sender), in memory only.
pub(crate) type SenderLanguages = Arc<std::sync::Mutex<HashMap<(String, String), String>>>;

fn whatlang_lang(code: &str) -> Option<Lang> {
    let (_, iso3) = WHATLANG_CODES.iter().find(|(iso1, _)| *iso1 == code)?;
    Lang::from_code(*iso3)
}

/// The room languages whatlang does not know: a room with any of them asks the backend.
pub(crate) fn unsupported_langs(langs: &[String]) -> Vec<String> {
    langs
        .iter()
        .filter(|code| whatlang_lang(code).is_none())
        .cloned()
        .collect()
}

struct RoomDetector {
    detector: Detector,
    langs: Vec<(Lang, String)>,
}

/// Per language set; `None` for a set with a language whatlang does not know.
type DetectorCache = std::sync::Mutex<HashMap<Vec<String>, Option<Arc<RoomDetector>>>>;

static DETECTORS: LazyLock<DetectorCache> = LazyLock::new(Default::default);

fn room_detector(langs: &[String]) -> Option<Arc<RoomDetector>> {
    let mut cache = DETECTORS.lock().unwrap_or_else(|e| e.into_inner());
    cache
        .entry(langs.to_vec())
        .or_insert_with(|| {
            let langs = langs
                .iter()
                .map(|code| Some((whatlang_lang(code)?, code.clone())))
                .collect::<Option<Vec<_>>>()?;
            let detector = Detector::with_allowlist(langs.iter().map(|(lang, _)| *lang).collect());
            Some(Arc::new(RoomDetector { detector, langs }))
        })
        .clone()
}

/// Detects the language of `prose` among `langs`. `None` when a room language is unknown to
/// whatlang or whatlang has no answer. A script that belongs to one language only (Greek, say)
/// is reported as that language even when the room does not have it.
pub(crate) fn detect(prose: &str, langs: &[String]) -> Option<Detection> {
    let room = room_detector(langs)?;
    let info = room.detector.detect(prose)?;
    let lang = room
        .langs
        .iter()
        .find(|(lang, _)| *lang == info.lang())
        .map(|(_, code)| code.clone())
        .or_else(|| {
            WHATLANG_CODES
                .iter()
                .find(|(_, iso3)| *iso3 == info.lang().code())
                .map(|(iso1, _)| (*iso1).to_owned())
        })
        .unwrap_or_else(|| info.lang().code().to_owned());
    Some(Detection {
        lang,
        confidence: info.confidence(),
        reliable: info.is_reliable(),
    })
}

/// The text without what says nothing about its language: URLs, Matrix IDs (`@user:server`,
/// `#room:server`, `!room:server`), inline and fenced code, and placeholders.
pub(crate) fn prose_for_detection(text: &str) -> String {
    protect::without_protected(text)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// At least two letters: a lone URL, an emoji or a number is nothing to translate.
pub(crate) fn has_translatable_text(prose: &str) -> bool {
    prose.chars().filter(|c| c.is_alphabetic()).nth(1).is_some()
}

/// The language a message is taken to be in: a reliable detection, else the sender's last
/// reliable language in this room, else the unreliable detection. A remembered language counts
/// as certain for `min_confidence`.
pub(crate) fn choose_language(
    detection: Detection,
    remembered: Option<String>,
    langs: &[String],
) -> (String, f64) {
    if !detection.reliable {
        if let Some(lang) = remembered.filter(|lang| langs.contains(lang)) {
            return (lang, 1.0);
        }
    }
    (detection.lang, detection.confidence)
}

/// Detects the language of `text` for a message from `sender` in `room_id`: locally among the
/// room's languages, or through `remote` (the backend's `/detect`) when a room language is
/// unknown to whatlang.
pub(crate) async fn detect_source_language<F, Fut>(
    memory: &SenderLanguages,
    langs: &[String],
    room_id: &str,
    sender: &str,
    text: &str,
    remote: F,
) -> Result<(String, f64), SkipReason>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Option<(String, f64)>>,
{
    let prose = prose_for_detection(text);
    if !has_translatable_text(&prose) {
        return Err(SkipReason::NoTranslatableText);
    }
    if !unsupported_langs(langs).is_empty() {
        return remote(prose).await.ok_or(SkipReason::LangDetectFailed);
    }
    let detection = detect(&prose, langs).ok_or(SkipReason::LanguageNotConfigured)?;
    let key = (room_id.to_owned(), sender.to_owned());
    let mut memory = memory.lock().unwrap_or_else(|e| e.into_inner());
    if detection.reliable && langs.contains(&detection.lang) {
        memory.insert(key, detection.lang.clone());
        return Ok((detection.lang, detection.confidence));
    }
    Ok(choose_language(detection, memory.get(&key).cloned(), langs))
}
