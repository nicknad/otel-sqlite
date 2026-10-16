use std::time::Duration;

use anyhow::{Result, bail};
use serde::Deserialize;

/// A duration knob as written in the config file.
///
/// Accepted forms: a bare integer (interpreted as seconds) or a string with
/// an explicit unit such as `"250ms"`, `"10s"`, `"5m"`, `"2h"`, `"1d"`.
/// The markers `"off"`, `"none"`, `"never"`, `"disabled"` and the empty
/// string mean "explicitly disabled" for knobs that support switching off.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RawDuration {
    Enabled(Duration),
    Disabled,
}

impl RawDuration {
    pub(crate) fn parse(text: &str) -> std::result::Result<Self, String> {
        let trimmed = text.trim();
        if trimmed.is_empty()
            || trimmed.eq_ignore_ascii_case("off")
            || trimmed.eq_ignore_ascii_case("none")
            || trimmed.eq_ignore_ascii_case("never")
            || trimmed.eq_ignore_ascii_case("disabled")
        {
            return Ok(Self::Disabled);
        }
        let split_at = trimmed
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(|| format!("duration `{text}` lacks a unit (e.g. \"30s\")"))?;
        let (number, unit) = trimmed.split_at(split_at);
        let amount: f64 = number
            .trim()
            .parse()
            .map_err(|_| format!("duration `{text}` does not start with a number"))?;
        let unit = unit.trim();
        let scale_secs = if unit.eq_ignore_ascii_case("ms")
            || unit.eq_ignore_ascii_case("millis")
            || unit.eq_ignore_ascii_case("millisecond")
            || unit.eq_ignore_ascii_case("milliseconds")
        {
            0.001
        } else if unit.eq_ignore_ascii_case("s")
            || unit.eq_ignore_ascii_case("sec")
            || unit.eq_ignore_ascii_case("secs")
            || unit.eq_ignore_ascii_case("second")
            || unit.eq_ignore_ascii_case("seconds")
        {
            1.0
        } else if unit.eq_ignore_ascii_case("m")
            || unit.eq_ignore_ascii_case("min")
            || unit.eq_ignore_ascii_case("mins")
            || unit.eq_ignore_ascii_case("minute")
            || unit.eq_ignore_ascii_case("minutes")
        {
            60.0
        } else if unit.eq_ignore_ascii_case("h")
            || unit.eq_ignore_ascii_case("hr")
            || unit.eq_ignore_ascii_case("hrs")
            || unit.eq_ignore_ascii_case("hour")
            || unit.eq_ignore_ascii_case("hours")
        {
            3_600.0
        } else if unit.eq_ignore_ascii_case("d")
            || unit.eq_ignore_ascii_case("day")
            || unit.eq_ignore_ascii_case("days")
        {
            86_400.0
        } else {
            return Err(format!("unknown duration unit `{unit}` in `{text}`"));
        };
        Ok(Self::Enabled(Duration::from_millis(
            (amount * scale_secs * 1_000.0).round() as u64,
        )))
    }
}

impl<'de> Deserialize<'de> for RawDuration {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Seconds(u64),
            Text(String),
        }
        match Repr::deserialize(deserializer)? {
            Repr::Seconds(seconds) => Ok(Self::Enabled(Duration::from_secs(seconds))),
            Repr::Text(text) => Self::parse(&text).map_err(serde::de::Error::custom),
        }
    }
}

pub(crate) fn apply_required_duration(
    target: &mut Duration,
    raw: Option<RawDuration>,
    key: &str,
) -> Result<()> {
    let Some(raw) = raw else { return Ok(()) };
    match raw {
        RawDuration::Enabled(value) => *target = value,
        RawDuration::Disabled => {
            bail!("config key `{key}` requires a duration; \"off\" is not accepted")
        }
    }
    Ok(())
}

pub(crate) fn apply_optional_duration(target: &mut Option<Duration>, raw: Option<RawDuration>) {
    let Some(raw) = raw else { return };
    match raw {
        RawDuration::Enabled(value) => *target = Some(value),
        // Explicit opt-out ("off") vs. missing key (keeps the default).
        RawDuration::Disabled => *target = None,
    }
}
