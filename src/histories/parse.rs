//! Exported workflow histories read into typed events.
//!
//! Accepts the JSON that the Temporal CLI (`temporal workflow show --output json`), the Web UI
//! and tctl write: `{"events": [...]}`, `{"history": {"events": [...]}}` or a bare array of
//! events. Event types may be spelled `EVENT_TYPE_ACTIVITY_TASK_SCHEDULED` or
//! `ActivityTaskScheduled`, attribute keys may be camelCase or snake_case, durations `"2.5s"` or
//! `{seconds, nanos}`, and 64-bit integers numbers or strings, as protobuf's JSON mapping allows.
//! Payloads (inputs, results, details, memos) are never read.

use serde_json::Value;

/// The history event kinds the importer interprets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    WorkflowStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowTimedOut,
    WorkflowTerminated,
    WorkflowCanceled,
    WorkflowContinuedAsNew,
    WftScheduled,
    WftStarted,
    WftCompleted,
    WftTimedOut,
    WftFailed,
    ActivityScheduled,
    ActivityStarted,
    ActivityCompleted,
    ActivityFailed,
    ActivityTimedOut,
    ActivityCancelRequested,
    ActivityCanceled,
    TimerStarted,
    TimerFired,
    TimerCanceled,
    Marker,
    Signaled,
    ChildInitiated,
    ChildStartFailed,
    ChildStarted,
    ChildCompleted,
    ChildFailed,
    ChildCanceled,
    ChildTimedOut,
    ChildTerminated,
    /// anything else (updates, Nexus operations, search attributes, external signals, …)
    Other,
}

impl Kind {
    /// From a normalised event type name (`activitytaskscheduled`).
    fn from_name(name: &str) -> Kind {
        match name {
            "workflowexecutionstarted" => Kind::WorkflowStarted,
            "workflowexecutioncompleted" => Kind::WorkflowCompleted,
            "workflowexecutionfailed" => Kind::WorkflowFailed,
            "workflowexecutiontimedout" => Kind::WorkflowTimedOut,
            "workflowexecutionterminated" => Kind::WorkflowTerminated,
            "workflowexecutioncanceled" => Kind::WorkflowCanceled,
            "workflowexecutioncontinuedasnew" => Kind::WorkflowContinuedAsNew,
            "workflowtaskscheduled" => Kind::WftScheduled,
            "workflowtaskstarted" => Kind::WftStarted,
            "workflowtaskcompleted" => Kind::WftCompleted,
            "workflowtasktimedout" => Kind::WftTimedOut,
            "workflowtaskfailed" => Kind::WftFailed,
            "activitytaskscheduled" => Kind::ActivityScheduled,
            "activitytaskstarted" => Kind::ActivityStarted,
            "activitytaskcompleted" => Kind::ActivityCompleted,
            "activitytaskfailed" => Kind::ActivityFailed,
            "activitytasktimedout" => Kind::ActivityTimedOut,
            "activitytaskcancelrequested" => Kind::ActivityCancelRequested,
            "activitytaskcanceled" => Kind::ActivityCanceled,
            "timerstarted" => Kind::TimerStarted,
            "timerfired" => Kind::TimerFired,
            "timercanceled" => Kind::TimerCanceled,
            "markerrecorded" => Kind::Marker,
            "workflowexecutionsignaled" => Kind::Signaled,
            "startchildworkflowexecutioninitiated" => Kind::ChildInitiated,
            "startchildworkflowexecutionfailed" => Kind::ChildStartFailed,
            "childworkflowexecutionstarted" => Kind::ChildStarted,
            "childworkflowexecutioncompleted" => Kind::ChildCompleted,
            "childworkflowexecutionfailed" => Kind::ChildFailed,
            "childworkflowexecutioncanceled" => Kind::ChildCanceled,
            "childworkflowexecutiontimedout" => Kind::ChildTimedOut,
            "childworkflowexecutionterminated" => Kind::ChildTerminated,
            _ => Kind::Other,
        }
    }
}

/// One history event: its id, time, kind and attributes.
#[derive(Clone, Debug)]
pub struct Event {
    pub id: i64,
    /// microseconds since the Unix epoch
    pub time_us: i64,
    pub kind: Kind,
    /// the event type, normalised (`activitytaskscheduled`), to name kinds that aren't modelled
    pub type_name: String,
    /// the event's attributes (`Value::Null` when it has none)
    pub attrs: Value,
}

/// Parse one exported history into its events, in order.
pub fn parse_history(text: &str) -> Result<Vec<Event>, String> {
    let v: Value = serde_json::from_str(text).map_err(|e| format!("not JSON: {e}"))?;
    let events = match &v {
        Value::Array(a) => a,
        Value::Object(_) => get(&v, "events")
            .or_else(|| get(&v, "history").and_then(|h| get(h, "events")))
            .and_then(Value::as_array)
            .ok_or("no `events` array")?,
        _ => return Err("expected a history object or an array of events".into()),
    };
    events
        .iter()
        .enumerate()
        .map(|(i, e)| parse_event(e, i).map_err(|m| format!("event {}: {m}", i + 1)))
        .collect()
}

fn parse_event(e: &Value, index: usize) -> Result<Event, String> {
    let obj = e.as_object().ok_or("not an object")?;
    // the attributes key also names the event type, in either spelling
    let (attr_name, attrs) = obj
        .iter()
        .find_map(|(k, v)| {
            let n = normalise(k);
            n.strip_suffix("eventattributes")
                .map(|t| (t.to_string(), v.clone()))
        })
        .unwrap_or_default();
    let type_name = get(e, "eventType")
        .and_then(Value::as_str)
        .map(|t| normalise(t.trim_start_matches("EVENT_TYPE_")))
        .filter(|t| !t.is_empty() && t != "unspecified")
        .unwrap_or(attr_name);
    let time_us = get(e, "eventTime")
        .and_then(timestamp_us)
        .ok_or("no readable eventTime")?;
    Ok(Event {
        id: get(e, "eventId").and_then(int).unwrap_or(index as i64 + 1),
        time_us,
        kind: Kind::from_name(&type_name),
        type_name,
        attrs,
    })
}

/// Lower-case with underscores removed: `EVENT_TYPE_X_Y`, `XY` and `xY` compare equal.
fn normalise(s: &str) -> String {
    s.chars()
        .filter(|c| *c != '_')
        .collect::<String>()
        .to_ascii_lowercase()
}

/// A field by its camelCase name, or its snake_case form.
pub fn get<'a>(v: &'a Value, camel: &str) -> Option<&'a Value> {
    v.get(camel).or_else(|| {
        let mut snake = String::with_capacity(camel.len() + 4);
        for c in camel.chars() {
            if c.is_ascii_uppercase() {
                snake.push('_');
                snake.push(c.to_ascii_lowercase());
            } else {
                snake.push(c);
            }
        }
        v.get(&snake)
    })
}

/// A nested field, e.g. `path(v, &["activityType", "name"])`.
pub fn path<'a>(v: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().try_fold(v, |v, k| get(v, k))
}

/// A string field inside an object, e.g. the `name` of `activityType`.
pub fn name(v: &Value, key: &str) -> Option<String> {
    path(v, &[key, "name"])
        .or_else(|| get(v, key).filter(|x| x.is_string()))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// An integer written as a number or a string.
pub fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// A float written as a number or a string.
pub fn float(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// A protobuf duration in microseconds: `"2.5s"`, `{seconds, nanos}` or a number of seconds.
pub fn duration_us(v: &Value) -> Option<u64> {
    let secs = match v {
        Value::String(s) => s.trim().strip_suffix('s')?.trim().parse::<f64>().ok()?,
        Value::Number(n) => n.as_f64()?,
        Value::Object(_) => {
            get(v, "seconds").and_then(float).unwrap_or(0.0)
                + get(v, "nanos").and_then(float).unwrap_or(0.0) / 1e9
        }
        _ => return None,
    };
    (secs.is_finite() && secs >= 0.0).then(|| (secs * 1e6).round() as u64)
}

/// A protobuf timestamp in microseconds since the epoch: RFC 3339 (`2026-09-28T10:00:00.123Z`)
/// or `{seconds, nanos}`.
pub fn timestamp_us(v: &Value) -> Option<i64> {
    match v {
        Value::String(s) => rfc3339_us(s),
        Value::Object(_) => {
            let s = get(v, "seconds").and_then(int)?;
            let n = get(v, "nanos").and_then(int).unwrap_or(0);
            Some(s * 1_000_000 + n / 1_000)
        }
        _ => None,
    }
}

/// Parse `YYYY-MM-DDTHH:MM:SS[.fraction](Z|±HH:MM)` into microseconds since the epoch.
fn rfc3339_us(s: &str) -> Option<i64> {
    let s = s.trim();
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't' | b' ') {
        return None;
    }
    let num = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    let mut i = 19;
    let mut frac_us = 0i64;
    if b.get(i) == Some(&b'.') {
        let start = i + 1;
        i = start;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let digits = s.get(start..i)?;
        let micros: String = digits.chars().chain("000000".chars()).take(6).collect();
        frac_us = micros.parse().ok()?;
    }
    let offset_s = match s.get(i..)? {
        "Z" | "z" => 0,
        tz if tz.len() >= 5 && (tz.starts_with('+') || tz.starts_with('-')) => {
            let sign = if tz.starts_with('-') { -1 } else { 1 };
            let digits: String = tz[1..].chars().filter(char::is_ascii_digit).collect();
            let (oh, om) = (
                digits.get(0..2)?.parse::<i64>().ok()?,
                digits.get(2..4)?.parse::<i64>().ok()?,
            );
            sign * (oh * 3600 + om * 60)
        }
        _ => return None,
    };
    let days = days_from_civil(y, mo, d);
    let secs = days * 86_400 + h * 3600 + mi * 60 + sec - offset_s;
    Some(secs * 1_000_000 + frac_us)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_spellings_of_event_types_and_keys_parse() {
        let prefixed = r#"{"events": [
            {"eventId": "1", "eventTime": "2026-09-28T10:00:00Z",
             "eventType": "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
             "activityTaskScheduledEventAttributes": {"startToCloseTimeout": "10s"}}]}"#;
        let short = r#"[
            {"event_id": 1, "event_time": {"seconds": "1790589600", "nanos": 0},
             "event_type": "ActivityTaskScheduled",
             "activity_task_scheduled_event_attributes": {"start_to_close_timeout": "10s"}}]"#;
        for text in [prefixed, short] {
            let e = &parse_history(text).expect("parses")[0];
            assert_eq!(e.kind, Kind::ActivityScheduled);
            assert_eq!(e.id, 1);
            assert_eq!(
                get(&e.attrs, "startToCloseTimeout").and_then(duration_us),
                Some(10_000_000)
            );
        }
        // the attributes key names the type when eventType is missing
        let bare = r#"[{"eventTime": "2026-09-28T10:00:00Z", "timerFiredEventAttributes": {}}]"#;
        assert_eq!(parse_history(bare).unwrap()[0].kind, Kind::TimerFired);
    }

    #[test]
    fn timestamps_and_durations_follow_the_protobuf_json_mapping() {
        let t = |s: &str| rfc3339_us(s).expect(s);
        assert_eq!(t("1970-01-01T00:00:00Z"), 0);
        assert_eq!(t("2026-09-28T10:00:00Z"), 1_790_589_600_000_000);
        assert_eq!(
            t("2026-09-28T10:00:00.123456789Z") - t("2026-09-28T10:00:00Z"),
            123_456
        );
        assert_eq!(t("2026-09-28T12:00:00+02:00"), t("2026-09-28T10:00:00Z"));
        assert_eq!(
            t("2024-02-29T00:00:00Z") - t("2024-02-28T00:00:00Z"),
            86_400_000_000
        );
        let d = |v: Value| duration_us(&v);
        assert_eq!(d(Value::from("0.500s")), Some(500_000));
        assert_eq!(d(Value::from("315360000s")), Some(315_360_000_000_000));
        assert_eq!(
            d(serde_json::json!({"seconds": "1", "nanos": 250000000})),
            Some(1_250_000)
        );
        assert_eq!(d(Value::from("bad")), None);
    }
}
