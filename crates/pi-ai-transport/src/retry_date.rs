//! Finite deterministic Date.parse projection: RFC HTTP dates, canonical JS
//! Date.toString with numeric GMT offset, ISO dates and zoned ISO date-times.
//! Unzoned date-times depend on host timezone and require explicit injection.
use std::time::UNIX_EPOCH;

pub(crate) fn parse(value: &str) -> Option<f64> {
    let value = value.trim_matches(crate::is_ecma_whitespace);
    if let Ok(date) = httpdate::parse_http_date(value) {
        return Some(match date.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_secs_f64() * 1000.0,
            Err(e) => -e.duration().as_secs_f64() * 1000.0,
        });
    }
    iso(value).or_else(|| canonical(value))
}

fn number(value: &str) -> Option<i64> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        None
    } else {
        value.parse().ok()
    }
}
fn year(value: &str) -> Option<i64> {
    if value.len() == 4 {
        return number(value);
    }
    if value.len() == 7 && matches!(value.as_bytes()[0], b'+' | b'-') {
        let n = number(&value[1..])?;
        if value.starts_with('-') {
            if n == 0 { None } else { Some(-n) }
        } else {
            Some(n)
        }
    } else {
        None
    }
}
fn offset(value: &str, colon: bool) -> Option<i64> {
    if value == "Z" {
        return Some(0);
    }
    if !matches!(value.as_bytes().first(), Some(b'+' | b'-')) {
        return None;
    }
    let (hours, minutes) = if colon {
        if value.len() != 6 || value.as_bytes()[3] != b':' {
            return None;
        }
        (number(&value[1..3])?, number(&value[4..6])?)
    } else {
        if value.len() != 5 {
            return None;
        }
        (number(&value[1..3])?, number(&value[3..5])?)
    };
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some((hours * 60 + minutes) * if value.starts_with('-') { -1 } else { 1 })
}
fn time(value: &str) -> Option<(i64, i64, i64, i64)> {
    let mut parts = value.split(':');
    let hour = parts.next()?;
    let minute = parts.next()?;
    let second = parts.next();
    if parts.next().is_some() || hour.len() != 2 || minute.len() != 2 {
        return None;
    }
    let (second, millis) = if let Some(second) = second {
        if let Some((s, frac)) = second.split_once('.') {
            if s.len() != 2 || frac.len() != 3 {
                return None;
            }
            (number(s)?, number(frac)?)
        } else {
            if second.len() != 2 {
                return None;
            }
            (number(second)?, 0)
        }
    } else {
        (0, 0)
    };
    Some((number(hour)?, number(minute)?, second, millis))
}
fn iso(value: &str) -> Option<f64> {
    if !value.is_ascii() {
        return None;
    }
    let (date, clock) = value
        .split_once('T')
        .map_or((value, None), |(d, t)| (d, Some(t)));
    let year_len = if date.starts_with(['+', '-']) { 7 } else { 4 };
    let year = year(date.get(..year_len)?)?;
    let rest = date.get(year_len..)?;
    let (month, day) = match rest.len() {
        0 => (1, 1),
        3 if rest.starts_with('-') => (number(&rest[1..])?, 1),
        6 if rest.as_bytes()[0] == b'-' && rest.as_bytes()[3] == b'-' => {
            (number(&rest[1..3])?, number(&rest[4..])?)
        }
        _ => return None,
    };
    let (hour, minute, second, millis, zone) = if let Some(clock) = clock {
        if rest.len() != 6 {
            return None;
        }
        let split = clock.find(['Z', '+', '-'])?;
        let (t, z) = clock.split_at(split);
        let (h, m, s, ms) = time(t)?;
        (h, m, s, ms, offset(z, true)?)
    } else {
        (0, 0, 0, 0, 0)
    };
    timestamp((year, month, day), (hour, minute, second, millis), zone)
}
fn canonical(value: &str) -> Option<f64> {
    if !value.is_ascii() {
        return None;
    }
    let value = if let Some((prefix, suffix)) = value.split_once(" (") {
        if !suffix.ends_with(')') {
            return None;
        }
        prefix
    } else {
        value
    };
    let parts = value.split(' ').collect::<Vec<_>>();
    if parts.len() != 6 {
        return None;
    }
    if !["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].contains(&parts[0]) {
        return None;
    }
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|m| *m == parts[1])? as i64
        + 1;
    if parts[2].len() != 2 || parts[4].len() != 8 {
        return None;
    }
    let (hour, minute, second, millis) = time(parts[4])?;
    let zone = offset(parts[5].strip_prefix("GMT")?, false)?;
    timestamp(
        (year(parts[3])?, month, number(parts[2])?),
        (hour, minute, second, millis),
        zone,
    )
}
fn timestamp(
    (year, month, day): (i64, i64, i64),
    (hour, minute, second, millis): (i64, i64, i64, i64),
    zone: i64,
) -> Option<f64> {
    if !(1..=12).contains(&month)
        || !(0..=24).contains(&hour)
        || !(0..60).contains(&minute)
        || !(0..60).contains(&second)
        || !(0..1000).contains(&millis)
    {
        return None;
    }
    if hour == 24 && (minute != 0 || second != 0 || millis != 0) {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day < 1 || day > days {
        return None;
    }
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let m = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    let result = ((days * 24 + hour) * 60 + minute - zone) * 60_000 + second * 1000 + millis;
    if result.abs() > 8_640_000_000_000_000 {
        None
    } else {
        Some(result as f64)
    }
}
