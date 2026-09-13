use super::LegacyError;
use pi_agent_schema::session_values::SafeUnsigned;

/// Parse the frozen, timezone-explicit Gregorian legacy ISO domain into Unix ms.
/// Fractions with one through nine digits truncate to milliseconds.
pub fn parse_iso_timestamp(text: &str) -> Result<SafeUnsigned, LegacyError> {
    fn digits(bytes: &[u8]) -> Result<i64, LegacyError> {
        if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
            return Err(LegacyError::Timestamp);
        }
        Ok(bytes.iter().fold(0, |n, b| n * 10 + i64::from(b - b'0')))
    }
    fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8], LegacyError> {
        if input.len() < n {
            return Err(LegacyError::Timestamp);
        }
        let (head, tail) = input.split_at(n);
        *input = tail;
        Ok(head)
    }
    fn sep(input: &mut &[u8], expected: u8) -> Result<(), LegacyError> {
        if take(input, 1)? != [expected] {
            return Err(LegacyError::Timestamp);
        }
        Ok(())
    }
    let mut input = text.as_bytes();
    let year = if input.first() == Some(&b'+') {
        sep(&mut input, b'+')?;
        let y = digits(take(&mut input, 6)?)?;
        if y == 0 {
            return Err(LegacyError::Timestamp);
        }
        y
    } else {
        digits(take(&mut input, 4)?)?
    };
    sep(&mut input, b'-')?;
    let month = digits(take(&mut input, 2)?)?;
    sep(&mut input, b'-')?;
    let day = digits(take(&mut input, 2)?)?;
    sep(&mut input, b'T')?;
    let hour = digits(take(&mut input, 2)?)?;
    sep(&mut input, b':')?;
    let minute = digits(take(&mut input, 2)?)?;
    sep(&mut input, b':')?;
    let second = digits(take(&mut input, 2)?)?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return Err(LegacyError::Timestamp),
    };
    if day < 1 || day > max_day || hour > 23 || minute > 59 || second > 59 {
        return Err(LegacyError::Timestamp);
    }
    let mut millis = 0;
    if input.first() == Some(&b'.') {
        sep(&mut input, b'.')?;
        let length = input.iter().take_while(|b| b.is_ascii_digit()).count();
        if !(1..=9).contains(&length) {
            return Err(LegacyError::Timestamp);
        }
        let fraction = take(&mut input, length)?;
        for index in 0..3 {
            millis = millis * 10 + fraction.get(index).map_or(0, |b| i64::from(b - b'0'));
        }
    }
    let offset = match input.first() {
        Some(b'Z') => {
            sep(&mut input, b'Z')?;
            0
        }
        Some(b'+') | Some(b'-') => {
            let positive = take(&mut input, 1)? == b"+";
            let hours = digits(take(&mut input, 2)?)?;
            sep(&mut input, b':')?;
            let minutes = digits(take(&mut input, 2)?)?;
            if hours > 23 || minutes > 59 {
                return Err(LegacyError::Timestamp);
            }
            (hours * 60 + minutes) * if positive { 1 } else { -1 }
        }
        _ => return Err(LegacyError::Timestamp),
    };
    if !input.is_empty() {
        return Err(LegacyError::Timestamp);
    }
    // Count Gregorian days before this date directly; no host timezone or clock.
    let preceding_year = year - 1;
    let mut days = 365 * preceding_year + preceding_year.div_euclid(4)
        - preceding_year.div_euclid(100)
        + preceding_year.div_euclid(400);
    for m in 1..month {
        days += match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            2 if leap => 29,
            2 => 28,
            _ => 30,
        };
    }
    days += day - 1;
    let epoch_days = 719162; // Days before 1970-01-01 in the proleptic Gregorian calendar.
    let total =
        (days - epoch_days) * 86_400_000 + (hour * 3600 + minute * 60 + second) * 1000 + millis
            - offset * 60_000;
    if !(0..=0xffff_ffff_ffff).contains(&total) {
        return Err(LegacyError::Timestamp);
    }
    SafeUnsigned::new(total as u64).map_err(|_| LegacyError::Timestamp)
}

pub(super) fn trim_ecmascript(text: &str) -> &str {
    text.trim_matches(|c| matches!(c, '\u{0009}'..='\u{000d}'|'\u{0020}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}'))
}
