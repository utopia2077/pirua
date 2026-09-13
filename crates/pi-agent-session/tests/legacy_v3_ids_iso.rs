mod legacy_v3_support;
use legacy_v3_support::*;
#[test]
fn independent_sha256_uuid_literals_match_exact_framing_and_bits() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../conformance/session/legacy-v3/id-vectors.json"
    ))
    .unwrap();
    for case in vectors.as_array().unwrap() {
        let header = SessionId::new(case["header"].as_str().unwrap()).unwrap();
        let domain = if case["domain"] == 0 {
            RemintDomain::Entry
        } else {
            RemintDomain::Aggregate
        };
        let time = n(case["timestamp"].as_u64().unwrap());
        let id = deterministic_id(&header, domain, case["old"].as_str().unwrap(), time).unwrap();
        assert_eq!(id, case["expected"].as_str().unwrap());
        assert_eq!(pi_agent_session::id::uuid_timestamp(&id).unwrap(), time);
    }
}
#[test]
fn independent_iso_epochs_offsets_fractions_leaps_and_maximum() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../conformance/session/legacy-v3/iso-vectors.json"
    ))
    .unwrap();
    for case in vectors.as_array().unwrap() {
        assert_eq!(
            parse_iso_timestamp(case["input"].as_str().unwrap())
                .unwrap()
                .get(),
            case["expected"].as_u64().unwrap(),
            "{}",
            case["input"]
        );
    }
    for bad in [
        "1970-01-01",
        "1970-01-01T00:00:00",
        "1970-01-01 00:00:00Z",
        "1970-01-01T24:00:00Z",
        "1970-01-01T23:59:60Z",
        "1970-13-01T00:00:00Z",
        "2001-02-29T00:00:00Z",
        "2100-02-29T00:00:00Z",
        "2000-04-31T00:00:00Z",
        "1970-01-00T00:00:00Z",
        "1970-01-01T00:00:00+24:00",
        "1970-01-01T00:00:00+01:60",
        "1970-01-01T00:00:00.Z",
        "1970-01-01T00:00:00.1234567890Z",
        "1970-01-01T00:00:00Zx",
        "1969-12-31T23:59:59.999Z",
        "+010889-08-02T05:31:50.656Z",
        "-002000-01-01T00:00:00Z",
        "+02000-01-01T00:00:00Z",
    ] {
        assert!(
            matches!(parse_iso_timestamp(bad), Err(LegacyError::Timestamp)),
            "{bad}"
        );
    }
}
#[test]
fn injected_identity_format_timestamp_and_collisions_reject() {
    struct Bad(&'static str);
    impl LegacyIdReminter for Bad {
        fn remint(
            &self,
            _: &SessionId,
            _: RemintDomain,
            _: &str,
            _: SafeUnsigned,
        ) -> Result<String, LegacyError> {
            Ok(self.0.into())
        }
    }
    let input = source(vec![header(), user("a", None)]);
    for bad in ["invalid", "00000000-0000-7000-8000-000000000000"] {
        assert!(matches!(
            normalize_with_reminter(input.clone(), &codec(), None, &Bad(bad)),
            Err(LegacyError::Identity)
        ));
    }
    struct WrongTime;
    impl LegacyIdReminter for WrongTime {
        fn remint(
            &self,
            header: &SessionId,
            domain: RemintDomain,
            old: &str,
            _: SafeUnsigned,
        ) -> Result<String, LegacyError> {
            deterministic_id(header, domain, old, n(0))
        }
    }
    assert!(matches!(
        normalize_with_reminter(input.clone(), &codec(), None, &WrongTime),
        Err(LegacyError::Identity)
    ));
    struct Collision;
    impl LegacyIdReminter for Collision {
        fn remint(
            &self,
            _: &SessionId,
            _: RemintDomain,
            _: &str,
            t: SafeUnsigned,
        ) -> Result<String, LegacyError> {
            Ok(format!(
                "{:08x}-{:04x}-7000-8000-000000000000",
                t.get() >> 16,
                t.get() & 65535
            ))
        }
    }
    assert!(matches!(
        normalize_with_reminter(
            source(vec![header(), user("a", None), user("b", Some("a"))]),
            &codec(),
            None,
            &Collision
        ),
        Err(LegacyError::Identity)
    ));
}
