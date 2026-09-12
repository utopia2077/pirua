mod session_support;
use pi_agent_session::id::*;
use session_support::*;
#[test]
fn checked_helper_follows_prior_day_and_rejects_bad_generator() {
    let ids = Ids::default();
    let normal = mint(&ids, None).unwrap();
    assert_eq!(uuid_timestamp(&normal).unwrap(), n(86_400_001));
    let a = mint(&ids, Some(n(86_399_999))).unwrap();
    let b = mint(&ids, Some(n(86_399_999))).unwrap();
    assert_eq!(uuid_timestamp(&a).unwrap(), n(86_399_999));
    assert_ne!(&a[14..], &b[14..]);
    struct Bad;
    impl IdGenerator for Bad {
        fn next(&self, _: Option<SafeUnsigned>) -> String {
            "00000000-0000-7000-8000-000000000000".into()
        }
    }
    assert!(mint(&Bad, Some(n(1))).is_err());
    assert!(mint(&Bad, Some(n(0x1_0000_0000_0000))).is_err());
    for id in [
        "",
        "not-uuid",
        "00000000-0000-6000-8000-000000000000",
        "00000000-0000-7000-c000-000000000000",
        "é000000-0000-7000-8000-000000000000",
    ] {
        assert!(uuid_timestamp(id).is_err());
    }
}
