use std::sync::Arc;
use std::time::Duration;

use pi_ai::{
    BoxFuture, CancellationToken, Cancelled, Clock, ContentTextInput, JsonParseError,
    UuidV7Entropy, UuidV7EntropyError, UuidV7Error, UuidV7Generator, content_text,
    parse_json_with_repair, parse_streaming_json, repair_json, short_hash,
};

struct FixedClock;

impl Clock for FixedClock {
    fn now_unix_ms(&self) -> u64 {
        1
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(std::future::pending())
    }
}

struct FixedEntropy;

impl UuidV7Entropy for FixedEntropy {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError> {
        Ok([0; 16])
    }
}

#[test]
fn portable_helpers_facade_reexports_exact_surface() {
    let _repair: fn(&str) -> String = repair_json;
    let _parse: fn(&str) -> Result<serde_json::Value, JsonParseError> =
        parse_json_with_repair::<serde_json::Value>;
    let _streaming: fn(Option<&str>) -> pi_ai::JsonValue = parse_streaming_json;
    let _hash: fn(&str) -> String = short_hash;
    let generator = UuidV7Generator::new(Arc::new(FixedClock), Arc::new(FixedEntropy));
    let uuid: Result<String, UuidV7Error> = generator.generate();
    assert!(uuid.expect("UUID").starts_with("00000000-0001-7000-8000-"));
    assert_eq!(content_text(ContentTextInput::Text("text")), "text");
    assert_eq!(short_hash("a"), "m8735310ae7sx");
}
