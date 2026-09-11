use pi_ai_core::SafeUnsigned;
/// Projects the immutable capture timestamp. A different capture must update this
/// projection together with its provenance; unexpected metadata returns absent.
pub fn captured_generated_at() -> Option<SafeUnsigned> {
    match crate::catalog::GENERATED_AT {
        "2026-09-07T06:59:46.000Z" => SafeUnsigned::new(1_788_764_386_000).ok(),
        _ => None,
    }
}
