//! Digests of the checked, bundled canonical profile descriptors.

use ring::digest::{SHA256, digest};

/// Returns `None` when this build has no canonical descriptor for the version.
pub fn canonical_profile_digest(profile: &str, major: u32, minor: u32) -> Option<[u8; 32]> {
    if major != 1 {
        return None;
    }
    let descriptor: &[u8] = match (profile, minor) {
        ("tool.v1", 0) => include_bytes!("../schema/descriptors/tool-1.0.pb"),
        ("tool.v1", 1) => include_bytes!("../schema/descriptors/tool-1.1.pb"),
        ("hook.v1", 0) => include_bytes!("../schema/descriptors/hook-1.0.pb"),
        ("hook.v1", 1) => include_bytes!("../schema/descriptors/hook-1.1.pb"),
        ("provider.v1", 0) => include_bytes!("../schema/descriptors/provider-1.0.pb"),
        ("provider.v1", 1) => include_bytes!("../schema/descriptors/provider-1.1.pb"),
        ("event-sink.v1", 0) => include_bytes!("../schema/descriptors/event_sink-1.0.pb"),
        ("event-sink.v1", 1) => include_bytes!("../schema/descriptors/event_sink-1.1.pb"),
        ("resource.v1", 0) => include_bytes!("../schema/descriptors/resource-1.0.pb"),
        ("resource.v1", 1) => include_bytes!("../schema/descriptors/resource-1.1.pb"),
        ("host-action.v1", 0) => include_bytes!("../schema/descriptors/host_action-1.0.pb"),
        ("host-action.v1", 1) => include_bytes!("../schema/descriptors/host_action-1.1.pb"),
        ("telemetry-sink.v1", 0) => include_bytes!("../schema/descriptors/telemetry_sink-1.0.pb"),
        ("telemetry-sink.v1", 1) => include_bytes!("../schema/descriptors/telemetry_sink-1.1.pb"),
        ("coding.v1", 1) => include_bytes!("../schema/current/coding-v1.json"),
        _ => return None,
    };
    let mut bytes = Vec::with_capacity(profile.len() + 8 + descriptor.len());
    bytes.extend_from_slice(profile.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(format!("{major}.{minor}").as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(descriptor);
    Some(
        digest(&SHA256, &bytes)
            .as_ref()
            .try_into()
            .expect("SHA-256 length"),
    )
}
