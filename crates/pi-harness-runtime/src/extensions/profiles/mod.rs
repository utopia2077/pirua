//! Native consumers for host-authenticated extension profile values.
#![allow(missing_docs)]

pub(crate) mod actions;
pub mod connection;
pub(crate) mod custody;
pub mod events;
pub mod hooks;
pub mod native;
pub mod provider;
pub mod resources;
pub(crate) mod service;
pub mod telemetry;
pub(crate) mod tool_route;

pub(crate) fn fault() -> pi_agent_schema::HarnessRuntimeError {
    crate::restore::fault("extension profile native contract failed")
}

pub fn bound(
    registry: &pi_extension_protocol::profiles::Registry,
    root: &str,
    value: &serde_json::Value,
) -> Result<
    pi_extension_protocol::profiles::wire_v1_1::SchemaBoundValue,
    pi_extension_protocol::profiles::Error,
> {
    Ok(
        pi_extension_protocol::profiles::wire_v1_1::SchemaBoundValue {
            projection_id: root.into(),
            schema_digest_sha256: registry
                .digest(root)
                .ok_or(pi_extension_protocol::profiles::Error::HostContract)?
                .to_vec(),
            value: Some(pi_extension_protocol::profiles::json_wire(value)?),
        },
    )
}
