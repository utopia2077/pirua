// Run the deterministic manual-poll registry matrix in its own integration
// target as well as the ordinary behavior target. Keeping the same fake ports
// makes every race use explicit admission/drain steps rather than wall time.
#[path = "models_registry.rs"]
mod registry_cases;
