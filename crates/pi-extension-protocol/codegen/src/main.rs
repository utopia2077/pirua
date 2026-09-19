//! Read-only check of checked-in protobuf DTOs against canonical descriptors.

use std::{env, fs, path::PathBuf, process::Command, time::SystemTime};

struct TemporaryDirectory(PathBuf);

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let protocol_dir = crate_dir.parent().ok_or("missing protocol parent")?;
    let schema = protocol_dir.join("schema");
    let generated = protocol_dir.join("src/wire/generated");
    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let scratch = env::temp_dir().join(format!("pi-codegen-check-{}-{nonce}", std::process::id()));
    fs::create_dir(&scratch)?;
    let _cleanup = TemporaryDirectory(scratch.clone());

    for (minor, version) in [("1.0", "v1_0"), ("1.1", "v1_1")] {
        let output = scratch.join(version);
        fs::create_dir(&output)?;
        let include = schema.join(version);
        let descriptor = schema.join(format!("descriptors/tool-{minor}.pb"));
        let mut config = prost_build::Config::new();
        config.out_dir(&output);
        config.file_descriptor_set_path(&descriptor);
        config.skip_protoc_run();
        config.compile_protos(&[include.join("tool.proto")], &[include])?;

        let checked = fs::read(generated.join(format!("{version}.rs")))?;
        let actual = fs::read(output.join("pi.extension.v1.rs"))?;
        if actual != checked {
            return Err(format!("generated Rust differs from checked-in {version}.rs").into());
        }
        println!(
            "PASS {version}: generated Rust matches checked source ({} bytes)",
            actual.len()
        );
    }

    // This gate rebuilds every FileDescriptorSet with pinned protoc 3.20.3,
    // compares canonical bytes, and checks descriptor MethodOptions/table.
    let status = Command::new("sh")
        .arg("verify.sh")
        .current_dir(&schema)
        .status()?;
    if !status.success() {
        return Err(format!("canonical schema verification failed: {status}").into());
    }
    Ok(())
}
