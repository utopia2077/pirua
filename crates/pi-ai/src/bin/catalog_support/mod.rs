use std::{
    env,
    process::{Command, ExitCode},
};

pub fn run(family: &str) -> ExitCode {
    let arguments: Vec<_> = env::args_os().skip(1).collect();
    if arguments.len() == 1 && arguments[0] == "--help" {
        let binary = if family == "text" {
            "catalog_gen"
        } else {
            "image_catalog_gen"
        };
        println!(
            "{family} catalog developer tool\nUsage: [--check] | --output PATH | --help\nRun from the workspace root. Example: cargo run -p pi-ai --features dev-catalog-tools --bin {binary} -- --check\nRequires checked-in Python driver/capture, Python 3.11+, pinned Node/ICU and local pinned upstream Git objects.\nRuns complete offline generators and validates canonical output; no feeds are fetched.\n--output creates a NEW deterministic selected-family JSON file; existing files are never replaced."
        );
        return ExitCode::SUCCESS;
    }
    if !std::path::Path::new("scripts/catalog_cli.py").is_file() {
        eprintln!(
            "catalog tools must run from the workspace root; scripts/catalog_cli.py is missing"
        );
        return ExitCode::FAILURE;
    }
    let status = Command::new("/usr/bin/python3")
        .arg("scripts/catalog_cli.py")
        .arg(family)
        .args(arguments)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .status();
    match status {
        Ok(status) if status.success() => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(_) => {
            eprintln!("catalog developer driver could not start");
            ExitCode::FAILURE
        }
    }
}
