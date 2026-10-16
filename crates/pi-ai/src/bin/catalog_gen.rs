fn main() -> std::process::ExitCode {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.iter().any(|argument| argument == "--help") {
        println!(
            "Verify Pi v1.4.0 selected release catalogs offline\nUsage: catalog_gen [--check]"
        );
        return std::process::ExitCode::SUCCESS;
    }
    if arguments.iter().any(|argument| argument != "--check") {
        eprintln!("Unknown argument");
        return std::process::ExitCode::FAILURE;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    match std::process::Command::new("python3")
        .arg(root.join("conformance/catalog/v1.4.0/check.py"))
        .status()
    {
        Ok(status) if status.success() => std::process::ExitCode::SUCCESS,
        _ => std::process::ExitCode::FAILURE,
    }
}
