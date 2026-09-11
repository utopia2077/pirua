#[path = "pi_ai_cli/mod.rs"]
mod cli;
fn main() -> std::process::ExitCode {
    let result = (|| {
        let args = std::env::args_os()
            .skip(1)
            .map(|arg| arg.into_string().map_err(|_| cli::Error::UnknownCommand))
            .collect::<Result<Vec<_>, _>>()?;
        let plan = cli::plan(&args)?;
        let console = cli::console::Console::new();
        if cli::present(plan, &console)? {
            return Ok(());
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|_| cli::Error::Runtime)?;
        runtime.block_on(cli::run(
            plan,
            &console,
            &cli::file::AuthFile,
            &cli::native::NativeLogin,
        ))
    })();
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
