use pi_acp_server::{Application, Options, serve};
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let options = Options::parse(std::env::args().skip(1))?;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(async {
            let application = Application::new(options).await?;
            serve(application, tokio::io::stdin(), tokio::io::stdout()).await?;
            Ok(())
        })
}
