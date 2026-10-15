use pi_codemode::{DeclaredTool, ExecuteOptions, Outcome, Sandbox, SandboxOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker = std::env::args_os()
        .nth(1)
        .ok_or("usage: local /absolute/path/to/pi-codemode-worker")?;
    let sandbox = Sandbox::new(SandboxOptions::new(worker))?;
    let result = sandbox.execute(
        "const x = await tools.echo({ answer: 42 }); text(x); return x.answer;",
        ExecuteOptions {
            tools: vec![DeclaredTool::new("echo")],
            ..Default::default()
        },
        |call, cancellation| {
            if cancellation.is_cancelled() {
                return Outcome::error("cancelled");
            }
            match call.name.as_str() {
                "echo" => Outcome::Success { value: call.args },
                _ => Outcome::error("unknown tool"),
            }
        },
    )?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    sandbox.close();
    Ok(())
}
