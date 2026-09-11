//! Private one-shot developer login composition, not a credential store.
pub mod console;
pub mod file;
pub mod native;
use pi_ai_core::{
    AuthEvent, AuthInteraction, AuthInteractionError, AuthPrompt, BoxFuture, CancellationToken,
    OAuthCredential,
};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Selected {
    Anthropic,
    Codex,
    OpenRouter,
}
impl Selected {
    pub const ALL: [Self; 3] = [Self::Anthropic, Self::Codex, Self::OpenRouter];
    pub fn id(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::Codex => "openai-codex",
            Self::OpenRouter => "openrouter",
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Anthropic => "Anthropic",
            Self::Codex => "OpenAI Codex",
            Self::OpenRouter => "OpenRouter",
        }
    }
    fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.id() == value)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    UnknownCommand,
    UnknownProvider,
    InvalidSelection,
    Interaction,
    Login,
    File,
    InvalidAuthFile,
    Runtime,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownCommand => "Unknown command",
            Self::UnknownProvider => "Unknown provider",
            Self::InvalidSelection => "Invalid selection",
            Self::Interaction => "Input/output failed",
            Self::Login => "Login failed",
            Self::File => "Could not save auth file",
            Self::InvalidAuthFile => "Invalid auth file",
            Self::Runtime => "Runtime unavailable",
        })
    }
}
impl std::error::Error for Error {}
pub trait Io: Send + Sync {
    fn open(&self) -> Result<(), Error>;
    fn close(&self);
    fn output(&self, text: &str) -> Result<(), Error>;
    fn line<'a>(
        &'a self,
        question: &'a str,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, Error>>;
}
pub trait Files {
    fn exists(&self) -> Result<bool, Error>;
    fn read(&self) -> Result<String, Error>;
    fn write(&self, text: &str) -> Result<(), Error>;
}
pub trait Login {
    fn login<'a>(
        &'a self,
        provider: Selected,
        interaction: &'a dyn AuthInteraction,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, Error>>;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plan {
    Help,
    List,
    Login(Option<Selected>),
}
pub fn plan(args: &[String]) -> Result<Plan, Error> {
    match args.first().map(String::as_str) {
        None | Some("help" | "--help" | "-h" | "") => Ok(Plan::Help),
        Some("list") => Ok(Plan::List),
        Some("login") => match args.get(1).filter(|s| !s.is_empty()) {
            None => Ok(Plan::Login(None)),
            Some(id) => Selected::parse(id)
                .map(|p| Plan::Login(Some(p)))
                .ok_or(Error::UnknownProvider),
        },
        _ => Err(Error::UnknownCommand),
    }
}
pub fn present(plan: Plan, io: &dyn Io) -> Result<bool, Error> {
    match plan {
        Plan::Help => {
            let providers = Selected::ALL
                .into_iter()
                .map(|p| format!("  {:20} {}", p.id(), p.name()))
                .collect::<Vec<_>>()
                .join("\n");
            io.output(&format!(
                concat!(
                    "Usage: npx @earendil-works/pi-ai <command> [provider]\n\n",
                    "Commands:\n  login [provider]  Login to an OAuth provider\n",
                    "  list              List available providers\n\nProviders:\n{}"
                ),
                providers
            ))?;
        }
        Plan::List => {
            for p in Selected::ALL {
                io.output(&format!("{:20} {}", p.id(), p.name()))?;
            }
        }
        Plan::Login(_) => return Ok(false),
    }
    Ok(true)
}
// ECMAScript whitespace, optional sign, decimal digit prefix; only bounded indexes matter.
pub fn selection(value: &str, len: usize) -> Option<usize> {
    let value = value.trim_start_matches(|c: char| {
        matches!(c,
            '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}'
            | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}'
            | '\u{205f}' | '\u{3000}' | '\u{feff}'
        )
    });
    let value = value.strip_prefix('+').unwrap_or(value);
    let mut number = 0usize;
    let mut digits = false;
    for b in value.bytes().take_while(u8::is_ascii_digit) {
        digits = true;
        number = number.checked_mul(10)?.checked_add(usize::from(b - b'0'))?;
        if number > len {
            return None;
        }
    }
    if digits && number > 0 {
        Some(number - 1)
    } else {
        None
    }
}
struct Interface<'a>(&'a dyn Io);
impl Drop for Interface<'_> {
    fn drop(&mut self) {
        self.0.close();
    }
}
pub struct Interaction<'a> {
    pub io: &'a dyn Io,
}
impl Interaction<'_> {
    pub async fn answer(
        &self,
        prompt: AuthPrompt,
        cancellation: &CancellationToken,
    ) -> Result<String, Error> {
        let (message, placeholder) = match prompt {
            AuthPrompt::Select { message, options } => {
                self.io.output(&format!("\n{message}"))?;
                for (i, o) in options.iter().enumerate() {
                    self.io.output(&format!("  {}. {}", i + 1, o.label))?;
                }
                let answer = self
                    .io
                    .line(
                        &format!("Enter number (1-{}): ", options.len()),
                        cancellation,
                    )
                    .await?;
                return selection(&answer, options.len())
                    .map(|i| options[i].id.clone())
                    .ok_or(Error::InvalidSelection);
            }
            AuthPrompt::Text {
                message,
                placeholder,
            }
            | AuthPrompt::Secret {
                message,
                placeholder,
            }
            | AuthPrompt::ManualCode {
                message,
                placeholder,
            } => (message, placeholder),
        };
        let hint = placeholder
            .filter(|p| !p.is_empty())
            .map(|p| format!(" ({p})"))
            .unwrap_or_default();
        self.io
            .line(&format!("{message}{hint}: "), cancellation)
            .await
    }
}
impl AuthInteraction for Interaction<'_> {
    fn prompt<'a>(
        &'a self,
        prompt: AuthPrompt,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>> {
        Box::pin(async move {
            self.answer(prompt, cancellation)
                .await
                .map_err(|_| AuthInteractionError::new())
        })
    }
    fn notify(&self, event: AuthEvent) -> Result<(), AuthInteractionError> {
        let result = (|| {
            match event {
                AuthEvent::AuthUrl { url, instructions } => {
                    self.io
                        .output(&format!("\nOpen this URL in your browser:\n{url}"))?;
                    if let Some(s) = instructions.filter(|s| !s.is_empty()) {
                        self.io.output(&s)?;
                    }
                }
                AuthEvent::DeviceCode {
                    user_code,
                    verification_uri,
                    ..
                } => {
                    self.io.output(&format!(
                        "\nOpen this URL in your browser:\n{verification_uri}"
                    ))?;
                    self.io.output(&format!("Enter code: {user_code}"))?;
                }
                AuthEvent::Info { message, .. } | AuthEvent::Progress { message } => {
                    self.io.output(&message)?
                }
            }
            Ok::<(), Error>(())
        })();
        result.map_err(|_| AuthInteractionError::new())
    }
}
pub async fn run(
    plan: Plan,
    io: &dyn Io,
    files: &dyn Files,
    login: &dyn Login,
) -> Result<(), Error> {
    if present(plan, io)? {
        return Ok(());
    }
    let cancellation = CancellationToken::new();
    let provider = match plan {
        Plan::Login(Some(p)) => p,
        Plan::Login(None) => {
            io.open()?;
            let guard = Interface(io);
            for (i, p) in Selected::ALL.iter().enumerate() {
                io.output(&format!("  {}. {}", i + 1, p.name()))?;
            }
            let answer = io.line("Enter number (1-3): ", &cancellation).await?;
            let provider = selection(&answer, 3).map(|i| Selected::ALL[i]);
            drop(guard);
            provider.ok_or(Error::UnknownProvider)?
        }
        _ => return Ok(()),
    };
    io.open()?;
    let _guard = Interface(io);
    let credential = login
        .login(provider, &Interaction { io }, &cancellation)
        .await?;
    let previous = if files.exists()? {
        files.read().ok()
    } else {
        None
    };
    let text = file::updated(previous.as_deref(), provider, &credential)?;
    files.write(&text)?;
    io.output("\nCredentials saved to auth.json")
}
