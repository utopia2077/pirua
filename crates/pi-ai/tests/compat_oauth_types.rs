#![cfg(feature = "compat")]

use pi_ai::compat::{OAuthManualCodeInputCallback, OAuthProgressCallback};
use pi_ai::*;
use std::{
    error::Error,
    future::Future,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("fixture must be ready"),
    }
}
fn prompt() -> OAuthPrompt {
    OAuthPrompt {
        message: "enter code".into(),
        placeholder: Some(String::new()),
        allow_empty: Some(false),
    }
}
fn info() -> OAuthAuthInfo {
    OAuthAuthInfo {
        url: "https://synthetic.invalid/?code=private".into(),
        instructions: None,
    }
}
fn device() -> OAuthDeviceCodeInfo {
    OAuthDeviceCodeInfo {
        user_code: "private-device".into(),
        verification_uri: "https://synthetic.invalid".into(),
        interval_seconds: Some(SafeUnsigned::MAX),
        expires_in_seconds: Some(SafeUnsigned::new(0).unwrap()),
    }
}
fn select() -> OAuthSelectPrompt {
    OAuthSelectPrompt {
        message: "choose".into(),
        options: vec![
            OAuthSelectOption {
                id: "b".into(),
                label: "second".into(),
            },
            OAuthSelectOption {
                id: "a".into(),
                label: "first".into(),
            },
            OAuthSelectOption {
                id: "b".into(),
                label: "repeated".into(),
            },
        ],
    }
}
struct Host {
    optional: bool,
    failure: Option<&'static str>,
    selected: Option<String>,
    events: Mutex<Vec<&'static str>>,
    optional_calls: AtomicUsize,
    signal: CancellationToken,
}
impl Host {
    fn new(optional: bool) -> Self {
        Self {
            optional,
            failure: None,
            selected: None,
            events: Mutex::new(vec![]),
            optional_calls: AtomicUsize::new(0),
            signal: CancellationToken::new(),
        }
    }
    fn call(&self, stage: &'static str) -> Result<(), AuthInteractionError> {
        self.events.lock().unwrap().push(stage);
        if self.failure == Some(stage) {
            Err(AuthInteractionError::with_source(std::io::Error::other(
                stage,
            )))
        } else {
            Ok(())
        }
    }
}
impl OAuthLoginCallbacks for Host {
    fn on_auth(&self, value: OAuthAuthInfo) -> Result<(), AuthInteractionError> {
        assert_eq!(value, info());
        self.call("auth")
    }
    fn on_device_code(&self, value: OAuthDeviceCodeInfo) -> Result<(), AuthInteractionError> {
        assert_eq!(value, device());
        self.call("device")
    }
    fn on_prompt<'a>(
        &'a self,
        value: OAuthPrompt,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>> {
        Box::pin(async move {
            assert_eq!(value, prompt());
            self.call("prompt")?;
            Ok(String::new())
        })
    }
    fn on_select<'a>(
        &'a self,
        value: OAuthSelectPrompt,
    ) -> BoxFuture<'a, Result<Option<String>, AuthInteractionError>> {
        Box::pin(async move {
            assert_eq!(value, select());
            self.call("select")?;
            Ok(self.selected.clone())
        })
    }
    fn progress(&self) -> Option<&(dyn OAuthProgressCallback + '_)> {
        self.optional.then_some(self)
    }
    fn manual_code_input(&self) -> Option<&(dyn OAuthManualCodeInputCallback + '_)> {
        self.optional.then_some(self)
    }
    fn signal(&self) -> Option<&CancellationToken> {
        self.optional.then_some(&self.signal)
    }
}
impl OAuthProgressCallback for Host {
    fn notify(&self, message: &str) -> Result<(), AuthInteractionError> {
        assert_eq!(message, "progress");
        self.optional_calls.fetch_add(1, Ordering::SeqCst);
        self.call("progress")
    }
}
impl OAuthManualCodeInputCallback for Host {
    fn input<'a>(&'a self) -> BoxFuture<'a, Result<String, AuthInteractionError>> {
        Box::pin(async move {
            self.optional_calls.fetch_add(1, Ordering::SeqCst);
            self.call("manual")?;
            Ok("manual-code".into())
        })
    }
}

#[test]
fn compat_oauth_required_trait_objects_preserve_values_order_and_select_absence() {
    for selected in [None, Some(String::new()), Some("b".into())] {
        let mut host = Host::new(false);
        host.selected = selected.clone();
        let callbacks: &dyn OAuthLoginCallbacks = &host;
        callbacks.on_auth(info()).unwrap();
        callbacks.on_device_code(device()).unwrap();
        let future = callbacks.on_prompt(prompt());
        assert_eq!(*host.events.lock().unwrap(), ["auth", "device"]);
        assert_eq!(ready(future).unwrap(), "");
        assert_eq!(ready(callbacks.on_select(select())).unwrap(), selected);
        assert_eq!(
            *host.events.lock().unwrap(),
            ["auth", "device", "prompt", "select"]
        );
    }
}

#[test]
fn compat_oauth_optional_presence_is_observable_without_invocation() {
    for present in [false, true] {
        let host = Host::new(present);
        let callbacks: &dyn OAuthLoginCallbacks = &host;
        assert_eq!(callbacks.progress().is_some(), present);
        assert_eq!(callbacks.manual_code_input().is_some(), present);
        assert_eq!(callbacks.signal().is_some(), present);
        assert_eq!(host.optional_calls.load(Ordering::SeqCst), 0);
        if present {
            callbacks.progress().unwrap().notify("progress").unwrap();
            assert_eq!(
                ready(callbacks.manual_code_input().unwrap().input()).unwrap(),
                "manual-code"
            );
            let signal = callbacks.signal().unwrap();
            assert!(std::ptr::eq(signal, &host.signal));
            assert!(!signal.is_cancelled());
            host.signal.cancel();
            ready(signal.cancelled());
            assert!(signal.is_cancelled());
            assert_eq!(host.optional_calls.load(Ordering::SeqCst), 2);
        }
    }
}

#[test]
fn compat_oauth_sync_and_async_errors_retain_the_callback_source() {
    for stage in ["auth", "device", "prompt", "select", "progress", "manual"] {
        let mut host = Host::new(true);
        host.failure = Some(stage);
        let c: &dyn OAuthLoginCallbacks = &host;
        let error = match stage {
            "auth" => c.on_auth(info()).unwrap_err(),
            "device" => c.on_device_code(device()).unwrap_err(),
            "prompt" => ready(c.on_prompt(prompt())).unwrap_err(),
            "select" => ready(c.on_select(select())).unwrap_err(),
            "progress" => c.progress().unwrap().notify("progress").unwrap_err(),
            "manual" => ready(c.manual_code_input().unwrap().input()).unwrap_err(),
            _ => unreachable!(),
        };
        assert_eq!(error.source().unwrap().to_string(), stage);
        assert_eq!(*host.events.lock().unwrap(), [stage]);
    }
}

#[test]
fn compat_oauth_values_and_ports_redact_transient_content() {
    let sentinel = "synthetic-private-marker";
    let p = OAuthPrompt {
        message: sentinel.into(),
        placeholder: Some(sentinel.into()),
        allow_empty: None,
    };
    let a = OAuthAuthInfo {
        url: sentinel.into(),
        instructions: Some(sentinel.into()),
    };
    let d = OAuthDeviceCodeInfo {
        user_code: sentinel.into(),
        verification_uri: sentinel.into(),
        interval_seconds: None,
        expires_in_seconds: None,
    };
    let o = OAuthSelectOption {
        id: sentinel.into(),
        label: sentinel.into(),
    };
    let s = OAuthSelectPrompt {
        message: sentinel.into(),
        options: vec![o.clone()],
    };
    let host = Host::new(true);
    let callbacks: &dyn OAuthLoginCallbacks = &host;
    let error = AuthInteractionError::with_source(std::io::Error::other(sentinel));
    let rendered = format!(
        "{p:?} {a:?} {d:?} {o:?} {s:?} {callbacks:?} {:?} {:?} {error:?} {error}",
        callbacks.progress().unwrap(),
        callbacks.manual_code_input().unwrap()
    );
    assert!(!rendered.contains(sentinel));
    assert!(rendered.contains("[REDACTED]"));
    assert_eq!(p.message, sentinel);
    assert_eq!(a.url, sentinel);
    assert_eq!(d.user_code, sentinel);
}

#[test]
fn compat_oauth_credentials_reexport_is_the_existing_nominal_core_value() {
    let core = pi_ai_core::OAuthCredentials {
        refresh: SecretString::new("refresh"),
        access: SecretString::new("access"),
        expires: SafeUnsigned::MAX,
        extra: OAuthExtraFields::default(),
    };
    let compat: pi_ai::compat::OAuthCredentials = core.clone();
    let root: pi_ai::OAuthCredentials = compat;
    let core_again: pi_ai_core::OAuthCredentials = root;
    assert_eq!(core_again, core);
    assert_eq!(
        std::any::TypeId::of::<pi_ai::compat::OAuthCredentials>(),
        std::any::TypeId::of::<pi_ai_core::OAuthCredentials>()
    );
}

fn source(mode: &str) -> String {
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/ai-compat/oauth-types/source.py");
    let output = std::process::Command::new("/usr/bin/python3")
        .arg(script)
        .arg(mode)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}
#[test]
fn compat_oauth_complete_pinned_source_declarations_and_exports() {
    assert_eq!(
        source("--check").trim(),
        "seven symbols: complete pinned declaration and export proof matched"
    );
}
#[test]
fn compat_oauth_source_shape_controls_reject_meaningful_changes() {
    let result = source("--causal");
    assert_eq!(
        result
            .lines()
            .filter(|line| line.ends_with("rejected by declaration shape"))
            .count(),
        7
    );
    assert!(result.contains("two positive shape controls admitted"));
}
