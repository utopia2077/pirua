use super::{
    browser::{self, BrowserConfig, BrowserKind},
    common::*,
};
use crate::auth::{OAuthServices, checked_auth_integer};
use pi_ai_core::{
    AuthEvent, AuthInteraction, AuthPrompt, AuthProviderError, AuthSelectOption, BoxFuture,
    CancellationToken, JsonValue, ModelAuth, OAuthAuth, OAuthCredential, OAuthExtraFields,
};
use pi_ai_transport::generate_pkce;
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const REDIRECT: &str = "http://localhost:1455/auth/callback";
const DEVICE_REDIRECT: &str = "https://auth.openai.com/deviceauth/callback";
#[derive(Clone)]
pub struct OpenAICodexOAuth {
    services: OAuthServices,
}
impl OpenAICodexOAuth {
    pub fn new(services: OAuthServices) -> Self {
        Self { services }
    }
    async fn token_request(&self, body: String, c: &CancellationToken) -> Result<OAuthCredential> {
        let response = post(
            &self.services,
            TOKEN_URL,
            &[("Content-Type", "application/x-www-form-urlencoded")],
            body,
            c,
            None,
            TokenRead::Codex,
        )
        .await?;
        if !response.ok() {
            return Err(OAuthFlowError::http(
                OAuthFlowErrorKind::Exchange,
                response.status,
            ));
        }
        let value = response.json()?;
        let mut credential = token(&value, self.services.clock.now_unix_ms(), 0)?;
        if credential.credentials.access.expose_secret().is_empty()
            || credential.credentials.refresh.expose_secret().is_empty()
        {
            return Err(invalid());
        }
        let account = account_id(credential.credentials.access.expose_secret())?;
        credential.credentials.extra = OAuthExtraFields::new(BTreeMap::from([(
            "accountId".into(),
            JsonValue::String(account),
        )]))
        .map_err(host)?;
        Ok(credential)
    }
    async fn exchange(
        &self,
        code: &str,
        verifier: &str,
        redirect: &str,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        self.token_request(
            form(&[
                ("grant_type", "authorization_code"),
                ("client_id", CLIENT_ID),
                ("code", code),
                ("code_verifier", verifier),
                ("redirect_uri", redirect),
            ]),
            c,
        )
        .await
    }
    async fn browser(
        &self,
        i: &dyn AuthInteraction,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let pkce = generate_pkce(self.services.crypto.as_ref()).map_err(host)?;
        let verifier = pkce.verifier.expose_secret().to_owned();
        let mut state = [0; 16];
        self.services.crypto.fill_random(&mut state).map_err(host)?;
        let state = state.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let authorize_url = format!(
            "https://auth.openai.com/oauth/authorize?{}",
            form(&[
                ("response_type", "code"),
                ("client_id", CLIENT_ID),
                ("redirect_uri", REDIRECT),
                ("scope", "openid profile email offline_access"),
                ("code_challenge", &pkce.challenge),
                ("code_challenge_method", "S256"),
                ("state", &state),
                ("id_token_add_organizations", "true"),
                ("codex_cli_simplified_flow", "true"),
                ("originator", "pi")
            ])
        );
        browser::login(
            &self.services,
            i,
            c,
            BrowserConfig {
                kind: BrowserKind::Codex,
                state,
                authorize_url,
            },
            |code, _| Box::pin(async move { self.exchange(&code, &verifier, REDIRECT, c).await }),
        )
        .await
    }
    async fn device(
        &self,
        i: &dyn AuthInteraction,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let response = post(
            &self.services,
            "https://auth.openai.com/api/accounts/deviceauth/usercode",
            &[("Content-Type", "application/json")],
            serde_json::json!({"client_id":CLIENT_ID}).to_string(),
            c,
            None,
            TokenRead::CodexDeviceStart,
        )
        .await?;
        if !response.ok() {
            return Err(OAuthFlowError::http(
                if response.status == 404 {
                    OAuthFlowErrorKind::DeviceDisabled
                } else {
                    OAuthFlowErrorKind::DeviceDenied
                },
                response.status,
            ));
        }
        let value = response.json()?;
        let id = nonempty(&value, "device_auth_id")?;
        let user_code = nonempty(&value, "user_code")?;
        let interval = match value.get("interval") {
            Some(Value::Number(n)) => n.as_f64(),
            Some(Value::String(s)) => number(s),
            _ => None,
        }
        .ok_or_else(invalid)?;
        let interval_event = checked_auth_integer(interval).map_err(|_| invalid())?;
        i.notify(AuthEvent::DeviceCode {
            user_code: user_code.into(),
            verification_uri: "https://auth.openai.com/codex/device".into(),
            interval_seconds: Some(interval_event),
            expires_in_seconds: Some(checked_auth_integer(900.0).map_err(|_| invalid())?),
        })
        .map_err(host)?;
        let deadline = self.services.clock.now_unix_ms() as f64 + 900000.0;
        let mut interval_ms = (interval * 1000.0).floor().max(1000.0);
        let mut slowed = false;
        while (self.services.clock.now_unix_ms() as f64) < deadline {
            if c.is_cancelled() {
                return Err(cancelled());
            }
            let response = post(
                &self.services,
                "https://auth.openai.com/api/accounts/deviceauth/token",
                &[("Content-Type", "application/json")],
                serde_json::json!({"device_auth_id":id,"user_code":user_code}).to_string(),
                c,
                None,
                TokenRead::CodexPoll,
            )
            .await?;
            if response.ok() {
                let value = response.json()?;
                let code = nonempty(&value, "authorization_code")?;
                let verifier = nonempty(&value, "code_verifier")?;
                return self.exchange(code, verifier, DEVICE_REDIRECT, c).await;
            }
            if !matches!(response.status, 403 | 404) {
                let value = response.json().unwrap_or(Value::Null);
                let error = value.get("error");
                let code = error
                    .and_then(|e| e.as_str().or_else(|| e.get("code").and_then(Value::as_str)));
                match code {
                    Some("deviceauth_authorization_pending") => {}
                    Some("slow_down") => {
                        slowed = true;
                        interval_ms = (interval_ms + 5000.0).max(1000.0);
                    }
                    _ => {
                        return Err(OAuthFlowError::http(
                            OAuthFlowErrorKind::DeviceDenied,
                            response.status,
                        ));
                    }
                }
            }
            let remaining = deadline - self.services.clock.now_unix_ms() as f64;
            if remaining <= 0.0 {
                break;
            }
            self.services
                .clock
                .sleep(Duration::from_millis(interval_ms.min(remaining) as u64), c)
                .await
                .map_err(|_| cancelled())?;
        }
        Err(OAuthFlowError::new(if slowed {
            OAuthFlowErrorKind::DeviceSlowDownTimeout
        } else {
            OAuthFlowErrorKind::DeviceTimeout
        }))
    }
    async fn login_flow(
        &self,
        i: &dyn AuthInteraction,
        c: &CancellationToken,
    ) -> Result<OAuthCredential> {
        let method = i
            .prompt(
                AuthPrompt::Select {
                    message: "Select OpenAI Codex login method:".into(),
                    options: vec![
                        AuthSelectOption {
                            id: "browser".into(),
                            label: "Browser login (default)".into(),
                            description: None,
                        },
                        AuthSelectOption {
                            id: "device_code".into(),
                            label: "Device code login (headless)".into(),
                            description: None,
                        },
                    ],
                },
                c,
            )
            .await
            .map_err(host)?;
        match method.as_str() {
            "browser" => self.browser(i, c).await,
            "device_code" => self.device(i, c).await,
            _ => Err(OAuthFlowError::new(OAuthFlowErrorKind::UnknownMethod)),
        }
    }
}
fn nonempty<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(invalid)
}
fn number(s: &str) -> Option<f64> {
    let s = s.trim_matches(ecma_space);
    if s.is_empty() {
        return Some(0.0);
    }
    for (prefix, base) in [
        ("0x", 16),
        ("0X", 16),
        ("0b", 2),
        ("0B", 2),
        ("0o", 8),
        ("0O", 8),
    ] {
        if let Some(value) = s.strip_prefix(prefix) {
            return u64::from_str_radix(value, base).ok().map(|n| n as f64);
        }
    }
    s.parse().ok()
}
fn account_id(token: &str) -> Result<String> {
    let parts: Vec<_> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(invalid());
    }
    let bytes = atob(parts[1])?;
    let text: String = bytes.into_iter().map(char::from).collect();
    let value: Value = serde_json::from_str(&text).map_err(|_| invalid())?;
    value
        .get("https://api.openai.com/auth")
        .and_then(|v| v.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(invalid)
}
// WHATWG atob's standard alphabet and Latin-1 result, not a JWT verification API.
fn atob(value: &str) -> Result<Vec<u8>> {
    let mut bytes: Vec<_> = value
        .bytes()
        .filter(|b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r' | b'\x0c'))
        .collect();
    if bytes.len() % 4 == 0 && bytes.last() == Some(&b'=') {
        bytes.pop();
        if bytes.last() == Some(&b'=') {
            bytes.pop();
        }
    }
    if bytes.len() % 4 == 1 {
        return Err(invalid());
    }
    let mut out = Vec::new();
    let mut bits = 0_u32;
    let mut count = 0;
    for byte in bytes {
        let n = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return Err(invalid()),
        };
        bits = (bits << 6) | u32::from(n);
        count += 6;
        if count >= 8 {
            count -= 8;
            out.push((bits >> count) as u8);
            bits &= (1 << count) - 1;
        }
    }
    Ok(out)
}
impl OAuthAuth for OpenAICodexOAuth {
    fn name(&self) -> &str {
        "OpenAI (ChatGPT Plus/Pro)"
    }
    fn is_subscription(&self) -> bool {
        true
    }
    fn login<'a>(
        &'a self,
        i: &'a dyn AuthInteraction,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move { self.login_flow(i, c).await.map_err(provider) })
    }
    fn refresh<'a>(
        &'a self,
        v: &'a OAuthCredential,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move {
            self.token_request(
                form(&[
                    ("grant_type", "refresh_token"),
                    ("refresh_token", v.credentials.refresh.expose_secret()),
                    ("client_id", CLIENT_ID),
                ]),
                c,
            )
            .await
            .map_err(provider)
        })
    }
    fn to_auth<'a>(
        &'a self,
        v: &'a OAuthCredential,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<ModelAuth, AuthProviderError>> {
        Box::pin(async move {
            Ok(ModelAuth {
                api_key: Some(v.credentials.access.clone()),
                ..Default::default()
            })
        })
    }
}
