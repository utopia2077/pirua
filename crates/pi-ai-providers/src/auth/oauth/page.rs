use pi_ai_transport::CallbackResponse;
fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
pub(super) fn page(
    status: u16,
    success: bool,
    message: &str,
    details: Option<&str>,
    no_store: bool,
) -> CallbackResponse {
    let title = if success {
        "Authentication successful"
    } else {
        "Authentication failed"
    };
    let details = details
        .filter(|s| !s.is_empty())
        .map(|s| format!("<div class=\"details\">{}</div>", escape(s)))
        .unwrap_or_default();
    CallbackResponse {
        status,
        content_type: "text/html; charset=utf-8".into(),
        no_store,
        body: format!(
            include_str!("page.html"),
            title = title,
            heading = title,
            message = escape(message),
            details = details
        ),
    }
}
