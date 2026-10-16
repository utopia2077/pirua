use super::*;
use crate::{CancellationToken, Error, LATEST_PROTOCOL_VERSION, Result};
use reqwest::Client;
use serde::de::DeserializeOwned;
use url::Url;

pub fn parse_www_authenticate(header: Option<&str>) -> OAuthChallenge {
    let Some(header) = header else {
        return OAuthChallenge::default();
    };
    let (scheme, fields) = header
        .trim_start()
        .split_once(char::is_whitespace)
        .unwrap_or((header, ""));
    if !scheme.eq_ignore_ascii_case("bearer") && !scheme.eq_ignore_ascii_case("dpop") {
        return OAuthChallenge::default();
    }
    let mut result = OAuthChallenge::default();
    // Scan quoted fields so commas/spaces inside a quoted scope stay intact.
    let mut fields = fields.trim();
    while !fields.is_empty() {
        fields = fields.trim_start_matches(|c: char| c.is_whitespace() || c == ',');
        let Some((key, rest)) = fields.split_once('=') else {
            break;
        };
        let (value, tail) = if let Some(rest) = rest.strip_prefix('"') {
            match rest.split_once('"') {
                Some(pair) => pair,
                None => break,
            }
        } else {
            rest.split_once(|c: char| c == ',' || c.is_whitespace())
                .unwrap_or((rest, ""))
        };
        if !value.is_empty() {
            match key.trim().to_ascii_lowercase().as_str() {
                "resource_metadata" => result.resource_metadata_url = Url::parse(value).ok(),
                "scope" => result.scope = Some(value.into()),
                "error" => result.error = Some(value.into()),
                "error_description" => result.error_description = Some(value.into()),
                _ => {}
            }
        }
        fields = tail;
    }
    result
}
fn miss(status: u16) -> bool {
    (400..500).contains(&status) || status == 502
}
fn parse_url(value: &str) -> Result<Url> {
    Url::parse(value).map_err(|_| Error::invalid("invalid OAuth metadata URL"))
}
pub(crate) fn secure_endpoint(url: Url) -> Result<Url> {
    let loopback = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
    );
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(Error::OAuth {
            code: "insecure_endpoint".into(),
            message: "OAuth endpoints require HTTPS except loopback HTTP".into(),
        });
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::invalid("OAuth endpoint userinfo is forbidden"));
    }
    Ok(url)
}
pub(crate) async fn metadata<T: DeserializeOwned>(
    client: &Client,
    url: Url,
    cancellation: &CancellationToken,
) -> Result<Option<T>> {
    let url = secure_endpoint(url)?;
    let response = tokio::select! { _ = cancellation.cancelled() => return Err(Error::Cancelled), response = client.get(url).header("accept","application/json").header("mcp-protocol-version",LATEST_PROTOCOL_VERSION).send() => response? };
    if miss(response.status().as_u16()) {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(Error::Http {
            status: response.status().as_u16(),
            body: "OAuth metadata discovery failed".into(),
        });
    }
    let bytes = crate::transport::http::read_bounded(
        response,
        crate::DEFAULT_MAX_MESSAGE_BYTES,
        cancellation,
        cancellation,
    )
    .await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}
pub fn build_discovery_urls(issuer: &Url) -> Vec<Url> {
    let path = issuer.path().trim_end_matches('/');
    let origin = issuer.origin().ascii_serialization();
    let mut urls = vec![
        Url::parse(&format!(
            "{origin}/.well-known/oauth-authorization-server{path}"
        ))
        .unwrap(),
        Url::parse(&format!("{origin}/.well-known/openid-configuration{path}")).unwrap(),
    ];
    if !path.is_empty() {
        urls.push(Url::parse(&format!("{origin}{path}/.well-known/openid-configuration")).unwrap());
    }
    urls
}
pub(crate) async fn discover_authorization_server(
    client: &Client,
    issuer: &Url,
    cancellation: &CancellationToken,
) -> Result<Option<AuthorizationServerMetadata>> {
    for url in build_discovery_urls(issuer) {
        if let Some(metadata) =
            metadata::<AuthorizationServerMetadata>(client, url, cancellation).await?
        {
            if metadata.issuer.trim_end_matches('/') != issuer.as_str().trim_end_matches('/') {
                return Err(Error::OAuth {
                    code: "issuer_mismatch".into(),
                    message: "authorization server issuer does not match discovery".into(),
                });
            }
            validate_authorization_metadata(&metadata)?;
            return Ok(Some(metadata));
        }
    }
    Ok(None)
}
fn validate_authorization_metadata(metadata: &AuthorizationServerMetadata) -> Result<()> {
    for endpoint in [
        &metadata.issuer,
        &metadata.authorization_endpoint,
        &metadata.token_endpoint,
    ] {
        secure_endpoint(parse_url(endpoint)?)?;
    }
    if let Some(endpoint) = &metadata.registration_endpoint {
        secure_endpoint(parse_url(endpoint)?)?;
    }
    Ok(())
}
pub async fn discover_server_info(
    client: &Client,
    server: &Url,
    resource_url: Option<&Url>,
    configured_metadata: Option<&Url>,
    cancellation: &CancellationToken,
) -> Result<DiscoveryState> {
    let resource_metadata = async {
        if let Some(url) = resource_url {
            return metadata::<ProtectedResourceMetadata>(client, url.clone(), cancellation).await;
        }
        let origin = server.origin().ascii_serialization();
        let path = server.path().trim_end_matches('/');
        let url = parse_url(&format!(
            "{origin}/.well-known/oauth-protected-resource{path}"
        ))?;
        if let Some(metadata) = metadata(client, url, cancellation).await? {
            return Ok(Some(metadata));
        }
        if !path.is_empty() {
            return metadata(
                client,
                parse_url(&format!("{origin}/.well-known/oauth-protected-resource"))?,
                cancellation,
            )
            .await;
        }
        Ok(None)
    }
    .await;
    let resource_metadata = match resource_metadata {
        Err(error @ (Error::Cancelled | Error::Io(_))) => return Err(error),
        Ok(value) => value,
        Err(_) => None,
    };
    if let Some(url) = configured_metadata {
        let metadata: AuthorizationServerMetadata = metadata(client, url.clone(), cancellation)
            .await?
            .ok_or_else(|| Error::invalid("configured OAuth metadata not found"))?;
        validate_authorization_metadata(&metadata)?;
        return Ok(DiscoveryState {
            authorization_server_url: metadata.issuer.clone(),
            authorization_server_metadata: Some(metadata),
            resource_metadata,
        });
    }
    let issuer = resource_metadata
        .as_ref()
        .and_then(|metadata| metadata.authorization_servers.first())
        .map(|value| parse_url(value))
        .transpose()?
        .unwrap_or_else(|| server.join("/").unwrap());
    let metadata = discover_authorization_server(client, &issuer, cancellation).await?;
    Ok(DiscoveryState {
        authorization_server_url: issuer.to_string(),
        authorization_server_metadata: metadata,
        resource_metadata,
    })
}
pub fn select_resource(
    server: &Url,
    metadata: Option<&ProtectedResourceMetadata>,
) -> Result<Option<String>> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let resource = parse_url(&metadata.resource)?;
    let requested_path = format!("{}/", server.path().trim_end_matches('/'));
    let resource_path = format!("{}/", resource.path().trim_end_matches('/'));
    if server.origin() != resource.origin() || !requested_path.starts_with(&resource_path) {
        return Err(Error::invalid(
            "protected resource does not match MCP server",
        ));
    }
    Ok(Some(metadata.resource.clone()))
}
