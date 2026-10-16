mod support;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use pi_mcp::{oauth::*, transport::*, *};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;

fn token(access: &str, refresh: &str, scope: &str) -> OAuthTokens {
    OAuthTokens {
        access_token: access.into(),
        token_type: "Bearer".into(),
        refresh_token: Some(refresh.into()),
        scope: Some(scope.into()),
        expires_in: Some(3600.0),
        id_token: None,
    }
}
fn provider_options(server: &Server) -> OAuthOptions {
    OAuthOptions::new(
        server.url.clone(),
        "http://127.0.0.1:43210/callback".parse().unwrap(),
        json!({"client_name":"Rust MCP"})
            .as_object()
            .unwrap()
            .clone(),
    )
}
fn configured_client() -> ClientInformation {
    ClientInformation {
        client_id: "test-client".into(),
        client_secret: None,
        extra: Object::new(),
    }
}
fn form(request: &Request) -> std::collections::BTreeMap<String, String> {
    url::form_urlencoded::parse(&request.body)
        .into_owned()
        .collect()
}
fn discovery(request: &Request) -> Option<Response> {
    let origin = format!("http://{}", request.headers["host"]);
    if request
        .path
        .starts_with("/.well-known/oauth-protected-resource")
    {
        return Some(Response::json(
            json!({"resource":format!("{origin}/mcp"),"authorization_servers":[origin],"scopes_supported":["read","offline_access"]}),
        ));
    }
    if request
        .path
        .starts_with("/.well-known/oauth-authorization-server")
    {
        return Some(Response::json(
            json!({"issuer":origin,"authorization_endpoint":format!("{origin}/authorize"),"token_endpoint":format!("{origin}/token"),"registration_endpoint":format!("{origin}/register"),"response_types_supported":["code"],"code_challenge_methods_supported":["S256"],"token_endpoint_auth_methods_supported":["none"],"authorization_response_iss_parameter_supported":true}),
        ));
    }
    None
}
#[tokio::test]
async fn real_discovery_registration_pkce_exchange_and_shared_rotating_refresh() {
    let refreshes = Arc::new(AtomicUsize::new(0));
    let refresh_count = refreshes.clone();
    let verifier = Arc::new(Mutex::new(None::<String>));
    let expected_verifier = verifier.clone();
    let mut server=server(Arc::new(move |request|{let refresh_count=refresh_count.clone();let verifier=expected_verifier.clone();Box::pin(async move {
        if let Some(response)=discovery(&request) {return response;}
        if request.path=="/register" {let metadata=request.json();assert_eq!(metadata["application_type"],"native");assert_eq!(metadata["client_name"],"Rust MCP");return Response::json(json!({"client_id":"registered-client","token_endpoint_auth_method":"none"}));}
        if request.path=="/token" {
            let params=form(&request);assert_eq!(params["client_id"],"registered-client");assert!(params["resource"].ends_with("/mcp"));
            if params["grant_type"]=="authorization_code" {assert_eq!(params["code"],"valid-code");assert_eq!(Some(&params["code_verifier"]),verifier.lock().unwrap().as_ref());assert_eq!(params["redirect_uri"],"http://127.0.0.1:43210/callback");return Response::json(json!({"access_token":"initial","token_type":"Bearer","refresh_token":"refresh-one","expires_in":"3600"}));}
            assert_eq!(params["refresh_token"],"refresh-one");assert_eq!(refresh_count.fetch_add(1,Ordering::SeqCst),0,"rotating token refreshed twice");
            return Response::json(json!({"access_token":"renewed","token_type":"Bearer","refresh_token":"refresh-two"})).delayed(Duration::from_millis(35));
        }
        if request.method=="DELETE" {assert_eq!(request.headers["authorization"],"Bearer renewed");return Response::empty(204);}
        let message=request.json();let auth=request.headers.get("authorization").map(String::as_str);
        if message["method"]=="initialize" {assert_eq!(auth,Some("Bearer initial"));return Response::json(initialized(message["id"].clone())).header("mcp-session-id","auth-session");}
        if message["method"]=="ping" {
            if auth==Some("Bearer initial") {return Response::empty(401).header("www-authenticate","Bearer error=\"invalid_token\"");}
            assert_eq!(auth,Some("Bearer renewed"));return Response::json(json!({"jsonrpc":"2.0","id":message["id"],"result":{}}));
        }
        Response::empty(202)
    })})).await;
    let oauth = Arc::new(OAuthProvider::new(provider_options(&server)).unwrap());
    let redirect = match oauth.authorize(FlowOptions::default()).await.unwrap() {
        AuthorizationResult::Redirect(url) => url,
        _ => panic!("expected redirect"),
    };
    let params: std::collections::BTreeMap<_, _> = redirect.query_pairs().into_owned().collect();
    assert_eq!(params["code_challenge_method"], "S256");
    assert_eq!(params["prompt"], "consent");
    assert_eq!(params["scope"], "read offline_access");
    assert!(!params["state"].is_empty());
    let stored = oauth.state().await.unwrap();
    let code_verifier = stored.code_verifier.unwrap();
    assert_eq!(code_verifier.len(), 43);
    let expected = URL_SAFE_NO_PAD
        .encode(ring::digest::digest(&ring::digest::SHA256, code_verifier.as_bytes()).as_ref());
    assert_eq!(params["code_challenge"], expected);
    *verifier.lock().unwrap() = Some(code_verifier);
    assert_eq!(
        oauth
            .authorize(FlowOptions {
                authorization_code: Some("valid-code".into()),
                issuer: Some(server.url.origin().ascii_serialization()),
                ..Default::default()
            })
            .await
            .unwrap(),
        AuthorizationResult::Authorized
    );
    assert_eq!(
        oauth
            .state()
            .await
            .unwrap()
            .tokens
            .as_ref()
            .unwrap()
            .scope
            .as_deref(),
        Some("read offline_access")
    );
    let mut options = HttpOptions::new(server.url.clone());
    options.auth = Some(oauth.clone());
    options.open_get_stream = false;
    let client = McpClient::connect(
        ClientOptions::new("rust", "1"),
        Arc::new(StreamableHttpTransport::new(options).unwrap()),
    )
    .await
    .unwrap();
    let (a, b) = tokio::join!(
        client.ping(RequestOptions::default()),
        client.ping(RequestOptions::default())
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    let tokens = oauth.state().await.unwrap().tokens.unwrap();
    assert_eq!(tokens.refresh_token.as_deref(), Some("refresh-two"));
    assert_eq!(tokens.scope.as_deref(), Some("read offline_access"));
    client.close().await.unwrap();
    let mut refreshed = false;
    while let Ok(request) = server.requests.try_recv() {
        if request.path == "/token" && form(&request)["grant_type"] == "refresh_token" {
            refreshed = true;
        }
    }
    assert!(refreshed);
}
#[tokio::test]
async fn insufficient_scope_uses_redirect_with_union_without_refresh() {
    let refreshes = Arc::new(AtomicUsize::new(0));
    let count = refreshes.clone();
    let server = server(Arc::new(move |request| {
        let count = count.clone();
        Box::pin(async move {
            if let Some(response) = discovery(&request) {
                return response;
            }
            if request.path == "/token" {
                count.fetch_add(1, Ordering::SeqCst);
                return Response::empty(500);
            }
            let message = request.json();
            if message["method"] == "initialize" {
                return Response::json(initialized(message["id"].clone()));
            }
            Response::empty(403).header(
                "www-authenticate",
                "Bearer error=\"insufficient_scope\", scope=\"write read\"",
            )
        })
    }))
    .await;
    let mut options = provider_options(&server);
    options.client_information = Some(configured_client());
    let oauth = Arc::new(OAuthProvider::new(options).unwrap());
    oauth
        .save_tokens(token("old", "refresh", "read"))
        .await
        .unwrap();
    let mut options = HttpOptions::new(server.url.clone());
    options.auth = Some(oauth);
    options.open_get_stream = false;
    // Initialized notification itself requests step-up in this fixture.
    let result = McpClient::connect(
        ClientOptions::new("c", "1"),
        Arc::new(StreamableHttpTransport::new(options).unwrap()),
    )
    .await;
    let url = match result {
        Err(Error::AuthorizationRequired { authorization_url }) => {
            url::Url::parse(&authorization_url).unwrap()
        }
        _ => panic!("expected authorization redirect"),
    };
    let params: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
    assert_eq!(params["scope"], "read write");
    assert_eq!(refreshes.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn cancelled_request_is_never_redispatched_after_shared_refresh() {
    let pings = Arc::new(AtomicUsize::new(0));
    let count = pings.clone();
    let (refresh_started, mut started) = tokio::sync::mpsc::unbounded_channel();
    let server = server(Arc::new(move |request| {
        let count = count.clone();
        let started = refresh_started.clone();
        Box::pin(async move {
            if let Some(response) = discovery(&request) {
                return response;
            }
            if request.path == "/token" {
                started.send(()).unwrap();
                return Response::json(json!({"access_token":"new","token_type":"Bearer"}))
                    .delayed(Duration::from_millis(80));
            }
            let message = request.json();
            if message["method"] == "initialize" {
                return Response::json(initialized(message["id"].clone()));
            }
            if message["method"] == "ping" {
                count.fetch_add(1, Ordering::SeqCst);
                return Response::empty(401).header("www-authenticate", "Bearer");
            }
            Response::empty(202)
        })
    }))
    .await;
    let mut options = provider_options(&server);
    options.client_information = Some(configured_client());
    let oauth = Arc::new(OAuthProvider::new(options).unwrap());
    oauth
        .save_tokens(token("old", "refresh", "read"))
        .await
        .unwrap();
    let mut options = HttpOptions::new(server.url.clone());
    options.auth = Some(oauth.clone());
    options.open_get_stream = false;
    let client = McpClient::connect(
        ClientOptions::new("c", "1"),
        Arc::new(StreamableHttpTransport::new(options).unwrap()),
    )
    .await
    .unwrap();
    let calls = client.clone();
    let task = tokio::spawn(async move {
        calls
            .ping(RequestOptions {
                timeout: Some(Duration::from_millis(35)),
                ..Default::default()
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), started.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(Error::Timeout(_))));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if oauth
                .state()
                .await
                .unwrap()
                .tokens
                .as_ref()
                .unwrap()
                .access_token
                == "new"
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A barrier after refresh gives any forbidden retry a chance to reach peer.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(pings.load(Ordering::SeqCst), 1);
    client.close().await.unwrap();
}
#[tokio::test]
async fn exact_server_binding_and_issuer_checks_prevent_credential_or_code_leaks() {
    let tokens = Arc::new(AtomicUsize::new(0));
    let count = tokens.clone();
    let server = server(Arc::new(move |request| {
        let count = count.clone();
        Box::pin(async move {
            if let Some(response) = discovery(&request) {
                return response;
            }
            if request.path == "/token" {
                count.fetch_add(1, Ordering::SeqCst);
            }
            Response::empty(404)
        })
    }))
    .await;
    let store = Arc::new(MemoryOAuthStateStore::default());
    let mut options = provider_options(&server);
    options.store = store.clone();
    options.client_information = Some(configured_client());
    let provider = OAuthProvider::new(options.clone()).unwrap();
    provider
        .save_tokens(token("secret", "refresh", "read"))
        .await
        .unwrap();
    options.server_url = server.url.join("/another-mcp").unwrap();
    let other = OAuthProvider::new(options).unwrap();
    assert!(other.state().await.unwrap().tokens.is_none());
    let result = provider
        .authorize(FlowOptions {
            authorization_code: Some("sensitive-code".into()),
            issuer: Some("https://different.example".into()),
            ..Default::default()
        })
        .await;
    assert!(matches!(result,Err(Error::OAuth {code,..}) if code=="issuer_mismatch"));
    assert_eq!(tokens.load(Ordering::SeqCst), 0);
    assert!(matches!(
        other
            .on_unauthorized(UnauthorizedContext {
                server_url: server.url.clone(),
                status: 401,
                challenge: None,
                rejected_token: None
            })
            .await,
        Err(Error::Invalid(_))
    ));
    let mut wrong_transport = HttpOptions::new(server.url.join("/wrong-mcp").unwrap());
    wrong_transport.auth = Some(Arc::new(provider));
    assert!(matches!(
        StreamableHttpTransport::new(wrong_transport),
        Err(Error::Invalid(_))
    ));
}
#[tokio::test]
async fn metadata_issuer_mismatch_and_insecure_token_endpoint_are_rejected() {
    for insecure in [false, true] {
        let server=server(Arc::new(move |request|Box::pin(async move {
            if request.path.starts_with("/.well-known/oauth-protected-resource") {return Response::empty(404);}
            let origin=format!("http://{}",request.headers["host"]);
            Response::json(json!({"issuer":if insecure {origin.clone()} else {"https://wrong.example".into()},"authorization_endpoint":format!("{origin}/authorize"),"token_endpoint":if insecure {"http://insecure.example/token".into()} else {format!("{origin}/token")},"response_types_supported":["code"]}))
        }))).await;
        let mut options = provider_options(&server);
        options.client_information = Some(configured_client());
        let provider = OAuthProvider::new(options).unwrap();
        assert!(matches!(
            provider.authorize(FlowOptions::default()).await,
            Err(Error::OAuth { .. })
        ));
    }
}
#[tokio::test]
async fn configured_metadata_is_used_and_aborted_flow_does_not_redirect() {
    let server=server(Arc::new(|request|Box::pin(async move {
        let origin=format!("http://{}",request.headers["host"]);
        if request.path=="/configured" {return Response::json(json!({"issuer":format!("{origin}/other-issuer"),"authorization_endpoint":format!("{origin}/authorize-custom"),"token_endpoint":format!("{origin}/token"),"response_types_supported":["code"]}));}
        Response::empty(404)
    }))).await;
    let mut options = provider_options(&server);
    options.client_information = Some(configured_client());
    let provider = OAuthProvider::new(options).unwrap();
    let flow = FlowOptions {
        authorization_server_metadata_url: Some(server.url.join("/configured").unwrap()),
        ..Default::default()
    };
    match provider.authorize(flow.clone()).await.unwrap() {
        AuthorizationResult::Redirect(url) => assert_eq!(url.path(), "/authorize-custom"),
        _ => panic!("expected redirect"),
    }
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        provider
            .authorize(FlowOptions {
                cancellation,
                ..flow
            })
            .await,
        Err(Error::Cancelled)
    );
}
#[test]
fn challenge_parser_and_resource_path_boundary() {
    let challenge = parse_www_authenticate(Some(
        "Bearer realm=\"MCP\", scope=\"read write\", resource_metadata=\"https://example.com/metadata\", error=insufficient_scope",
    ));
    assert_eq!(challenge.scope.as_deref(), Some("read write"));
    assert_eq!(challenge.error.as_deref(), Some("insufficient_scope"));
    assert_eq!(challenge.resource_metadata_url.unwrap().path(), "/metadata");
    let metadata = ProtectedResourceMetadata {
        resource: "https://example.com/mcp".into(),
        authorization_servers: Vec::new(),
        scopes_supported: Vec::new(),
        extra: Object::new(),
    };
    assert!(
        select_resource(
            &"https://example.com/mcp/sub".parse().unwrap(),
            Some(&metadata)
        )
        .is_ok()
    );
    assert!(
        select_resource(
            &"https://example.com/mcp-evil".parse().unwrap(),
            Some(&metadata)
        )
        .is_err()
    );
    assert!(
        select_resource(
            &"https://other.example/mcp".parse().unwrap(),
            Some(&metadata)
        )
        .is_err()
    );
    assert_eq!(
        step_up_scope(Some("read write"), Some("write admin")),
        Some("read write admin".into())
    );
    assert_eq!(step_up_scope(Some("read"), None), None);
}

#[tokio::test]
async fn token_client_auth_methods_follow_metadata_and_invalid_grant_starts_new_authorization() {
    for method in ["client_secret_basic", "client_secret_post", "none"] {
        let token_calls = Arc::new(AtomicUsize::new(0));
        let count = token_calls.clone();
        let server = server(Arc::new(move |request| {
            let count = count.clone(); Box::pin(async move {
                let origin = format!("http://{}", request.headers["host"]);
                if request.path.starts_with("/.well-known/oauth-authorization-server") {
                    return Response::json(json!({"issuer":origin,"authorization_endpoint":format!("{origin}/authorize"),"token_endpoint":format!("{origin}/token"),"response_types_supported":["code"],"token_endpoint_auth_methods_supported":["client_secret_basic","client_secret_post","none"]}));
                }
                if let Some(response) = discovery(&request) { return response; }
                assert_eq!(request.path, "/token"); let params = form(&request);
                match method {
                    "client_secret_basic" => {
                        assert_eq!(request.headers["authorization"], "Basic dGVzdC1jbGllbnQ6c2VjcmV0");
                        assert!(!params.contains_key("client_secret"));
                    }
                    "client_secret_post" => { assert_eq!(params["client_id"], "test-client"); assert_eq!(params["client_secret"], "secret"); }
                    _ => { assert_eq!(params["client_id"], "test-client"); assert!(!params.contains_key("client_secret")); assert!(!request.headers.contains_key("authorization")); }
                }
                if count.fetch_add(1, Ordering::SeqCst) == 0 { Response::json(json!({"error":"invalid_grant","error_description":"expired refresh grant"})) }
                else { Response::json(json!({"access_token":"new-token","token_type":"Bearer"})) }
            })
        })).await;
        let mut options = provider_options(&server);
        let mut client = configured_client();
        client.client_secret = Some("secret".into());
        client
            .extra
            .insert("token_endpoint_auth_method".into(), json!(method));
        options.client_information = Some(client);
        let provider = OAuthProvider::new(options).unwrap();
        provider
            .save_tokens(token("old", "expired-refresh", "read"))
            .await
            .unwrap();
        assert!(matches!(
            provider.authorize(FlowOptions::default()).await.unwrap(),
            AuthorizationResult::Redirect(_)
        ));
        assert!(provider.state().await.unwrap().tokens.is_none());
        assert_eq!(token_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider
                .authorize(FlowOptions {
                    authorization_code: Some("new-code".into()),
                    ..Default::default()
                })
                .await
                .unwrap(),
            AuthorizationResult::Authorized
        );
        assert_eq!(
            provider
                .state()
                .await
                .unwrap()
                .tokens
                .as_ref()
                .unwrap()
                .access_token,
            "new-token"
        );
    }
}

#[tokio::test]
async fn authorization_parameters_replace_stale_endpoint_values_and_preserve_unrelated_query() {
    for optional in [false, true] {
        let server = server(Arc::new(move |request| Box::pin(async move {
            let origin = format!("http://{}", request.headers["host"]);
            if request.path.starts_with("/.well-known/oauth-protected-resource") {
                return if optional {
                    Response::json(json!({"resource":format!("{origin}/mcp"),"authorization_servers":[origin]}))
                } else { Response::empty(404) };
            }
            if request.path.starts_with("/.well-known/oauth-authorization-server") {
                let stale = "response_type=token&client_id=stale-client&client_id=another-client&state=stale-state&code_challenge=stale-challenge&code_challenge_method=plain&redirect_uri=https%3A%2F%2Fstale.example%2Fcallback&scope=stale-scope&prompt=stale-prompt&resource=https%3A%2F%2Fstale.example&tenant=keep&tenant=keep-too";
                return Response::json(json!({"issuer":origin,"authorization_endpoint":format!("{origin}/authorize?{stale}"),"token_endpoint":format!("{origin}/token"),"response_types_supported":["code"]}));
            }
            Response::empty(404)
        }))).await;
        let mut options = provider_options(&server);
        let mut client = configured_client();
        client.client_id = "actual-client".into();
        options.client_information = Some(client);
        let provider = OAuthProvider::new(options).unwrap();
        let redirect = match provider
            .authorize(FlowOptions {
                scope: optional.then(|| "read offline_access".into()),
                ..Default::default()
            })
            .await
            .unwrap()
        {
            AuthorizationResult::Redirect(url) => url,
            other => panic!("expected redirect: {other:?}"),
        };
        let values = |name: &str| {
            redirect
                .query_pairs()
                .filter(|(key, _)| key == name)
                .map(|(_, value)| value.into_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(values("client_id"), ["actual-client"]);
        assert_eq!(values("response_type"), ["code"]);
        assert_eq!(values("redirect_uri"), ["http://127.0.0.1:43210/callback"]);
        assert_eq!(values("code_challenge_method"), ["S256"]);
        let state = provider.state().await.unwrap();
        assert_eq!(values("state"), [state.oauth_state.unwrap()]);
        let verifier = state.code_verifier.unwrap();
        let expected_challenge = URL_SAFE_NO_PAD
            .encode(ring::digest::digest(&ring::digest::SHA256, verifier.as_bytes()).as_ref());
        assert_eq!(values("code_challenge"), [expected_challenge]);
        assert_eq!(values("tenant"), ["keep", "keep-too"]);
        if optional {
            assert_eq!(values("scope"), ["read offline_access"]);
            assert_eq!(values("prompt"), ["consent"]);
            assert_eq!(values("resource"), [server.url.as_str()]);
        } else {
            // The target only sets optional parameters supplied by this flow.
            assert_eq!(values("scope"), ["stale-scope"]);
            assert_eq!(values("prompt"), ["stale-prompt"]);
            assert_eq!(values("resource"), ["https://stale.example"]);
        }
    }
}
