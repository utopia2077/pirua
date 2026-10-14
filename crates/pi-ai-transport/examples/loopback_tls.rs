//! Fresh loopback TLS material for the Node conformance parent process.
//! Stdout is a private pipe; callers must capture it without logging or saving it.
fn main() {
    let material = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()])
        .expect("generate ephemeral loopback certificate");
    let output = serde_json::json!({
        "certificateDer": material.cert.der().as_ref(),
        "privateKeyDer": material.signing_key.serialize_der(),
    });
    println!("{output}");
}
