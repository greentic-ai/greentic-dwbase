use dwbase_wit_guest::ClientConfig;
use serde::Serialize;

/// Configure a guest to talk to a local node.
#[no_mangle]
pub extern "C" fn client_config() -> ClientConfig {
    ClientConfig {
        endpoint: "http://127.0.0.1:8080".into(),
    }
}

#[derive(Serialize)]
struct Observation<'a> {
    text: &'a str,
}

/// Example payload to remember from a WASM guest.
pub fn sample_payload() -> String {
    serde_json::to_string(&Observation { text: "hello from wasm" }).unwrap()
}

/// Example ask text.
pub fn sample_question() -> String {
    "What did the wasm worker say?".into()
}
