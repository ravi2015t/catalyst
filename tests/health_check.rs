use catalyst::configurations::get_configuration;
use catalyst::startup::run;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::TcpListener;

struct TestApp {
    address: String,
}

#[tokio::test]
async fn health_check_works() {
    let test_app = spawn_app().await;
    let client = reqwest::Client::new();

    let response = client
        .get(&format!("{}/health_check", test_app.address))
        .send()
        .await
        .expect("Failed to send health_check call");

    assert!(response.status().is_success());
    assert_eq!(Some(0), response.content_length());
}

async fn spawn_app() -> TestApp {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind address");
    let port = listener.local_addr().unwrap().port();
    let server = run(listener).expect("Failed to bind address");
    let _ = tokio::spawn(server);
    TestApp {
        address: format!("http://127.0.0.1:{}", port),
    }
}

#[tokio::test]
async fn check_calculate() {
    let test_app = spawn_app().await;
    // Create a reqwest HTTP client
    let client = reqwest::Client::new();

    // Define the input data as a map of key-value pairs with dynamic types
    let mut input_data = HashMap::new();
    input_data.insert("bool_value".to_string(), json!(true));
    input_data.insert("int_value".to_string(), json!(42));
    input_data.insert("float_value".to_string(), json!(3.14));
    input_data.insert("string_value".to_string(), json!("Hello, World!"));

    // Create a JSON object from the input data
    let json_data: Value = json!({
        "data": input_data,
    });

    // Send a POST request to the Actix web server
    let response = client
        .post(&format!("{}/calculate", test_app.address))
        .json(&json_data)
        .send()
        .await
        .expect("Failed to send calculate call");
    assert!(response.status().is_success());
}
