use catalyst::configurations::get_configuration;
use catalyst::startup::run;
use env_logger::Env;
use std::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let configuration = get_configuration().expect("Failed to read configuration");
    let address = format!("127.0.0.1:{}", configuration.application_port);
    log::info!("Starting Application on {}", configuration.application_port);
    let listener = TcpListener::bind(address).expect("Failed to bind address");
    run(listener)?.await
}
