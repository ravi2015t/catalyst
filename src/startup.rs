use std::net::TcpListener;

use crate::routes::{caculate, health_check};
use actix_web::dev::Server;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};

pub fn run(listener: TcpListener) -> Result<Server, std::io::Error> {
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .route("/health_check", web::get().to(health_check))
            .route("/calculate", web::post().to(caculate))
    })
    .listen(listener)?
    .run();
    Ok(server)
}
