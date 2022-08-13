mod handlers;
mod schema;
mod trace_layer;

use crate::{app::App, config::Config};
use axum::{routing::get, Extension, Router};
use std::{
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    sync::Arc,
};
use tower_http::trace::TraceLayer;
use tracing::info;

pub async fn run(config: Config, app: App<'static>) {
    let listen_address = parse_listen_addr(&config.listen_address).expect("Invalid listen address");

    let app = Router::new()
        .route("/channels", get(handlers::get_channels))
        .route(
            "/:channel_id_type/:channel/:year/:month/:day",
            get(handlers::get_channel_logs),
        )
        // .route(
        //     "/:channel_id_type/:channel/:user_id_type/:user/:year/:month",
        //     get(handlers::get_user_logs),
        // )
        .layer(Extension(app))
        .layer(Extension(Arc::new(config)))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace_layer::make_span_with)
                .on_response(trace_layer::on_response),
        );

    info!("Listening on {listen_address}");

    axum::Server::bind(&listen_address)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

pub fn parse_listen_addr(addr: &str) -> Result<SocketAddr, AddrParseError> {
    if addr.starts_with(':') {
        SocketAddr::from_str(&format!("0.0.0.0{addr}"))
    } else {
        SocketAddr::from_str(addr)
    }
}
