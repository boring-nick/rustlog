mod handlers;
mod responders;
mod schema;
mod trace_layer;

use crate::{app::App, config::Config};
use axum::{routing::get, Extension, Router};
use axum_extra::routing::SpaRouter;
use std::{
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    sync::Arc,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;

pub async fn run(config: Config, app: App<'static>) {
    let listen_address = parse_listen_addr(&config.listen_address).expect("Invalid listen address");

    let cors = CorsLayer::permissive();

    let spa = SpaRouter::new("/_app", "web/build");

    let app = Router::new()
        .merge(spa)
        .route("/channels", get(handlers::get_channels))
        .route("/list", get(handlers::list_available_user_logs))
        .route(
            "/:channel_id_type/:channel/:year/:month/:day",
            get(handlers::get_channel_logs),
        )
        // For some reason axum considers it a path overlap if user id type is dynamic
        .route(
            "/:channel_id_type/:channel/user/:user/:year/:month",
            get(handlers::get_user_logs_by_name),
        )
        .route(
            "/:channel_id_type/:channel/userid/:user/:year/:month",
            get(handlers::get_user_logs_by_id),
        )
        .layer(Extension(app))
        .layer(Extension(Arc::new(config)))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace_layer::make_span_with)
                .on_response(trace_layer::on_response),
        )
        .layer(cors);

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
