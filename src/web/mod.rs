mod frontend;
mod handlers;
mod responders;
mod schema;
mod trace_layer;

use crate::{app::App, ShutdownRx};
use axum::{routing::get, Extension, Router, ServiceExt};
use prometheus::{Encoder, TextEncoder};
use std::{
    net::{AddrParseError, SocketAddr},
    str::FromStr,
};
use tower_http::{cors::CorsLayer, normalize_path::NormalizePath, trace::TraceLayer};
use tracing::{debug, info};

pub async fn run(app: App<'static>, mut shutdown_rx: ShutdownRx) {
    let listen_address =
        parse_listen_addr(&app.config.listen_address).expect("Invalid listen address");

    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/channels", get(handlers::get_channels))
        .route("/list", get(handlers::list_available_logs))
        .route(
            "/:channel_id_type/:channel",
            get(handlers::redirect_to_latest_channel_logs),
        )
        // For some reason axum considers it a path overlap if user id type is dynamic
        .route(
            "/:channel_id_type/:channel/user/:user",
            get(handlers::redirect_to_latest_user_name_logs),
        )
        .route(
            "/:channel_id_type/:channel/userid/:user",
            get(handlers::redirect_to_latest_user_id_logs),
        )
        .route(
            "/:channel_id_type/:channel/:year/:month/:day",
            get(handlers::get_channel_logs),
        )
        .route(
            "/:channel_id_type/:channel/user/:user/:year/:month",
            get(handlers::get_user_logs_by_name),
        )
        .route(
            "/:channel_id_type/:channel/userid/:user/:year/:month",
            get(handlers::get_user_logs_by_id),
        )
        /*.route(
            "/:channel_id_type/:channel/random",
            get(handlers::random_channel_line),
        )
        .route(
            "/:channel_id_type/:channel/userid/:user/random",
            get(handlers::random_user_line_by_id),
        )
        .route(
            "/:channel_id_type/:channel/user/:user/random",
            get(handlers::random_user_line_by_name),
        )*/
        .route("/metrics", get(metrics))
        .layer(Extension(app))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace_layer::make_span_with)
                .on_response(trace_layer::on_response),
        )
        .route("/assets/*asset", get(frontend::static_asset))
        .fallback(frontend::static_asset)
        .layer(cors);
    let app = NormalizePath::trim_trailing_slash(app);

    info!("Listening on {listen_address}");

    axum::Server::bind(&listen_address)
        .serve(app.into_make_service())
        .with_graceful_shutdown(async move {
            shutdown_rx.changed().await.ok();
            debug!("Shutting down web task");
        })
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

async fn metrics() -> Vec<u8> {
    let metric_families = prometheus::gather();

    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    buffer
}
