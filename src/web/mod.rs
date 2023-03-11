mod frontend;
mod handlers;
mod responders;
mod schema;
mod trace_layer;

use crate::app::App;
use axum::{routing::get, Extension, Router};
use std::{
    net::{AddrParseError, SocketAddr},
    str::FromStr,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;

pub async fn run(app: App<'static>) {
    let listen_address =
        parse_listen_addr(&app.config.listen_address).expect("Invalid listen address");

    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/channels", get(handlers::get_channels))
        .route("/list", get(handlers::list_available_logs))
        // The redirect routes are duplicated to handle a trailing slash properly
        .route(
            "/:channel_id_type/:channel",
            get(handlers::redirect_to_latest_channel_logs),
        )
        .route(
            "/:channel_id_type/:channel/",
            get(handlers::redirect_to_latest_channel_logs),
        )
        // For some reason axum considers it a path overlap if user id type is dynamic
        .route(
            "/:channel_id_type/:channel/user/:user",
            get(handlers::redirect_to_latest_user_name_logs),
        )
        .route(
            "/:channel_id_type/:channel/user/:user/",
            get(handlers::redirect_to_latest_user_name_logs),
        )
        .route(
            "/:channel_id_type/:channel/userid/:user",
            get(handlers::redirect_to_latest_user_id_logs),
        )
        .route(
            "/:channel_id_type/:channel/userid/:user/",
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
        .route(
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
        )
        .layer(Extension(app))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace_layer::make_span_with)
                .on_response(trace_layer::on_response),
        )
        .route("/assets/*asset", get(frontend::static_asset))
        .fallback(frontend::static_asset)
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
