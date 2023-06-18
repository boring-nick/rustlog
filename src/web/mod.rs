mod frontend;
mod handlers;
mod responders;
pub mod schema;
mod trace_layer;

use crate::{app::App, ShutdownRx};
use aide::{
    axum::{
        routing::{get, get_with, post},
        ApiRouter, IntoApiResponse,
    },
    openapi::OpenApi,
    redoc::Redoc,
};
use axum::{response::IntoResponse, Extension, Json, ServiceExt};
use prometheus::TextEncoder;
use std::{
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    sync::Arc,
};
use tower_http::{cors::CorsLayer, normalize_path::NormalizePath, trace::TraceLayer};
use tracing::{debug, info};

pub async fn run(app: App<'static>, mut shutdown_rx: ShutdownRx) {
    aide::gen::on_error(|error| {
        panic!("Could not generate docs: {error}");
    });
    aide::gen::infer_responses(true);
    aide::gen::extract_schemas(true);

    let listen_address =
        parse_listen_addr(&app.config.listen_address).expect("Invalid listen address");

    let cors = CorsLayer::permissive();

    let mut api = OpenApi::default();

    let app = ApiRouter::new()
        .api_route(
            "/channels",
            get_with(handlers::get_channels, |op| {
                op.description("List logged channels")
            }),
        )
        .api_route(
            "/list",
            get_with(handlers::list_available_logs, |op| {
                op.description("List available logs")
            }),
        )
        .api_route(
            "/:channel_id_type/:channel",
            get_with(handlers::redirect_to_latest_channel_logs, |op| {
                op.description("Get latest channel logs")
            }),
        )
        // For some reason axum considers it a path overlap if user id type is dynamic
        .api_route(
            "/:channel_id_type/:channel/user/:user",
            get_with(handlers::redirect_to_latest_user_name_logs, |op| {
                op.description("Get latest user logs")
            }),
        )
        .api_route(
            "/:channel_id_type/:channel/userid/:user",
            get_with(handlers::redirect_to_latest_user_id_logs, |op| {
                op.description("Get latest user logs")
            }),
        )
        .api_route(
            "/:channel_id_type/:channel/:year/:month/:day",
            get_with(handlers::get_channel_logs, |op| {
                op.description("Get channel logs from the given day")
            }),
        )
        .api_route(
            "/:channel_id_type/:channel/user/:user/:year/:month",
            get_with(handlers::get_user_logs_by_name, |op| {
                op.description("Get user logs in a channel from the given month")
            }),
        )
        .api_route(
            "/:channel_id_type/:channel/userid/:user/:year/:month",
            get_with(handlers::get_user_logs_by_id, |op| {
                op.description("Get user logs in a channel from the given month")
            }),
        )
        .api_route(
            "/:channel_id_type/:channel/random",
            get_with(handlers::random_channel_line, |op| {
                op.description("Get a random line from the channel's logs")
            }),
        )
        .api_route(
            "/:channel_id_type/:channel/userid/:user/random",
            get_with(handlers::random_user_line_by_id, |op| {
                op.description("Get a random line from the user's logs in a channel")
            }),
        )
        .api_route(
            "/:channel_id_type/:channel/user/:user/random",
            get_with(handlers::random_user_line_by_name, |op| {
                op.description("Get a random line from the user's logs in a channel")
            }),
        )
        .api_route("/optout", post(handlers::optout))
        .route("/docs", Redoc::new("/openapi.json").axum_route())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace_layer::make_span_with)
                .on_response(trace_layer::on_response),
        )
        .route("/openapi.json", get(serve_openapi))
        .route("/metrics", get(metrics))
        .route("/assets/*asset", get(frontend::static_asset))
        .finish_api(&mut api)
        .layer(Extension(Arc::new(api)))
        .layer(Extension(app))
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

async fn metrics() -> String {
    let metric_families = prometheus::gather();

    let encoder = TextEncoder::new();
    encoder.encode_to_string(&metric_families).unwrap()
}
async fn serve_openapi(Extension(api): Extension<Arc<OpenApi>>) -> impl IntoApiResponse {
    Json(api.as_ref()).into_response()
}
