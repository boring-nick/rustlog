mod admin;
mod frontend;
mod handlers;
mod responders;
pub mod schema;
mod trace_layer;

use self::handlers::no_cache_header;
use crate::{app::App, bot::BotMessage, web::admin::admin_auth, ShutdownRx};
use aide::{
    axum::{
        routing::{get, get_with, post, post_with},
        ApiRouter, IntoApiResponse,
    },
    openapi::OpenApi,
    redoc::Redoc,
};
use axum::{
    extract::Request,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Extension, Json, ServiceExt,
};
use axum_prometheus::PrometheusMetricLayerBuilder;
use prometheus::TextEncoder;
use std::{
    net::{AddrParseError, SocketAddr},
    str::FromStr,
    sync::Arc,
};
use tokio::{net::TcpListener, sync::mpsc::Sender};
use tower_http::{
    compression::CompressionLayer, cors::CorsLayer, normalize_path::NormalizePath,
    trace::TraceLayer, CompressionLevel,
};
use tracing::{debug, info};

const CAPABILITIES: &[&str] = &["arbitrary-range-query", "search", "stats", "namehistory"];

pub async fn run(app: App, mut shutdown_rx: ShutdownRx, bot_tx: Sender<BotMessage>) {
    aide::generate::on_error(|error| {
        panic!("Could not generate docs: {error}");
    });
    aide::generate::infer_responses(true);
    aide::generate::extract_schemas(true);

    metrics_prometheus::install();

    let listen_address =
        parse_listen_addr(&app.config.listen_address).expect("Invalid listen address");

    let cors = CorsLayer::permissive();

    let mut api = OpenApi::default();

    let admin_routes = ApiRouter::new()
        .api_route(
            "/channels",
            post_with(admin::add_channels, |mut op| {
                admin::admin_auth_doc(&mut op);
                op.tag("Admin").description("Join the specified channels")
            })
            .delete_with(admin::remove_channels, |mut op| {
                admin::admin_auth_doc(&mut op);
                op.tag("Admin").description("Leave the specified channels")
            }),
        )
        .route_layer(middleware::from_fn_with_state(app.clone(), admin_auth))
        .layer(Extension(bot_tx));

    let app = ApiRouter::new()
        .nest("/admin", admin_routes)
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
            "/{channel_id_type}/{channel}",
            get_with(handlers::get_channel_logs, |op| {
                op.description("Get channel logs. If the `to` and `from` query params are not given, redirect to latest available day")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/stats",
            get_with(handlers::get_channel_stats, |op| {
                op.description("Get channel stats")
            }),
        )
        // For some reason axum considers it a path overlap if user id type is dynamic
        .api_route(
            "/{channel_id_type}/{channel}/user/{user}",
            get_with(handlers::get_user_logs_by_name, |op| {
                op.description("Get user logs by name. If the `to` and `from` query params are not given, redirect to latest available month")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/userid/{user}",
            get_with(handlers::get_user_logs_id, |op| {
                op.description("Get user logs by id. If the `to` and `from` query params are not given, redirect to latest available month")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/{year}/{month}/{day}",
            get_with(handlers::get_channel_logs_by_date, |op| {
                op.description("Get channel logs from the given day")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/user/{user}/{year}/{month}",
            get_with(handlers::get_user_logs_by_date_name, |op| {
                op.description("Get user logs in a channel from the given month")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/userid/{user}/{year}/{month}",
            get_with(handlers::get_user_logs_by_date_id, |op| {
                op.description("Get user logs in a channel from the given month")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/random",
            get_with(handlers::random_channel_line, |op| {
                op.description("Get a random line from the channel's logs")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/userid/{user}/random",
            get_with(handlers::random_user_line_by_id, |op| {
                op.description("Get a random line from the user's logs in a channel")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/user/{user}/random",
            get_with(handlers::random_user_line_by_name, |op| {
                op.description("Get a random line from the user's logs in a channel")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/user/{user}/search",
            get_with(handlers::search_user_logs_by_name, |op| {
                op.description("Search user logs using the provided query")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/userid/{user}/search",
            get_with(handlers::search_user_logs_by_id, |op| {
                op.description("Search user logs using the provided query")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/user/{user}/stats",
            get_with(handlers::get_user_stats_by_name, |op| {
                op.description("Get user stats")
            }),
        )
        .api_route(
            "/{channel_id_type}/{channel}/userid/{user}/stats",
            get_with(handlers::get_user_stats_by_id, |op| {
                op.description("Get user stats")
            }),
        )
        .api_route(
            "/namehistory/{user_id}",
            get_with(handlers::get_user_name_history, |op| {
                op.description("Get user name history by provided user id")
            }),
        )
        .api_route("/optout", post(handlers::optout))
        .api_route("/capabilities", get(capabilities))
        .route("/docs", Redoc::new("/openapi.json").axum_route())
        .route("/openapi.json", get(serve_openapi))
        .route("/assets/{*asset}", get(frontend::static_asset))
        .fallback(frontend::static_asset)
        .layer(middleware::from_fn(capabilities_header_middleware))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace_layer::make_span_with)
                .on_response(trace_layer::on_response),
        )
        .layer(
            PrometheusMetricLayerBuilder::new()
                .with_prefix("rustlog")
                .build(),
        )
        .route("/metrics", get(metrics))
        .finish_api(&mut api)
        .layer(Extension(Arc::new(api)))
        .with_state(app)
        .layer(cors)
        .layer(CompressionLayer::new().quality(CompressionLevel::Fastest));
    let app = NormalizePath::trim_trailing_slash(app);

    info!("Listening on {listen_address}");

    let listener = TcpListener::bind(&listen_address)
        .await
        .expect("Could not create TCP listener");

    axum::serve(listener, ServiceExt::<Request>::into_make_service(app))
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

async fn capabilities() -> Json<Vec<&'static str>> {
    Json(CAPABILITIES.to_vec())
}

async fn capabilities_header_middleware(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "x-rustlog-capabilities",
        CAPABILITIES.join(",").try_into().unwrap(),
    );
    response
}

async fn metrics() -> impl IntoApiResponse {
    let metric_families = prometheus::gather();

    let encoder = TextEncoder::new();
    let metrics = encoder.encode_to_string(&metric_families).unwrap();
    (no_cache_header(), metrics)
}
async fn serve_openapi(Extension(api): Extension<Arc<OpenApi>>) -> impl IntoApiResponse {
    Json(api.as_ref()).into_response()
}
