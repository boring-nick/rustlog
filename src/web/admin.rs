use crate::{app::App, bot::BotMessage, error::Error};
use aide::{
    openapi::{
        HeaderStyle, Parameter, ParameterData, ParameterSchemaOrContent, ReferenceOr, SchemaObject,
    },
    transform::TransformOperation,
};
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
    Extension, Json,
};
use reqwest::StatusCode;
use schemars::JsonSchema;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;

pub async fn admin_auth(
    app: State<App>,
    request: Request,
    next: Next,
) -> Result<Response, impl IntoResponse> {
    if let Some(admin_key) = &app.config.admin_api_key {
        if request
            .headers()
            .get("X-Api-Key")
            .and_then(|value| value.to_str().ok())
            == Some(admin_key)
        {
            let response = next.run(request).await;
            return Ok(response);
        }
    }

    Err((StatusCode::FORBIDDEN, "No, I don't think so"))
}

pub fn admin_auth_doc(op: &mut TransformOperation) {
    let schema = aide::gen::in_context(|ctx| ctx.schema.subschema_for::<String>());

    op.inner_mut()
        .parameters
        .push(ReferenceOr::Item(Parameter::Header {
            parameter_data: ParameterData {
                name: "X-Api-Key".to_owned(),
                description: Some("Configured admin API key".to_owned()),
                required: true,
                deprecated: None,
                format: ParameterSchemaOrContent::Schema(SchemaObject {
                    json_schema: schema,
                    external_docs: None,
                    example: None,
                }),
                example: None,
                examples: Default::default(),
                explode: None,
                extensions: Default::default(),
            },
            style: HeaderStyle::Simple,
        }));
}

#[derive(Deserialize, JsonSchema)]
pub struct ChannelsRequest {
    /// List of channel ids
    pub channels: Vec<String>,
}

pub async fn add_channels(
    Extension(bot_tx): Extension<Sender<BotMessage>>,
    app: State<App>,
    Json(ChannelsRequest { channels }): Json<ChannelsRequest>,
) -> Result<(), Error> {
    let users = app.get_users(channels, vec![], false).await?;
    let names = users.into_values().collect();

    bot_tx.send(BotMessage::JoinChannels(names)).await.unwrap();

    Ok(())
}

pub async fn remove_channels(
    Extension(bot_tx): Extension<Sender<BotMessage>>,
    app: State<App>,
    Json(ChannelsRequest { channels }): Json<ChannelsRequest>,
) -> Result<(), Error> {
    let users = app.get_users(channels, vec![], false).await?;
    let names = users.into_values().collect();

    bot_tx.send(BotMessage::PartChannels(names)).await.unwrap();

    Ok(())
}
