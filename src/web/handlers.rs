use super::schema::{Channel, ChannelIdType, ChannelLogsParams, ChannelsList, UserLogsParams};
use crate::{app::App, config::Config, error::Error, logs::schema::ChannelLogDate, Result};
use axum::{extract::Path, Extension, Json};
use std::sync::Arc;

pub async fn get_channels(
    app: Extension<App<'_>>,
    config: Extension<Arc<Config>>,
) -> Json<ChannelsList> {
    let channels = app
        .get_users(config.channels.clone(), vec![])
        .await
        .unwrap();

    Json(ChannelsList {
        channels: channels
            .into_iter()
            .map(|(user_id, name)| Channel { name, user_id })
            .collect(),
    })
}

pub async fn get_channel_logs(
    app: Extension<App<'_>>,
    Path(channel_log_params): Path<ChannelLogsParams>,
) -> Result<String> {
    let channel_id = match channel_log_params.channel_id_type {
        ChannelIdType::Name => {
            let (id, _) = app
                .get_users(vec![], vec![channel_log_params.channel.clone()])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| Error::NotFound)?;
            id
        }

        ChannelIdType::Id => channel_log_params.channel.clone(),
    };

    let log_date = ChannelLogDate::try_from(&channel_log_params)?;

    Ok(app
        .logs
        .read_channel(&channel_id, log_date)
        .await?
        .join("\n"))
}

pub async fn get_user_logs_by_name(
    app: Extension<App<'_>>,
    path: Path<UserLogsParams>,
) -> Result<String> {
    let user_id = app
        .get_users(vec![], vec![path.user.clone()])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::NotFound)?
        .0;

    get_user_logs(app, path, user_id).await
}

pub async fn get_user_logs_by_id(
    app: Extension<App<'_>>,
    path: Path<UserLogsParams>,
) -> Result<String> {
    let user_id = path.user.clone();
    get_user_logs(app, path, user_id).await
}

async fn get_user_logs(
    app: Extension<App<'_>>,
    Path(UserLogsParams {
        channel_id_type,
        channel,
        user: _,
        year,
        month,
    }): Path<UserLogsParams>,
    user_id: String,
) -> Result<String> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => {
            let (id, _) = app
                .get_users(vec![], vec![channel])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| Error::NotFound)?;
            id
        }

        ChannelIdType::Id => channel,
    };

    Ok(app
        .logs
        .read_user(&channel_id, &user_id, &year, &month)
        .await?)
}
