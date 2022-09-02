use super::{
    responders::logs::{LogsResponse, Message},
    schema::{
        AvailableLogDate, AvailableLogs, AvailableLogsParams, Channel, ChannelIdType,
        ChannelIdentifier, ChannelLogsPath, ChannelsList, LogsParams, UserIdentifier, UserLogsPath,
    },
};
use crate::{
    app::App,
    config::Config,
    error::Error,
    logs::{format_message_from_raw, schema::ChannelLogDate},
    Result,
};
use axum::{
    extract::{Path, Query},
    Extension, Json,
};
use itertools::Itertools;
use std::sync::Arc;
use tracing::error;

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
    Path(channel_log_params): Path<ChannelLogsPath>,
    Query(LogsParams { json }): Query<LogsParams>,
) -> Result<LogsResponse> {
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

    let lines = app.logs.read_channel(&channel_id, log_date).await?;

    if json == "true" || json == "1" {
        match lines
            .into_iter()
            .map(|line| Message::parse_from_raw_irc(line))
            .try_collect()
        {
            Ok(messages) => Ok(LogsResponse::Json(messages)),
            Err(err) => {
                error!("Could not parse messages: {err}");
                Err(Error::Internal)
            }
        }
    } else {
        let messages = lines
            .into_iter()
            .map(|line| format_message_from_raw(&line))
            .try_collect()?;

        Ok(LogsResponse::Plain(messages))
    }
}

pub async fn get_user_logs_by_name(
    app: Extension<App<'_>>,
    path: Path<UserLogsPath>,
    params: Query<LogsParams>,
) -> Result<LogsResponse> {
    let user_id = app
        .get_users(vec![], vec![path.user.clone()])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::NotFound)?
        .0;

    get_user_logs(app, path, params, user_id).await
}

pub async fn get_user_logs_by_id(
    app: Extension<App<'_>>,
    path: Path<UserLogsPath>,
    params: Query<LogsParams>,
) -> Result<LogsResponse> {
    let user_id = path.user.clone();
    get_user_logs(app, path, params, user_id).await
}

async fn get_user_logs(
    app: Extension<App<'_>>,
    Path(UserLogsPath {
        channel_id_type,
        channel,
        user: _,
        year,
        month,
    }): Path<UserLogsPath>,
    Query(LogsParams { json }): Query<LogsParams>,
    user_id: String,
) -> Result<LogsResponse> {
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

    let lines = app
        .logs
        .read_user(&channel_id, &user_id, &year, &month)
        .await?;

    if json == "true" || json == "1" {
        match lines
            .into_iter()
            .map(|line| Message::parse_from_raw_irc(line))
            .try_collect()
        {
            Ok(messages) => Ok(LogsResponse::Json(messages)),
            Err(err) => {
                error!("Could not parse messages: {err}");
                Err(Error::Internal)
            }
        }
    } else {
        let messages = lines
            .into_iter()
            .map(|line| format_message_from_raw(&line))
            .try_collect()?;

        Ok(LogsResponse::Plain(messages))
    }
}

pub async fn list_available_user_logs(
    app: Extension<App<'_>>,
    Query(AvailableLogsParams { user, channel }): Query<AvailableLogsParams>,
) -> Result<Json<AvailableLogs>> {
    let user_id = match user {
        UserIdentifier::UserId(id) => id,
        UserIdentifier::User(name) => app.user_id_from_name(name).await?,
    };
    let channel_id = match channel {
        ChannelIdentifier::ChannelId(id) => id,
        ChannelIdentifier::Channel(name) => app.user_id_from_name(name).await?,
    };

    let available_logs = app
        .logs
        .get_available_user_logs(&channel_id, &user_id)
        .await?;

    let mut results = Vec::new();

    for (year, months) in available_logs {
        let available_dates = months.into_iter().map(|month| AvailableLogDate {
            year: year.clone(),
            month,
        });
        results.extend(available_dates);
    }

    Ok(Json(AvailableLogs {
        available_logs: results,
    }))
}
