use super::{
    responders::logs::{LogsResponse, LogsResponseType, ProcessedLogs, ProcessedLogsType},
    schema::{
        AvailableLogDate, AvailableLogs, AvailableLogsParams, Channel, ChannelIdType,
        ChannelLogsPath, ChannelParam, ChannelsList, LogsParams, UserLogsPath, UserParam,
    },
};
use crate::{
    app::App,
    error::Error,
    logs::schema::{ChannelLogDate, Message},
    Result,
};
use axum::{
    extract::{Path, Query},
    response::Redirect,
    Extension, Json,
};
use chrono::{Datelike, Utc};
use tracing::debug;

pub async fn get_channels(app: Extension<App<'_>>) -> Json<ChannelsList> {
    let channel_ids = app.config.channels.read().unwrap().clone();

    let channels = app
        .get_users(Vec::from_iter(channel_ids), vec![])
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
    Query(logs_params): Query<LogsParams>,
) -> Result<LogsResponse> {
    debug!("Params: {logs_params:?}");

    let channel_id = match channel_log_params.channel_id_type {
        ChannelIdType::Name => app
            .get_users(vec![], vec![channel_log_params.channel.clone()])
            .await?
            .into_keys()
            .next()
            .ok_or(Error::NotFound)?,
        ChannelIdType::Id => channel_log_params.channel.clone(),
    };

    let log_date = ChannelLogDate::try_from(&channel_log_params)?;

    let lines = app.logs.read_channel(&channel_id, log_date).await?;

    let response_type = if logs_params.raw {
        LogsResponseType::Raw(lines)
    } else {
        let messages = lines
            .into_iter()
            .filter_map(|line| Message::parse_from_raw_irc(line).ok())
            .collect();

        let logs_type = if logs_params.json {
            ProcessedLogsType::Json
        } else {
            ProcessedLogsType::Text
        };

        LogsResponseType::Processed(ProcessedLogs {
            messages,
            logs_type,
        })
    };

    Ok(LogsResponse {
        response_type,
        reverse: logs_params.reverse,
    })
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
        .ok_or(Error::NotFound)?
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
    Query(logs_params): Query<LogsParams>,
    user_id: String,
) -> Result<LogsResponse> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => {
            let (id, _) = app
                .get_users(vec![], vec![channel])
                .await?
                .into_iter()
                .next()
                .ok_or(Error::NotFound)?;
            id
        }

        ChannelIdType::Id => channel,
    };

    let lines = app
        .logs
        .read_user(&channel_id, &user_id, &year, &month)
        .await?;

    let response_type = if logs_params.raw {
        LogsResponseType::Raw(lines)
    } else {
        let messages = lines
            .into_iter()
            .filter_map(|line| Message::parse_from_raw_irc(line).ok())
            .collect();

        let logs_type = if logs_params.json {
            ProcessedLogsType::Json
        } else {
            ProcessedLogsType::Text
        };

        LogsResponseType::Processed(ProcessedLogs {
            messages,
            logs_type,
        })
    };

    Ok(LogsResponse {
        response_type,
        reverse: logs_params.reverse,
    })
}

pub async fn list_available_user_logs(
    app: Extension<App<'_>>,
    Query(AvailableLogsParams { user, channel }): Query<AvailableLogsParams>,
) -> Result<Json<AvailableLogs>> {
    let user_id = match user {
        UserParam::UserId(id) => id,
        UserParam::User(name) => app.get_user_id_by_name(&name).await?,
    };
    let channel_id = match channel {
        ChannelParam::ChannelId(id) => id,
        ChannelParam::Channel(name) => app.get_user_id_by_name(&name).await?,
    };

    let available_logs = app
        .logs
        .get_available_user_logs(&channel_id, &user_id)
        .await?;

    let mut results = Vec::new();

    for (year, months) in available_logs {
        let available_dates = months.into_iter().map(|month| AvailableLogDate {
            year: year.to_string(),
            month: month.to_string(),
        });
        results.extend(available_dates);
    }

    Ok(Json(AvailableLogs {
        available_logs: results,
    }))
}

pub async fn redirect_to_latest_channel_logs(
    Path((channel_id_type, channel)): Path<(String, String)>,
) -> Redirect {
    let today = Utc::today();
    let year = today.year();
    let month = today.month();
    let day = today.day();

    let new_uri = format!("/{channel_id_type}/{channel}/{year}/{month}/{day}");
    Redirect::to(&new_uri)
}

pub async fn redirect_to_latest_user_name_logs(path: Path<(String, String, String)>) -> Redirect {
    redirect_to_latest_user_logs(path, "user")
}

pub async fn redirect_to_latest_user_id_logs(path: Path<(String, String, String)>) -> Redirect {
    redirect_to_latest_user_logs(path, "userid")
}

fn redirect_to_latest_user_logs(
    Path((channel_id_type, channel, user)): Path<(String, String, String)>,
    user_id_type: &str,
) -> Redirect {
    let today = Utc::today();
    let year = today.year();
    let month = today.month();

    let new_uri = format!("/{channel_id_type}/{channel}/{user_id_type}/{user}/{year}/{month}");
    Redirect::to(&new_uri)
}
