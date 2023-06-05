use std::time::{Duration, Instant};

use super::{
    responders::logs::{LogsResponse, LogsResponseType, ProcessedLogs, ProcessedLogsType},
    schema::{
        AvailableLogs, AvailableLogsParams, Channel, ChannelIdType, ChannelLogsPath, ChannelParam,
        ChannelsList, LogsParams, LogsPathChannel, UserLogsPath, UserParam,
    },
};
use crate::{
    app::App,
    db::{
        read_available_channel_logs, read_available_user_logs, read_channel,
        read_random_channel_line, read_random_user_line, read_user,
    },
    error::Error,
    logs::schema::{ChannelLogDate, UserLogDate},
    Result,
};
use axum::{
    extract::{Path, Query, RawQuery},
    response::Redirect,
    Extension, Json,
};
use chrono::{Datelike, Utc};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
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

    let channel_id = match channel_log_params.channel_info.channel_id_type {
        ChannelIdType::Name => app
            .get_users(
                vec![],
                vec![channel_log_params.channel_info.channel.clone()],
            )
            .await?
            .into_keys()
            .next()
            .ok_or(Error::NotFound)?,
        ChannelIdType::Id => channel_log_params.channel_info.channel.clone(),
    };

    app.check_opted_out(&channel_id, None)?;

    let log_date = ChannelLogDate::try_from(channel_log_params.date)?;
    debug!("Querying logs for date {log_date:?}");

    let started_at = Instant::now();
    let lines = read_channel(&app.db, &channel_id, log_date).await?;
    debug!("Querying DB took {}ms", started_at.elapsed().as_millis());

    let response_type = if logs_params.raw {
        LogsResponseType::Raw(lines)
    } else {
        let logs_type = if logs_params.json {
            ProcessedLogsType::Json
        } else {
            ProcessedLogsType::Text
        };

        let started_at = Instant::now();
        let response = LogsResponseType::Processed(ProcessedLogs::parse_raw(lines, logs_type));
        debug!("Parsing logs took {}ms", started_at.elapsed().as_millis());
        response
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
    Path(user_logs_path): Path<UserLogsPath>,
    Query(logs_params): Query<LogsParams>,
    user_id: String,
) -> Result<LogsResponse> {
    let log_date = UserLogDate::try_from(&user_logs_path)?;

    let channel_id = match user_logs_path.channel_info.channel_id_type {
        ChannelIdType::Name => {
            let (id, _) = app
                .get_users(vec![], vec![user_logs_path.channel_info.channel])
                .await?
                .into_iter()
                .next()
                .ok_or(Error::NotFound)?;
            id
        }

        ChannelIdType::Id => user_logs_path.channel_info.channel,
    };

    app.check_opted_out(&channel_id, Some(&user_id))?;

    let lines = read_user(&app.db, &channel_id, &user_id, log_date).await?;

    let response_type = if logs_params.raw {
        LogsResponseType::Raw(lines)
    } else {
        let logs_type = if logs_params.json {
            ProcessedLogsType::Json
        } else {
            ProcessedLogsType::Text
        };

        LogsResponseType::Processed(ProcessedLogs::parse_raw(lines, logs_type))
    };

    Ok(LogsResponse {
        response_type,
        reverse: logs_params.reverse,
    })
}

pub async fn list_available_logs(
    app: Extension<App<'_>>,
    Query(AvailableLogsParams { user, channel }): Query<AvailableLogsParams>,
) -> Result<Json<AvailableLogs>> {
    let channel_id = match channel {
        ChannelParam::ChannelId(id) => id,
        ChannelParam::Channel(name) => app.get_user_id_by_name(&name).await?,
    };

    let available_logs = if let Some(user) = user {
        let user_id = match user {
            UserParam::UserId(id) => id,
            UserParam::User(name) => app.get_user_id_by_name(&name).await?,
        };
        read_available_user_logs(&app.db, &channel_id, &user_id).await?
    } else {
        read_available_channel_logs(&app.db, &channel_id).await?
    };

    if !available_logs.is_empty() {
        Ok(Json(AvailableLogs { available_logs }))
    } else {
        Err(Error::NotFound)
    }
}

pub async fn redirect_to_latest_channel_logs(
    Path(LogsPathChannel {
        channel_id_type,
        channel,
    }): Path<LogsPathChannel>,
    RawQuery(query): RawQuery,
) -> Redirect {
    let today = Utc::now();
    let year = today.year();
    let month = today.month();
    let day = today.day();

    let mut new_uri = format!("/{channel_id_type}/{channel}/{year}/{month}/{day}");
    if let Some(query) = query {
        new_uri.push('?');
        new_uri.push_str(&query);
    }

    Redirect::to(&new_uri)
}

pub async fn redirect_to_latest_user_name_logs(
    path: Path<(String, String, String)>,
    query: RawQuery,
) -> Redirect {
    redirect_to_latest_user_logs(path, query, "user")
}

pub async fn redirect_to_latest_user_id_logs(
    path: Path<(String, String, String)>,
    query: RawQuery,
) -> Redirect {
    redirect_to_latest_user_logs(path, query, "userid")
}

fn redirect_to_latest_user_logs(
    Path((channel_id_type, channel, user)): Path<(String, String, String)>,
    RawQuery(query): RawQuery,
    user_id_type: &str,
) -> Redirect {
    let today = Utc::now();
    let year = today.year();
    let month = today.month();

    let mut new_uri = format!("/{channel_id_type}/{channel}/{user_id_type}/{user}/{year}/{month}");
    if let Some(query) = query {
        new_uri.push('?');
        new_uri.push_str(&query);
    }
    Redirect::to(&new_uri)
}

pub async fn random_channel_line(
    app: Extension<App<'_>>,
    Path((channel_id_type, channel)): Path<(ChannelIdType, String)>,
    Query(LogsParams { json, raw, reverse }): Query<LogsParams>,
) -> Result<LogsResponse> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => app.get_user_id_by_name(&channel).await?,
        ChannelIdType::Id => channel,
    };

    let random_line = read_random_channel_line(&app.db, &channel_id).await?;
    let lines = vec![random_line];

    let response_type = if raw {
        LogsResponseType::Raw(lines)
    } else {
        let logs_type = if json {
            ProcessedLogsType::Json
        } else {
            ProcessedLogsType::Text
        };

        LogsResponseType::Processed(ProcessedLogs::parse_raw(lines, logs_type))
    };

    Ok(LogsResponse {
        response_type,
        reverse,
    })
}

pub async fn random_user_line_by_name(
    app: Extension<App<'_>>,
    Path((channel_id_type, channel, user_name)): Path<(ChannelIdType, String, String)>,
    query: Query<LogsParams>,
) -> Result<LogsResponse> {
    let user_id = app.get_user_id_by_name(&user_name).await?;
    random_user_line(app, channel_id_type, channel, user_id, query).await
}

pub async fn random_user_line_by_id(
    app: Extension<App<'_>>,
    Path((channel_id_type, channel, user_id)): Path<(ChannelIdType, String, String)>,
    query: Query<LogsParams>,
) -> Result<LogsResponse> {
    random_user_line(app, channel_id_type, channel, user_id, query).await
}

async fn random_user_line(
    app: Extension<App<'_>>,
    channel_id_type: ChannelIdType,
    channel: String,
    user_id: String,
    Query(LogsParams { json, raw, reverse }): Query<LogsParams>,
) -> Result<LogsResponse> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => app.get_user_id_by_name(&channel).await?,
        ChannelIdType::Id => channel,
    };

    let random_line = read_random_user_line(&app.db, &channel_id, &user_id).await?;
    let lines = vec![random_line];

    let response_type = if raw {
        LogsResponseType::Raw(lines)
    } else {
        let logs_type = if json {
            ProcessedLogsType::Json
        } else {
            ProcessedLogsType::Text
        };

        LogsResponseType::Processed(ProcessedLogs::parse_raw(lines, logs_type))
    };

    Ok(LogsResponse {
        response_type,
        reverse,
    })
}

pub async fn optout(app: Extension<App<'_>>) -> Json<String> {
    let mut rng = thread_rng();
    let optout_code: String = (0..5).map(|_| rng.sample(Alphanumeric) as char).collect();

    app.optout_codes.insert(optout_code.clone());

    {
        let codes = app.optout_codes.clone();
        let optout_code = optout_code.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            if codes.remove(&optout_code).is_some() {
                debug!("Dropping optout code {optout_code}");
            }
        });
    }

    Json(optout_code)
}
