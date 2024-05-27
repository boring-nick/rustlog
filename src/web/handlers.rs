use super::{
    responders::logs::LogsResponse,
    schema::{
        AvailableLogs, AvailableLogsParams, Channel, ChannelIdType, ChannelLogsByDatePath,
        ChannelParam, ChannelsList, LogsParams, LogsPathChannel, UserLogPathParams, UserLogsPath,
        UserParam,
    },
};
use crate::{
    app::App,
    db::{
        read_available_channel_logs, read_available_user_logs, read_channel,
        read_random_channel_line, read_random_user_line, read_user,
    },
    error::Error,
    logs::{schema::LogRangeParams, stream::LogsStream},
    web::schema::LogsPathDate,
    Result,
};
use aide::axum::IntoApiResponse;
use axum::{
    extract::{Path, Query, RawQuery, State},
    response::{IntoResponse, Redirect, Response},
    Json,
};
use axum_extra::{headers::CacheControl, TypedHeader};
use chrono::{Days, Months, NaiveDate, NaiveTime, Utc};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::time::Duration;
use tracing::debug;

pub async fn get_channels(app: State<App>) -> impl IntoApiResponse {
    let channel_ids = app.config.channels.read().unwrap().clone();

    let channels = app
        .get_users(Vec::from_iter(channel_ids), vec![], None)
        .await
        .unwrap();

    let json = Json(ChannelsList {
        channels: channels
            .into_iter()
            .map(|(user_id, name)| Channel { name, user_id })
            .collect(),
    });
    (cache_header(600), json)
}

pub async fn get_channel_logs(
    Path(LogsPathChannel {
        channel_id_type,
        channel,
    }): Path<LogsPathChannel>,
    range_params: Option<Query<LogRangeParams>>,
    RawQuery(query): RawQuery,
    app: State<App>,
) -> Result<Response> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => app.get_user_id_by_name(&channel).await?,
        ChannelIdType::Id => channel.clone(),
    };

    if let Some(Query(params)) = range_params {
        let logs = get_channel_logs_inner(&app, &channel_id, params).await?;
        Ok(logs.into_response())
    } else {
        let available_logs = read_available_channel_logs(&app.db, &channel_id).await?;
        let latest_log = available_logs.first().ok_or(Error::NotFound)?;

        let mut new_uri = format!("/{channel_id_type}/{channel}/{latest_log}");
        if let Some(query) = query {
            new_uri.push('?');
            new_uri.push_str(&query);
        }

        Ok(Redirect::to(&new_uri).into_response())
    }
}

pub async fn get_channel_logs_by_date(
    app: State<App>,
    Path(channel_log_params): Path<ChannelLogsByDatePath>,
    Query(logs_params): Query<LogsParams>,
) -> Result<impl IntoApiResponse> {
    debug!("Params: {logs_params:?}");

    let channel_id = match channel_log_params.channel_info.channel_id_type {
        ChannelIdType::Name => {
            app.get_user_id_by_name(&channel_log_params.channel_info.channel)
                .await?
        }
        ChannelIdType::Id => channel_log_params.channel_info.channel.clone(),
    };

    let LogsPathDate { year, month, day } = channel_log_params.date;

    let from = NaiveDate::from_ymd_opt(year.parse()?, month.parse()?, day.parse()?)
        .ok_or_else(|| Error::InvalidParam("Invalid date".to_owned()))?
        .and_time(NaiveTime::default())
        .and_utc();
    let to = from
        .checked_add_days(Days::new(1))
        .ok_or_else(|| Error::InvalidParam("Date out of range".to_owned()))?;

    let params = LogRangeParams {
        from,
        to,
        logs_params,
    };

    get_channel_logs_inner(&app, &channel_id, params).await
}

async fn get_channel_logs_inner(
    app: &App,
    channel_id: &str,
    channel_log_params: LogRangeParams,
) -> Result<impl IntoApiResponse> {
    app.check_opted_out(channel_id, None)?;

    let stream = read_channel(&app.db, channel_id, &channel_log_params).await?;

    let logs = LogsResponse {
        response_type: channel_log_params.logs_params.response_type(),
        stream,
    };

    let cache = if Utc::now() < channel_log_params.to {
        no_cache_header()
    } else {
        cache_header(36000)
    };

    Ok((cache, logs))
}

pub async fn get_user_logs_by_name(
    path: Path<UserLogPathParams>,
    range_params: Option<Query<LogRangeParams>>,
    query: RawQuery,
    app: State<App>,
) -> Result<impl IntoApiResponse> {
    get_user_logs(path, range_params, query, false, app).await
}

pub async fn get_user_logs_id(
    path: Path<UserLogPathParams>,
    range_params: Option<Query<LogRangeParams>>,
    query: RawQuery,
    app: State<App>,
) -> Result<impl IntoApiResponse> {
    get_user_logs(path, range_params, query, true, app).await
}

async fn get_user_logs(
    Path(UserLogPathParams {
        channel_id_type,
        channel,
        user,
    }): Path<UserLogPathParams>,
    range_params: Option<Query<LogRangeParams>>,
    RawQuery(query): RawQuery,
    user_is_id: bool,
    app: State<App>,
) -> Result<impl IntoApiResponse> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => app.get_user_id_by_name(&channel).await?,
        ChannelIdType::Id => channel.clone(),
    };
    let user_id = if user_is_id {
        user.clone()
    } else {
        app.get_user_id_by_name(&user).await?
    };

    if let Some(Query(params)) = range_params {
        let logs = get_user_logs_inner(&app, &channel_id, &user_id, params).await?;
        Ok(logs.into_response())
    } else {
        let available_logs = read_available_user_logs(&app.db, &channel_id, &user_id).await?;
        let latest_log = available_logs.first().ok_or(Error::NotFound)?;

        let user_id_type = if user_is_id { "userid" } else { "user" };

        let mut new_uri =
            format!("/{channel_id_type}/{channel}/{user_id_type}/{user}/{latest_log}");
        if let Some(query) = query {
            new_uri.push('?');
            new_uri.push_str(&query);
        }
        Ok(Redirect::to(&new_uri).into_response())
    }
}

pub async fn get_user_logs_by_date_name(
    app: State<App>,
    path: Path<UserLogsPath>,
    params: Query<LogsParams>,
) -> Result<impl IntoApiResponse> {
    let user_id = app.get_user_id_by_name(&path.user).await?;

    get_user_logs_by_date(app, path, params, user_id).await
}

pub async fn get_user_logs_by_date_id(
    app: State<App>,
    path: Path<UserLogsPath>,
    params: Query<LogsParams>,
) -> Result<impl IntoApiResponse> {
    let user_id = path.user.clone();
    get_user_logs_by_date(app, path, params, user_id).await
}

async fn get_user_logs_by_date(
    app: State<App>,
    Path(user_logs_path): Path<UserLogsPath>,
    Query(logs_params): Query<LogsParams>,
    user_id: String,
) -> Result<impl IntoApiResponse> {
    let channel_id = match user_logs_path.channel_info.channel_id_type {
        ChannelIdType::Name => {
            app.get_user_id_by_name(&user_logs_path.channel_info.channel)
                .await?
        }
        ChannelIdType::Id => user_logs_path.channel_info.channel.clone(),
    };

    let year = user_logs_path.year.parse()?;
    let month = user_logs_path.month.parse()?;

    let from = NaiveDate::from_ymd_opt(year, month, 1)
        .ok_or_else(|| Error::InvalidParam("Invalid date".to_owned()))?
        .and_time(NaiveTime::default())
        .and_utc();
    let to = from
        .checked_add_months(Months::new(1))
        .ok_or_else(|| Error::InvalidParam("Date out of range".to_owned()))?;

    let params = LogRangeParams {
        from,
        to,
        logs_params,
    };

    get_user_logs_inner(&app, &channel_id, &user_id, params).await
}

async fn get_user_logs_inner(
    app: &App,
    channel_id: &str,
    user_id: &str,
    log_params: LogRangeParams,
) -> Result<impl IntoApiResponse> {
    app.check_opted_out(channel_id, Some(user_id))?;

    let stream = read_user(&app.db, channel_id, user_id, &log_params).await?;

    let logs = LogsResponse {
        stream,
        response_type: log_params.logs_params.response_type(),
    };

    let cache = if Utc::now() < log_params.to {
        no_cache_header()
    } else {
        cache_header(36000)
    };

    Ok((cache, logs))
}

pub async fn list_available_logs(
    Query(AvailableLogsParams { user, channel }): Query<AvailableLogsParams>,
    app: State<App>,
) -> Result<impl IntoApiResponse> {
    let channel_id = match channel {
        ChannelParam::ChannelId(id) => id,
        ChannelParam::Channel(name) => app.get_user_id_by_name(&name).await?,
    };

    let available_logs = if let Some(user) = user {
        let user_id = match user {
            UserParam::UserId(id) => id,
            UserParam::User(name) => app.get_user_id_by_name(&name).await?,
        };
        app.check_opted_out(&channel_id, Some(&user_id))?;
        read_available_user_logs(&app.db, &channel_id, &user_id).await?
    } else {
        app.check_opted_out(&channel_id, None)?;
        read_available_channel_logs(&app.db, &channel_id).await?
    };

    if !available_logs.is_empty() {
        Ok((cache_header(600), Json(AvailableLogs { available_logs })))
    } else {
        Err(Error::NotFound)
    }
}

pub async fn random_channel_line(
    app: State<App>,
    Path(LogsPathChannel {
        channel_id_type,
        channel,
    }): Path<LogsPathChannel>,
    Query(logs_params): Query<LogsParams>,
) -> Result<impl IntoApiResponse> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => app.get_user_id_by_name(&channel).await?,
        ChannelIdType::Id => channel,
    };

    let random_line = read_random_channel_line(&app.db, &channel_id).await?;
    let stream = LogsStream::new_provided(vec![random_line])?;

    let logs = LogsResponse {
        stream,
        response_type: logs_params.response_type(),
    };
    Ok((no_cache_header(), logs))
}

pub async fn random_user_line_by_name(
    app: State<App>,
    Path(UserLogPathParams {
        channel_id_type,
        channel,
        user,
    }): Path<UserLogPathParams>,
    query: Query<LogsParams>,
) -> Result<impl IntoApiResponse> {
    let user_id = app.get_user_id_by_name(&user).await?;
    random_user_line(app, channel_id_type, channel, user_id, query).await
}

pub async fn random_user_line_by_id(
    app: State<App>,
    Path(UserLogPathParams {
        channel_id_type,
        channel,
        user,
    }): Path<UserLogPathParams>,
    query: Query<LogsParams>,
) -> Result<impl IntoApiResponse> {
    random_user_line(app, channel_id_type, channel, user, query).await
}

async fn random_user_line(
    app: State<App>,
    channel_id_type: ChannelIdType,
    channel: String,
    user_id: String,
    Query(logs_params): Query<LogsParams>,
) -> Result<impl IntoApiResponse> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => app.get_user_id_by_name(&channel).await?,
        ChannelIdType::Id => channel,
    };

    app.check_opted_out(&channel_id, Some(&user_id))?;

    let random_line = read_random_user_line(&app.db, &channel_id, &user_id).await?;
    let stream = LogsStream::new_provided(vec![random_line])?;

    let logs = LogsResponse {
        stream,
        response_type: logs_params.response_type(),
    };
    Ok((no_cache_header(), logs))
}

pub async fn optout(app: State<App>) -> Json<String> {
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

fn cache_header(secs: u64) -> TypedHeader<CacheControl> {
    TypedHeader(
        CacheControl::new()
            .with_public()
            .with_max_age(Duration::from_secs(secs)),
    )
}

pub fn no_cache_header() -> TypedHeader<CacheControl> {
    TypedHeader(CacheControl::new().with_no_cache())
}
