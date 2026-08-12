use crate::{
    app::App,
    db::schema::{StructuredMessage, UnstructuredMessage},
    logs::extract::{extract_channel_and_user_from_raw, extract_raw_timestamp},
    recent_messages::RecentMessagesClient,
    ShutdownRx,
};
use anyhow::{anyhow, Context};
use chrono::Utc;
use dashmap::{DashMap, DashSet};
use lazy_static::lazy_static;
use moka::sync::Cache;
use prometheus::{register_int_counter_vec, IntCounterVec};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Mutex, Notify, OwnedSemaphorePermit, Semaphore,
    },
    task::JoinHandle,
    time::{sleep, sleep_until, Instant},
};
use tracing::{debug, error, info, log::warn, trace};
use twitch_irc::{
    login::LoginCredentials,
    message::{AsRawIRC, IRCMessage, ServerMessage},
    ClientConfig, SecureTCPTransport, TwitchIRCClient,
};

const CHANNEL_REJOIN_INTERVAL_SECONDS: u64 = 3600;
const CHANNELS_REFETCH_RETRY_INTERVAL_SECONDS: u64 = 5;
const RECENT_MESSAGE_DEDUPE_TTL_SECONDS: u64 = 30;
const RECENT_MESSAGE_DEDUPE_CAPACITY: u64 = 50_000;
const RECENT_MESSAGE_INFLIGHT_CAPACITY: u64 = 10_000;
const RECENT_MESSAGE_INFLIGHT_TTL_SECONDS: u64 = 180;
const RECENT_MESSAGE_MAX_CONCURRENT_REQUESTS: usize = 4;
const RECENT_MESSAGE_REQUEST_SPACING_MILLIS: u64 = 250;
const RECENT_MESSAGE_STARTUP_DELAYS_SECONDS: [u64; 5] = [0, 5, 15, 30, 60];
const RECENT_MESSAGE_RECONNECT_DELAYS_SECONDS: [u64; 6] = [0, 5, 15, 30, 60, 120];

type TwitchClient<C> = TwitchIRCClient<SecureTCPTransport, C>;

#[derive(Debug)]
pub enum BotMessage {
    JoinChannels(Vec<String>),
    PartChannels(Vec<String>),
}

lazy_static! {
    static ref MESSAGES_RECEIVED_COUNTERS: IntCounterVec = register_int_counter_vec!(
        "rustlog_messages_received",
        "How many messages were written",
        &["channel_id"]
    )
    .unwrap();
}

const COMMAND_PREFIX: &str = "!rustlog ";

pub async fn run<C: LoginCredentials>(
    login_credentials: C,
    app: App,
    writer_tx: Sender<StructuredMessage<'static>>,
    shutdown_rx: ShutdownRx,
    command_rx: Receiver<BotMessage>,
) {
    let bot = Bot::new(app, writer_tx);
    bot.run(login_credentials, shutdown_rx, command_rx).await;
}

#[derive(Clone)]
struct Bot {
    app: App,
    writer_tx: Sender<StructuredMessage<'static>>,
    recent_messages: RecentMessagesClient,
    recent_message_dedupe: Cache<String, ()>,
    backfill_inflight: Cache<String, ()>,
    backfill_pending: Arc<DashSet<String>>,
    backfill_wakeups: Arc<DashMap<String, Arc<Notify>>>,
    backfill_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    seen_channel_joins: Arc<DashSet<String>>,
    recent_message_request_slots: Arc<Semaphore>,
    recent_message_next_request: Arc<Mutex<Instant>>,
    recent_message_request_spacing: Duration,
    startup_backfill_delays: Arc<[Duration]>,
    reconnect_backfill_delays: Arc<[Duration]>,
    #[cfg(test)]
    skip_existing_message_lookup: bool,
}

struct PreparedMessage {
    message: StructuredMessage<'static>,
    dedupe_key: String,
}

impl Bot {
    pub fn new(app: App, writer_tx: Sender<StructuredMessage<'static>>) -> Bot {
        let recent_messages = RecentMessagesClient::from_config(&app.config);
        Self::new_with_recent_messages(app, writer_tx, recent_messages)
    }

    fn new_with_recent_messages(
        app: App,
        writer_tx: Sender<StructuredMessage<'static>>,
        recent_messages: RecentMessagesClient,
    ) -> Bot {
        Self {
            app,
            writer_tx,
            recent_messages,
            recent_message_dedupe: Cache::builder()
                .time_to_live(Duration::from_secs(RECENT_MESSAGE_DEDUPE_TTL_SECONDS))
                .max_capacity(RECENT_MESSAGE_DEDUPE_CAPACITY)
                .build(),
            backfill_inflight: Cache::builder()
                .time_to_live(Duration::from_secs(RECENT_MESSAGE_INFLIGHT_TTL_SECONDS))
                .max_capacity(RECENT_MESSAGE_INFLIGHT_CAPACITY)
                .build(),
            backfill_pending: Arc::new(DashSet::new()),
            backfill_wakeups: Arc::new(DashMap::new()),
            backfill_tasks: Arc::new(Mutex::new(Vec::new())),
            seen_channel_joins: Arc::new(DashSet::new()),
            recent_message_request_slots: Arc::new(Semaphore::new(
                RECENT_MESSAGE_MAX_CONCURRENT_REQUESTS,
            )),
            recent_message_next_request: Arc::new(Mutex::new(Instant::now())),
            recent_message_request_spacing: Duration::from_millis(
                RECENT_MESSAGE_REQUEST_SPACING_MILLIS,
            ),
            startup_backfill_delays: RECENT_MESSAGE_STARTUP_DELAYS_SECONDS
                .map(Duration::from_secs)
                .into(),
            reconnect_backfill_delays: RECENT_MESSAGE_RECONNECT_DELAYS_SECONDS
                .map(Duration::from_secs)
                .into(),
            #[cfg(test)]
            skip_existing_message_lookup: false,
        }
    }

    pub async fn run<C: LoginCredentials>(
        self,
        login_credentials: C,
        mut shutdown_rx: ShutdownRx,
        mut command_rx: Receiver<BotMessage>,
    ) {
        let own_login = match login_credentials.get_credentials().await {
            Ok(credentials) => Some(credentials.login),
            Err(err) => {
                warn!("Could not determine IRC login for recent-message backfill: {err}");
                None
            }
        };
        let client_config = ClientConfig::new_simple(login_credentials);
        let (mut receiver, client) = TwitchIRCClient::<SecureTCPTransport, C>::new(client_config);

        let app = self.app.clone();
        let join_client = client.clone();
        tokio::spawn(async move {
            loop {
                let channel_ids = app.config.channels.read().unwrap().clone();

                let interval = match app
                    .get_users(Vec::from_iter(channel_ids), vec![], true)
                    .await
                {
                    Ok(users) => {
                        info!("Joining {} channels", users.len());
                        for channel_login in users.into_values() {
                            debug!("Logging channel {channel_login}");
                            join_client
                                .join(channel_login)
                                .expect("Failed to join channel");
                        }
                        CHANNEL_REJOIN_INTERVAL_SECONDS
                    }
                    Err(err) => {
                        error!("Could not fetch users list: {err}");
                        CHANNELS_REFETCH_RETRY_INTERVAL_SECONDS
                    }
                };
                sleep(Duration::from_secs(interval)).await;
            }
        });

        let bot = self.clone();
        let msg_client = client.clone();
        tokio::spawn(async move {
            while let Some(msg) = command_rx.recv().await {
                match msg {
                    BotMessage::JoinChannels(channels) => {
                        if let Err(err) = bot
                            .update_channels(
                                &msg_client,
                                &channels.iter().map(String::as_str).collect::<Vec<_>>(),
                                ChannelAction::Join,
                            )
                            .await
                        {
                            error!("Could not join channels: {err}");
                        }
                    }
                    BotMessage::PartChannels(channels) => {
                        if let Err(err) = bot
                            .update_channels(
                                &msg_client,
                                &channels.iter().map(String::as_str).collect::<Vec<_>>(),
                                ChannelAction::Part,
                            )
                            .await
                        {
                            error!("Could not join channels: {err}");
                        }
                    }
                }
            }
        });

        loop {
            tokio::select! {
                Some(msg) = receiver.recv() => {
                    if let Err(e) = self.handle_message(msg, &client, own_login.as_deref()).await {
                        error!("Could not handle message: {e}");
                    }
                }
                _ = shutdown_rx.changed() => {
                    debug!("Shutting down bot task");
                    self.stop_backfill_tasks().await;
                    break;
                }
            }
        }
    }

    async fn handle_message<C: LoginCredentials>(
        &self,
        msg: ServerMessage,
        client: &TwitchClient<C>,
        own_login: Option<&str>,
    ) -> anyhow::Result<()> {
        if let Some(own_login) = own_login {
            self.trigger_backfill_for_own_join(&msg, own_login).await;
        }

        let raw_irc = msg.as_raw_irc();
        let prepared = self.prepare_message(IRCMessage::from(msg.clone()), &raw_irc)?;
        let Some(prepared) = prepared else {
            return Ok(());
        };
        if !self.claim_message(&prepared.dedupe_key) {
            return Ok(());
        }

        if let ServerMessage::Privmsg(privmsg) = &msg {
            trace!("Processing message {}", privmsg.message_text);
            if let Some(cmd) = privmsg.message_text.strip_prefix(COMMAND_PREFIX) {
                if let Err(err) = self
                    .handle_command(cmd, client, &privmsg.sender.id, &privmsg.sender.login)
                    .await
                {
                    warn!("Could not handle command {cmd}: {err:#}");
                }
            }
        }

        self.write_prepared(prepared).await?;

        Ok(())
    }

    async fn trigger_backfill_for_own_join(&self, msg: &ServerMessage, own_login: &str) {
        if let ServerMessage::Join(join) = msg {
            if join.user_login.eq_ignore_ascii_case(own_login) {
                let channel_login = normalize_channel_login(&join.channel_login);
                let schedule = if self.seen_channel_joins.insert(channel_login.clone()) {
                    BackfillSchedule::Startup
                } else {
                    BackfillSchedule::Reconnect
                };
                self.trigger_recent_messages_fetch(channel_login, "successful join", schedule)
                    .await;
            }
        }
    }

    fn check_admin(&self, user_login: &str) -> anyhow::Result<()> {
        if self
            .app
            .config
            .admins
            .iter()
            .any(|login| login == user_login)
        {
            Ok(())
        } else {
            Err(anyhow!("User {user_login} is not an admin"))
        }
    }

    fn prepare_message(
        &self,
        irc_message: IRCMessage,
        raw_irc: &str,
    ) -> anyhow::Result<Option<PreparedMessage>> {
        if irc_message.command == "ROOMSTATE" {
            return Ok(None);
        }
        if let Some((channel_id, maybe_user_id)) = extract_channel_and_user_from_raw(&irc_message) {
            let timestamp = extract_raw_timestamp(&irc_message)
                .unwrap_or_else(|| Utc::now().timestamp_millis().try_into().unwrap());
            let user_id = maybe_user_id.unwrap_or_default().to_owned();
            let unstructured = UnstructuredMessage {
                channel_id,
                user_id: &user_id,
                timestamp,
                raw: raw_irc,
            };
            let mut message = StructuredMessage::from_unstructured(&unstructured)?.into_owned();
            message.channel_login = normalize_channel_login(&message.channel_login).into();
            let dedupe_key = structured_event_identity(&message);
            return Ok(Some(PreparedMessage {
                message,
                dedupe_key,
            }));
        }

        Ok(None)
    }

    fn claim_message(&self, dedupe_key: &str) -> bool {
        self.recent_message_dedupe
            .entry(dedupe_key.to_owned())
            .or_insert_with(|| ())
            .is_fresh()
    }

    async fn write_prepared(&self, prepared: PreparedMessage) -> anyhow::Result<()> {
        let message = prepared.message;
        if self
            .app
            .config
            .opt_out
            .contains_key(message.channel_id.as_ref())
            || self
                .app
                .config
                .opt_out
                .contains_key(message.user_id.as_ref())
        {
            return Ok(());
        }

        if !message.channel_id.is_empty() {
            MESSAGES_RECEIVED_COUNTERS
                .with_label_values(&[message.channel_id.as_ref()])
                .inc();
        }
        self.writer_tx.send(message).await?;
        Ok(())
    }

    async fn trigger_recent_messages_fetch(
        &self,
        channel_login: String,
        reason: &'static str,
        schedule: BackfillSchedule,
    ) {
        if !self.recent_messages.enabled() {
            return;
        }

        let channel_login = normalize_channel_login(&channel_login);
        let wakeup = self
            .backfill_wakeups
            .entry(channel_login.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();
        if !self
            .backfill_inflight
            .entry(channel_login.clone())
            .or_insert_with(|| ())
            .is_fresh()
        {
            if matches!(schedule, BackfillSchedule::Reconnect) {
                self.backfill_pending.insert(channel_login.clone());
                self.backfill_inflight.insert(channel_login, ());
                wakeup.notify_waiters();
            }
            return;
        }

        let bot = self.clone();
        let mut delays: Arc<[Duration]> = match schedule {
            #[cfg(test)]
            BackfillSchedule::OneShot => [Duration::ZERO].into(),
            BackfillSchedule::Startup => self.startup_backfill_delays.clone(),
            BackfillSchedule::Reconnect => self.reconnect_backfill_delays.clone(),
        };
        let reconnect_delays = self.reconnect_backfill_delays.clone();
        let handle = tokio::spawn(async move {
            'restart_schedule: loop {
                let started_at = Instant::now();
                let active_delays = delays.clone();
                for (attempt, delay) in active_delays.iter().enumerate() {
                    let deadline = started_at + *delay;
                    loop {
                        if bot.backfill_pending.remove(&channel_login).is_some() {
                            delays = reconnect_delays.clone();
                            info!(
                                "Restarting recent-message recovery schedule for {channel_login} after another successful join"
                            );
                            continue 'restart_schedule;
                        }
                        tokio::select! {
                            _ = sleep_until(deadline) => break,
                            _ = wakeup.notified() => continue,
                        }
                    }

                    let expected_channel_id = match bot
                        .resolve_expected_channel_id(&channel_login)
                        .await
                    {
                        Ok(channel_id) => channel_id,
                        Err(err) => {
                            warn!(
                                "Recent-message backfill could not verify {channel_login} after {reason} (attempt {} of {}): {err:#}",
                                attempt + 1,
                                active_delays.len()
                            );
                            continue;
                        }
                    };
                    if let Err(err) = bot
                        .fetch_recent_messages_for_channel(&channel_login, &expected_channel_id)
                        .await
                    {
                        warn!(
                            "Recent-message backfill failed for {channel_login} after {reason} (attempt {} of {}): {err:#}",
                            attempt + 1,
                            active_delays.len()
                        );
                    }

                    if bot.backfill_pending.remove(&channel_login).is_some() {
                        delays = reconnect_delays.clone();
                        info!(
                            "Restarting recent-message recovery schedule for {channel_login} after another successful join"
                        );
                        continue 'restart_schedule;
                    }
                }

                bot.backfill_inflight.invalidate(&channel_login);
                if bot.backfill_pending.remove(&channel_login).is_some() {
                    bot.backfill_inflight.insert(channel_login.clone(), ());
                    delays = reconnect_delays.clone();
                    continue;
                }
                break;
            }
        });
        let mut tasks = self.backfill_tasks.lock().await;
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    async fn resolve_expected_channel_id(&self, channel_login: &str) -> anyhow::Result<String> {
        match self.app.users.get_id(channel_login) {
            Some(Some(channel_id)) => return Ok(channel_id),
            Some(None) => return Err(anyhow!("Twitch reports the channel is unavailable")),
            None => {}
        }

        self.app
            .get_users(vec![], vec![channel_login.to_owned()], false)
            .await?
            .into_iter()
            .find_map(|(channel_id, login)| {
                (normalize_channel_login(&login) == channel_login).then_some(channel_id)
            })
            .context("Twitch did not return the expected channel ID")
    }

    async fn fetch_recent_messages_for_channel(
        &self,
        channel_login: &str,
        expected_channel_id: &str,
    ) -> anyhow::Result<()> {
        let channel_login = normalize_channel_login(channel_login);
        let _request_permit = self.acquire_recent_message_request_permit().await?;
        let response = self.recent_messages.fetch(&channel_login).await?;
        if response.error.is_some() || response.error_code.is_some() {
            warn!(
                "Recent-message backfill skipped for {channel_login}: error={:?} error_code={:?}",
                response.error, response.error_code
            );
            return Ok(());
        }

        let response_count = response.messages.len();
        let mut candidates = Vec::new();
        let mut parse_failures = 0_usize;
        let mut missing_timestamps = 0_usize;
        let mut channel_login_mismatches = 0_usize;
        let mut channel_id_mismatches = 0_usize;
        for (response_index, raw) in response.messages.into_iter().enumerate() {
            let normalized_raw = normalize_robotty_message(&raw);
            let parsed = IRCMessage::parse(normalized_raw.as_ref()).map_err(anyhow::Error::from);
            match parsed {
                Ok(message) => {
                    if extract_raw_timestamp(&message).is_none() {
                        missing_timestamps += 1;
                        continue;
                    }
                    match self.prepare_message(message, normalized_raw.as_ref()) {
                        Ok(Some(prepared)) => {
                            if prepared.message.channel_login != channel_login {
                                channel_login_mismatches += 1;
                            } else if prepared.message.channel_id != expected_channel_id {
                                channel_id_mismatches += 1;
                            } else {
                                candidates.push((response_index, prepared));
                            }
                        }
                        Ok(None) => {}
                        Err(_) => parse_failures += 1,
                    }
                }
                Err(_) => parse_failures += 1,
            }
        }

        candidates.sort_by_key(|(response_index, prepared)| {
            (prepared.message.timestamp, *response_index)
        });
        let mut response_keys = HashSet::new();
        let before_response_dedupe = candidates.len();
        candidates.retain(|(_, prepared)| response_keys.insert(prepared.dedupe_key.clone()));
        let response_duplicates = before_response_dedupe - candidates.len();
        let candidates = candidates
            .into_iter()
            .map(|(_, prepared)| prepared)
            .collect::<Vec<_>>();

        let existing_keys = self.existing_message_keys(&candidates).await?;
        let mut existing_duplicates = 0_usize;
        let mut recent_duplicates = 0_usize;
        let mut stored = 0_usize;
        let mut write_failures = 0_usize;
        for prepared in candidates {
            if existing_keys.contains(&prepared.dedupe_key) {
                existing_duplicates += 1;
                continue;
            }
            if !self.claim_message(&prepared.dedupe_key) {
                recent_duplicates += 1;
                continue;
            }
            if let Err(err) = self.write_prepared(prepared).await {
                write_failures += 1;
                warn!("Could not store recent-message backfill event for {channel_login}: {err}");
            } else {
                stored += 1;
            }
        }

        info!(
            "Recent-message backfill summary for {channel_login}: received={response_count} stored={stored} response_duplicates={response_duplicates} existing_duplicates={existing_duplicates} recent_duplicates={recent_duplicates} malformed={parse_failures} missing_timestamp={missing_timestamps} channel_login_mismatch={channel_login_mismatches} channel_id_mismatch={channel_id_mismatches} write_failures={write_failures}"
        );

        Ok(())
    }

    async fn acquire_recent_message_request_permit(&self) -> anyhow::Result<OwnedSemaphorePermit> {
        let permit = self
            .recent_message_request_slots
            .clone()
            .acquire_owned()
            .await
            .context("Recent-message request gate was closed")?;
        let mut next_request = self.recent_message_next_request.lock().await;
        let now = Instant::now();
        if *next_request > now {
            sleep_until(*next_request).await;
        }
        *next_request = Instant::now() + self.recent_message_request_spacing;
        Ok(permit)
    }

    async fn existing_message_keys(
        &self,
        candidates: &[PreparedMessage],
    ) -> anyhow::Result<HashSet<String>> {
        #[cfg(test)]
        if self.skip_existing_message_lookup {
            return Ok(HashSet::new());
        }

        let candidate_keys = candidates
            .iter()
            .map(|prepared| prepared.dedupe_key.clone())
            .collect::<HashSet<_>>();
        if candidate_keys.is_empty() {
            return Ok(HashSet::new());
        }

        let mut existing_keys = HashSet::new();
        let mut groups = HashMap::<String, (u64, u64)>::new();
        for prepared in candidates {
            let entry = groups
                .entry(prepared.message.channel_id.to_string())
                .or_insert((prepared.message.timestamp, prepared.message.timestamp));
            entry.0 = entry.0.min(prepared.message.timestamp);
            entry.1 = entry.1.max(prepared.message.timestamp);
        }

        for (channel_id, (from_millis, to_millis)) in groups {
            let buffered = self
                .app
                .flush_buffer
                .messages_by_channel(from_millis..to_millis.saturating_add(1), &channel_id)
                .await;
            existing_keys.extend(
                buffered
                    .iter()
                    .map(structured_event_identity)
                    .filter(|key| candidate_keys.contains(key)),
            );

            let stored_messages = self
                .app
                .db
                .query(
                    "SELECT ?fields FROM message_structured WHERE channel_id = ? AND timestamp >= ? AND timestamp <= ?",
                )
                .bind(channel_id)
                .bind(from_millis as f64 / 1000.0)
                .bind(to_millis as f64 / 1000.0)
                .fetch_all::<StructuredMessage<'static>>()
                .await
                .context("could not check stored recent-message identities")?;
            existing_keys.extend(
                stored_messages
                    .iter()
                    .map(structured_event_identity)
                    .filter(|key| candidate_keys.contains(key)),
            );
        }

        Ok(existing_keys)
    }

    async fn stop_backfill_tasks(&self) {
        let tasks = std::mem::take(&mut *self.backfill_tasks.lock().await);
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
    }

    async fn handle_command<C: LoginCredentials>(
        &self,
        cmd: &str,
        client: &TwitchClient<C>,
        sender_id: &str,
        sender_login: &str,
    ) -> anyhow::Result<()> {
        debug!("Processing command {cmd}");
        let mut split = cmd.split_whitespace();
        if let Some(action) = split.next() {
            let args: Vec<&str> = split.collect();

            match action {
                "join" => {
                    self.check_admin(sender_login)?;
                    self.update_channels(client, &args, ChannelAction::Join)
                        .await?
                }
                "leave" | "part" => {
                    self.check_admin(sender_login)?;
                    self.update_channels(client, &args, ChannelAction::Part)
                        .await?
                }
                "optout" => {
                    self.optout_user(&args, sender_login, sender_id).await?;
                }
                _ => (),
            }
        }

        Ok(())
    }

    async fn optout_user(
        &self,
        args: &[&str],
        sender_login: &str,
        sender_id: &str,
    ) -> anyhow::Result<()> {
        let arg = args.first().context("No optout code provided")?;
        if self.app.optout_codes.remove(*arg).is_some() {
            self.app.optout_user(sender_id).await?;

            Ok(())
        } else if self.check_admin(sender_login).is_ok() {
            let user_id = self.app.get_user_id_by_name(arg).await?;

            self.app.optout_user(&user_id).await?;

            Ok(())
        } else {
            Err(anyhow!("Invalid optout code"))
        }
    }

    async fn update_channels<C: LoginCredentials>(
        &self,
        client: &TwitchClient<C>,
        channels: &[&str],
        action: ChannelAction,
    ) -> anyhow::Result<()> {
        if channels.is_empty() {
            return Err(anyhow!("no channels specified"));
        }

        let channels = self
            .app
            .get_users(
                vec![],
                channels.iter().map(ToString::to_string).collect(),
                false,
            )
            .await?;

        {
            let mut config_channels = self.app.config.channels.write().unwrap();

            for (channel_id, channel_name) in channels {
                match action {
                    ChannelAction::Join => {
                        info!("Joining channel {channel_name}");
                        config_channels.insert(channel_id);
                        client.join(channel_name)?;
                    }
                    ChannelAction::Part => {
                        info!("Parting channel {channel_name}");
                        config_channels.remove(&channel_id);
                        self.seen_channel_joins
                            .remove(&normalize_channel_login(&channel_name));
                        client.part(channel_name);
                    }
                }
            }
        }

        self.app.config.save()?;

        Ok(())
    }
}

enum ChannelAction {
    Join,
    Part,
}

#[derive(Clone, Copy)]
enum BackfillSchedule {
    #[cfg(test)]
    OneShot,
    Startup,
    Reconnect,
}

fn normalize_channel_login(channel_login: &str) -> String {
    channel_login
        .trim_matches(|character: char| character.is_ascii_whitespace() || character == '\0')
        .trim_start_matches('#')
        .to_ascii_lowercase()
}

fn trim_irc_framing(raw: &str) -> &str {
    raw.trim_matches(|character: char| character.is_ascii_whitespace() || character == '\0')
}

fn normalize_robotty_message(raw: &str) -> Cow<'_, str> {
    let raw = trim_irc_framing(raw);
    let Some(tagged) = raw.strip_prefix('@') else {
        return Cow::Borrowed(raw);
    };
    let Some((tags, remainder)) = tagged.split_once(' ') else {
        return Cow::Borrowed(raw);
    };
    if tags
        .split(';')
        .all(|tag| tag.is_empty() || tag.contains('='))
    {
        return Cow::Borrowed(raw);
    }

    let mut normalized = String::with_capacity(raw.len() + 8);
    normalized.push('@');
    for (index, tag) in tags.split(';').enumerate() {
        if index > 0 {
            normalized.push(';');
        }
        normalized.push_str(tag);
        if !tag.is_empty() && !tag.contains('=') {
            normalized.push('=');
        }
    }
    normalized.push(' ');
    normalized.push_str(remainder);
    Cow::Owned(normalized)
}

fn structured_event_identity(message: &StructuredMessage<'_>) -> String {
    if let Some(message_id) = message.id() {
        format!("uuid:{message_id}")
    } else {
        let canonical = serde_json::to_vec(message)
            .expect("serializing a structured message for deduplication cannot fail");
        format!("row:{}", blake3::hash(&canonical).to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::{structured_event_identity, BackfillSchedule, Bot, ServerMessage};
    use crate::{
        app::{cache::UsersCache, App},
        config::Config,
        db::writer::FlushBuffer,
        recent_messages::{RecentMessagesClient, RecentMessagesRuntimeSummary},
    };
    use axum::{
        extract::Path, http::StatusCode, response::IntoResponse, routing::get, Json, Router,
    };
    use dashmap::DashSet;
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex as StdMutex,
        },
        time::{Duration, Instant as StdInstant},
    };
    use tokio::{net::TcpListener, sync::mpsc, time::timeout};
    use twitch_api::{
        twitch_oauth2::{AccessToken, AppAccessToken, ClientId, ClientSecret},
        HelixClient,
    };
    use twitch_irc::message::IRCMessage;

    const REPLAYED_COMMAND: &str = "@room-id=1;user-id=200;tmi-sent-ts=1704067200000;id=robotty-command;display-name=admin;badges=;color=;user-type=;emotes=;flags= :admin!admin@admin.tmi.twitch.tv PRIVMSG #channelone :!rustlog optout secret-code";
    const UUID_MESSAGE: &str = "@room-id=1;user-id=200;tmi-sent-ts=1704067200000;id=272e342c-5864-4c59-b730-25908cdb7f57;display-name=user;badges=;color=;user-type=;emotes=;flags= :user!user@user.tmi.twitch.tv PRIVMSG #channelone :hello";
    const ROBOTTY_VALUELESS_MESSAGE: &str = "@display-name=user;flags;badge-info;id=272e342c-5864-4c59-b730-25908cdb7f57;badges;user-type;color=#00FF7F;emotes;room-id=1;tmi-sent-ts=1704067200000;user-id=200 :user!user@user.tmi.twitch.tv PRIVMSG #channelone :hello";

    fn test_app() -> App {
        let config: Config = serde_json::from_value(serde_json::json!({
            "clickhouseUrl": "http://127.0.0.1:9",
            "clickhouseDb": "rustlog",
            "channels": ["1"],
            "clientID": "client",
            "clientSecret": "secret",
            "admins": ["admin"],
            "optOut": {}
        }))
        .unwrap();
        let optout_codes = Arc::new(DashSet::new());
        optout_codes.insert("secret-code".to_string());
        let users = UsersCache::default();
        users.insert("1".to_string(), "channelone".to_string());
        App {
            helix_client: HelixClient::default(),
            token: Arc::new(AppAccessToken::from_existing_unchecked(
                AccessToken::new("token".to_string()),
                None,
                ClientId::new("client".to_string()),
                ClientSecret::new("secret".to_string()),
                None,
                None,
            )),
            users,
            optout_codes,
            db: Arc::new(clickhouse::Client::default().with_url("http://127.0.0.1:9")),
            config: Arc::new(config),
            flush_buffer: FlushBuffer::default(),
        }
    }

    fn recent_messages_client(address: std::net::SocketAddr) -> RecentMessagesClient {
        RecentMessagesClient::from_summary(RecentMessagesRuntimeSummary {
            enabled: true,
            base_url: Some(format!("http://{address}/api/v2/recent-messages")),
            limit: 800,
            warnings: Vec::new(),
        })
    }

    async fn bot_with_server(
        router: Router,
    ) -> (
        Bot,
        mpsc::Receiver<crate::db::schema::StructuredMessage<'static>>,
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let (writer_tx, writer_rx) = mpsc::channel(10);
        let mut bot =
            Bot::new_with_recent_messages(test_app(), writer_tx, recent_messages_client(address));
        bot.recent_message_request_spacing = Duration::ZERO;
        bot.skip_existing_message_lookup = true;
        (bot, writer_rx, server)
    }

    async fn wait_for_backfills(bot: &Bot) {
        let tasks = std::mem::take(&mut *bot.backfill_tasks.lock().await);
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn replay_stores_command_once_without_command_side_effects() {
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(|| async {
                Json(serde_json::json!({
                    "messages": ["not raw irc", REPLAYED_COMMAND, REPLAYED_COMMAND],
                    "error": null,
                    "error_code": null
                }))
            }),
        );
        let (bot, mut writer_rx, server) = bot_with_server(router).await;

        bot.fetch_recent_messages_for_channel("channelone", "1")
            .await
            .unwrap();

        let stored = timeout(Duration::from_secs(1), writer_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.user_friendly_text(), "!rustlog optout secret-code");
        assert!(bot.app.optout_codes.contains("secret-code"));
        assert!(!bot.app.config.opt_out.contains_key("200"));
        assert!(timeout(Duration::from_millis(50), writer_rx.recv())
            .await
            .is_err());
        server.abort();
    }

    #[tokio::test]
    async fn normalizes_valid_items_and_rejects_untrusted_robotty_metadata() {
        let valid = format!(
            " \r\n\0{}\0\n ",
            ROBOTTY_VALUELESS_MESSAGE.replace("#channelone", "#ChAnNeLoNe")
        );
        let wrong_login = UUID_MESSAGE
            .replace("#channelone", "#otherchannel")
            .replace(
                "272e342c-5864-4c59-b730-25908cdb7f57",
                "372e342c-5864-4c59-b730-25908cdb7f57",
            );
        let wrong_room_id = UUID_MESSAGE.replace("room-id=1", "room-id=2").replace(
            "272e342c-5864-4c59-b730-25908cdb7f57",
            "472e342c-5864-4c59-b730-25908cdb7f57",
        );
        let missing_timestamp = UUID_MESSAGE
            .replace("tmi-sent-ts=1704067200000;", "")
            .replace(
                "272e342c-5864-4c59-b730-25908cdb7f57",
                "572e342c-5864-4c59-b730-25908cdb7f57",
            );
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(move || {
                let messages = vec![
                    valid.clone(),
                    wrong_login.clone(),
                    wrong_room_id.clone(),
                    missing_timestamp.clone(),
                    "not raw irc".to_string(),
                ];
                async move {
                    Json(serde_json::json!({
                        "messages": messages,
                        "error": null,
                        "error_code": null
                    }))
                }
            }),
        );
        let (bot, mut writer_rx, server) = bot_with_server(router).await;

        bot.fetch_recent_messages_for_channel(" \0#CHANNELONE\r\n", "1")
            .await
            .unwrap();

        let stored = timeout(Duration::from_secs(1), writer_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.channel_login, "channelone");
        assert_eq!(stored.channel_id, "1");
        assert_eq!(stored.timestamp, 1_704_067_200_000);
        assert_eq!(
            stored.id().as_deref(),
            Some("272e342c-5864-4c59-b730-25908cdb7f57")
        );
        assert_eq!(stored.color, Some(0x00ff7f));
        assert!(timeout(Duration::from_millis(50), writer_rx.recv())
            .await
            .is_err());
        server.abort();
    }

    #[tokio::test]
    async fn robotty_items_are_stored_in_timestamp_order() {
        let later = UUID_MESSAGE
            .replace("1704067200000", "1704067202000")
            .replace(
                "272e342c-5864-4c59-b730-25908cdb7f57",
                "672e342c-5864-4c59-b730-25908cdb7f57",
            );
        let earlier = UUID_MESSAGE
            .replace("1704067200000", "1704067201000")
            .replace(
                "272e342c-5864-4c59-b730-25908cdb7f57",
                "772e342c-5864-4c59-b730-25908cdb7f57",
            );
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(move || {
                let messages = vec![later.clone(), earlier.clone()];
                async move {
                    Json(serde_json::json!({
                        "messages": messages,
                        "error": null,
                        "error_code": null
                    }))
                }
            }),
        );
        let (bot, mut writer_rx, server) = bot_with_server(router).await;

        bot.fetch_recent_messages_for_channel("channelone", "1")
            .await
            .unwrap();

        assert_eq!(writer_rx.recv().await.unwrap().timestamp, 1_704_067_201_000);
        assert_eq!(writer_rx.recv().await.unwrap().timestamp, 1_704_067_202_000);
        server.abort();
    }

    #[tokio::test]
    async fn robotty_requests_are_spaced_and_concurrency_is_bounded() {
        let requests = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let starts = Arc::new(StdMutex::new(Vec::new()));
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get({
                let requests = requests.clone();
                let active = active.clone();
                let max_active = max_active.clone();
                let starts = starts.clone();
                move || {
                    let requests = requests.clone();
                    let active = active.clone();
                    let max_active = max_active.clone();
                    let starts = starts.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        let active_now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(active_now, Ordering::SeqCst);
                        starts.lock().unwrap().push(StdInstant::now());
                        tokio::time::sleep(Duration::from_millis(75)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        Json(serde_json::json!({
                            "messages": [],
                            "error": null,
                            "error_code": null
                        }))
                    }
                }
            }),
        );
        let (mut bot, _writer_rx, server) = bot_with_server(router).await;
        bot.recent_message_request_spacing = Duration::from_millis(25);
        bot.recent_message_request_slots = Arc::new(tokio::sync::Semaphore::new(2));

        for index in 0..8 {
            let channel_id = (index + 10).to_string();
            let channel_login = format!("channel{index}");
            bot.app.users.insert(channel_id, channel_login.clone());
            bot.trigger_recent_messages_fetch(channel_login, "test", BackfillSchedule::OneShot)
                .await;
        }
        wait_for_backfills(&bot).await;

        let mut starts = starts.lock().unwrap().clone();
        starts.sort_unstable();
        assert_eq!(requests.load(Ordering::SeqCst), 8);
        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        assert!(starts.last().unwrap().duration_since(starts[0]) >= Duration::from_millis(140));
        server.abort();
    }

    #[tokio::test]
    async fn successful_own_joins_trigger_backfill_and_suppress_concurrent_fetches() {
        let requests = Arc::new(AtomicUsize::new(0));
        let handler_requests = requests.clone();
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(move || {
                let requests = handler_requests.clone();
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Json(serde_json::json!({
                        "messages": [],
                        "error": null,
                        "error_code": null
                    }))
                }
            }),
        );
        let (mut bot, _writer_rx, server) = bot_with_server(router).await;
        bot.startup_backfill_delays = vec![Duration::ZERO; 5].into();
        bot.reconnect_backfill_delays = vec![Duration::ZERO; 6].into();
        let own_join = ServerMessage::try_from(
            IRCMessage::parse(
                ":justinfan12345!justinfan12345@justinfan12345.tmi.twitch.tv JOIN #channelone",
            )
            .unwrap(),
        )
        .unwrap();
        let other_join = ServerMessage::try_from(
            IRCMessage::parse(":viewer!viewer@viewer.tmi.twitch.tv JOIN #channelone").unwrap(),
        )
        .unwrap();

        bot.trigger_backfill_for_own_join(&own_join, "justinfan12345")
            .await;
        wait_for_backfills(&bot).await;
        assert_eq!(requests.load(Ordering::SeqCst), 5);

        bot.trigger_backfill_for_own_join(&own_join, "justinfan12345")
            .await;
        bot.trigger_backfill_for_own_join(&own_join, "justinfan12345")
            .await;
        bot.trigger_backfill_for_own_join(&other_join, "justinfan12345")
            .await;
        wait_for_backfills(&bot).await;
        let after_coalesced_reconnect = requests.load(Ordering::SeqCst);
        assert!((11..=12).contains(&after_coalesced_reconnect));

        bot.trigger_backfill_for_own_join(&own_join, "justinfan12345")
            .await;
        wait_for_backfills(&bot).await;
        assert_eq!(
            requests.load(Ordering::SeqCst),
            after_coalesced_reconnect + 6
        );
        server.abort();
    }

    #[tokio::test]
    async fn stored_id_lookup_failure_prevents_backfill_insert() {
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(|| async {
                Json(serde_json::json!({
                    "messages": [UUID_MESSAGE],
                    "error": null,
                    "error_code": null
                }))
            }),
        );
        let (mut bot, mut writer_rx, server) = bot_with_server(router).await;
        bot.skip_existing_message_lookup = false;

        assert!(bot
            .fetch_recent_messages_for_channel("channelone", "1")
            .await
            .is_err());
        assert!(writer_rx.try_recv().is_err());

        server.abort();
    }

    #[tokio::test]
    async fn one_channel_failure_does_not_stop_another_backfill() {
        let working_message = REPLAYED_COMMAND.replace("#channelone", "#working");
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(move |Path(channel): Path<String>| {
                let working_message = working_message.clone();
                async move {
                    if channel == "broken" {
                        StatusCode::BAD_GATEWAY.into_response()
                    } else {
                        Json(serde_json::json!({
                            "messages": [working_message],
                            "error": null,
                            "error_code": null
                        }))
                        .into_response()
                    }
                }
            }),
        );
        let (bot, mut writer_rx, server) = bot_with_server(router).await;
        bot.app.users.insert("1".to_string(), "working".to_string());
        bot.app.users.insert("2".to_string(), "broken".to_string());

        bot.trigger_recent_messages_fetch("broken".to_string(), "test", BackfillSchedule::OneShot)
            .await;
        bot.trigger_recent_messages_fetch("working".to_string(), "test", BackfillSchedule::OneShot)
            .await;
        wait_for_backfills(&bot).await;

        assert!(writer_rx.recv().await.is_some());
        server.abort();
    }

    #[tokio::test]
    async fn replay_dedupes_against_a_live_message_and_skips_remote_errors() {
        let router = Router::new().route(
            "/api/v2/recent-messages/{channel}",
            get(|Path(channel): Path<String>| async move {
                if channel == "remoteerror" {
                    Json(serde_json::json!({
                        "messages": [REPLAYED_COMMAND],
                        "error": "temporarily unavailable",
                        "error_code": "503"
                    }))
                } else {
                    Json(serde_json::json!({
                        "messages": [REPLAYED_COMMAND],
                        "error": null,
                        "error_code": null
                    }))
                }
            }),
        );
        let (bot, mut writer_rx, server) = bot_with_server(router).await;
        let message = IRCMessage::parse(REPLAYED_COMMAND).unwrap();
        let prepared = bot
            .prepare_message(message, REPLAYED_COMMAND)
            .unwrap()
            .unwrap();
        assert!(bot.claim_message(&prepared.dedupe_key));
        bot.write_prepared(prepared).await.unwrap();

        bot.fetch_recent_messages_for_channel("channelone", "1")
            .await
            .unwrap();
        bot.fetch_recent_messages_for_channel("remoteerror", "1")
            .await
            .unwrap();

        assert!(writer_rx.recv().await.is_some());
        assert!(timeout(Duration::from_millis(50), writer_rx.recv())
            .await
            .is_err());
        server.abort();
    }

    #[test]
    fn event_identity_uses_uuid_and_structured_hash_fallback() {
        let with_id = crate::db::schema::StructuredMessage::from_unstructured(
            &crate::db::schema::UnstructuredMessage {
                channel_id: "1",
                user_id: "200",
                timestamp: 1_704_067_200_000,
                raw: UUID_MESSAGE,
            },
        )
        .unwrap();
        assert_eq!(
            structured_event_identity(&with_id),
            "uuid:272e342c-5864-4c59-b730-25908cdb7f57"
        );

        let without_id = crate::db::schema::StructuredMessage::from_unstructured(
            &crate::db::schema::UnstructuredMessage {
                channel_id: "1",
                user_id: "200",
                timestamp: 1_704_067_200_000,
                raw: REPLAYED_COMMAND,
            },
        )
        .unwrap();
        let identity = structured_event_identity(&without_id);
        assert!(identity.starts_with("row:"));
        assert_eq!(identity, structured_event_identity(&without_id));
    }
}
