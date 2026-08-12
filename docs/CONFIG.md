# Configuration

Configuration is stored in a `config.json` file.

Available options:
- `clickhouseUrl` (string): Connection URL for Clickhouse. Note that it should start with the protocol (`http://`)
- `clickhouseDb` (string): Clickhouse database name.
- `clickhouseUsername` (string): Clickhouse username.
- `clickhousePassword` (string): Clickhouse password.
- `clickhouseFlushInterval` (number): Interval (in seconds) of how often messages should be flushed to the database. A lower value means that logs are available sooner at the expensive of higher database load. Defaults to 10.
- `listenAddress` (string): Listening address for the web server. Defaults to `0.0.0.0:8025`.
- `recentMessagesEnabled` (boolean): Enables best-effort Robotty recent-message recovery. Defaults to `false`.
- `recentMessagesUrl` (string): Robotty-compatible recent-messages base URL. Defaults to `https://recent-messages.robotty.de/api/v2/recent-messages`. Only HTTP and HTTPS URLs are accepted.
- `recentMessagesLimit` (number): Maximum number of recent messages requested per channel. Defaults to `800`.
- `channels` (array of strings): List of channel ids to be logged.
- `clientId` (string): Twitch client id.
- `clientSecret` (string): Twitch client secret.
- `admins` (array of strings): List of usernames who are allowed to use administration commands.
- `optOut` (object of strings: booleans): List of user ids who opted out from being logged.
- `adminAPIKey` (string): API key for admin requests

When enabled, rustlog requests recent messages after it successfully joins a channel at startup, after reconnecting, and after a runtime channel addition. Backfill is best-effort: request, response, and individual IRC parsing failures are logged without stopping live Twitch ingest.

Example config:
```json
{
  "clickhouseUrl": "http://clickhouse:8123",
  "clickhouseDb": "rustlog",
  "clickhouseUsername": "user",
  "clickhousePassword": "SuperSecretPassword",
  "listenAddress": "0.0.0.0:8025",
  "recentMessagesEnabled": false,
  "recentMessagesUrl": "https://recent-messages.robotty.de/api/v2/recent-messages",
  "recentMessagesLimit": 800,
  "channels": ["12345"],
  "clientID": "id",
  "clientSecret": "secret",
  "admins": [],
  "optOut": {},
  "adminAPIKey": "verysecurekey"
}
```
