# Configuration

Configuration is stored in a `config.json` file.

Available options:
- `clickhouseUrl` (string): Connection URL for Clickhouse. Note that it should start with the protocol (`http://`)
- `clickhouseDb` (string): Clickhouse database name.
- `clickhouseUsername` (string): Clickhouse username.
- `clickhousePassword` (string): Clickhouse password.
- `listenAddress` (string): Listening address for the web server. Defaults to `0.0.0.0:8025`.
- `channels` (array of strings): List of channel ids to be logged.
- `clientId` (string): Twitch client id.
- `clientSecret` (string): Twitch client secret.
- `admins` (array of strings): List of usernames who are allowed to use administration commands.
- `optOut` (object of strings: booleans): Liste of user ids who opted out from being logged.