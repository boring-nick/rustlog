# Rustlog

## Description
Rustlog is a Twitch logging service based on [justlog](https://github.com/gempir/justlog). It provides the same web UI and API, but it utilizes [Clickhouse](https://clickhouse.com) for storage instead of text files.


## Installation

Create a `config.json` file (see [CONFIG.md](./docs/CONFIG.md))

### Docker
```yaml
version: "3.8"
  
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse
    volumes:
      - "./ch-data:/var/lib/clickhouse:rw"
    environment:
      CLICKHOUSE_DB: "rustlog"
    restart: unless-stopped
        
  rustlog:
    image: ghcr.io/boring-nick/rustlog:master
    container_name: rustlog
    ports:
      - 8025:8025 
    volumes:
      - "./config.json:/config.json"
    depends_on: 
      - clickhouse
    restart: unless-stopped
```

### From source

- Set up Clickhouse
- `cargo install --locked --git https://github.com/boring-nick/rustlog`
- You can now run the `rustlog` binary

## Advantages over justlog

- Significantly better storage efficiency (2x+ improvement) thanks to not duplicating log files and better compression (using ZSTD in Clickhouse)
- Blazing fast log queries with response streaming and a [highly performant IRC parser](https://github.com/jprochazk/twitch-rs)
- Support for ndjson logs responses

## Migrating from justlog
See [MIGRATION.md](./docs/MIGRATION.md)
