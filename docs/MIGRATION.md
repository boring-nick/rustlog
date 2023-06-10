# Migrating from justlog

Rustlog supports migrating your existing data from justlog. This process will read all log files and write them into the database.

## Config
Rustlog uses a config format nearly identical to justlog, however you still need to add Clickhouse connection settings to it. See [config.example.json](../config.example.json) for the keys starting with `clickhouse`.

After this, you should have a running rustlog instance logging new messages.

## Data
First, rustlog needs to have access to the justlog logs directory. If using docker, you need to add it as a volume mount to the container.

After that, you can run the migration command.

Docker:
```
docker exec -it rustlog rustlog migrate --source-dir /logs --jobs 1
```
Manual installation:
```
rustlog migrate --source-dir /path/to/logs --jobs 1
```
The `--jobs` parameter defines how many threads rustlog will use for migrating. If your logs are on a HDD, you should keep it at 1, as IO will likely be the bottleneck anyway. If you have an SSD, then setting the value to half of your CPU threads should generally work well.

The migration can take anywhere from a few minutes to a few hours depending on your amount of logs and system resources.
