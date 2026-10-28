# Configuration reference

[中文](configuration.zh-CN.md)

> Applies to Amberlog 0.1.0.

This document lists all Amberlog configuration keys: what each one means, its default value, its range and its environment variable name. The environment variable names, ranges, required keys and exit codes in this document were all tested on Linux.

## 1. Where configuration comes from

Amberlog takes configuration from three sources, from highest to lowest priority:

1. **Environment variables**: their names start with `AMBERLOG_`.
2. **Configuration file**: TOML format, given with `--config <path>`. Without `--config`, Amberlog reads no configuration file.
3. **Built-in defaults**.

Amberlog reads configuration only at startup. After you change the configuration, restart the process to apply the change. Exception: the contents of the certificate and private-key files for inbound TLS are read again automatically (see [`server.tls.cert_file`](#server)).

### 1.1 Configuration keys and environment variable names

A configuration key uses dots to separate levels, for example `object_store.url`. In the configuration file, write it as a TOML table:

```toml
[object_store]
url = "s3://my-bucket/amberlog"
```

The environment variable name follows from these rules:

1. Add the prefix `AMBERLOG_`.
2. Change the configuration key to uppercase.
3. Replace each dot `.` with two underscores `__`. Single underscores that are already in the key name stay unchanged.

Examples:

| Configuration key | Environment variable |
|---|---|
| `object_store.url` | `AMBERLOG_OBJECT_STORE__URL` |
| `server.tls.cert_file` | `AMBERLOG_SERVER__TLS__CERT_FILE` |
| `alerts.missed_collection.enabled` | `AMBERLOG_ALERTS__MISSED_COLLECTION__ENABLED` |

The environment variable name must be exactly the uppercase form of the configuration key. A name in lowercase or with a spelling error (such as `AMBERLOG_server__http_addr`) is treated as an unknown configuration key.

An environment variable whose value is an empty string counts as "set, with the value empty string". It does not count as unset. For `wal.dir`, `auth.token` and some file-path keys, an empty string is in turn treated as not configured (see the description of each key).

### 1.2 Unknown configuration keys

When the configuration file or the environment variables contain a configuration key that Amberlog does not know, the process refuses to start. It exits with exit code 2 and names the key on stderr. This rule prevents a misspelled key from being ignored silently.

**So other programs in the same environment must not use environment variables that start with `AMBERLOG_`.** For example, do not name a collector's token variable `AMBERLOG_TOKEN`.

Three configuration keys can be set only in the configuration file: `index_hints`, `alerts.rules` and `alerts.missed_collection.webhook_headers`. If you set them with environment variables (names that start with `AMBERLOG_INDEX_HINTS`, `AMBERLOG_ALERTS__RULES` or `AMBERLOG_ALERTS__MISSED_COLLECTION__WEBHOOK_HEADERS`), they are also treated as unknown configuration keys.

## 2. How to write values

| Type | In the configuration file | In environment variables |
|---|---|---|
| Boolean | TOML boolean: `true`, `false` | Only lowercase `true` and `false`. `True`, `TRUE`, `1`, `0`, `yes` and `on` are all invalid |
| Integer | TOML integer (no quotes) | Decimal integer, optionally with a leading `-`, within the range of a 64-bit signed integer. `_`, `+`, a decimal point or an exponent (such as `1e3`) are all invalid |
| Duration | TOML string, such as `"5m"` | The same form, such as `5m` |
| String, path | TOML string | As is |

Write a **duration** as `<integer><unit>`:

- The integer is one or more decimal digits, with no sign, decimal point, space or underscore.
- The unit is one of `ms`, `s`, `m`, `h`, `d`, in lowercase only. `d` equals 86400 seconds.
- Converted to microseconds, the value must not exceed 2^63−1 (about 106751991 days).

Valid examples: `500ms`, `10s`, `5m`, `72h`, `30d`. Invalid examples: `5`, `1.5h`, `5 s`, `5S`, `+5s`, `5us`.

In the configuration file, the TOML type of a value must be correct. For example, `auth.enabled = "false"` (a string) and `flush.max_records = "10"` (a string) both cause a type error and exit code 2.

**Relative paths** are resolved against the working directory of the process.

When a value is invalid (wrong type, format or range), the process exits with exit code 2 and names the key on stderr.

## 3. Required keys for each subcommand

| Subcommand | Required configuration keys |
|---|---|
| `amberlog serve` | `object_store.url`, `wal.dir`; also `auth.token` when `auth.enabled` is true (the default); also `object_store.s3.conditional_put` when `object_store.url` is `s3://` |
| `amberlog catalog` | `object_store.url`; also `auth.token` when `auth.enabled` is true |
| `amberlog retire` | `object_store.url`, `wal.dir`; also `object_store.s3.conditional_put` when `object_store.url` is `s3://` |
| `amberlog evidence export`, `amberlog evidence verify` | `object_store.url` |

When a required key is missing, the process writes one error line that contains the missing key name to stderr and exits with exit code 2.

With every subcommand, Amberlog recognizes and validates all configuration keys. A configuration key that a subcommand does not use is still validated, but has no effect. For the configuration keys that each subcommand uses, see [section 6](#6-configuration-keys-in-effect-for-each-mode).

## 4. All configuration keys

The "Range" column gives the allowed values. A value out of range is treated as an invalid value. Byte-count defaults are followed by their conversion in parentheses.

### server

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `server.http_addr` | `AMBERLOG_SERVER__HTTP_ADDR` | `0.0.0.0:8800` | `<IPv4 address>:<port>` or `[<IPv6 address>]:<port>`, port 0–65535. Host names (such as `localhost`) and IPv6 zone identifiers are not accepted | Listen address. All endpoints (`/api/`, `/metrics`, `/healthz`, `/readyz`, `/ui`, `/iceberg/`) are on this address. When the address is valid but Amberlog cannot listen on it (for example, the port is in use), the process exits with exit code 4 |
| `server.shutdown_timeout` | `AMBERLOG_SERVER__SHUTDOWN_TIMEOUT` | `10s` | Duration | After SIGTERM or SIGINT, the maximum time to wait for in-flight requests to complete. When this time ends, Amberlog stops waiting |
| `server.tls.cert_file` | `AMBERLOG_SERVER__TLS__CERT_FILE` | None | File path. An empty string is treated as not configured | Certificate-chain file (PEM) for inbound TLS. The first certificate is the server certificate; the intermediate certificates follow it. Configure it together with `server.tls.key_file` |
| `server.tls.key_file` | `AMBERLOG_SERVER__TLS__KEY_FILE` | None | File path. An empty string is treated as not configured | Private-key file (PEM) for inbound TLS. See "Inbound TLS" below |
| `server.behind_tls_proxy` | `AMBERLOG_SERVER__BEHIND_TLS_PROXY` | `false` | Boolean | Plaintext declaration: states that a reverse proxy in front of `server.http_addr` terminates TLS. See "Rules for plaintext listening" below |

**Rules for plaintext listening** (`serve` and `catalog`):

| Certificate configured | Listen address | `server.behind_tls_proxy` | Result |
|---|---|---|---|
| Yes | Any | `false` | Listens with TLS only |
| Yes | Any | `true` | Exits with exit code 2: you cannot configure a certificate and the plaintext declaration together |
| No | Loopback address (`127.0.0.0/8` or `::1`) | Any | Listens in plaintext |
| No | Non-loopback address | `false` | Exits with exit code 2; stderr names `server.http_addr` and `server.behind_tls_proxy` |
| No | Non-loopback address | `true` | Listens in plaintext and writes one `plaintext_listen` warning log line at startup |

`0.0.0.0` and `[::]` are not loopback addresses. So when you start with the default `server.http_addr`, you must configure a certificate or set `server.behind_tls_proxy = true`. The offline subcommands (`retire`, `evidence export`, `evidence verify`) do not listen, so this rule does not apply to them.

**Inbound TLS:**

- After you configure a certificate, this address accepts only TLS 1.2 and TLS 1.3 connections. Plaintext HTTP requests get no HTTP response.
- Client certificates are not required.
- Accepted private keys: RSA without a passphrase (2048, 3072 or 4096 bits, PKCS#8 or PKCS#1 encoding), ECDSA (P-256, P-384, PKCS#8 or SEC1 encoding), Ed25519 (PKCS#8 encoding). The private key must match the first certificate in the certificate file.
- When a file does not exist, cannot be read or has invalid contents, or when the private key does not match the certificate, the process exits with exit code 2.
- You can replace the contents of the two files while the process runs, without a restart: the process reads the two files at intervals of no more than 60 seconds. After the contents change to a new valid certificate and private key, new connections use the new certificate within 120 seconds at most. Connections that already exist are not affected. When the new contents are invalid, the process continues to use the old certificate and writes one `tls_reload_failed` error log line.

### storage

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `storage.prefix` | `AMBERLOG_STORAGE__PREFIX` | `amberlog` | Matches `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$` | All objects that Amberlog writes are under this prefix, below the location that `object_store.url` points to |
| `storage.commit_max_retries` | `AMBERLOG_STORAGE__COMMIT_MAX_RETRIES` | `8` | Integer, not less than 0 | Maximum number of retries when a commit (a hot-tier snapshot or a cold-tier commit) meets a concurrent conflict. Retries use exponential backoff |
| `storage.delete_delay` | `AMBERLOG_STORAGE__DELETE_DELAY` | `1h` | Duration, greater than 0 | After a data file is no longer used because of lake entry, expiry deletion or compaction, Amberlog keeps it for at least this long before it physically deletes it. Snapshot metadata that the cold tier no longer uses is also kept for this long. This gives running queries and external-engine reads time to complete |

### object_store

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `object_store.url` | `AMBERLOG_OBJECT_STORE__URL` | None (required) | `file:///<absolute path>` or `s3://<bucket name>[/<path>]`; see below | Where the data is stored |
| `object_store.s3.conditional_put` | `AMBERLOG_OBJECT_STORE__S3__CONDITIONAL_PUT` | None | `create_only`, or its synonym `etag` | The operator confirms that this storage supports "create only if it does not exist" with the request header `If-None-Match: *`. Required with `s3://` when you run `serve` or `retire`. See "Conditional-write probe" below |
| `object_store.s3.endpoint` | `AMBERLOG_OBJECT_STORE__S3__ENDPOINT` | None | An absolute URL that starts with `http://` or `https://` | Endpoint address of S3-compatible storage, such as `https://minio.example.com:9000`. When the endpoint starts with `http://`, Amberlog still connects in plaintext and writes one `object_store_plaintext` warning log line at startup |
| `object_store.s3.region` | `AMBERLOG_OBJECT_STORE__S3__REGION` | `us-east-1` | Non-empty string | Region |
| `object_store.s3.access_key_id` | `AMBERLOG_OBJECT_STORE__S3__ACCESS_KEY_ID` | None | String | Access key ID. Configure it together with `secret_access_key`. When only one of the two is configured, the process exits with exit code 2 and names the missing one. When neither is configured, Amberlog gets credentials from the [AWS credential environment variables](#7-s3-credential-environment-variables) and the credential chain |
| `object_store.s3.secret_access_key` | `AMBERLOG_OBJECT_STORE__S3__SECRET_ACCESS_KEY` | None | String | Secret access key |
| `object_store.s3.force_path_style` | `AMBERLOG_OBJECT_STORE__S3__FORCE_PATH_STYLE` | `false` | Boolean | When true, request addresses use path style (`<endpoint>/<bucket name>/<object key>`), not virtual-hosted style (`<bucket name>.<endpoint>/<object key>`) |
| `object_store.s3.ca_file` | `AMBERLOG_OBJECT_STORE__S3__CA_FILE` | None | File path. An empty string is treated as not configured | In addition to the root certificates that the system trusts, Amberlog also trusts all certificates in this file (PEM) to verify the certificate of the object-storage endpoint. It does not replace the system root certificates. With `s3://`, when the file does not exist, cannot be read or has invalid contents, the process exits with exit code 2. Amberlog always verifies the certificate of the object-storage endpoint; there is no configuration to skip verification |
| `object_store.s3.server_side_encryption` | `AMBERLOG_OBJECT_STORE__S3__SERVER_SIDE_ENCRYPTION` | None | `sse_s3` or `sse_kms` | Server-side encryption. `sse_s3`: each write request carries `x-amz-server-side-encryption: AES256`. `sse_kms`: each write request carries `x-amz-server-side-encryption: aws:kms`. When it is not configured, requests carry no encryption header, and objects are stored with the default encryption setting of the storage bucket |
| `object_store.s3.sse_kms_key_id` | `AMBERLOG_OBJECT_STORE__S3__SSE_KMS_KEY_ID` | None | Non-empty string. You can configure it only when `server_side_encryption` is `sse_kms`; otherwise the process exits with exit code 2 | KMS key ID. Write requests also carry `x-amz-server-side-encryption-aws-kms-key-id`. When it is not configured, the storage provider's default KMS key is used |

**How to write `object_store.url`:**

- The scheme can only be lowercase `file` or `s3`. `S3://`, `File://` and `http://` are all invalid.
- `file://` must be followed by an absolute path, that is, written with three slashes: `file:///var/lib/amberlog/data`.
- `s3://` is followed by the bucket name, and optionally by a path that is used as the object-key prefix: `s3://my-bucket/amberlog`. A bucket name consists of letters, digits, `-`, `.`, `_` and `~`, and its case is used as written.
- It must not contain a user name and password (`user:pass@`), a query string (`?`) or a fragment (`#`).
- The path part is percent-decoded segment by segment according to RFC 3986. It must not contain two adjacent `/`, and a decoded segment must not be `.` or `..`. A trailing `/` does not change the meaning.

**Conditional-write probe:** When `serve` or `retire` starts, Amberlog writes the same new key under `<storage.prefix>/_system/` twice, one after the other, with "create only if it does not exist". The first write must succeed. The second write must fail with "already exists". If the second write also succeeds, or the storage answers this kind of write with HTTP 400 or 501, the storage does not support Amberlog's commit protocol, and the process exits with exit code 2.

**`file://` and server-side encryption:** When `object_store.url` is `file://`, the values of the `object_store.s3.*` keys are still validated, but have no effect. For example, Amberlog does not read the file that `ca_file` points to.

### wal

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `wal.dir` | `AMBERLOG_WAL__DIR` | None (required for `serve` and `retire`) | Directory path. An empty string is treated as not configured | Write-ahead log (WAL) directory. Records that have been acknowledged but not yet committed to object storage are written here first. If the directory does not exist, Amberlog creates it. When Amberlog cannot create it or cannot write to it, the process exits with exit code 3 |
| `wal.alert_bytes` | `AMBERLOG_WAL__ALERT_BYTES` | `8589934592` (8 GiB) | Integer, not less than 1 | Global. When the backlog bytes in the WAL change from not more than this value to more than it, Amberlog writes one `wal_backlog_high` warning log line. It does not write the line again until the backlog falls below this value |

### auth

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `auth.enabled` | `AMBERLOG_AUTH__ENABLED` | `true` | Boolean | When false, authentication is off: no endpoint requires a token, `auth.token` is no longer required, and Amberlog writes one `auth_disabled` warning log line at startup |
| `auth.token` | `AMBERLOG_AUTH__TOKEN` | None (required for `serve` and `catalog` when authentication is on) | Only `A`–`Z`, `a`–`z`, `0`–`9` and `.` `_` `~` `+` `/` `-`, optionally followed by one or more `=` at the end (the `b64token` of RFC 6750). An empty string is treated as not configured | Access token. Clients send `Authorization: Bearer <token>` in the request header. Requests under `/api/`, `/metrics` and `/iceberg/` require the token; `/healthz` and `/readyz` do not |

You can generate a token like this:

```sh
openssl rand -hex 32
```

### ingest and flush

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `ingest.max_body_bytes` | `AMBERLOG_INGEST__MAX_BODY_BYTES` | `33554432` (32 MiB) | Integer, not less than 1 | Byte limit of a request body. For a compressed request body, Amberlog compares both the received byte count and the decompressed byte count with the limit. When the limit is exceeded, the response is HTTP 413 with `reason` `payload_too_large`. Applies to all POST endpoints under `/api/` |
| `ingest.max_future` | `AMBERLOG_INGEST__MAX_FUTURE` | `24h` | Duration | How much later than the server receive time a record's `_timestamp` can be. Later records are rejected one by one, with `reason` `timestamp_out_of_range` |
| `ingest.max_past` | `AMBERLOG_INGEST__MAX_PAST` | None (no limit) | Duration | When it is configured, records whose `_timestamp` is earlier than "receive time minus this value" are rejected one by one, with `reason` `timestamp_out_of_range` |
| `ingest.max_columns` | `AMBERLOG_INGEST__MAX_COLUMNS` | `1000` | Integer, not less than 1 | Maximum number of columns per dataset, including `_timestamp`. When the new columns of a record would make the column count exceed the limit, the record is rejected, with `reason` `too_many_columns`. Records whose fields all fall on existing columns are not affected |
| `ingest.max_backlog_memory_bytes` | `AMBERLOG_INGEST__MAX_BACKLOG_MEMORY_BYTES` | `268435456` (256 MiB) | Integer, not less than 1 | Global. The maximum number of bytes in memory for records that have been acknowledged but not yet committed (counted as the bytes they take in the WAL). Records that do not fit in memory stay only in the WAL and are committed later as usual. When object storage is unavailable for a long time, this key prevents memory from growing without limit with the backlog |
| `flush.interval` | `AMBERLOG_FLUSH__INTERVAL` | `5s` | Duration, greater than 0 | Period of write commits: Amberlog writes the acknowledged records into data files and commits them to object storage |
| `flush.max_records` | `AMBERLOG_FLUSH__MAX_RECORDS` | `100000` | Integer, not less than 1 | Per dataset. When the records waiting for a commit reach this count, Amberlog starts a commit without waiting for `flush.interval`. It is also the maximum number of records in one commit |

### query

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `query.default_size` | `AMBERLOG_QUERY__DEFAULT_SIZE` | `100` | Integer, not less than 0, not greater than `query.max_size` | Maximum number of rows returned when a `_search` request has no `size` |
| `query.max_size` | `AMBERLOG_QUERY__MAX_SIZE` | `10000` | Integer, not less than 1 | Upper limit of `size` in a `_search` request. When it is exceeded, the response is HTTP 400 with `reason` `invalid_request`. Alert-rule evaluation also takes results up to this row count |
| `query.max_concurrency` | `AMBERLOG_QUERY__MAX_CONCURRENCY` | `4` | Integer, not less than 1 | Maximum number of queries that run at the same time, `_search` and alert-rule evaluation together (the built-in rule `missed_collection` is not counted). When the limit is reached, a new `_search` gets HTTP 429 at once with `reason` `too_many_queries`; it does not wait in a queue |
| `query.max_memory_bytes` | `AMBERLOG_QUERY__MAX_MEMORY_BYTES` | `536870912` (512 MiB) | Integer, not less than 1 | Limit of the memory that the system measures for one query. When it is exceeded, the query is stopped, and `_search` responds with HTTP 400 and `reason` `query_memory_exceeded` |

### log

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `log.level` | `AMBERLOG_LOG__LEVEL` | `info` | `error`, `warn`, `info`, `debug`, `trace` (lowercase only) | Level of the runtime log. This level does not filter [stable events](#8-stable-log-events) |
| `log.format` | `AMBERLOG_LOG__FORMAT` | `json` | `json` or `text` | The runtime log goes to stderr. With `json`, each line is one JSON object with `ts` (RFC 3339 UTC), `level` and `msg`. The line format of `text` can change between versions; use `json` when you build alerts on the log or parse it |

### lifecycle and compaction

Amberlog puts the records of each dataset into hour buckets by `_timestamp`, one hour bucket per hour. After an hour bucket ends, it is first sealed and then enters the lake (moves from the hot tier into the Apache Iceberg table of the cold tier).

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `lifecycle.seal_grace` | `AMBERLOG_LIFECYCLE__SEAL_GRACE` | `10m` | Duration, greater than 0 | How long after the end of an hour bucket it is sealed. After the seal, small files of similar size in the hour bucket are compacted |
| `lifecycle.lake_watermark` | `AMBERLOG_LIFECYCLE__LAKE_WATERMARK` | `72h` | Duration, greater than 0, and greater than `lifecycle.seal_grace` | How long after the end of an hour bucket it enters the lake. After lake entry, the data is held in Iceberg tables that external engines can read directly |
| `lifecycle.lake_alert_after` | `AMBERLOG_LIFECYCLE__LAKE_ALERT_AFTER` | `24h` | Duration, greater than 0 | When the lake-entry backlog time of a dataset changes from not more than this value to more than it, Amberlog writes one `lake_backlog_high` warning log line |
| `compaction.target_file_bytes` | `AMBERLOG_COMPACTION__TARGET_FILE_BYTES` | `134217728` (128 MiB) | Integer, not less than 1 | Target file size. Seal compaction compacts only files smaller than half of it. Cold compaction compacts files smaller than it; of these, files written by an earlier cold compaction must be smaller than half of it. Each file that lake entry and compaction write is not larger than it (except files that contain only one record) |
| `cold_compaction.check_interval` | `AMBERLOG_COLD_COMPACTION__CHECK_INTERVAL` | `24h` | Duration, greater than 0 | Check interval of cold compaction. Cold compaction compacts the small files in a day partition into large files |

### Hot-tier inspection and snapshot retention

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `hot_fsck.interval` | `AMBERLOG_HOT_FSCK__INTERVAL` | `1h` | Duration, greater than 0 | Period of hot-tier inspection. The inspection removes orphan files, and physically deletes data files that are no longer used and are older than `storage.delete_delay`. Cold-tier orphan removal, hot-tier snapshot expiry and the cleanup of alert evaluation records also run with this period |
| `hot_fsck.min_orphan_age` | `AMBERLOG_HOT_FSCK__MIN_ORPHAN_AGE` | `1h` | Duration, greater than 0 | An orphan file (a file that was written but that no commit references) is removed only when at least this long has passed since its upload completed |
| `hot_retention.snapshots` | `AMBERLOG_HOT_RETENTION__SNAPSHOTS` | `1000` | Integer, not less than 2 | Number of most recent snapshots that hot-tier snapshot expiry keeps |
| `hot_retention.age` | `AMBERLOG_HOT_RETENTION__AGE` | `168h` | Duration, greater than 0 | How long hot-tier snapshot expiry keeps snapshots. The most recent `hot_retention.snapshots` snapshots and the snapshots created within `hot_retention.age` are all kept |
| `cold_retention.snapshots` | `AMBERLOG_COLD_RETENTION__SNAPSHOTS` | `1` | Integer, not less than 1 | Number of most recent snapshots that the cold-tier Iceberg tables keep |
| `cold_retention.age` | `AMBERLOG_COLD_RETENTION__AGE` | `168h` | Duration, greater than 0 | How long the cold tier keeps snapshots. The most recent `cold_retention.snapshots` snapshots and the snapshots whose timestamp is within `cold_retention.age` are all kept. When an external engine runs a time-travel query, it can query only the snapshots that are kept |
| `cold_retention.check_interval` | `AMBERLOG_COLD_RETENTION__CHECK_INTERVAL` | `24h` | Duration, greater than 0 | Interval at which cold-tier snapshot retention runs |

### retention (data retention period)

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `retention.period` | `AMBERLOG_RETENTION__PERIOD` | None (no deletion) | Duration, greater than 0, not greater than `36500d`, and greater than `lifecycle.lake_watermark` | Data retention period. When it is configured, a data file is deleted when the latest `_timestamp` in the file plus the retention period has passed. This applies to both the hot tier and the cold tier. When it is not configured, no data is deleted because of its age |
| `retention.check_interval` | `AMBERLOG_RETENTION__CHECK_INTERVAL` | `1h` | Duration, greater than 0 | Interval at which expiry deletion runs. It has an effect only when `retention.period` is configured |

Deleted data leaves a deletion record in the audit records. The `retention` and `expires_at` columns of the dataset ledger show values based on `retention.period`.

### reconciliation (missed-collection check)

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `reconciliation.grace` | `AMBERLOG_RECONCILIATION__GRACE` | `5m` | Duration that converts to whole minutes, not less than `1m`, not greater than `24h` | Grace period. After a counting window ends, Amberlog compares the reported sent counts of the window with the confirmed counts, and gives the missed-collection verdict, only after the grace period; before that, the verdict is `pending`. Records and sent-count reports that arrive after the grace period count as late |

### alerts (alerting)

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `alerts.rules` | Cannot be set with an environment variable | None (zero rules) | TOML array of tables, at most 100 rules; see below | Alert rules |
| `alerts.max_concurrency` | `AMBERLOG_ALERTS__MAX_CONCURRENCY` | `1` | Integer, not less than 1 | Number of alert evaluations that run queries at the same time (alert slots) |
| `alerts.history_retention` | `AMBERLOG_ALERTS__HISTORY_RETENTION` | `30d` | Duration, not less than `1h`, not greater than `36500d` | Retention period of the evaluation records |
| `alerts.ca_file` | `AMBERLOG_ALERTS__CA_FILE` | None | File path. An empty string is treated as not configured | CA certificates (PEM) that Amberlog also trusts, in addition to the system root certificates, when it verifies the certificate of an `https://` webhook receiver. When you run `serve` and the file does not exist, cannot be read or has invalid contents, the process exits with exit code 2. This key and `object_store.s3.ca_file` are separate: neither one is used in place of the other |
| `alerts.missed_collection.enabled` | `AMBERLOG_ALERTS__MISSED_COLLECTION__ENABLED` | `false` | Boolean | Whether to enable the built-in alert rule `missed_collection`: it sends a notification when the missed-collection verdict of a window changes to `missing`. When it is true and `webhook_url` is not configured, the process exits with exit code 2 |
| `alerts.missed_collection.interval` | `AMBERLOG_ALERTS__MISSED_COLLECTION__INTERVAL` | `5m` | Duration that converts to whole minutes, not less than `1m`, not greater than `24h` | Evaluation interval of the built-in rule |
| `alerts.missed_collection.webhook_url` | `AMBERLOG_ALERTS__MISSED_COLLECTION__WEBHOOK_URL` | None | Same as `webhook_url` of an alert rule | Notification address of the built-in rule |
| `alerts.missed_collection.webhook_headers` | Cannot be set with an environment variable | None | Same as `webhook_headers` of an alert rule | Request headers sent with the notification requests of the built-in rule |

Alerts are evaluated only when you run `amberlog serve`. `catalog` and the offline subcommands still validate the rules, but do not evaluate them and do not send notifications.

Write each **alert rule** as `[[alerts.rules]]`. A rule can have only these fields:

| Field | Required | Type | Description |
|---|---|---|---|
| `name` | Yes | String | Matches `^[a-z0-9][a-z0-9_]{0,63}$`, does not start with `amberlog_`, and is not `missed_collection`. All rule names are different from each other (including the names of rules with `enabled = false`) |
| `interval` | Yes | String (duration) | Evaluation interval, whole minutes, `1m`–`24h`. Evaluations are aligned to whole multiples of `interval` in UTC time |
| `window` | No | String (duration) | The time range of each evaluation query is this length of time before the evaluation time. Whole minutes, `1m`–`7d`. When it is not configured, it equals `interval` |
| `org` | No | String | Can only be `default` (the current version has only this one organization) |
| `sql` | Yes | String | Query statement, written the same way as `sql` in `_search`. At startup, Amberlog checks the syntax and the table names. It does not check whether the datasets and columns exist |
| `condition` | Yes | Table | Row-count condition `{ rows = "> 0" }`, or column condition `{ column = "n", op = ">", threshold = 100 }`. The comparison operators are `>`, `>=`, `<`, `<=`, `=`, `!=` |
| `webhook_url` | Yes | String | Notification address. It starts with `https://` or `http://`, has a host name, and contains no user name and password and no fragment. Amberlog still sends to an `http://` address in plaintext, and writes one `webhook_plaintext` warning log line at startup (the address in the log has the query string removed) |
| `webhook_headers` | No | Table | Request headers sent with the notification requests, such as an authorization header. They cannot be `Host`, `Content-Type`, `Content-Length`, `Transfer-Encoding`, `Connection` or `User-Agent`. The values are not written to the log, the evaluation records or the metrics |
| `enabled` | No | Boolean | Default true. A rule with false is still validated, but is not evaluated |

When a rule does not meet the requirements above, the process exits with exit code 2. stderr names `alerts.rules` and the `name` of the rule with the error (when `name` itself has the problem, stderr gives the position of the rule in the array of tables, starting from 1).

### index_hints (index hints)

| Configuration key | Environment variable | Default | Range | Description |
|---|---|---|---|---|
| `index_hints` | Cannot be set with an environment variable | None | TOML table; see below | Builds equality bloom filters for the given columns of the given datasets, to speed up queries of the `column = value` kind |

Each key is a dataset name in the form `<org>/<stream>`. `<org>` can only be `default`, and `<stream>` matches `^[a-z0-9][a-z0-9_]{0,63}$`. Each value is an array of column-name strings. The elements are non-empty and different from each other, and the array can be empty. Amberlog does not report an error when a dataset or column does not exist yet.

```toml
[index_hints]
"default/app" = ["host", "trace_id"]
"default/nginx" = ["request_id"]
```

Index hints do not change query results. They change only how much data a query must read. They exist only in the configuration and are not written to object storage. When you retire with `amberlog retire`, the files that lake entry writes also get filters based on the index hints, so use the same configuration for retirement as for `serve`.

## 5. Constraints between configuration keys

The combinations below cause exit code 2, and stderr names the keys involved:

| Constraint | When violated |
|---|---|
| `query.default_size` is not greater than `query.max_size` | Names both keys |
| `lifecycle.lake_watermark` is greater than `lifecycle.seal_grace` | Names both keys |
| When `retention.period` is configured, it is greater than `lifecycle.lake_watermark` | Names both keys |
| `server.tls.cert_file` and `server.tls.key_file` are configured together | Names the one that is not configured |
| When a certificate is configured, `server.behind_tls_proxy` is not true | Names both keys |
| `object_store.s3.access_key_id` and `object_store.s3.secret_access_key` are configured together | Names the one that is not configured |
| When `object_store.s3.sse_kms_key_id` is configured, `object_store.s3.server_side_encryption` is `sse_kms` | Names both keys |
| When `alerts.missed_collection.enabled` is true, `alerts.missed_collection.webhook_url` is configured | Names `alerts.missed_collection` |

## 6. Configuration keys in effect for each mode

| Subcommand | Configuration keys in effect |
|---|---|
| `amberlog serve` | All |
| `amberlog catalog` (read-only catalog mode) | `server.http_addr`, `server.shutdown_timeout`, `server.tls.cert_file`, `server.tls.key_file`, `server.behind_tls_proxy`, `storage.prefix`, `object_store.url`, `object_store.s3.endpoint`, `object_store.s3.region`, `object_store.s3.access_key_id`, `object_store.s3.secret_access_key`, `object_store.s3.force_path_style`, `object_store.s3.ca_file`, `auth.enabled`, `auth.token`, `log.level`, `log.format` |
| `amberlog retire` | `storage.prefix`, `storage.commit_max_retries`, `storage.delete_delay`, `object_store.url`, all `object_store.s3.*` keys, `wal.dir`, `ingest.max_columns`, `flush.max_records`, `ingest.max_backlog_memory_bytes`, `compaction.target_file_bytes`, `index_hints`, `retention.period` (only written into the retirement record; expiry deletion does not run), `log.level`, `log.format` |
| `amberlog evidence export`, `amberlog evidence verify` | `storage.prefix`, `object_store.url`, the `object_store.s3.*` keys except the two server-side encryption keys, `log.level`, `log.format`. They do not create, read or write `wal.dir` |

The [S3 credential environment variables](#7-s3-credential-environment-variables) are in effect in every mode.

The other configuration keys are still validated, but have no effect. For example, when you run `retire`, a non-loopback `server.http_addr` does not make it fail.

## 7. S3 credential environment variables

When `object_store.s3.access_key_id` and `secret_access_key` are not configured, Amberlog gets S3 credentials only from the 13 environment variables below (and from the credential sources they point to). The variable names must be all uppercase.

| Purpose | Environment variables |
|---|---|
| Static keys and session token | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_TOKEN` |
| Web Identity | `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`, `AWS_ROLE_SESSION_NAME`, `AWS_ENDPOINT_URL_STS` |
| Container credentials | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` |
| Instance metadata | `AWS_METADATA_ENDPOINT`, `AWS_IMDSV1_FALLBACK` |

Environment variables that start with `AWS_` and are not among these 13 do not change Amberlog's behavior, for example `AWS_ENDPOINT_URL`, `AWS_REGION` and `AWS_SERVER_SIDE_ENCRYPTION`. Only the `object_store.s3.*` configuration keys set the endpoint, the region, conditional writes and server-side encryption.

When one of these 13 variables has an invalid value, the process exits with exit code 3.

## 8. Stable log events

The log lines of the 14 events below have the field `event`, whose value is the event name. Fields of these lines are only added, never removed or renamed, and `log.level` does not filter these lines. You can use them to build alerts. The text of other log lines can change between versions.

| Event | `level` | When it is written | Other fields |
|---|---|---|---|
| `starting` | `info` | The first log line after the configuration passes validation. It is not written when the configuration has an error (exit code 2) | `version` |
| `auth_disabled` | `warn` | `auth.enabled` is false; written after `starting` and before `ready` | |
| `plaintext_listen` | `warn` | Amberlog listens in plaintext on a non-loopback address (see the rules for plaintext listening) | `http_addr` |
| `object_store_plaintext` | `warn` | `object_store.s3.endpoint` starts with `http://` | `endpoint` (with user name, password and query string removed) |
| `webhook_plaintext` | `warn` | Once for each enabled rule whose `webhook_url` is `http://` | `rule`, `url` (with the query string removed) |
| `wal_stale` | `warn` | When the WAL is replayed at startup, the WAL of a dataset is stale and has stranded records | `dataset`, `recovered`, `discarded` |
| `ready` | `info` | The first time `GET /readyz` can respond with 200; once per process | `version`, `http_addr` (the address actually listened on), `object_store` (with user name, password and query string removed), `mode` (`serve` or `catalog`), `tls` (boolean) |
| `dataset_disabled` | `error` | A dataset is disabled because its format version is incompatible or its manifest cannot be read; once per dataset | `dataset`, `state` (`format_version` or `manifest_unreadable`) |
| `digest_mismatch` | `error` | During compaction, lake entry or cold compaction, the digest of a data file does not match the recorded digest | `dataset`, `tier` (`hot` or `cold`), `path` |
| `wal_backlog_high` | `warn` | See `wal.alert_bytes` | `wal_bytes`, `alert_bytes` |
| `lake_backlog_high` | `warn` | See `lifecycle.lake_alert_after` | `dataset`, `bucket`, `lag_seconds`, `alert_after_seconds` |
| `tls_reload_failed` | `error` | The certificate reload reads invalid contents; written only once for the same invalid contents | `cert_file`, `key_file` |
| `reconciliation_missing` | `warn` | The missed-collection check gives a window the verdict `missing`: the two counts differ, and the confirmed count is the lower one | `dataset`, `window_start`, `sources`, `missing` |
| `source_silent` | `warn` | The missed-collection check finds that a source has stopped reporting | `dataset`, `source`, `last_window_start` |

When `log.format` is `text`, these events are also written, but the line format can change between versions.

## 9. Exit codes

| Exit code | Meaning |
|---|---|
| 0 | Normal exit (including `--version`) |
| 1 | Other startup failure |
| 2 | Command-line usage error; the configuration file does not exist, cannot be read or is not valid TOML; configuration error (a required key is missing, an unknown key, an invalid value, a constraint between configuration keys, an invalid TLS certificate or CA file, an invalid alert rule or index hint); the object storage does not support conditional writes; the storage prefix has already been retired (`serve`) |
| 3 | `wal.dir` cannot be created or cannot be written to; object storage cannot be accessed; reading the WAL or existing data failed; an S3 credential environment variable has an invalid value |
| 4 | Amberlog cannot listen on `server.http_addr` |
| 5, 6 | Only for the offline subcommands: 5 means the command finished but found problems, 6 means the command did not finish. For the meaning in each subcommand, see sections 6.2, 6.3 and 7.2 of the [Operations guide](operations.md) |

At startup, Amberlog checks these steps in order: configuration, `wal.dir`, object storage, WAL replay, listening. The first step that fails sets the exit code.

## 10. Command line

```text
amberlog serve [--config <path>]
amberlog catalog [--config <path>]
amberlog retire [--config <path>]
amberlog evidence export [--config <path>] --dataset <org>/<stream> [--from <t>] [--to <t>] --output <path>
amberlog evidence verify [--config <path>] --evidence <path> [--previous <path>] [--retention <duration>]
amberlog --version
```

- `--config=<path>` and `--config <path>` are equivalent. When `--config` appears more than once or has no path, it is treated as a usage error.
- `amberlog --version` writes one line: `amberlog <version> (<commit>)`.
- When there is no subcommand, or the subcommand or an argument is unknown, the process writes the usage to stderr and exits with exit code 2.
- You can configure Amberlog only with the configuration file and environment variables. There are no command-line arguments for the individual configuration keys.

## 11. Examples

### 11.1 MinIO behind a reverse proxy that terminates TLS

```toml
# /etc/amberlog/amberlog.toml
[server]
http_addr = "0.0.0.0:8800"
behind_tls_proxy = true

[object_store]
url = "s3://amberlog-data/prod"

[object_store.s3]
endpoint = "https://minio.example.com:9000"
force_path_style = true
conditional_put = "create_only"

[wal]
dir = "/var/lib/amberlog/wal"

[retention]
period = "1095d"
```

You can also pass keys and tokens with environment variables. If you do, know this: other processes of the same user can see environment variables, and in a container `docker inspect` can see them too. For production deployment, we recommend that you put them in a configuration file that only the user that runs Amberlog can read; see sections 4.3 and 4.4 of the [Installation and deployment guide](install.md). With environment variables:

```sh
export AMBERLOG_AUTH__TOKEN='change-me-to-a-random-token'
export AMBERLOG_OBJECT_STORE__S3__ACCESS_KEY_ID='amberlog-app'
export AMBERLOG_OBJECT_STORE__S3__SECRET_ACCESS_KEY='example-secret-change-me'
amberlog serve --config /etc/amberlog/amberlog.toml
```

### 11.2 In-process TLS and alert rules

```toml
[server]
http_addr = "0.0.0.0:8443"

[server.tls]
cert_file = "/etc/amberlog/tls/server.crt"
key_file = "/etc/amberlog/tls/server.key"

[object_store]
url = "s3://amberlog-data"

[object_store.s3]
region = "us-west-2"
conditional_put = "create_only"
server_side_encryption = "sse_kms"

[wal]
dir = "/var/lib/amberlog/wal"

[alerts]
max_concurrency = 2

[[alerts.rules]]
name = "error_burst"
interval = "1m"
window = "5m"
sql = "SELECT count(*) AS n FROM app WHERE level = 'error'"
condition = { column = "n", op = ">", threshold = 100 }
webhook_url = "https://hooks.example.com/amberlog"
webhook_headers = { Authorization = "Bearer example-webhook-token" }

[alerts.missed_collection]
enabled = true
webhook_url = "https://hooks.example.com/amberlog-missed"
```

### 11.3 Local trial with a local directory

```sh
mkdir -p /tmp/amberlog-data
AMBERLOG_SERVER__HTTP_ADDR=127.0.0.1:8800 \
AMBERLOG_OBJECT_STORE__URL=file:///tmp/amberlog-data \
AMBERLOG_WAL__DIR=/tmp/amberlog-wal \
AMBERLOG_AUTH__TOKEN=local-trial-token \
amberlog serve
```

On a loopback address you do not need to configure a certificate. The directory that `file://` points to must already exist; otherwise the process exits with exit code 3.
