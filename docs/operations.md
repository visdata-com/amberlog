# Operations guide

[中文](operations.zh-CN.md)

> Applies to Amberlog 0.1.0.

This guide is for operators. It tells you how to check that Amberlog is healthy, which metrics and logs need alerts, what to do about a backlog, how to shut down and restart, how to export and verify audit evidence, and how to do a graceful retirement. Unless a section says otherwise, all commands and outputs in this guide were actually run in a test environment (MinIO, an S3-compatible store, on Linux).

For configuration keys, see the [Configuration reference](configuration.md). For the HTTP interface, see the [API reference](api.md). In the examples, `$AMBERLOG` is the address of Amberlog and `$TOKEN` is the value of `auth.token`.

## 1. A few terms first

- **Write instance**: a process that runs as `amberlog serve`. It accepts ingestion and queries. In the background it does commits, seal compaction, lake entry, hot-tier inspection and other work. Only one write instance can run on one storage (the same `object_store.url` and `storage.prefix`) at a time.
- **WAL** (write-ahead log): local files under `wal.dir`. An ingestion request is first written into the WAL and fsynced; only then does Amberlog return 200. Later, the background work **commits** the records to object storage.
- **Hot tier** and **cold tier**: records are first kept in the hot tier, in Amberlog's own format. They are grouped into hour buckets. `lifecycle.lake_watermark` (default 72 hours) after an hour bucket ends, the bucket **enters the lake**: it becomes part of a standard Apache Iceberg table in the cold tier, which external engines can read directly.
- **Offline subcommands**: `amberlog retire`, `amberlog evidence export` and `amberlog evidence verify`. They do not listen on a port. Each one does one job and then exits.

## 2. Health checks

| Endpoint | Token required | Returns 200 when | Use it as |
|---|---|---|---|
| `GET /healthz` | No | The process can handle HTTP requests | Liveness probe. When it fails, restart the process |
| `GET /readyz` | No | `wal.dir` is writable; the latest object-storage probe succeeded; the process is not shutting down | Readiness probe. When it fails, stop sending traffic to the process |

The object-storage probe runs at least once every 30 seconds. It writes `<storage.prefix>/_system/readyz-probe` and reads it back.

When the process is not ready, `/readyz` returns 503, and `details.checks` shows which check failed:

```sh
curl -sS "$AMBERLOG/readyz"
```

```json
{"code":503,"reason":"not_ready","message":"not ready","details":{"checks":{"wal":false,"object_store":true,"shutting_down":false}}}
```

(We got this example by making `wal.dir` unwritable while the process was running.)

- `wal` is false: `wal.dir` is not writable, for example because the disk is full or the permissions were changed. Ingestion requests return 503 when a WAL write or fsync actually fails. While the WAL files that are already open can still be written, ingestion requests return 200 as usual.
- `object_store` is false: the object-storage probe failed, for example because the network is down, the credentials are no longer valid, or the storage service is down.
- `shutting_down` is true: the process received SIGTERM or SIGINT and is shutting down.

**When `/readyz` returns 503, `/api/` requests are not refused because of it.** Each request is answered by its own rules. For example, when object storage is unavailable, ingestion still writes into the WAL and returns 200, and the records are committed after object storage recovers. Queries that must read object storage return 503 `unavailable`.

You can configure Kubernetes probes like this:

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 8800 }
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /readyz, port: 8800 }
  periodSeconds: 10
  failureThreshold: 3
```

When in-process TLS is enabled, add `scheme: HTTPS`.

## 3. Metrics and alerts

### 3.1 Scraping

`GET /metrics` returns the Prometheus text format and requires the token. Prometheus scrape configuration:

```yaml
scrape_configs:
  - job_name: amberlog
    metrics_path: /metrics
    authorization:
      credentials_file: /etc/prometheus/amberlog-token   # the file contains the value of auth.token
    static_configs:
      - targets: ["amberlog.example.com:8800"]
```

When TLS is enabled, add `scheme: https`, and configure `tls_config.ca_file` if necessary.

```sh
curl -sS "$AMBERLOG/metrics" -H "Authorization: Bearer $TOKEN" | grep -v '^#'
```

```text
amberlog_ingest_records_total{org="default",stream="app",outcome="accepted"} 301
amberlog_ingest_records_total{org="default",stream="app",outcome="rejected"} 0
amberlog_manifest_commits_total{org="default",stream="app",result="ok"} 1
amberlog_manifest_commits_total{org="default",stream="app",result="failed"} 0
amberlog_ready 1
amberlog_wal_bytes 23105
amberlog_backlog_memory_bytes 21986
amberlog_lake_lag_seconds{org="default",stream="app"} 0
...
```

The full list of stable metrics is in [section 8.3 of the API reference](api.md#83-get-metrics). Only stable metrics are promised not to be renamed or removed. Use only stable metrics in alert rules.

### 3.2 Recommended alerts

The table gives the expressions in Prometheus syntax. Adjust the thresholds to your own environment.

| Alert | Expression | Meaning and what to do |
|---|---|---|
| Not ready | `amberlog_ready == 0` for 2 minutes | Check `details.checks` of `/readyz`; see section 2 |
| Commit failed | `increase(amberlog_manifest_commits_total{result="failed"}[10m]) > 0` | Records were written into the WAL but cannot be committed to object storage. While object storage is unavailable, each dataset that has records waiting for commit counts one failure per `flush.interval`. See section 4.1 |
| WAL backlog | `amberlog_wal_bytes > <value of wal.alert_bytes>` | A `wal_backlog_high` log event also appears. See section 4.1 |
| Memory backlog | `amberlog_backlog_memory_bytes` is close to `ingest.max_backlog_memory_bytes` | There are too many records waiting for commit to keep in memory; the excess stays only in the WAL. This means that commits cannot keep up or have stopped |
| Lake-entry backlog | `amberlog_lake_lag_seconds > 86400` | A `lake_backlog_high` log event also appears. See section 4.2 |
| Dataset disabled | `amberlog_dataset_disabled == 1` | A `dataset_disabled` log event also appears. See section 4.3 |
| Query refused | `increase(amberlog_search_rejected_total[5m]) > 0` | `reason="too_many_queries"`: the concurrent queries exceed `query.max_concurrency`; `reason="query_memory_exceeded"`: a single query exceeds `query.max_memory_bytes` |
| Records rejected one by one | `increase(amberlog_ingest_records_total{outcome="rejected"}[10m]) > 0` | The collector sends records that do not follow the rules. The reasons for rejection are in `errors` in the ingestion response; see [section 2.4 of the API reference](api.md#24-per-record-rejection) |
| Possible missed collection | `amberlog_reconciliation_windows{verdict="missing"} > 0` | In the last 24 hours, a minute window has a confirmed count lower than the sent count that the collector reported. A `reconciliation_missing` log event also appears. Query the system table `_reconciliation`; see [section 6.2 of the API reference](api.md#62-_reconciliation-reconciliation) |
| Reporting stopped | `amberlog_reconciliation_windows{verdict="silent"} > 0` | A collection source stopped reporting sent counts. A `source_silent` log event also appears |
| Alert notification failed | `increase(amberlog_alert_deliveries_total{result="abandoned"}[1h]) > 0` | An Amberlog alert rule fired, but the webhook could not be delivered. Check `delivery` and `last_http_status` in `_alert_evaluations` |

The missed-collection check and the built-in alert rule `missed_collection` work only for sources that report sent counts with `basis` set to `timestamp` and send `X-Amberlog-Source` when they ingest. See [section 3 of the API reference](api.md#3-sent-count-reporting-_sent).

### 3.3 Alerts on logs

The runtime log goes to stderr. When `log.format` is `json` (the default), each line is one JSON object. The 14 **stable events** have an `event` field. Their field names do not change, and `log.level` does not filter them. You can build alerts on `event` in your log system. The full list is in [section 8 of the Configuration reference](configuration.md#8-stable-log-events).

Set alerts on at least these events:

| Event | `level` | Meaning |
|---|---|---|
| `wal_backlog_high` | `warn` | The WAL backlog exceeds `wal.alert_bytes` |
| `lake_backlog_high` | `warn` | The lake-entry backlog exceeds `lifecycle.lake_alert_after` |
| `dataset_disabled` | `error` | A dataset is disabled |
| `digest_mismatch` | `error` | The content of a data file does not match the recorded digest. The file may have been changed from outside |
| `tls_reload_failed` | `error` | The certificate file was replaced with invalid content. The process still uses the old certificate |
| `reconciliation_missing` | `warn` | The missed-collection check found a minute window where the two counts differ: the confirmed count is lower than the sent count that the collector reported |
| `source_silent` | `warn` | A source stopped reporting |
| `auth_disabled`, `plaintext_listen`, `object_store_plaintext`, `webhook_plaintext` | `warn` | Security notices at startup: authentication is off, the listener uses plaintext, object storage or the webhook uses plaintext |

When Amberlog answers a request with 5xx, it writes a log line with `level` `error`. The line contains the `x-request-id` of that response and the cause of the error. When a user reports an error, ask for the `x-request-id` from the response headers and use it to search the log.

## 4. Handling backlogs and faults

### 4.1 WAL backlog and commit failures

Ingested records go into the WAL first. Then the background work commits them to object storage. When commits cannot keep up or stop, the WAL grows. The event from a test run:

```json
{"ts":"2026-10-21T06:44:07.914513Z","level":"warn","msg":"wal.dir has grown past wal.alert_bytes: confirmed records are piling up locally","event":"wal_backlog_high","wal_bytes":23499,"alert_bytes":2000}
```

(The example set `wal.alert_bytes` to 2000 bytes. The default is 8 GiB.)

Check in this order:

1. Check whether `amberlog_ready` and `object_store` of `/readyz` are false. If they are, object storage is unavailable: check the network, the storage service, the credentials and the certificates. After object storage recovers, the backlog records are committed automatically. No manual action is necessary.
2. Check whether `amberlog_manifest_commits_total{result="failed"}` is increasing.
3. Check for `dataset_disabled` events. The records of a disabled dataset stay in the WAL and are not committed; see section 4.3.
4. Check whether the ingestion volume exceeds the commit capacity. When ingestion stays above the commit capacity, the WAL keeps growing.

While you handle the problem:

- **Do not delete files in `wal.dir`.** They hold records that were already acknowledged with 200 but not yet committed. If you delete them, the records are lost.
- Keep free space on the disk of `wal.dir`. When the disk is full, `/readyz` returns 503 and ingestion requests return 503.
- The records waiting for commit that are kept in memory do not exceed `ingest.max_backlog_memory_bytes`. The excess stays only in the WAL and is committed later as usual. So a long object-storage outage does not use up memory, but it does use disk space.

### 4.2 Lake-entry backlog

An hour bucket must enter the lake `lifecycle.lake_watermark` after it ends. `amberlog_lake_lag_seconds` is the number of seconds by which the oldest hour bucket that is due but has not entered the lake is past its lake-entry time. When it exceeds `lifecycle.lake_alert_after` (default 24 hours), this event appears:

```json
{"ts":"2026-10-21T06:44:09.914658Z","level":"warn","msg":"hour buckets past the lake watermark have not entered the cold tier for longer than lifecycle.lake_alert_after","event":"lake_backlog_high","dataset":"default/app","bucket":"2026102008","lag_seconds":74649,"alert_after_seconds":3600}
```

(The example set `lifecycle.lake_watermark` to `1h` and `lifecycle.lake_alert_after` to `1h`, and then ingested data from the previous day, so there was a backlog right after startup. Then this hour bucket entered the lake and the backlog went away.)

`bucket` is the UTC hour of the hour bucket, as `YYYYMMDDHH`. Use the ledger to see the state of each dataset:

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" \
  -d '{"query":{"sql":"SELECT dataset, tier, pending_lake_record_count FROM _ledger","start_time":0,"end_time":1}}'
```

```json
{"took":27,"hits":[{"dataset":"default/app","tier":"hot_cold","pending_lake_record_count":0}],"total":1,"from":0,"size":100,"scan_records":0,"scan_files":0,"scan_bytes":0}
```

When `tier` is `hot_pending_lake` and `pending_lake_record_count` is greater than 0, some data is due to enter the lake but has not entered it. Common causes:

- Object storage is unavailable. Lake entry continues automatically after it recovers.
- Before lake entry, Amberlog checks the digests of the data files and finds a mismatch (a `digest_mismatch` event appears). This hour bucket stops entering the lake. The data stays in the hot tier and can be searched as usual. The data files may have been changed from outside; see section 4.4.
- The digests match, but rewriting this hour bucket into cold-tier data files fails. Amberlog writes a log line with `level` `warn` that contains `dataset`, `bucket` and the cause of the failure, `reason`. It also adds 1 to the non-stable metric `amberlog_lake_rewrite_failures_total{org,stream}`. This process no longer attempts lake entry for this hour bucket. The data stays in the hot tier and can be searched as usual. Later hour buckets of the same dataset enter the lake as usual. After a process restart, Amberlog tries once more. This log line is not a stable event, and its text can change between versions. For alerts, use `amberlog_lake_lag_seconds`. (This case was not tested: a release build cannot produce a rewrite failure.)

### 4.3 Disabled datasets

Amberlog disables a dataset when it reads a storage format version of that dataset that the current program does not support, or when it cannot understand the manifest of the dataset:

- It writes a `dataset_disabled` event with `state` `format_version` or `manifest_unreadable`. `amberlog_dataset_disabled` is 1. `status` in `_ledger` is `disabled_format_version` or `disabled_manifest_unreadable`.
- Ingestion and queries for that dataset return 503 `unsupported_format_version` when the format version is not compatible, and 500 `internal` when the manifest is unreadable. Other datasets are not affected.
- Records that were acknowledged but not yet committed stay in the WAL and are not lost.

What to do:

- `format_version`: the data was written by a newer version of Amberlog, or it is in an old format written by an early version. Use a version that can read this format.
- `manifest_unreadable`: the manifest was changed from outside or is damaged. Do not change the files in object storage yourself. Keep everything as it is and report the problem.

### 4.4 Data changed from outside

Amberlog assumes that it is the only writer of the objects under `<storage.prefix>/`. When someone changes or deletes these objects without going through Amberlog (for example with the storage management tools, or by changing the cold-tier tables with an external engine):

- When compaction, lake entry or cold compaction finds that a data file does not match the recorded digest, Amberlog writes a `digest_mismatch` event. That file no longer takes part in compaction or lake entry.
- Audit evidence export and verification report the inconsistency; see section 6.

Recommendations:

- Except for Amberlog's own credentials, do not give any person or program write access to `<storage.prefix>/`. Give external engines read-only access to the cold-tier prefix `<storage.prefix>/_iceberg/` only.
- Turn on versioning for the object-storage bucket. Then you can recover the original objects after an accidental deletion or change.
- While a write instance manages this storage, do not use an external engine for maintenance on the cold-tier tables, such as expiring snapshots, rewriting data files or removing orphans. Amberlog does this work itself.

## 5. Shutdown, restart and upgrade

### 5.1 Shutdown

Send SIGTERM (or SIGINT) to the process. The process then:

1. Stops accepting new `/api/` requests. New requests return 503 `unavailable`, and `/readyz` returns 503.
2. Waits for in-flight requests to complete, for at most `server.shutdown_timeout` (default 10 seconds).
3. Commits the records in memory to object storage and writes out the counts.
4. Exits with exit code 0.

When the commit fails (for example because object storage is unavailable at that moment), the records stay in the WAL. They are replayed at the next startup and are not lost. If you send a second signal during shutdown, the process exits immediately with a non-zero exit code. The WAL is kept in this case too.

In a `systemd` unit, keep the default `KillSignal=SIGTERM`, and set `TimeoutStopSec` longer than `server.shutdown_timeout` to leave time for the commit, for example 60 seconds.

### 5.2 Restart

Restart with **the same `wal.dir`**. At startup, Amberlog first replays the WAL. When `/readyz` first returns 200, the replay is complete and all acknowledged records can be searched.

- After a restart that follows a forced kill (SIGKILL) or a host crash, acknowledged records are not lost and not duplicated.
- If you start with a different `wal.dir`, the records in the original WAL that were not yet committed do not come in. Do not do this unless you have confirmed that the original WAL is empty.
- When startup fails, look at the exit code: 2 is a configuration error; 3 means that `wal.dir` or object storage is unavailable (including a damaged WAL); 4 means that the port cannot be listened on. For details, see [section 9 of the Configuration reference](configuration.md#9-exit-codes).

### 5.3 Upgrade

Patch releases (for example 0.1.0 → 0.1.1) do not change the data format. You can upgrade and downgrade between them freely. For a minor release that changes the data format, the changelog has upgrade notes: you must upgrade one minor release at a time, without skipping any, and you cannot downgrade. The cold-tier tables always stay readable. See section 6.2 of [Known limitations and roadmap](known-limitations.md).

1. Read the changelog of the new version. Check for incompatible changes in configuration keys, interfaces and the data format, and check for upgrade notes.
2. Stop the old process as described in section 5.1, and wait until it exits with exit code 0.
3. Start the new version with the same configuration and the same `wal.dir`.
4. Wait until `/readyz` returns 200, and check for `dataset_disabled` events.

## 6. Audit evidence: export and verification

Each Amberlog commit records the file digests and the links between the previous and the next snapshot. Deletions also leave a record (a deletion record). Evidence export "pins" this chain of a dataset at one moment and packs it into an **evidence package**. Evidence verification checks it at any later time: whether the data now in object storage is an extension of that chain, and whether it was changed or deleted early.

Both jobs use the same executable. They do not need to connect to a running Amberlog, and they do not write to object storage. So you can run them while the write instance is running, and an auditor can also run them on their own machine with read-only credentials.

### 6.1 Credentials and configuration

- The only required configuration key is `object_store.url`. `object_store.url` and `storage.prefix` must be the same as for the write instance. The S3 endpoint, region, certificate and other configuration keys work as usual. `wal.dir` and `auth.token` are not needed.
- The credentials must be able to read and list all of `<storage.prefix>/`, including `_system/` and `_iceberg/`. Credentials for only some prefixes are not enough. For an object-storage bucket with SSE-KMS, the credentials also need decrypt permission.
- Issue the audit credentials separately from the cold-tier read-only credentials for external engines.

### 6.2 Exporting evidence

```text
amberlog evidence export [--config <path>] --dataset <org>/<stream> [--from <time>] [--to <time>] --output <evidence package path>
```

| Argument | Description |
|---|---|
| `--dataset` | The dataset, for example `default/app` |
| `--from`, `--to` | The time range, on a UTC hour, written as `YYYY-MM-DDTHH:00:00Z`. The range includes the start and excludes the end. Give both or neither; with neither, the range is all time |
| `--output` | The path of the evidence package. It must not exist yet. Its directory must exist and be writable. With local disk storage, it cannot be inside the storage directory |

```sh
amberlog evidence export --config /etc/amberlog/amberlog.toml \
  --dataset default/app --output app-full.evidence.tar > app-full.receipt.json
echo $?
```

```text
0
```

stdout carries the **receipt**: one line of JSON that records the chain head pinned at export.

```json
{"evidence_format_version":1,"dataset":"default/app","exported_at":1792565051708062,"amberlog_version":"0.1.0",
 "snapshot":{"snapshot_id":3,"path":"manifest/00000000000000000003.json","content_digest":"sha256:847a33e5e386e49ff318f96a39692d63e4da03af0f0f2cdfec79cf229746d714"},
 "retired_segment":null,"cold_deletion_segment":null,
 "cold":{"metadata_version":1,"path":"metadata/v1.metadata.json","content_digest":"sha256:2712c64af9ad5cc519cd00f67457cc7a33b7a6e5aefba5bbd077607350708483"}}
```

(The lines are wrapped for readability. The actual output is one line.)

To export only one time range:

```sh
amberlog evidence export --config /etc/amberlog/amberlog.toml --dataset default/app \
  --from 2026-10-20T08:00:00Z --to 2026-10-20T09:00:00Z \
  --output app-2026102008.evidence.tar > app-2026102008.receipt.json
```

The **evidence package** is an uncompressed tar file. You can unpack it with `tar` and look inside:

| Member | Contents |
|---|---|
| `evidence.json` | The dataset, the time range, the export time, the program version, the receipt, the mapping to Iceberg snapshots, and the retirement record |
| `receipt.json` | The receipt, byte for byte the same as the line on stdout |
| `files.jsonl` | One line per data file in the time range: tier (`hot`, `cold`), path, state (`live` or `deleted`), digest, record count, byte count and time range. A deleted file also has the reason for deletion, the cutoff time and the deletion time |
| `check.json` | The result of the check at export: `result` is `ok` or `problems_found` |
| `objects/` | Unchanged copies of the metadata involved: snapshots, all manifest pieces that make up the manifest of each hour bucket, trail segments, cold-tier metadata, and the retirement record |
| `SHA256SUMS` | The SHA-256 of every other member. To check them, run `sha256sum -c SHA256SUMS` in the unpacked directory |

```sh
mkdir x && tar -xf app-full.evidence.tar -C x && cd x && sha256sum -c SHA256SUMS | tail -2 && cat check.json
```

```text
objects/default/app/manifest/buckets/2026102106-01m5ek7qqk8z5hcz7awgpy50k0.json: OK
receipt.json: OK
{"result":"ok","problems":[],"transitions":[],"notes":[]}
```

The evidence package does not contain the data files themselves. But it contains the manifest pieces, and these hold statistics fragments of the values of each column. **Keep evidence packages as sensitive material.**

Exit codes of export:

| Exit code | Meaning |
|---|---|
| 0 | The evidence package is complete, and the check at export found no problems |
| 5 | The evidence package is complete, and `check.json` records problems |
| 2 | Wrong arguments or configuration; `--output` already exists; the dataset does not exist |
| 3 | Object storage could not be read at startup |
| 6 | Not completed: object storage became unavailable during export; the chain moved forward during export so that pinned objects were already recycled (run it again); an abort signal was received; or the data is in an old format from an early version. No evidence package is left behind |

When the exit code is not 0 or 5, no file appears at `--output`, and you can run the export again with the same path.

### 6.3 Verifying evidence

```text
amberlog evidence verify [--config <path>] --evidence <evidence package> [--previous <old receipt or old evidence package>] [--retention <duration>]
```

| Argument | Description |
|---|---|
| `--evidence` | The evidence package to verify |
| `--previous` | A receipt or evidence package saved earlier. If you give it, verification also proves that the current chain is an extension of that earlier one |
| `--retention` | The agreed retention period, for example `1095d`. If you give it, verification also checks that deletion at expiry did not happen earlier than agreed |

```sh
amberlog evidence verify --config /etc/amberlog/amberlog.toml \
  --evidence app-full.evidence.tar --retention 1095d > app-full.verify.json
echo $?
```

```text
0
```

stdout carries a one-line JSON report (wrapped below; `...` marks fields left out):

```json
{"result":"pass","dataset":"default/app","verified_at":1792565052012298,"evidence_exported_at":1792565051708062,
 "anchors":[{"source":"evidence","kind":"package","snapshot_id":3,"exported_at":1792565051708062}],
 "current":{"snapshot":{"snapshot_id":3,...},"retired_segment":null,"cold_deletion_segment":null,"cold":{"metadata_version":1,...}},
 "retention_checked":true,"checked":{"data_files":3,"digests_recomputed":2},
 "problems":[],"unprovable":[],
 "transitions":[{"kind":"traced_still_present","object":"default/app/data/2026/10/20/08/01m5ek7qq6jg62cfz4ptmxaawm.parquet","detail":"the deleted file is still in the object store (delayed physical deletion)"}],
 "notes":[]}
```

| Member | Description |
|---|---|
| `result` | `pass` (passed), `inconsistent` (an inconsistency was found) or `unprovable` (cannot be proved) |
| `anchors` | The receipts used as starting points: the one in the evidence package, and the one given with `--previous` |
| `current` | The chain head at verification time |
| `checked` | The number of data files checked, and the number of data files that were read again and had their digests recomputed |
| `problems` | Inconsistencies. When there are any, `result` is `inconsistent` |
| `unprovable` | Items that cannot be proved, for example a file that cannot be read, or a snapshot recorded in the old receipt that was recycled after its retention ran out |
| `transitions` | Normal intermediate states. They do not affect the result. In the example above, `traced_still_present` means: the file has entered the lake and has a deletion record, and the original file is still waiting for the delayed physical deletion (`storage.delete_delay`) |
| `notes` | Notices. They do not affect the result |

Verification reads all bytes of every live data file in the range and recomputes the digest. It does not sample. A verification over all time reads about as much as the total data volume. Plan for the time and the traffic.

Exit codes of verification:

| Exit code | Meaning |
|---|---|
| 0 | Passed |
| 5 | An inconsistency was found; see `problems` |
| 6 | Cannot be proved; see `unprovable`. Or an abort signal was received during verification (there is no report in this case) |
| 2 | Wrong arguments, files or configuration; `--evidence` is not an evidence package; `--previous` and `--evidence` are not for the same dataset |
| 3 | Object storage could not be read at startup |

When `unprovable` contains only `snapshot_anchor_recycled` and `problems` is empty, this is normal: the snapshot recorded in the old receipt passed the snapshot retention (`hot_retention.snapshots` and `hot_retention.age`) and was recycled. The trail segments and the cold-tier versions were checked as usual. When `unavailable` or `data_unreadable` appears, check the credentials, the network and the storage, and then run verification again. When `format_unsupported` appears, this version of the program does not know the format version of the evidence package, of the old receipt, or of the dataset at this moment. Verification stops there, and no other item is checked. Verify with a version that matches this format.

### 6.4 Recommended audit procedure

**Operator:**

1. Export an evidence package for each dataset on the agreed schedule (for example every month).
2. Give the evidence packages and receipts to the auditor.

**Auditor:**

1. Store the receipts in a place that the operator cannot change.
2. Keep the whole evidence package, not only the receipt.
3. Verify: `amberlog evidence verify --evidence <new evidence package> --previous <receipt or evidence package saved last time> --retention <agreed retention period>`.
4. Read the report by `result` and by each array.

When you verify with `--previous`, a rewrite of the whole chain after that earlier export can also be detected. A test run: after the first evidence package was exported, a further batch of data was ingested, and a second evidence package was exported. Then the second package was verified with the first receipt (some fields are left out of the report):

```sh
amberlog evidence verify --config /etc/amberlog/amberlog.toml \
  --evidence app-full-2.evidence.tar --previous app-full.receipt.json
```

```json
{"result":"pass",...,"anchors":[{"source":"evidence","kind":"package","snapshot_id":4,...},{"source":"previous","kind":"receipt","snapshot_id":3,...}],...}
```

**What the evidence can and cannot prove:**

- It can prove: the data described in the evidence package is consistent with the chain in object storage at export time; the chain at verification time is an extension of the chain head recorded in the old receipt; the content of live data files has not changed; every data file deleted at expiry held only records older than its cutoff time (and, when `--retention` is given, each cutoff time is no later than the deletion time minus the agreed retention period).
- It cannot prove: if someone with write access to the storage rewrites the whole chain from the start, and this happens before the auditor saves the first receipt, it cannot be detected. So the auditor must save receipts early and regularly.
- It cannot prove, with only a receipt: someone with write access writes a valid new hot-tier snapshot that drops a live data file without a deletion record. Verification with the earlier evidence package that lists this file detects it (`vanished`). This is why the auditor keeps the whole evidence package.
- It is not a second, independent implementation: the verification tool is the same program that writes the data. "Independent" here means only that verification does not depend on a running Amberlog instance. Without Amberlog, you can check part of the package by hand: run `sha256sum -c SHA256SUMS` in the unpacked directory, and compare the digests and sizes of the data files with `files.jsonl`. These manual checks cover only some of the items that verification checks.
- It does not cover: records that were acknowledged but not yet committed to object storage (they are in the WAL); old snapshots that snapshot recycling has already deleted; whether record counts stay the same through lake entry and seal compaction; whether snapshot recycling deleted too much; count records; the retirement record, which is not on the digest chain (verification only compares it with the copy in the evidence package, and reports a difference as a note).

## 7. Graceful retirement

When you stop using Amberlog, use `amberlog retire`. It moves all data of the hot tier into the Iceberg tables of the cold tier, finishes the remaining work, and at the end writes the **retirement record** `<storage.prefix>/_system/retirement.json`. After that, standard Iceberg tables hold all the data. DuckDB, Apache Spark and other engines find the tables by the in-bucket convention, without a running Amberlog. Trino finds them through Amberlog's read-only catalog process, which only reads object storage (section 3 of the [External engines guide](external-engines.md)).

### 7.1 Before retirement

Do these steps in order:

1. Send SIGTERM to the write instance, and wait until it exits with exit code 0.
2. Make sure that no other write instance is running on this storage. The read-only catalog (`amberlog catalog`) and the evidence subcommands do not count.
3. Prepare **the same configuration** that `serve` used (the configuration file and all `AMBERLOG_` environment variables), including the same `wal.dir`. `retire` replays this WAL and also moves the records in it that were not yet committed into the cold tier.
4. Make sure that the object-storage credentials can read, write, list and delete the objects under `<storage.prefix>/`.

The only required configuration keys for `retire` are `object_store.url`, `wal.dir`, and, with S3, `object_store.s3.conditional_put`. `auth.token` is not needed.

### 7.2 Running retirement

```sh
AMBERLOG_STORAGE__DELETE_DELAY=1s amberlog retire --config /etc/amberlog/amberlog.toml > retire-report.json
echo $?
```

```text
0
```

`retire` does these steps in order: it replays the WAL and commits; it makes every hour bucket that still has data enter the lake (whether or not its lake-entry time has come); it physically deletes the hot-tier files that have entered the lake; after all of this is complete, it writes the retirement record. Before the physical deletion it waits for `storage.delete_delay` (default 1 hour). There are no in-flight queries during retirement, so you can set this value small for this run only, as above. The value must be greater than 0.

stdout carries a one-line report:

```json
{"status":"retired","interrupted":false,"record":"amberlog/_system/retirement.json",
 "datasets":[{"dataset":"default/app","settled":true,"snapshot_id":5,"cold_metadata_version":2,"buckets_laked":1,"files_deleted":3}],
 "exceptions":[]}
```

| Field | Description |
|---|---|
| `status` | `retired` (this run wrote the retirement record), `already_retired` (retired before), `exceptions` (there are exceptions that a person must handle), `incomplete` (not completed) |
| `interrupted` | Whether SIGTERM or SIGINT was received |
| `record` | The location of the retirement record |
| `datasets` | For each dataset: whether it is settled (`settled`), the last hot-tier snapshot and cold-tier version, the number of hour buckets that entered the lake in this run, and the number of files deleted in this run |
| `exceptions` | For each exception: the dataset, the kind (`kind`), the hour bucket and the objects involved |

| Exit code | Meaning | Next step |
|---|---|---|
| 0 | `retired` or `already_retired` | Do section 7.4 |
| 5 | `exceptions` | Handle the exceptions as described in the table below, then run it again |
| 6 | `incomplete`: an abort signal was received, or object storage became unavailable during the run | Remove the cause, then run it again |
| 2 | Configuration or usage error (no report) | Correct it, then run it again |
| 3 | At startup, `wal.dir` or object storage was unavailable, or the WAL is damaged (no report) | Repair it manually, then run it again |

| Exception `kind` | Meaning | What to do |
|---|---|---|
| `digest_mismatch` | The content of a data file of an hour bucket does not match the recorded digest. This hour bucket did not enter the lake | The data file was changed. Restore the original file from a backup or from the storage versioning, then run it again |
| `input_missing` | A data file of an hour bucket is missing. This hour bucket did not enter the lake | Restore the files listed in `paths` to their original locations, then run it again |
| `dataset_disabled` | The dataset is disabled (see section 4.3) | Use a version that can read this format, then run it again |
| `unsettled` | All steps were done, but the dataset is still not settled | Check the objects in `paths`, then run it again |
| `store_unavailable`, `delete_failed`, `record_write_failed` | Object storage is unavailable, or there is no delete permission | Restore access and add the missing permission, then run it again |

When there is any exception, `retire` does not write the retirement record. There is no option to count a run with exceptions as complete.

**You can run `retire` again after it stops part-way or crashes.** `retire` does not write a progress file. For each step, it decides from the current state of object storage and the WAL, so steps that are already done are skipped. After a repeated run completes, each acknowledged record appears exactly once in the cold tier. Until the retirement record is written, `serve` can still start as usual. So if you give up retirement part-way, you can resume service directly.

### 7.3 After retirement

- If you run `retire` again, it returns `already_retired` with exit code 0.
- `serve` refuses to start on retired storage and exits with exit code 2:

```text
{"ts":"...","level":"error","msg":"the storage prefix has been retired: amberlog/_system/retirement.json exists (delete it only to revive the prefix on purpose)","exit_code":2}
```

- There are two ways to read the data:
  - DuckDB, Spark and other engines that support the in-bucket convention read the cold-tier tables directly. No Amberlog process is necessary. The tables are in `<storage.prefix>/_iceberg/<org>/<stream>/`, and `metadata/version-hint.text` already contains the current version number.
  - Run the read-only catalog `amberlog catalog`. External engines find the tables through its Iceberg REST endpoint `/iceberg/`; Trino can use only this way. It needs only read access to the cold-tier prefix, and it runs as usual after retirement.

```sh
curl -sS "$AMBERLOG/iceberg/v1/amberlog/namespaces/default/tables" -H "Authorization: Bearer $TOKEN"
```

```json
{"identifiers":[{"namespace":["default"],"name":"app"}]}
```

- After retirement, the object-storage bucket still holds metadata such as hot-tier snapshots, manifests, deletion records and count records, and possibly orphan files. Amberlog no longer manages them. You decide what to do with them.
- After retirement, Amberlog no longer does deletion at expiry. You become responsible for deleting expired data, and no deletion records are left any more.

### 7.4 Export evidence right after retirement

The retirement record itself is not evidence. After `retire` exits with exit code 0, immediately do one export and one verification over all time for each dataset. Archive the receipt, the evidence package and the verification report together, as the evidence for the moment of retirement:

```sh
amberlog evidence export --config /etc/amberlog/amberlog.toml --dataset default/app \
  --output app-retired.evidence.tar > app-retired.receipt.json
amberlog evidence verify --config /etc/amberlog/amberlog.toml \
  --evidence app-retired.evidence.tar --previous app-full.receipt.json > app-retired.verify.json
```

In a test run, both commands ended with exit code 0, and the verification result was `pass`. The `evidence.json` of the evidence package contains the retirement record.

After retirement, any change to the cold-tier tables (including deleting expired partitions, expiring snapshots or rewriting files with an external engine) makes later verifications with this evidence report inconsistencies. Verification cannot tell whether this was your maintenance or an unauthorized change. So complete this step before any maintenance.

## 8. Known behavior

This section lists only the behavior that you can meet during operations. For all known limitations of this version, see [Known limitations and roadmap](known-limitations.md).

- **When the directory of local disk storage disappears while the process runs, `/readyz` does not report not ready.** With `file://` storage, if the storage directory is moved away or its volume is unmounted while the process runs, Amberlog creates the directory again and `/readyz` still returns 200. After that, commits fail and queries return 500. Use the `amberlog_manifest_commits_total{result="failed"}` alert (section 3.2) to detect this case, and protect the storage volume with a mount check. This will be fixed in the next stage (section 7.1 of [Known limitations and roadmap](known-limitations.md)). S3 storage is not affected.
