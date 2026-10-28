# API reference

[中文](api.zh-CN.md)

> Applies to Amberlog 0.1.0.

This document describes the HTTP interface of Amberlog: ingestion, search, the field list, system tables, the error structure, authentication and the operational endpoints. Every example was actually run in a test environment. The responses shown are the actual responses, except for values that change every time, such as `took`, `scan_*` and `x-request-id`.

For configuration keys, see the [Configuration reference](configuration.md).

## 1. Overview

### 1.1 Endpoints

All endpoints are on the same address, the one that `server.http_addr` gives.

| Method | Path | Token required | Purpose |
|---|---|---|---|
| `POST` | `/api/{org}/{stream}/_json` | Yes | Write records ([section 2](#2-ingestion-_json)) |
| `POST` | `/api/{org}/{stream}/_sent` | Yes | Report the sent counts of a collector ([section 3](#3-sent-count-reporting-_sent)) |
| `POST` | `/api/{org}/_search` | Yes | SQL search; also used to query system tables ([section 4](#4-search-_search), [section 6](#6-system-tables)) |
| `GET` | `/api/{org}/{stream}/_fields` | Yes | Field list ([section 5](#5-field-list-_fields)) |
| `GET` | `/healthz` | No | Liveness probe ([section 8](#8-operational-endpoints)) |
| `GET` | `/readyz` | No | Readiness probe |
| `GET` | `/metrics` | Yes | Prometheus metrics |
| `GET` | `/ui` | No | Search UI (open it in a browser) |
| `GET`, `HEAD` | `/iceberg/v1/...` | Yes | Apache Iceberg REST read-only catalog, for external query engines ([section 9](#9-iceberg-read-only-catalog)) |

Path parameters:

- `{org}`: the organization. This release has only `default`. Any other value returns HTTP 404 with `reason` `org_not_found`.
- `{stream}`: the dataset name. It must match `^[a-z0-9][a-z0-9_]{0,63}$`: lowercase letters, digits and underscores, not starting with an underscore, 1–64 characters long. A name that breaks this rule returns HTTP 400 with `reason` `invalid_stream_name`. Amberlog creates the dataset automatically at the first write.

This document calls `<org>/<stream>` a **dataset**, for example `default/app`.

### 1.2 Variables in the examples

The examples use two environment variables:

```text
export AMBERLOG=http://127.0.0.1:8800       # Amberlog address; use https:// when TLS is enabled
export TOKEN='change-me-to-a-random-token'  # the value of the configuration key auth.token
```

If inbound TLS is enabled and an internal CA issued the certificate, add `--cacert <CA certificate>` to `curl`.

The examples in this document ran on a new instance, in the order in which they appear. Later examples query the data that earlier examples wrote. Apart from the required configuration keys, this instance also had `reconciliation.grace = "1m"` and the alert rule in [section 6.3](#63-_alert_evaluations-alert-evaluation-records).

The examples write records with fixed timestamps around 2026-10-20 08:00 UTC, and the outputs shown come from a run within 72 hours after that time. If you run the examples more than 72 hours after these timestamps, their hour bucket is already past the lake-entry watermark (default 72 hours) and enters the lake soon after the write. Then three outputs differ: `tier` in the `_ledger` example of section 6.1 is `hot_cold` instead of `hot`; the get-table example in section 9 returns the table metadata instead of 404; and, if more than 96 hours have passed (the lake-entry watermark plus `lifecycle.lake_alert_after`, both at their defaults), the log has one `lake_backlog_high` warning.

### 1.3 Authentication

Requests under `/api/`, `/metrics` and `/iceberg/` must carry this request header:

```text
Authorization: Bearer <token>
```

- The token is the value of the configuration key `auth.token`. Amberlog compares it byte by byte. The scheme name `Bearer` is case-insensitive.
- Amberlog does not accept a token in a query parameter or in a cookie.
- If the token is missing, the scheme is not `Bearer`, the token is wrong, or the request has more than one `Authorization` header, Amberlog returns HTTP 401 with `reason` `unauthorized` and the response header `WWW-Authenticate: Bearer`.
- Under `/api/`, authentication comes before routing: without a valid token, a path that does not exist also returns 401, not 404.
- `/healthz`, `/readyz` and `/ui` do not require a token.
- When the configuration key `auth.enabled` is false, no endpoint requires a token.

This release has only this one token and no authorization model: a request with the token can access all `/api/` endpoints and the catalog endpoints.

```sh
curl -sS -i -X POST "$AMBERLOG/api/default/app/_json" -d '[]'
```

```text
HTTP/1.1 401 Unauthorized
content-type: application/json
www-authenticate: Bearer
x-request-id: 06cd844ed432f7470000000d

{"code":401,"reason":"unauthorized","message":"missing or invalid credentials"}
```

### 1.4 Request ID

Every response has the response header `x-request-id`. If the request has this header, Amberlog returns its value unchanged. Otherwise Amberlog generates one. When Amberlog answers with a 5xx status, it writes one line with `level` `error` to its runtime log. The line contains this ID and the cause of the error. To investigate a problem, give the ID to the operators. They can then find the matching line in the log.

```sh
curl -sS -i "$AMBERLOG/healthz" -H "x-request-id: my-trace-123"
```

```text
HTTP/1.1 200 OK
content-type: application/json
x-request-id: my-trace-123

{"status":"ok"}
```

### 1.5 Request body

These rules apply to all POST endpoints under `/api/` (`_json`, `_sent`, `_search`):

- **Format**: JSON. Amberlog does not accept or reject a request because of its `Content-Type` request header. You can leave the header out.
- **Compression**: `Content-Encoding` can be absent, or one of `identity`, `gzip`, `x-gzip` and `zstd` (case-insensitive). For any other value (such as `br` or `deflate`), or when the header lists more than one encoding, Amberlog returns HTTP 415 with `reason` `unsupported_encoding`. If decompression fails, it returns HTTP 400 with `reason` `invalid_encoding`.
- **Size**: neither the number of bytes received nor the number of bytes after decompression can exceed the configuration key `ingest.max_body_bytes` (default 32 MiB). If one of them does, Amberlog returns HTTP 413 with `reason` `payload_too_large`. There is no separate limit for a single record.
- **Valid JSON**: a request body is not valid JSON if it starts with a UTF-8 byte order mark, contains invalid UTF-8, contains an unpaired UTF-16 surrogate escape (such as `"\ud800"`), or contains a number outside the double-precision range (such as `1e400`). A `_search` or `_sent` request body that nests more than 128 levels deep is also not valid. For `_json`, a problem of this kind rejects only the record that has the problem; see [section 2.4](#24-per-record-rejection).
- **Duplicate keys**: when the same key occurs more than once in a JSON object, Amberlog uses the last value.
- Response bodies are not compressed.

## 2. Ingestion `_json`

```text
POST /api/{org}/{stream}/_json
```

The request body is a JSON array of records. Each element is one record. If the request body is a single JSON object, Amberlog handles it as a batch that contains one record.

### 2.1 Example

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Amberlog-Source: web-1" \
  -d '[
    {"_timestamp": "2026-10-20T08:00:00Z", "level": "info", "message": "user login", "user": {"id": 42, "name": "alice"}, "latency_ms": 12.5},
    {"_timestamp": 1792483260000, "level": "error", "message": "db timeout", "latency_ms": 3000.0, "tags": ["db", "timeout"]},
    {"_timestamp": 1792483320000000, "level": "warn", "message": "slow request", "latency_ms": 870.0},
    {"_secret": "x"},
    "not an object",
    {"_timestamp": "yesterday", "message": "bad time"}
  ]'
```

```json
{"code":200,"status":[{"name":"app","successful":3,"failed":3,"errors":[
  {"index":3,"reason":"reserved_field","message":"top-level keys starting with '_' are reserved"},
  {"index":4,"reason":"not_an_object","message":"the record is not a JSON object"},
  {"index":5,"reason":"timestamp_invalid","message":"_timestamp is not a valid timestamp"}]}]}
```

(For readability, long responses here and below are wrapped over several lines.)

Compress the request body with gzip:

```sh
printf '[{"level":"info","message":"compressed batch"}]' | gzip | \
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Encoding: gzip" --data-binary @-
```

```json
{"code":200,"status":[{"name":"app","successful":1,"failed":0,"errors":[]}]}
```

### 2.2 Response

| Case | Status | Response body |
|---|---|---|
| The batch is accepted, and it is not "all rejected" | 200 | `{"code":200,"status":[{"name":"<stream>","successful":n,"failed":m,"errors":[...]}]}` |
| Empty array `[]` | 200 | `successful` and `failed` are both 0 |
| At least one record, and every record is rejected individually | 400 | Error envelope with `reason` `all_records_rejected`. `details.status` has the same shape as `status` in the 200 response |
| The request body as a whole is not valid JSON | 400 | `reason` is `invalid_json`. No record is stored |
| Writing the local WAL fails | 503 | `reason` is `unavailable` |

- `status` has exactly one element. `successful` plus `failed` equals the number of records in the batch.
- `errors` has one element for each rejected record, in ascending order of `index`. `index` is the position of the record in the batch, starting at 0. For `reason`, see [section 2.4](#24-per-record-rejection).
- **Amberlog returns HTTP 200 only after all successful records are written to the local WAL and fsynced.** A record that received 200 is persisted. Within the throughput limit, acknowledged records can be searched within 10 seconds. When the backlog exceeds its memory limit, overflow records are kept only in the WAL and can be searched only after they are read back into memory (see section 4.1 of the [Known limitations](known-limitations.md)).
- When you receive 503, the batch is not acknowledged, and its records will not appear later. The client should retry the whole batch.
- Ingestion appends: when two records have identical content, Amberlog keeps both. There is no deduplication. If you retry a batch that already received 200, you get duplicate records.
- A rejected record does not affect the other records in the same batch.

An example where every record is rejected:

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" -d '[{"_x": 1}, {"bad name": 1}]'
```

```json
{"code":400,"reason":"all_records_rejected","message":"every record in the batch was rejected; retrying the same batch will not succeed",
 "details":{"status":[{"name":"app","successful":0,"failed":2,"errors":[
  {"index":0,"reason":"reserved_field","message":"top-level keys starting with '_' are reserved"},
  {"index":1,"reason":"invalid_field_name","message":"a field name is invalid or two fields flatten to the same name"}]}]}}
```

### 2.3 How records are stored

**Fields and types:**

| JSON value | Column type |
|---|---|
| String | `Utf8` |
| Boolean | `Boolean` |
| Number without a decimal point or exponent, within the int64 range | `Int64` |
| Number with a decimal point or exponent | `Float64` (rounded to the nearest double-precision value, as IEEE 754 specifies) |
| Integer outside the int64 range | `Utf8`. The value is the original decimal string, not rounded |
| Nested object | Flattened: the key names of the levels are joined with `.`. For example, `{"user":{"id":42}}` is stored as the column `user.id` |
| Array | The whole array is serialized as JSON text and stored as a `Utf8` column |
| `null`, missing field | The column is NULL. A new field whose value is `null` does not create a column |
| Empty object `{}` | Does not create a column |
| Empty string `""` | An empty string, not NULL |

- Flattening goes at most 8 levels deep. If the value of a key at level 8 is still a non-empty object, Amberlog does not flatten that object further. It serializes the whole object as JSON text and stores it as a `Utf8` column. The name of that column is the flattened name of the key.
- **Field names** (after flattening) can contain only `A`–`Z`, `a`–`z`, `0`–`9`, `-`, `_` and `.`. A field name must start and end with a letter or a digit, and must be at most 128 bytes long. `_` and `.` must not be next to each other (no `__`, `..`, `._` or `_.`).
- Top-level field names that start with `_` are reserved for the system. Only `_timestamp` can be written.
- The column limit for each dataset is the configuration key `ingest.max_columns` (default 1000). The limit includes `_timestamp`.

**Fields with inconsistent types**: when one field name has different types in different records, Amberlog still accepts the records. The type that occurs first takes the original name. Values of other types go into a **shadow column**, a column named `<field name>__<type suffix>`. The suffix is one of `int64`, `float64`, `boolean` and `utf8`. After the type of a column is registered, it does not change. In a query, a field name refers only to the column with that exact name. It does not include the values in the shadow columns.

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" -d '[{"_timestamp": "2026-10-20T08:03:00Z", "latency_ms": "n/a", "message": "type drift"}]'
curl -sS "$AMBERLOG/api/default/app/_fields" -H "Authorization: Bearer $TOKEN"
```

```json
{"code":200,"status":[{"name":"app","successful":1,"failed":0,"errors":[]}]}
{"fields":[{"name":"_timestamp","type":"Int64"},{"name":"latency_ms","type":"Float64"},
 {"name":"latency_ms__utf8","type":"Utf8"},{"name":"level","type":"Utf8"},{"name":"message","type":"Utf8"},
 {"name":"tags","type":"Utf8"},{"name":"user.id","type":"Int64"},{"name":"user.name","type":"Utf8"}]}
```

**`_timestamp`**: the time of the record. Amberlog stores and returns it as the number of UTC microseconds since the Unix epoch (Int64).

| Form | Meaning |
|---|---|
| Absent, `null` or `0` | The time at which the server received the request |
| Number, absolute value ≥ 1e17 | Nanoseconds |
| Number, absolute value in [1e14, 1e17) | Microseconds |
| Number, absolute value in [1e11, 1e14) | Milliseconds |
| Number, absolute value < 1e11 | Seconds; can have a fractional part |
| String | RFC 3339. Must have `Z` or a numeric offset, such as `2026-10-20T16:00:00+08:00` |

Any part smaller than one microsecond is truncated toward zero. Amberlog does no time zone conversion.

### 2.4 Per-record rejection

The `reason` values below appear only in `errors`. When one record has more than one problem, Amberlog reports only the first one, in the order of the table from top to bottom.

| `reason` | Meaning |
|---|---|
| `malformed_record` | A string in this record contains invalid UTF-8 or an unpaired surrogate escape, or the record contains a decimal number outside the double-precision range, or it nests more than 128 levels deep |
| `not_an_object` | This element is not a JSON object |
| `reserved_field` | The record has a top-level field, other than `_timestamp`, that starts with `_` |
| `invalid_field_name` | A field name breaks the rules, or two fields have the same name after flattening (such as `{"a.b":1,"a":{"b":2}}`) |
| `timestamp_invalid` | `_timestamp` is a string that cannot be parsed (including a time without a time zone), a boolean, an object or an array |
| `timestamp_out_of_range` | `_timestamp` is earlier than 1970-01-01, later than the receive time plus `ingest.max_future` (default 24 hours), or later than 9999-12-31T23:59:59.999999Z. When `ingest.max_past` is configured, also when `_timestamp` is earlier than the receive time minus that value |
| `too_many_columns` | The new columns that this record needs would make the column count exceed `ingest.max_columns` |

### 2.5 Source identifier `X-Amberlog-Source`

You can add the request header `X-Amberlog-Source: <source>` to a `_json` request. The source can be, for example, the name of a machine.

- The value must match `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`. If the value does not match or is empty, or the request has this header more than once, Amberlog returns HTTP 400 with `reason` `invalid_request`. No record is stored.
- The header is not written into the records. It does not change the record content or the response. Amberlog uses it only to keep separate counts per source, for [reconciliation](#62-_reconciliation-reconciliation) and missed-collection verdicts.

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" -H "X-Amberlog-Source: bad source" -d '[]'
```

```json
{"code":400,"reason":"invalid_request","message":"X-Amberlog-Source must appear at most once and match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"}
```

## 3. Sent-count reporting `_sent`

```text
POST /api/{org}/{stream}/_sent
```

A collector uses this endpoint to report how many records one source sent in one minute. Amberlog compares this number with the number of records that it actually received and acknowledged. The results are in the system tables `_reconciliation` and `_ledger`. Amberlog also uses them for missed-collection verdicts and for the built-in alert rule `missed_collection`.

Reconciliation only compares the counts that collectors report with the counts that Amberlog confirmed. A `consistent` verdict does not prove that the collector missed no records, and records that the collector never counted cannot be found this way.

The request body is a JSON array. Each element has these fields:

| Field | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Source identifier. Same rules as `X-Amberlog-Source` |
| `window_start` | Integer | Yes | Start of the minute, in UTC microseconds. Must be on a whole minute |
| `window_end` | Integer | Yes | Must equal `window_start + 60000000` |
| `sent` | Integer | Yes | Number of records sent. Not negative |
| `basis` | String | No | `delivery` (default) or `timestamp`. See below |

- When `basis` is `timestamp`, `sent` counts records by the minute that contains their `_timestamp`. Amberlog compares it, source by source, with the confirmed count by `_timestamp`, and gives a verdict for that minute ([section 6.2](#62-_reconciliation-reconciliation)). For missed-collection verdicts, use this kind of report, and send the ingestion requests with the same `X-Amberlog-Source`.
- When `basis` is `delivery`, the collector defines how `sent` is counted (for example, by the minute in which the records were sent). Amberlog only shows it next to `received` in the reconciliation table.
- When the same `(source, window_start, basis)` is reported more than once, the last report counts. Reports are not added up.
- Undefined fields are ignored.
- If any element breaks the rules, the whole request returns HTTP 400 with `reason` `invalid_request`, and Amberlog records no element.
- Amberlog returns 200 only after all elements are written to the local WAL and fsynced: `{"code":200,"accepted":<number of elements>}`. If writing the WAL fails, it returns 503.

The example below reports the three successful records of [section 2.1](#21-example), by the minute that contains their `_timestamp`:

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_sent" \
  -H "Authorization: Bearer $TOKEN" \
  -d '[
    {"source": "web-1", "basis": "timestamp", "window_start": 1792483200000000, "window_end": 1792483260000000, "sent": 1},
    {"source": "web-1", "basis": "timestamp", "window_start": 1792483260000000, "window_end": 1792483320000000, "sent": 1},
    {"source": "web-1", "basis": "timestamp", "window_start": 1792483320000000, "window_end": 1792483380000000, "sent": 1}
  ]'
```

```json
{"code":200,"accepted":3}
```

## 4. Search `_search`

```text
POST /api/{org}/_search
```

### 4.1 Request body

```json
{"query": {"sql": "SELECT ...", "start_time": 1792454400000000, "end_time": 1792540800000000, "size": 100, "from": 0, "timezone": "Asia/Shanghai"}}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `query.sql` | String | Yes | SQL. See [section 4.3](#43-sql) |
| `query.start_time` | Integer | Yes | Start of the time range, in UTC microseconds, inclusive |
| `query.end_time` | Integer | Yes | End of the time range, in UTC microseconds, exclusive. Must be greater than `start_time`. Neither value can be negative |
| `query.size` | Integer | No | Maximum number of rows to return. Default `query.default_size` (100). Cannot be negative or greater than `query.max_size` (10000) |
| `query.from` | Integer | No | Number of rows to skip in the final order. Default 0. Cannot be negative |
| `query.timezone` | String | No | IANA time zone name (such as `Asia/Shanghai`, case-insensitive) or `UTC`. Default `UTC`. Affects only how `histogram` divides time into time buckets. A fixed offset such as `+08:00` is not a time zone name |

Integer fields must be JSON integers without a decimal point or exponent. `1.0`, `"100"` and `null` are all treated as a wrong type. Undefined fields are ignored.

When you query a dataset, Amberlog automatically adds the condition `_timestamp >= start_time AND _timestamp < end_time`.

### 4.2 Successful response

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT _timestamp, level, message, \"user.name\", latency_ms FROM app",
  "start_time": 1792454400000000, "end_time": 1792540800000000}}'
```

```json
{"took":33,"hits":[
  {"_timestamp":1792483380000000,"message":"type drift"},
  {"_timestamp":1792483320000000,"level":"warn","message":"slow request","latency_ms":870.0},
  {"_timestamp":1792483260000000,"level":"error","message":"db timeout","latency_ms":3000.0},
  {"_timestamp":1792483200000000,"level":"info","message":"user login","user.name":"alice","latency_ms":12.5}],
 "total":4,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

| Field | Description |
|---|---|
| `took` | Elapsed time, in milliseconds |
| `hits` | The result rows. Each row is an object that maps output column names to values. The keys are in the same order as the `SELECT` list. **A column whose value is NULL does not appear in that row** |
| `total` | The number of rows in `hits` in this response (not the total number of matching rows) |
| `from`, `size` | The values actually used for this response |
| `scan_files`, `scan_bytes`, `scan_records` | The number of data files from which data was read, their total bytes and their total records. `scan_records` also includes rows in memory that are not yet written to files. All three are 0 when you query a system table |

How values are written: `Int64` as a JSON integer, `Float64` as a JSON number with a decimal point or exponent (such as `870.0`), `Boolean` as a JSON boolean, `Utf8` as a JSON string, and `_timestamp` as an integer in UTC microseconds.

**Row order and paging:**

- When there is no `ORDER BY` and the query is not an aggregate query, rows are in descending order of `_timestamp`.
- Amberlog first runs the SQL (including `LIMIT`) to get the sorted rows. Then it skips `from` rows. Then it takes at most `size` rows. `LIMIT` does not change `size` in the response.
- Rows with identical sort keys can come in a different order in two queries. For stable paging, add columns to `ORDER BY` that make each row distinct.

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT message FROM app ORDER BY _timestamp",
  "start_time": 1792454400000000, "end_time": 1792540800000000, "size": 2, "from": 2}}'
```

```json
{"took":26,"hits":[{"message":"slow request"},{"message":"type drift"}],"total":2,"from":2,"size":2,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

**All or nothing**: if Amberlog cannot read any object that it needs for the answer, the whole request returns an error. Amberlog does not return a result computed from part of the data.

### 4.3 SQL

Amberlog accepts a subset of SQL. A request can contain only one `SELECT` statement. The statement can end with one `;`, and it can contain `--` and `/* */` comments. The PostgreSQL 17 grammar decides whether a statement can be parsed.

```text
SELECT <* or output items, ...>
FROM <dataset name or system table name>
[WHERE <condition>]
[GROUP BY <item>, ...]
[ORDER BY <item> [ASC|DESC], ...]
[LIMIT <n>]
```

| Part | Supported forms |
|---|---|
| Output items | `*`; column names; aggregate calls; `histogram` calls. Each item can have `AS <alias>`. An alias cannot start with `_`. You cannot leave out `AS` |
| `FROM` | One dataset name (a `<stream>` in the same `{org}`) or one system table name. Table aliases, `JOIN` and subqueries are not supported |
| `WHERE` | Comparisons `=` `!=` `<>` `<` `<=` `>` `>=`; `AND`, `OR`, `NOT`; `IS [NOT] NULL`; `[NOT] IN (...)`; `[NOT] LIKE`; `str_match`, `match_all`; grouping with parentheses. Each side of a comparison is a column name or a literal |
| `GROUP BY` | Column names, `histogram` calls or output column names |
| `ORDER BY` | Column names, output column names, aggregate calls or `histogram` calls. Default `ASC`. NULLs sort after non-NULL values (before them with `DESC`) |
| `LIMIT` | An integer literal from 0 to 9223372036854775807 |
| Literals | Single-quoted strings (`''` stands for one single quote; `\` is an ordinary character), decimal numbers (can have a sign, a fractional part and an exponent), `TRUE`, `FALSE`, `NULL` |

Not supported: `JOIN`, subqueries, `UNION`, `HAVING`, `DISTINCT`, window functions, type casts (`::`, `CAST`), and functions that are not in the table above. If a query uses one of them, Amberlog returns HTTP 400 with `reason` `sql_unsupported`.

**Quoting column names**: Amberlog parses identifiers without quotes as lowercase. A column name that contains capital letters, `.` or `-` must be in double quotes, such as `"user.name"` and `"HTTP-Status"`.

**Comparison rules:**

- You can compare both `Int64` columns and `Float64` columns with numeric literals. An `Int64` column is compared by exact value. For a `Float64` column, Amberlog first rounds the literal to double precision and then compares.
- You cannot compare an `Int64` column with a `Float64` column, a number with a string, or a boolean with a non-boolean. Such a comparison returns HTTP 400 with `reason` `invalid_sql`.
- You cannot compare with the `NULL` literal (`= NULL`, `IN (NULL)`). Use `IS NULL`.
- Strings are compared character by character, by Unicode code point, and the comparison is case-sensitive. For booleans, `FALSE` is less than `TRUE`.
- `LIKE`: `%` matches any number of characters, `_` matches one character, and `\` escapes the next character. The match is case-sensitive. The left side must be a `Utf8` column, and the pattern must be a string literal.

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT message, latency_ms FROM app WHERE level IN ('"'error'"', '"'warn'"') AND latency_ms > 500 ORDER BY latency_ms DESC",
  "start_time": 1792454400000000, "end_time": 1792540800000000}}'
```

```json
{"took":32,"hits":[{"message":"db timeout","latency_ms":3000.0},{"message":"slow request","latency_ms":870.0}],"total":2,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

(In the example, `'"'error'"'` is how the shell writes a single quote inside a single-quoted string. The SQL actually receives `'error'`.)

### 4.4 Full-text matching: `str_match` and `match_all`

Both can appear only as a complete condition in `WHERE`. You can combine them with `AND`, `OR` and `NOT`.

| Form | True when |
|---|---|
| `str_match(<column>, '<text>')` | The column is not NULL and contains the text. Case-sensitive. The column must be a `Utf8` column |
| `match_all('<text>')` | At least one `Utf8` column of the row contains the text. This includes shadow columns and does not depend on the `SELECT` list. Only ASCII letters are matched case-insensitively |

`%`, `_` and `\` in the text match literally. They are not wildcards.

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT message FROM app WHERE match_all('"'TIMEOUT'"') OR str_match(\"user.name\", '"'ali'"')",
  "start_time": 1792454400000000, "end_time": 1792540800000000}}'
```

```json
{"took":29,"hits":[{"message":"db timeout"},{"message":"user login"}],"total":2,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

### 4.5 Aggregation

Amberlog supports `count(*)`, and `count`, `sum`, `avg`, `min` and `max` with one column name as the argument. Modifiers such as `DISTINCT` and `FILTER` are not supported.

| Function | Result |
|---|---|
| `count(*)` | Number of rows, Int64 |
| `count(<column>)` | Number of rows in which the column is not NULL, Int64 |
| `sum(<column>)` | Sum, same type as the column. Only for `Int64` and `Float64` columns |
| `avg(<column>)` | Average, Float64. Only for `Int64` and `Float64` columns |
| `min(<column>)`, `max(<column>)` | Minimum, maximum; same type as the column |

- A query is an **aggregate query** if it has `GROUP BY`, or if it has an aggregate call in `SELECT` or `ORDER BY`.
- With `GROUP BY`, there is one row per group. When there are no input rows, there are no result rows. Without `GROUP BY`, there is exactly one row. NULL forms a group of its own.
- In an aggregate query, `SELECT` cannot be `*`. It also cannot contain a column that is neither an aggregate call nor a grouping item.
- An aggregate query without `ORDER BY` returns its rows in no fixed order.
- When a group has no non-NULL values, `sum`, `avg`, `min` and `max` are NULL (the key does not appear).
- When the `sum` of an `Int64` column exceeds the Int64 range, or a `Float64` result is not a finite value, Amberlog returns HTTP 400 with `reason` `numeric_overflow`.
- For an aggregate call without an alias, the output column name is the function name in lowercase, such as `count`.

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT level, count(*) AS n, max(latency_ms) AS max_ms FROM app GROUP BY level ORDER BY n DESC, level",
  "start_time": 1792454400000000, "end_time": 1792540800000000}}'
```

```json
{"took":39,"hits":[{"level":"error","n":1,"max_ms":3000.0},{"level":"info","n":1,"max_ms":12.5},{"level":"warn","n":1,"max_ms":870.0},{"n":1}],"total":4,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

The last row is the group in which `level` is NULL (the `type drift` record), so it has no `level` key. The `latency_ms` value of that record is in the shadow column, so `max_ms` is NULL.

### 4.6 Time buckets: `histogram`

```text
histogram(_timestamp, '<interval>')
```

- Write the interval like a duration in the configuration: `<integer><ms|s|m|h|d>`, such as `'5m'`, `'1h'` and `'1d'`. `'60s'` and `'1m'` count as the same call.
- The value is the start of the time bucket that contains the row, in UTC microseconds, Int64.
- Amberlog divides time into buckets in the local time of the `query.timezone` of the request. For example, under `Asia/Shanghai`, `'1d'` divides at midnight Beijing time. The time zone data is compiled into the program. Amberlog does not read time zone files from the runtime environment.
- `histogram` can appear only in `SELECT`, `GROUP BY` and `ORDER BY`, and only as an item by itself. It produces rows only for buckets that have records. It does not fill in empty buckets.

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT histogram(_timestamp, '"'1h'"') AS hour, count(*) AS n FROM app GROUP BY hour ORDER BY hour",
  "start_time": 1792454400000000, "end_time": 1792540800000000, "timezone": "Asia/Shanghai"}}'
```

```json
{"took":36,"hits":[{"hour":1792483200000000,"n":4}],"total":1,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

`1792483200000000` is 2026-10-20T08:00:00Z, which is 16:00 Beijing time.

### 4.7 Search errors

When a `_search` request has more than one problem, Amberlog reports only the first one, roughly in this order:

1. `invalid_request` (400): a field is missing or has the wrong type, `size` or `from` is out of range, or the time zone name is not recognized.
2. `invalid_time_range` (400): `end_time` is not greater than `start_time`, or a value is negative.
3. `invalid_sql` (400): the SQL cannot be parsed, or it contains nothing except whitespace, comments and `;`.
4. `sql_unsupported` (400): the SQL uses a form outside the subset.
5. `stream_not_found` (400): the dataset in `FROM` does not exist, or its name breaks the rules.
6. The dataset is disabled: `unsupported_format_version` (503) or `internal` (500).
7. `too_many_queries` (429): the number of queries running at the same time has reached `query.max_concurrency`. Amberlog rejects the request immediately and does not queue it. You can retry later.
8. Reading object storage fails: `unavailable` (503).
9. `unknown_column` (400): a column does not exist.
10. `invalid_sql` (400): the two sides of a comparison have types that cannot be compared, a comparison with `NULL`, the left side of `LIKE` is not a `Utf8` column, duplicate output column names, or an aggregate or `histogram` written incorrectly.
11. During execution: reading data fails, `unavailable` (503) or `internal` (500); `query_memory_exceeded` (400, the memory of a single query exceeds `query.max_memory_bytes`); `numeric_overflow` (400).

```sh
for sql in "SELEC 1" "SELECT * FROM app a JOIN app b ON true" "SELECT * FROM nosuch" "SELECT nosuch FROM app" "SELECT * FROM app WHERE level = 1"; do
  curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" \
    -d "{\"query\":{\"sql\":\"$sql\",\"start_time\":1792454400000000,\"end_time\":1792540800000000}}"; echo
done
```

```text
{"code":400,"reason":"invalid_sql","message":"Invalid statement: syntax error at or near \"SELEC\""}
{"code":400,"reason":"sql_unsupported","message":"a FROM item other than a stream name (subquery, JOIN, function…) is not supported"}
{"code":400,"reason":"stream_not_found","message":"stream `nosuch` does not exist"}
{"code":400,"reason":"unknown_column","message":"column `nosuch` does not exist in `app`"}
{"code":400,"reason":"invalid_sql","message":"cannot compare column `level` (Utf8) with this literal"}
```

## 5. Field list `_fields`

```text
GET /api/{org}/{stream}/_fields
```

Returns all current columns of the dataset, including `_timestamp` and the shadow columns, in ascending order of the Unicode code points of the column names. `type` is one of `Int64`, `Float64`, `Boolean` and `Utf8`.

- It does not read object storage. It answers as usual when object storage is unavailable.
- It does not use a query concurrency slot.
- When the dataset does not exist, it returns HTTP 400 with `reason` `stream_not_found`.

```sh
curl -sS "$AMBERLOG/api/default/app/_fields" -H "Authorization: Bearer $TOKEN"
```

```json
{"fields":[{"name":"_timestamp","type":"Int64"},{"name":"latency_ms","type":"Float64"},
 {"name":"latency_ms__utf8","type":"Utf8"},{"name":"level","type":"Utf8"},{"name":"message","type":"Utf8"},
 {"name":"tags","type":"Utf8"},{"name":"user.id","type":"Int64"},{"name":"user.name","type":"Utf8"}]}
```

## 6. System tables

Query system tables with `_search`, in the same way as datasets: write the system table name after `FROM`. System tables are read-only. There is no endpoint that writes to them.

| System table | Each row is | Effect of the time range | Order without `ORDER BY` |
|---|---|---|---|
| `_ledger` | One dataset (the data asset ledger) | Does not change which rows appear. Only sets the range over which the source and reconciliation summary columns are counted | `dataset` ascending |
| `_reconciliation` | One one-minute window of one dataset (reconciliation) | Only windows whose `window_start` is in the range | `window_start` descending, then `dataset` ascending |
| `_alert_evaluations` | One alert evaluation | Only evaluations whose `scheduled_at` is in the range | `scheduled_at` descending, then `rule` ascending |

- When you query a system table, all `scan_*` values are 0.
- Amberlog generates the data of the system tables from counter objects and evaluation records, and writes it with a delay. After ingestion or an evaluation, the data appears in the system tables within about one minute.
- When the `WHERE` clause of a `_reconciliation` or `_ledger` query has `dataset = '<dataset>'` (joined with `AND`), Amberlog reads only the data of those datasets, and the query is faster. The same applies to `rule = '<rule name>'` for `_alert_evaluations`.

### 6.1 `_ledger`: data asset ledger

| Column | Type | Description |
|---|---|---|
| `dataset` | Utf8 | `<org>/<stream>` |
| `status` | Utf8 | `active`. `disabled_format_version` when the dataset is disabled because its format version is incompatible. `disabled_manifest_unreadable` when the dataset is disabled because its manifest cannot be read |
| `time_min`, `time_max` | Int64 | The earliest and the latest `_timestamp` in the dataset (hot tier and cold tier together) |
| `record_count`, `size_bytes` | Int64 | Number of records and bytes of data files (hot tier and cold tier together) |
| `tier` | Utf8 | The tier that holds the data: `hot`, `hot_pending_lake` (the hot tier has data whose lake-entry time has passed but that has not entered the lake yet), `cold`, `hot_cold`. NULL when there is no data |
| `pending_lake_record_count` | Int64 | Number of records whose lake-entry time has passed but that have not entered the lake yet |
| `retention` | Utf8 | The original text of the configuration key `retention.period`. NULL when it is not configured |
| `expires_at` | Int64 | `time_min` plus the retention period. NULL when no retention period is configured. For display only: deletion follows the rules for deleting expired data |
| `sources` | Utf8 | The sources that reported sent counts with `basis` `delivery` within the time range, in ascending code-point order, joined with `,` |
| `ts_sources` | Utf8 | The sources that reported sent counts with `basis` `timestamp` within the time range |
| `reconciliation` | Utf8 | `three_way` (there are `sources`) or `two_way` (there are none). `unknown` when the counter objects cannot be understood |
| `windows_pending`, `windows_consistent`, `windows_missing`, `windows_duplicate`, `windows_silent`, `windows_undeterminable` | Int64 | The number of windows with each verdict within the time range. For the verdicts, see [section 6.2](#62-_reconciliation-reconciliation) |
| `silent_sources` | Int64 | The number of sources whose reporting stopped within the time range |

For a row whose `status` is not `active`, the data-volume columns are NULL. For a row whose `reconciliation` is `unknown`, the verdict summary columns are NULL.

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT * FROM _ledger",
  "start_time": 1792483200000000, "end_time": 1792483380000000}}'
```

```json
{"took":30,"hits":[{"dataset":"default/app","status":"active","time_min":1792483200000000,"time_max":1792560462673048,
  "record_count":5,"size_bytes":4894,"tier":"hot","pending_lake_record_count":0,"reconciliation":"two_way","ts_sources":"web-1",
  "windows_pending":0,"windows_consistent":3,"windows_missing":0,"windows_duplicate":0,"windows_silent":0,"windows_undeterminable":0,
  "silent_sources":0}],"total":1,"from":0,"size":100,"scan_records":0,"scan_files":0,"scan_bytes":0}
```

`time_max` is the receive time of the record in the gzip example of section 2.1 (that record had no `_timestamp`). The time range of the query does not affect columns such as `time_min`, `time_max` and `record_count`.

### 6.2 `_reconciliation`: reconciliation

Each row is a one-minute window of one dataset.

| Column | Type | Description |
|---|---|---|
| `dataset` | Utf8 | `<org>/<stream>` |
| `window_start`, `window_end` | Int64 | The start and the end of the window, in UTC microseconds, `[window_start, window_end)` |
| `sent` | Int64 | The sum of the sent counts that the sources reported with `basis` `delivery` for this minute. NULL when there is no report |
| `sent_sources` | Utf8 | Those sources, in ascending code-point order, joined with `,` |
| `received` | Int64 | The number of records parsed from request bodies and assigned to this minute by **receive time** (including the records rejected individually) |
| `durable` | Int64 | The number of acknowledged records (HTTP 200 received) assigned to this minute by receive time |
| `gap_sent_received` | Int64 | `sent` minus `received` |
| `gap_received_durable` | Int64 | `received` minus `durable` |
| `ts_sent` | Int64 | The sum of the sent counts reported with `basis` `timestamp`, counted by the minute that contains `_timestamp` (only assessed sources). NULL when there are none |
| `ts_sent_sources` | Utf8 | The assessed sources, that is, the sources that reported with `basis` `timestamp` for this minute |
| `ts_durable` | Int64 | The sum of the confirmed counts of the assessed sources, counted by the minute that contains `_timestamp` (only ingestion requests that had `X-Amberlog-Source`) |
| `verdict` | Utf8 | The verdict. See the table below |

The values of `verdict` (the grace period is the configuration key `reconciliation.grace`, default 5 minutes):

| Value | Meaning |
|---|---|
| `pending` | The grace period after the end of the window has not passed yet, or a source is resending. No verdict yet |
| `consistent` | For every assessed source, the confirmed count equals its sent count |
| `missing` | For at least one assessed source, the confirmed count is less than its sent count (possible missed collection) |
| `duplicate` | Nothing is missing, but for at least one assessed source, the confirmed count is greater than its sent count (possible duplicate ingestion) |
| `silent` | A source reported within the previous 24 hours but did not report for this minute (reporting stopped) |
| `undeterminable` | There is no assessed source and no stopped reporting. No verdict is possible |

Amberlog computes the verdict at query time, from the counts at that time. When late records fill the gap, `missing` changes to `consistent`.

"A source is resending" means one of the following. In these cases, the affected windows count as `pending`:

- Within the last grace period, Amberlog acknowledged late records from this source (records written more than one grace period after the end of the minute that contains their `_timestamp`), or accepted a late report from it with `basis` `timestamp`.
- Within the last grace period, Amberlog returned 503 to a `_json` or `_sent` request for this dataset.
- Less than one grace period has passed since the process started.

So after you backfill historical data, wait one grace period before the verdicts become stable.

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT window_start, ts_sent, ts_durable, verdict FROM _reconciliation WHERE dataset = '"'default/app'"'",
  "start_time": 1792483200000000, "end_time": 1792483380000000}}'
```

```json
{"took":16,"hits":[
  {"window_start":1792483320000000,"ts_sent":1,"ts_durable":1,"verdict":"consistent"},
  {"window_start":1792483260000000,"ts_sent":1,"ts_durable":1,"verdict":"consistent"},
  {"window_start":1792483200000000,"ts_sent":1,"ts_durable":1,"verdict":"consistent"}],
 "total":3,"from":0,"size":100,"scan_records":0,"scan_files":0,"scan_bytes":0}
```

### 6.3 `_alert_evaluations`: alert evaluation records

You configure alert rules in the configuration file (see `alerts.rules` in the configuration reference). The examples in this document use this rule:

```toml
[[alerts.rules]]
name = "error_burst"
interval = "1m"
window = "5m"
sql = "SELECT count(*) AS n FROM app WHERE level = 'error'"
condition = { column = "n", op = ">", threshold = 1000 }
webhook_url = "https://hooks.example.com/amberlog"
```

Amberlog writes one record after each evaluation ends. The retention period of these records is the configuration key `alerts.history_retention` (default 30 days). When you remove a rule from the configuration, its evaluation records still appear within the retention period.

| Column | Type | Description |
|---|---|---|
| `rule` | Utf8 | Rule name |
| `scheduled_at` | Int64 | The scheduled time of the evaluation, in UTC microseconds |
| `started_at` | Int64 | The time at which the query started. NULL when no query ran |
| `finished_at` | Int64 | The time at which the evaluation ended |
| `window_start`, `window_end` | Int64 | The time range that this evaluation queried |
| `outcome` | Utf8 | `matched`, `not_matched`, `skipped` or `error` |
| `reason` | Utf8 | The reason for `skipped` and `error`, such as `previous_running`, `alert_slots_full`, `query_slots_full` and `shutting_down`, or the `reason` of the query error |
| `matched_rows` | Int64 | The number of rows that meet the condition. NULL for `skipped` and `error` |
| `truncated` | Boolean | Whether the query result had more than `query.max_size` rows and was truncated |
| `delivery` | Utf8 | The notification result: `delivered`, `delivered_after_retry`, `abandoned` or `not_attempted` |
| `attempts` | Int64 | The number of notification requests sent |
| `last_http_status` | Int64 | The status code received for the last notification request |

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d "{\"query\": {
  \"sql\": \"SELECT rule, scheduled_at, outcome, matched_rows, delivery FROM _alert_evaluations\",
  \"start_time\": $(( ($(date +%s) - 3600) * 1000000 )), \"end_time\": $(( ($(date +%s) + 60) * 1000000 ))}}"
```

```json
{"took":19,"hits":[
  {"rule":"error_burst","scheduled_at":1792560540000000,"outcome":"not_matched","matched_rows":0,"delivery":"not_attempted"},
  {"rule":"error_burst","scheduled_at":1792560480000000,"outcome":"not_matched","matched_rows":0,"delivery":"not_attempted"}],
 "total":2,"from":0,"size":100,"scan_records":0,"scan_files":0,"scan_bytes":0}
```

The rule is evaluated once a minute. The number of rows returned depends on how many minutes the instance has been running.

## 7. Error structure

### 7.1 Error envelope

Except under `/iceberg/`, the body of every 4xx and 5xx response is a JSON object like this one (`Content-Type: application/json`):

```json
{"code": 400, "reason": "invalid_json", "message": "the request body is not valid JSON"}
```

| Field | Description |
|---|---|
| `code` | Equals the HTTP status code |
| `reason` | A machine-readable reason. For the values, see the table below. **Base your program logic on `reason`** |
| `message` | A description for people. The text can change between releases. Do not base decisions on it |
| `details` | Optional. Contains `status` for `all_records_rejected`, and `checks` when `/readyz` returns 503 |

- An error response to a `HEAD` request has no body.
- The `message` of a 5xx response does not contain internal paths, object keys, storage endpoints, storage bucket names or raw errors from underlying libraries. The detailed cause is in the runtime log. Find it with `x-request-id`.
- A published `reason` is never removed and never changes its meaning. New releases can add new `reason` values. Clients must handle values that they do not recognize.
- The only status codes are 400, 401, 404, 405, 413, 415, 429, 500 and 503.
- A 405 response has an `Allow` header that lists the methods that the path supports. A 401 response has `WWW-Authenticate: Bearer`.

### 7.2 `reason` values

| `reason` | Status | Meaning |
|---|---|---|
| `invalid_json` | 400 | The request body is not valid JSON |
| `invalid_encoding` | 400 | Decompression as `Content-Encoding` specifies failed |
| `invalid_request` | 400 | A field is missing, has the wrong type, or has a value that breaks the rules (`_search`, `_sent`, `X-Amberlog-Source`) |
| `all_records_rejected` | 400 | Every record in the `_json` batch was rejected individually |
| `invalid_stream_name` | 400 | The dataset name in the path breaks the rules |
| `stream_not_found` | 400 | The queried dataset does not exist |
| `invalid_sql` | 400 | The SQL cannot be parsed, or its meaning breaks the rules |
| `sql_unsupported` | 400 | The SQL uses a form outside the subset |
| `invalid_time_range` | 400 | The time range of `_search` is not valid |
| `unknown_column` | 400 | The queried column does not exist |
| `numeric_overflow` | 400 | An aggregate result overflowed |
| `query_memory_exceeded` | 400 | The memory of a single query exceeds `query.max_memory_bytes` |
| `unauthorized` | 401 | The token is missing or not valid |
| `org_not_found` | 404 | The organization in the path does not exist |
| `not_found` | 404 | The path does not exist |
| `namespace_not_found`, `table_not_found` | 404 | Used only for `/iceberg/` |
| `method_not_allowed` | 405 | The path does not support this method |
| `payload_too_large` | 413 | The request body exceeds `ingest.max_body_bytes` |
| `unsupported_encoding` | 415 | The `Content-Encoding` is not supported |
| `too_many_queries` | 429 | The number of queries running at the same time is at the limit. Retry later |
| `internal` | 500 | Internal server error, or the manifest of the dataset cannot be read |
| `unsupported_format_version` | 503 | This version of the program does not support the storage format version of the dataset |
| `unavailable` | 503 | Object storage or the local WAL is temporarily unavailable, or the process is shutting down. You can retry |
| `not_ready` | 503 | Used only for `/readyz` |

For the per-record reasons in `errors[]`, see [section 2.4](#24-per-record-rejection). `type_conflict` is reserved and not used. This release no longer produces it.

### 7.3 Order of checks

Amberlog checks requests under `/api/` in the order below. The first step that fails determines the response:

1. Shutdown: while the process is shutting down, new requests return 503 `unavailable`.
2. Authentication: 401 `unauthorized`.
3. Route and method: 404 `not_found`, 405 `method_not_allowed`.
4. `{org}`: 404 `org_not_found`.
5. `{stream}`: 400 `invalid_stream_name`.
6. Dataset status (only for endpoints whose path contains `{stream}`): 503 `unsupported_format_version`, or 500 `internal`.
7. Source identifier (only for `_json`): 400 `invalid_request`.
8. `Content-Encoding`: 415 `unsupported_encoding`.
9. Request body size and decompression: 413 `payload_too_large`, 400 `invalid_encoding`.
10. Request body parsing: 400 `invalid_json` and others.

During shutdown, in-flight requests complete as usual within `server.shutdown_timeout` (default 10 seconds).

## 8. Operational endpoints

### 8.1 `GET /healthz`

Returns 200 when the process can handle HTTP requests. It does not check object storage or the WAL. Use it as the liveness probe.

```sh
curl -sS "$AMBERLOG/healthz"
```

```json
{"status":"ok"}
```

### 8.2 `GET /readyz`

Returns 200 `{"status":"ready"}` when all of these conditions are true:

- `wal.dir` is writable;
- the most recent object storage probe succeeded (Amberlog probes once in every 30 seconds: it writes `<storage.prefix>/_system/readyz-probe` and reads it back);
- the process is not shutting down.

Otherwise it returns 503 with `reason` `not_ready`. `details.checks` contains three booleans: `wal`, `object_store` and `shutting_down`. Use it as the readiness probe.

When `/readyz` returns 503, Amberlog does not reject `/api/` requests because of it. Each request is answered by its own rules.

A process in read-only catalog mode (`amberlog catalog`) does not write the probe object. It does only a read-only listing probe, and `wal` is always true.

```sh
curl -sS "$AMBERLOG/readyz"
```

```json
{"status":"ready"}
```

### 8.3 `GET /metrics`

Prometheus text format (`Content-Type: text/plain; version=0.0.4; charset=utf-8`). Requires a token. In the Prometheus scrape configuration, read the token from a file with `authorization.credentials_file`. Do not write the token directly into the configuration (section 3.1 of the [Operations guide](operations.md)).

```sh
curl -sS "$AMBERLOG/metrics" -H "Authorization: Bearer $TOKEN" | grep -v '^#' | grep amberlog_ingest_records_total
```

```text
amberlog_ingest_records_total{org="default",stream="app",outcome="accepted"} 5
amberlog_ingest_records_total{org="default",stream="app",outcome="rejected"} 5
```

**Stable metrics**: the metrics below will not be renamed or removed, and their existing labels will not be removed. You can safely use them in alerts and dashboards. Other `amberlog_` metrics can change between releases.

| Metric | Type | Description |
|---|---|---|
| `amberlog_build_info{version,commit}` | gauge | Always 1. The labels are the version number and the commit ID |
| `amberlog_ready` | gauge | 1 when `/readyz` would return 200 at this moment, otherwise 0 |
| `amberlog_ingest_records_total{org,stream,outcome}` | counter | The number of records that `_json` accepted (`accepted`) and rejected individually (`rejected`) |
| `amberlog_manifest_commits_total{org,stream,result}` | counter | The number of commits. `result` is `ok` or `failed`. While object storage is unavailable, one `failed` is counted per `flush.interval` |
| `amberlog_dataset_disabled{org,stream,state}` | gauge | Disabled datasets. `state` is `format_version` or `manifest_unreadable`, and the value is 1. When no dataset is disabled, this series does not exist |
| `amberlog_wal_bytes` | gauge | The total bytes of the files under `wal.dir` |
| `amberlog_backlog_memory_bytes` | gauge | The bytes in memory that records waiting to be committed use |
| `amberlog_lake_lag_seconds{org,stream}` | gauge | The lake-entry backlog: how many seconds the oldest hour bucket that is due for lake entry but has not entered the lake is past its lake-entry time. 0 when there is no backlog |
| `amberlog_search_rejected_total{reason}` | counter | The number of `_search` requests rejected because of `too_many_queries` or `query_memory_exceeded` |
| `amberlog_alert_evaluations_total{rule,outcome}` | counter | The number of alert evaluations, by outcome |
| `amberlog_alert_deliveries_total{rule,result}` | counter | Alert notification results. `result` is `delivered` or `abandoned` |
| `amberlog_reconciliation_windows{org,stream,verdict}` | gauge | The number of windows with each verdict in the most recent background missed-collection check (past 24 hours) |

## 9. Iceberg read-only catalog

Under `/iceberg/`, Amberlog provides a read-only subset of the Apache Iceberg REST catalog specification. External engines such as Apache Spark, Trino and DuckDB use it to find the cold-tier tables. An instance that runs as `amberlog serve` always provides it. You can also start a separate `amberlog catalog` process. That process provides only the catalog and needs only read permission on the cold-tier prefix.

| Method and path | Response |
|---|---|
| `GET /iceberg/v1/config` | `{"defaults":{},"overrides":{"prefix":"<storage.prefix>"},"endpoints":[...]}` |
| `GET /iceberg/v1/{prefix}/namespaces` | `{"namespaces":[["default"]]}`. Lists only namespaces that have at least one cold-tier table |
| `GET`, `HEAD /iceberg/v1/{prefix}/namespaces/{namespace}` | `{"namespace":["default"],"properties":{}}`. `HEAD` returns 204 |
| `GET /iceberg/v1/{prefix}/namespaces/{namespace}/tables` | `{"identifiers":[{"namespace":["default"],"name":"<stream>"}]}` |
| `GET`, `HEAD /iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}` | `{"metadata-location":"...","metadata":{...},"config":{}}`. `HEAD` returns 204 |

- The namespace is the organization name, and the table name is the dataset name. A table exists only after data enters the cold tier (lake entry).
- `{prefix}` must equal the configuration key `storage.prefix`.
- The catalog **does not hand out storage credentials** (`config` is always `{}`). To read the data files, external engines must use their own configured storage credentials, which can only read the cold-tier prefix.
- The error body has a different shape from the one under `/api/`. It follows the Iceberg REST specification: `{"error":{"message":"...","type":"NoSuchTableException","code":404,"reason":"table_not_found"}}`.
- A path that is not one of the seven endpoints returns 405 with an empty `Allow`.
- For how each external engine connects to this catalog, see section 5 of the [External engine guide](external-engines.md).

```sh
curl -sS "$AMBERLOG/iceberg/v1/config" -H "Authorization: Bearer $TOKEN"
```

```json
{"defaults":{},"overrides":{"prefix":"amberlog"},"endpoints":["GET /v1/config","GET /v1/{prefix}/namespaces",
 "GET /v1/{prefix}/namespaces/{namespace}","HEAD /v1/{prefix}/namespaces/{namespace}","GET /v1/{prefix}/namespaces/{namespace}/tables",
 "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}","HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"]}
```

```sh
curl -sS "$AMBERLOG/iceberg/v1/amberlog/namespaces/default/tables/app" -H "Authorization: Bearer $TOKEN"
```

```json
{"error":{"message":"the table does not exist","type":"NoSuchTableException","code":404,"reason":"table_not_found"}}
```

(The data of this dataset has not entered the lake yet, so the table does not exist yet. If you run the examples more than 72 hours after their timestamps, this request returns the table metadata instead; see section 1.2.)

## 10. Search UI `/ui`

Open `<Amberlog address>/ui` in a browser to search and to view the ledger and reconciliation. The page does not require a token. After you enter the token in the page, the page calls the `_search` and `_fields` endpoints above for you. The page does not send requests to external addresses.

```sh
curl -sS -o /dev/null -w '%{http_code} %{content_type}\n' "$AMBERLOG/ui"
```

```text
200 text/html; charset=utf-8
```
