# Search UI and alerting

[中文](ui-and-alerting.zh-CN.md)

> Applies to Amberlog 0.1.0.

This guide is for people who use Amberlog to search logs, look at data assets and reconciliation, and configure alerts. The first half describes the search UI in the browser. It has three screens: 检索 (Search), 台账 (Ledger) and 对账 (Reconciliation). The second half describes how to configure scheduled alerting and how to receive notifications.

In this release the search UI is in Chinese only, and the screenshots show the Chinese UI. This guide gives each Chinese label with its English meaning in parentheses at first use.

The screenshots and outputs in this guide all come from one real run: local disk storage, synthetic test data, and the browser time zone set to Asia/Shanghai. The HTTP interface is in the [API reference](api.md). The configuration keys are in the [Configuration reference](configuration.md).

## 1. Open the UI

Open `https://<Amberlog address>/ui` in a browser.

- Opening the page needs no token, and the page itself contains no data. You need the token only to query: enter the value of `auth.token` in the 口令 (Token) box at the top right.
- The token is kept only in the current browser tab. Closing the tab clears it. The token is not written to the address bar, to cookies or to persistent browser storage. When the token is wrong or missing, the message 口令无效或未填写 (invalid or missing token) appears next to the token box.
- 时区 (Time zone) sets the time zone in which the UI shows times. The default is the browser's time zone. Times are written as `YYYY-MM-DD HH:MM:SS.ffffff`, to the microsecond. Hover the mouse over a time to see it in UTC.
- 外观 (Appearance) has three choices: follow the system, light and dark.
- The browser keeps the time zone, the appearance and the query history, and uses them again the next time you open the UI.
- The scripts, styles and fonts that the page needs are all inside the page. The page does not access any address other than Amberlog, so it also works on an internal network.
- Switching between the three screens sends no request. Each screen keeps the inputs and results it already has.

## 2. Search screen

![Search screen: dataset app, last 1 hour, histogram and result table](images/ui-search.png)

### 2.1 Select a dataset and a time range

1. In the dataset box at the top left, select or type the name of a dataset, for example `app`. The suggestions come from the ledger.
2. Select a time range. The quick choices are the last 15 minutes, 1 hour, 6 hours, 24 hours, 7 days and 30 days. The default is the last 1 hour. You can also type times directly into the start and end boxes, as `YYYY-MM-DD HH:MM`, `YYYY-MM-DD HH:MM:SS` or with fractional seconds. The UI reads them in the selected time zone.
3. Click 查询 (Query).

Above the results, the UI shows the total count, the row range of this page, the time taken and the amount scanned. Each page has 100 rows. Use 上一页 (Previous) and 下一页 (Next) to move between pages.

### 2.2 Condition mode

In condition mode, type a condition into the condition box and press Enter. The condition becomes a chip in the condition box. Several conditions are combined with AND. You can delete each chip separately.

| Input | Becomes |
|---|---|
| `level = error` | A field condition. The operator can be `=`, `!=`, `<`, `<=`, `>`, `>=` or `LIKE` |
| `user.name IS NULL`, `user.name IS NOT NULL` | The field is null or is not null |
| `timeout` | A full-text condition: any text field contains this word |
| `"level = error"` | Put in double quotes, the whole text is a full-text condition |

- How values are compared: when the field has a text type, the `500` in `code = 500` is compared as text. When the field has a numeric type, it is compared as a number. When you put the value in double quotes, for example `code = "500"`, it is always compared as text. When the operator is `LIKE`, the value is always text.
- While you type, the UI suggests field names and operators. When the field panel already shows the common values of a field (section 2.4), the UI also suggests these values. The last suggestion is 按全文检索 (search as full text).
- A condition that you added or deleted but have not queried yet shows as a dashed chip, and a dot appears next to 查询 (Query). The change takes effect only after you click 查询 (Query).

![Condition mode: level = error and full-text timeout, with the matched word highlighted](images/ui-search-conditions.png)

Full-text search has no index. It scans all the text in the time range:

- When you add the first full-text condition, a time range longer than 24 hours is automatically reduced to the last 24 hours.
- With a full-text condition, the time range is at most 7 days. Above that, 查询 (Query) is not available. First narrow the range with field conditions, or query in parts.

### 2.3 Histogram

In condition mode, the bar chart above the results shows the number of records in each time interval. The bottom right corner shows how much time each bar represents.

- Click one bar, or drag across several bars, to set the time range to that interval and query again.
- 收起分布图 (Hide histogram) hides the chart. While it is hidden, the UI does not request histogram data.
- When you write SQL by hand, the UI draws no histogram.

### 2.4 Result table and field panel

- Click the `_timestamp` column header to switch between newest first and oldest first.
- You can hide each column. This applies only while the page stays open.
- The parts of the results that match a full-text condition are highlighted.

The field panel on the left lists all fields of the current dataset. The fields are grouped by the part of the name before the first `.`. Each field has a type mark in front of it.

- Click a field to list its 10 most common values, with counts, under the conditions and time range of the most recent query. This works only in condition mode.
- Each value has `=` and `≠` next to it. Click one to add the matching condition.
- When a field has too many values and counting them goes over the memory limit, the UI shows 取值太多，无法统计 (too many values to count).

![Field panel: the common values of http.uri expanded](images/ui-search-fields.png)

### 2.5 Record detail

Click a row of the result table to open the detail of that record on the right.

- The 字段 (Fields) tab lists all fields, with `message` first and the others by group. Each field has = 加入 (= add) and ≠ 排除 (≠ exclude). An added condition shows as a dashed chip. Click 关闭并查询 (Close and query) to apply it.
- The 原始 JSON (Raw JSON) tab shows the record exactly as it is in the search response.
- Press ↑ or ↓ to move to the previous or next record.
- 看前后记录 (Show surrounding records) shows about 20 records before this record and about 20 after it (each within 1 hour), so that you can see the context. By default it does not apply the current conditions. When you select 保留当前条件 (Keep current conditions), it shows only the records that match the conditions. 返回检索结果 (Back to search results) returns to the original results. This works only in condition mode.

![Record detail: the Fields tab](images/ui-search-detail.png)

### 2.6 SQL mode

Click SQL to switch to SQL mode and write SQL directly. When you switch from condition mode, the UI writes the current conditions as the equivalent SQL.

![SQL mode: counts grouped by level](images/ui-search-sql.png)

- The supported SQL is in section 4.3 of the [API reference](api.md). For unsupported SQL, the UI shows 查询写法有误 (invalid query) and the reason. For example, the `round` function is not supported.
- The time boxes at the top still set the time range.
- The UI draws no histogram, and you cannot sort the results by a column header.
- When you switch from SQL mode back to condition mode, the UI discards the SQL you changed. It asks you to confirm first.

### 2.7 Query history

Each query is added to 查询历史 (Query history), which the browser keeps. Select an entry to restore the search screen to the settings of that query. 清空查询历史 (Clear query history) deletes all entries.

### 2.8 Error messages

| Message | Cause and what to do |
|---|---|
| 口令无效或未填写 (invalid or missing token) | The token is wrong or missing |
| 查询写法有误 (invalid query) | The SQL or the condition is not supported. The reason is listed below the message |
| 这次查询需要的内存超过上限 (this query needs more memory than the limit) | Narrow the time range, or remove the full-text conditions and use field conditions |
| 服务繁忙：同时执行的查询已满 (service busy: the concurrent query limit is reached) | The number of queries running at the same time has reached the limit. Query again after the countdown ends |
| 暂时无法读取数据 (data temporarily unreadable) | Object storage is not accessible, or the service is stopping |
| 无法连接服务 (cannot connect to the service) | The browser cannot connect to Amberlog |

When a query has no result after 3 seconds, the UI shows the elapsed seconds and 取消 (Cancel). After you cancel, the server can still be running the query. If you query again at once, the UI can report that the service is busy.

## 3. Ledger screen

The ledger screen shows an overview of each dataset. Select a time range (the default is the last 24 hours) and click 查询 (Query).

![Ledger screen](images/ui-ledger.png)

| Column | Meaning |
|---|---|
| 状态 (Status) | 未停用 (Active), or 已停用 (Disabled) because of an incompatible format version or an unreadable manifest |
| 首条数据时间, 末条数据时间 (First record time, Last record time) | The earliest and latest `_timestamp` in the dataset |
| 条数, 字节数 (Records, Bytes) | The hot tier and the cold tier together |
| 所在层 (Tier) | 热层 (hot tier), 热层（待入湖） (hot tier, pending lake entry), 冷层 (cold tier), 热层与冷层 (hot and cold tier) |
| 待入湖条数 (Records pending lake entry) | The number of records that are due for lake entry but have not entered the lake yet |
| 保留期, 到期日 (Retention, Expiry date) | Shown only when `retention.period` is set. The expiry date is the time when the earliest data expires |
| 来源 (Sources) | The sources that reported sent counts by send time within the time range (section 4.1) |
| 对账状态 (Reconciliation status) | 三方 (three-way): there are sent counts by send time. 两方（发送计数缺失） (two-way, sent counts missing): there are none. 计数不可读 (counts unreadable) |
| 各判定的时间窗数 (Windows per verdict) | The number of minutes with each verdict within the time range. Verdicts are described in section 4.2 |
| 上报中断的来源数, 按时间戳上报的来源 (Silent sources, Sources reporting by timestamp) | See section 4.2 |

Each row has two links on the right. 对账 (Reconciliation) switches to the reconciliation screen for this dataset and the same time range. 检索 (Search) switches to the search screen for this dataset. The ledger screen is read-only. You cannot change anything on it.

The ledger lists all datasets. Everyone who has the token can see this list.

## 4. Reconciliation screen

The reconciliation screen lists the counts of one dataset minute by minute: how many records the collector says it sent, how many Amberlog received and confirmed, and whether the two match. Select a dataset and a time range, and click 查询 (Query). Select 只看非一致 (Only non-consistent) to list only the minutes whose verdict is not 一致 (Consistent).

![Reconciliation screen: only non-consistent minutes](images/ui-reconciliation.png)

### 4.1 Give reconciliation its data

Reconciliation depends on the collector reporting how many records it sent. When the collector only writes and does not report, the reconciliation screen has only the received count and the confirmed count.

1. Send each ingestion request with the request header `X-Amberlog-Source: <source>`, for example the machine name (section 2.5 of the [API reference](api.md)).
2. Every minute, the collector reports with `_sent` how many records it sent for this source (section 3 of the [API reference](api.md)).
   - `basis` is `timestamp`: the count is by the minute of each record's `_timestamp`. Only these reports take part in the verdicts.
   - `basis` is `delivery` (the default): the count follows the collector's own convention. It is only shown next to the received count, in the 发送数 (Sent) column.

| Column | Meaning |
|---|---|
| 窗口起点 (Window start) | The start of this minute |
| 发送数, 发送来源 (Sent, Sending sources) | The sum of the sent counts reported by send time, and their sources. When there is no report, the UI shows 未上报 (not reported) |
| 接收数, 本地持久化确认数 (Received, Locally persisted confirmed) | The number of records assigned to this minute by the time when Amberlog received the request |
| The two 差值 (difference) columns | Sent count minus received count, and received count minus confirmed count |
| 按时间戳的发送数, 按时间戳的确认数 (Sent by timestamp, Confirmed by timestamp) | The two numbers that the verdict compares, counted by the minute of `_timestamp` |
| 判定 (Verdict) | See section 4.2 |

### 4.2 How to read the verdicts

| Verdict | Meaning |
|---|---|
| 待定 (Pending) | The grace period (`reconciliation.grace`, default 5 minutes) after the end of this minute has not passed yet, or a source is still resending. No conclusion yet |
| 一致 (Consistent) | For each source that reports by timestamp, the confirmed count equals the sent count it reported |
| 缺失 (Missing) | For some source, the confirmed count is less than the sent count it reported |
| 重复 (Duplicate) | Nothing is missing, but for some source the confirmed count is more than the sent count it reported |
| 上报中断 (Silent) | A source reported in the previous 24 hours, but did not report for this minute |
| 不可判定 (Undeterminable) | No source reported by timestamp for this minute, and no source is silent |

When you read the verdicts, know these points:

- A verdict only compares the count that the collector reported with the count that Amberlog confirmed. 一致 (Consistent) means the two are equal. It does not prove that the collector missed no logs. 缺失 (Missing) means the two are not equal. Look for the cause at the collector.
- The verdict is calculated at query time from the counts at that time. When late records arrive and fill the gap, 缺失 (Missing) changes to 一致 (Consistent). After you write historical data, wait one grace period before the verdicts are stable.
- A record is added to the locally persisted confirmed count when it is written to the local WAL and acknowledged. The count does not show losses after that point, for example records not yet committed when the local disk is damaged. The reconciliation screen always shows this note at the top.
- The UI does not use colors to tell verdicts apart. Read the text.

## 5. Scheduled alerting

Amberlog can run an SQL query at a fixed interval. When the result meets a condition, Amberlog sends a `POST` request to an HTTP address (a webhook). The rules are in the configuration file. To change a rule, restart the process. Only the write instance (`amberlog serve`) evaluates rules.

### 5.1 Write a rule

```toml
[alerts]
max_concurrency = 1

# Every minute, look at the error records of the last 2 minutes and notify when there are more than 3.
[[alerts.rules]]
name = "error_burst"
interval = "1m"
window = "2m"
sql = "SELECT count(*) AS n FROM app WHERE level = 'error'"
condition = { column = "n", op = ">", threshold = 3 }
webhook_url = "https://hooks.example.com/amberlog"
webhook_headers = { "Authorization" = "Bearer <receiver token>" }
```

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Lower-case letters, digits and underscores, at most 64 characters. It must not start with `amberlog_`, must not be `missed_collection`, and must be unique among the rules |
| `interval` | Yes | The evaluation interval, in whole minutes, from `1m` to `24h`. Evaluations happen at whole multiples of the interval in UTC: with an interval of 5 minutes, at :00, :05 … of each hour |
| `window` | No | The length of the query range, in whole minutes, from `1m` to `7d`. The default is equal to `interval`. The evaluation at time T queries `[T − window, T)` |
| `sql` | Yes | One SQL statement, written as for search |
| `condition` | Yes | A row-count condition or a column condition, see section 5.2 |
| `webhook_url` | Yes | The address that receives notifications, `https://` or `http://` |
| `webhook_headers` | No | Custom request headers, for example the token the receiver requires |
| `enabled` | No | Default `true`. A rule set to `false` is still validated but not evaluated |

- There can be at most 100 rules. `alerts.rules` and `webhook_headers` can be set only in the configuration file, not with environment variables.
- When a rule is wrong, the process exits with exit code 2, and stderr names the wrong rule. At startup, Amberlog checks only the SQL syntax. Whether the datasets and columns that the SQL refers to exist is known only at evaluation time.
- Before you put a rule into use, try the same SQL and time range on the search screen or with `_search`. The evaluation sees the same result as such a query.

### 5.2 Two kinds of condition

- **Row-count condition** `{ rows = "> 0" }`: compares the number of rows in the result.
- **Column condition** `{ column = "n", op = ">", threshold = 3 }`: the rule matches when this column of any row in the result meets the comparison. The column must be an integer or floating-point column.

The comparison operators are `>`, `>=`, `<`, `<=`, `=` and `!=`.

SQL without aggregation returns at most `query.max_size` rows (default 10000). The rows above that are truncated. So, to decide by the number of records, write `count(*)` in the SQL and compare it with a column condition, as in the example in section 5.1. Keyword alerts are also written in SQL, for example `WHERE match_all('timeout')`.

### 5.3 Time windows and missed records

- Within the throughput limit, acknowledged records can be searched within 10 seconds. When the backlog exceeds its memory limit, overflow records are kept only in the WAL and can be searched only after they are read back into memory (see section 4.1 of [Known limitations and roadmap](known-limitations.md)). So records that arrive in about the last 10 seconds of the window can be not yet visible in this evaluation, and the range of the next evaluation does not include them. Overflow records can be missed in the same way even when they arrive earlier.
- Set `window` to `interval` plus 1 minute or more. The cost is that two adjacent evaluations overlap, so the same records can make both evaluations match. In a test, the rule in section 5.1 matched in two evaluations in a row for the same 5 error records.
- A larger `window` does not help with these records: late records whose timestamp is before the window; records covered by an evaluation that was skipped or failed with an error; the evaluations during downtime (after a restart, missed evaluations are not run).

### 5.4 Slots

- `alerts.max_concurrency` (default 1) is the number of evaluations that run queries at the same time. Rules that are due at the same time wait in order of rule name. If a rule does not get its turn before the next interval, this evaluation is recorded as skipped.
- The query of an evaluation also takes one query slot. It shares the `query.max_concurrency` slots (default 4) with search. When all slots are taken, this evaluation is recorded as skipped. It does not wait.
- Set `alerts.max_concurrency` lower than `query.max_concurrency` to leave slots for search.
- Evaluations do not affect ingestion and do not affect `/readyz`.

### 5.5 Webhook requests

On each match, Amberlog sends a `POST` to `webhook_url`. The request headers are `Content-Type: application/json`, `User-Agent: amberlog/<version>` and each entry of `webhook_headers`. A request body received in a test:

```json
{
  "version": 1,
  "rule": "error_burst",
  "evaluation_id": "error_burst-1792637880000000",
  "scheduled_at": "2026-10-22T02:58:00.000000Z",
  "window_start": "2026-10-22T02:56:00.000000Z",
  "window_end": "2026-10-22T02:58:00.000000Z",
  "matched_rows": 1,
  "truncated": false,
  "rows": [{"n": 5}],
  "sql": "SELECT count(*) AS n FROM app WHERE level = 'error'"
}
```

| Field | Meaning |
|---|---|
| `evaluation_id` | The rule name plus the scheduled time in UTC microseconds. All retries of one notification have the same value |
| `scheduled_at`, `window_start`, `window_end` | The scheduled time and the query range, RFC 3339 UTC |
| `matched_rows` | Row-count condition: the number of rows in the result. Column condition: the number of rows that meet the condition |
| `truncated` | Whether the result was truncated because it was larger than `query.max_size` |
| `rows` | The first 10 rows of the result (or of the rows that meet the condition). Columns whose value is NULL do not appear |
| `sql` | The SQL text of the rule |

The order of the fields is not fixed. Read the fields by name.

**Delivery and retries:**

- A 2xx response counts as delivered. Amberlog does not read the response body.
- An attempt times out when no response headers arrive within 10 seconds. On a timeout, a connection failure, or a 5xx, 408 or 429 response, Amberlog retries: at intervals of 1, 2, 4 and 8 seconds, at most 5 times. It does not adjust to `Retry-After`.
- Amberlog sends until the scheduled time plus `interval` at the latest. After that it does not send. A rule with an interval of 1 minute may not get all 5 attempts.
- On a 3xx or any other 4xx response, Amberlog does not retry and does not follow redirects.
- Notifications that were not delivered are not persisted. After a restart, they are not sent again.

**Recommendations for the receiver:**

- Return 2xx as soon as possible. Do slow processing after the response.
- Remove duplicates by `evaluation_id`: when the receiver processed a request but did not respond in time, it receives the request again. `evaluation_id` does not solve matches in two adjacent evaluations (section 5.3).
- When you cannot process a request for the moment, return 503 or 429. Amberlog then retries.
- Put tokens in `webhook_headers`, not in the query string of `webhook_url`. Amberlog does not write the request header values to its logs, evaluation records or metrics.

**TLS:** for `https://` addresses, Amberlog always verifies the certificate. There is no configuration key to skip verification. When the receiver uses an internal CA, set `alerts.ca_file`. Amberlog sends to `http://` addresses as usual and prints a `webhook_plaintext` warning at startup. Amberlog does not read environment variables such as `HTTP_PROXY`. It connects to the receiver directly.

Email, SMS, instant messaging and other channels are not built in. The receiver forwards notifications to them.

### 5.6 See the result of each evaluation

Each evaluation (including skipped ones) writes one evaluation record, kept for `alerts.history_retention` (default 30 days). Query the system table `_alert_evaluations`, in SQL mode on the search screen or with `_search`:

```sql
SELECT rule, scheduled_at, outcome, reason, delivery, attempts, last_http_status
FROM _alert_evaluations WHERE outcome != 'not_matched'
```

Result from a test (extract):

```json
{"rule":"missed_collection","scheduled_at":1792638000000000,"outcome":"matched","matched_rows":1,"delivery":"delivered","attempts":1,"last_http_status":200}
{"rule":"error_burst","scheduled_at":1792637940000000,"outcome":"matched","matched_rows":1,"delivery":"delivered","attempts":1,"last_http_status":200}
```

- `outcome`: `matched`, `not_matched`, `skipped` or `error`.
- `delivery`: `delivered`, `delivered_after_retry`, `abandoned`, or `not_attempted` (no match, so nothing to deliver).
- Without `ORDER BY`, the rows are ordered by scheduled time, latest first.

Common values of `reason`:

| Outcome | `reason` | How to read it |
|---|---|---|
| Skipped | `previous_running` | The previous evaluation has not finished yet |
| Skipped | `alert_slots_full` | The evaluation did not get an alert slot before the next interval. Consider increasing `alerts.max_concurrency` |
| Skipped | `query_slots_full` | All query slots are taken |
| Error | `stream_not_found` | The dataset has no data yet. This is common just after a new deployment starts. It stops when there is data |
| Error | `query_memory_exceeded` | The query went over the memory limit for a single query |
| Error | `condition_column_missing`, `condition_column_not_numeric` | The column of the column condition is not in the result, or is not a numeric column |

### 5.7 Built-in rule `missed_collection`

The built-in rule sends a notification when the verdict of a minute (section 4.2) changes to 缺失 (Missing). It does not run SQL. Its result comes from the background verdicts. The background calculates the verdicts at most every 60 seconds.

```toml
[alerts.missed_collection]
enabled = true
interval = "5m"
webhook_url = "https://hooks.example.com/amberlog"
webhook_headers = { "Authorization" = "Bearer <receiver token>" }
```

- It is off by default. When you turn it on, you must set `webhook_url`. Otherwise the process exits with exit code 2.
- Slots, evaluation records, request format and retries are the same as for ordinary rules. The `sql` of the request body is `builtin:missed_collection`. Each row of `rows` is one minute that changed to missing, with the columns `dataset`, `window_start`, `window_end`, `ts_sent`, `ts_durable` and `missing_sources`.
- Each minute is notified only once. Minutes notified before a restart are not notified again after the restart. Minutes that change to missing during downtime or just after a restart are not notified either. You can still see these minutes on the reconciliation screen.
- From the end of the minute to the notification, the longest time is about: the grace period, plus the interval of the background verdicts (at most 60 seconds), plus the `interval` of the rule.

In a test (grace period 1 minute, `interval` `1m`), the source `web-2` reported 12 records sent for the minute 02:57, but 10 records were actually written. The notification arrived at 03:00:

```json
{
  "version": 1,
  "rule": "missed_collection",
  "evaluation_id": "missed_collection-1792638000000000",
  "scheduled_at": "2026-10-22T03:00:00.000000Z",
  "window_start": "2026-10-22T02:59:00.000000Z",
  "window_end": "2026-10-22T03:00:00.000000Z",
  "matched_rows": 1,
  "truncated": false,
  "rows": [{"dataset": "default/app", "window_start": 1792637820000000, "window_end": 1792637880000000,
            "ts_sent": 12, "ts_durable": 10, "missing_sources": "web-2"}],
  "sql": "builtin:missed_collection"
}
```

### 5.8 Metrics

| Metric | Meaning |
|---|---|
| `amberlog_alert_evaluations_total{rule,outcome}` | Increases by 1 when an evaluation ends |
| `amberlog_alert_deliveries_total{rule,result}` | Increases by 1 when a delivery has a result. `result` is `delivered` or `abandoned` |

Set an alert on increases of `amberlog_alert_deliveries_total{result="abandoned"}`: an increase means that a notification was not delivered. When `outcome="skipped"` and `outcome="error"` keep increasing, look up the cause by `reason` in `_alert_evaluations`.

### 5.9 Not supported

- Real-time alerting (a decision as each record arrives): there is only scheduled evaluation.
- Running the evaluations missed during downtime, and sending undelivered notifications again after a restart.
- Adding, changing or deleting rules in the UI or through the HTTP interface: change the configuration file and restart.
- Built-in email, SMS and instant-messaging channels.
