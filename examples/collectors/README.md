# Collector examples

[中文](README.zh-CN.md)

> Applies to Amberlog 0.1.0.

This directory gives configurations for two common log collectors that send logs to Amberlog:

| Collector | Configuration file | Tested version |
|---|---|---|
| Vector | [`vector/vector.yaml`](vector/vector.yaml) | 0.51.1 (image `timberio/vector:0.51.1-debian`) |
| Fluent Bit | [`fluent-bit/fluent-bit.yaml`](fluent-bit/fluent-bit.yaml) | 4.1.1 (image `fluent/fluent-bit:4.1.1`) |

Both configurations do the same things:

1. Read the new lines in `/var/log/app/*.log`, starting from the end of each file.
2. Make each line one record. The original line goes into the field `message`, the file path goes into the field `file`, and the time when the line was read goes into `_timestamp`. Vector also keeps the host name of the collecting machine in `host`.
3. Encode a batch of records as one JSON array, compress it with gzip, and send it over HTTPS to the Amberlog ingestion endpoint.
4. Send the access token and the source ID with each request.
5. Retry when Amberlog is temporarily unavailable.

## What to change before use

1. **Address.** Replace `amberlog.example.internal` and the port `8800` with the address and port of your Amberlog.
2. **Dataset.** In the path `/api/default/app/_json`, replace `app` with your dataset name. A dataset name can contain only lower-case letters, digits and underscores, and must start with a letter or a digit. Amberlog creates the dataset on the first write.
3. **Log path.** Replace `/var/log/app/*.log` with your log files.
4. **CA certificate.** When Amberlog uses a self-signed certificate or a certificate issued by an internal CA, put the CA certificate at the path given in the configuration (Vector: `/etc/vector/amberlog-ca.crt`; Fluent Bit: `/fluent-bit/etc/amberlog-ca.crt`). For a self-signed certificate, the CA certificate is the server certificate itself. The subject alternative name (SAN) of the certificate must include the host name or IP address that the collector uses to connect. When you use a certificate issued by a public CA, remove this item.
5. **Two environment variables.** Set them before you start the collector:
   - `INGEST_TOKEN`: the Amberlog access token, that is, the value of the configuration key `auth.token`.
   - `SOURCE_ID`: the source ID of this machine, for example `web-1`.

**Do not start the names of these environment variables with `AMBERLOG_`.** Amberlog treats environment variables that start with `AMBERLOG_` as its own configuration keys. If you start Amberlog in the same environment, it does not recognize such a variable and refuses to start with exit code 2.

## Run

When you run the collector with Docker, mount the configuration, the CA certificate and the log directory into the container:

```sh
# Vector
docker run -d --name vector \
  -e INGEST_TOKEN="$INGEST_TOKEN" -e SOURCE_ID=web-1 \
  -v "$PWD/vector/vector.yaml:/etc/vector/vector.yaml:ro" \
  -v /path/to/amberlog-ca.crt:/etc/vector/amberlog-ca.crt:ro \
  -v /var/log/app:/var/log/app:ro \
  -v vector-data:/var/lib/vector \
  timberio/vector:0.51.1-debian --config /etc/vector/vector.yaml

# Fluent Bit
docker run -d --name fluent-bit \
  -e INGEST_TOKEN="$INGEST_TOKEN" -e SOURCE_ID=web-1 \
  -v "$PWD/fluent-bit/fluent-bit.yaml:/fluent-bit/etc/fluent-bit.yaml:ro" \
  -v /path/to/amberlog-ca.crt:/fluent-bit/etc/amberlog-ca.crt:ro \
  -v /var/log/app:/var/log/app:ro \
  -v fluent-bit-data:/var/lib/fluent-bit \
  fluent/fluent-bit:4.1.1 -c /fluent-bit/etc/fluent-bit.yaml
```

`/var/lib/vector` and `/var/lib/fluent-bit` keep the read positions. Put them on persistent volumes, so that after a restart the collector continues from the last position.

## Check that the data arrived

```sh
NOW=$(($(date +%s) * 1000000))
curl --cacert /path/to/amberlog-ca.crt -H "Authorization: Bearer $INGEST_TOKEN" \
  -d "{\"query\":{\"sql\":\"SELECT count(*) AS n FROM app\",\"start_time\":$((NOW - 3600000000)),\"end_time\":$((NOW + 60000000))}}" \
  https://amberlog.example.internal:8800/api/default/_search
```

## Rules of the ingestion endpoint

Both configurations follow the rules below. Follow them also when you change the configuration.

- **Path and method.** `POST /api/default/<dataset>/_json`.
- **Request body.** One JSON array. Each element is one record (one JSON object).
- **Authentication.** The request header `Authorization: Bearer <token>`.
- **Source ID.** The request header `X-Amberlog-Source` is optional. Its value can contain only letters, digits and `_.-`, must start with a letter or a digit, and can be at most 128 characters long. When the value does not follow these rules, Amberlog rejects the whole request with HTTP 400. Amberlog does not write the source ID into the records; it uses the source ID only to count records separately for each source.
- **Compression.** The request header `Content-Encoding` can be `gzip` or `zstd`. You can also send the body uncompressed.
- **Size.** By default the request body (after decompression) can be at most 32 MiB; a larger body is rejected with HTTP 413. The Amberlog configuration key `ingest.max_body_bytes` sets this limit.
- **Record time `_timestamp`.** It can be a number (Amberlog detects seconds, milliseconds, microseconds or nanoseconds from its size) or an RFC 3339 string with a time zone. When it is missing, Amberlog fills in the time when it received the record. By default it cannot be more than 24 hours later than the time of receipt; the configuration key `ingest.max_future` sets this limit.
- **Field names.** A field name can contain only letters, digits and `-_.`, and must start and end with a letter or a digit. Apart from `_timestamp`, field names that start with `_` are reserved, and Amberlog rejects records that contain such fields.

## Responses and retries

| Amberlog response | Meaning | What the collector does, or what to do |
|---|---|---|
| HTTP 200 | Amberlog accepted this batch. The successful records are already written to disk | Sends the next batch |
| HTTP 400 | The batch as a whole is not valid: for example it is not valid JSON, the source ID does not follow the rules, or all records were rejected | Tested: neither Vector nor Fluent Bit retries. Each drops the batch and writes an error to its own log |
| HTTP 401 | The token is wrong | Check `INGEST_TOKEN` |
| HTTP 413 | The request body is too large | Make the batches smaller |
| HTTP 503 | Amberlog is temporarily unavailable | Tested: Vector and Fluent Bit both retry the whole batch with backoff. By default Vector retries without a limit and waits at most 30 seconds between tries (the default of `retry_max_duration_secs`), until it succeeds. Fluent Bit retries at most `retry_limit` times; after that it drops the batch and writes an error to its own log. The example configuration sets 10; by Fluent Bit's backoff algorithm, using all 10 takes about 1 minute to 2 hours (estimated, not tested) |
| Connection failure | Amberlog is not running, or the network is down | Tested: same as 503 |

Note two points:

- **When some records of a batch are rejected, the response is still HTTP 200.** The `errors` field of the response body lists the index and the reason of each rejected record, but neither collector acts on it: Fluent Bit writes the response body to its own log, and Vector does not log it. The rejected records are not retried, and the collector does not report an error. You can see them in the Amberlog ingestion reconciliation table `_reconciliation`: rejected records count in `received` but not in `durable`.
- **Retries can produce duplicate records.** A batch for which Amberlog returns 503 is not written. When the collector retries the whole batch, and the earlier request was in fact already written to disk (for example, the response was lost on the network), Amberlog keeps two copies of these records. Amberlog stores records by appending and does not remove duplicates.

## Known differences and limitations

- **The record time is the collection time.** In both configurations, `_timestamp` is the time when the collector read the line, not the time written in the log line. To use the time from the log, parse the log line in the collector and write the parsed time into `_timestamp` (as an RFC 3339 string or a number).
- **Log lines in JSON format are not split into fields.** Both configurations put the whole original line into `message`. To split JSON logs into separate fields, add a parsing step in the collector (Vector: `parse_json`; Fluent Bit: the `json` parser). The field names that result must also follow the field-name rules above.
- **Vector does not read very small files yet.** Vector identifies a file by the content at its start. When a file is smaller than 256 bytes, Vector ignores it at first and writes `Currently ignoring file too small to fingerprint` to its log. Vector reads the file after it grows large enough.
- **Fluent Bit finds new files with a delay.** By default Fluent Bit scans the directory every 60 seconds (`refresh_interval`). A log file created after startup can wait up to this long before Fluent Bit starts to read it.
- **No sent counts are reported.** Amberlog ingestion reconciliation can receive the number of records sent per minute reported by the collecting side (`POST /api/default/<dataset>/_sent`) and then do a three-way reconciliation. These two collectors have no ready-made way to report this, so the examples do not report it, and the reconciliation table shows a two-way reconciliation.

## Test record

Both configurations were tested in a test environment with these steps:

1. Amberlog had TLS turned on (a self-signed certificate with the subject alternative name `127.0.0.1`). The object storage was MinIO.
2. Each collector wrote to its own dataset, with the source IDs `web-1` and `web-2`.
3. 50 lines and 30 lines were appended to two log files. Both datasets returned 80 records, and the `file` field told the two files apart.
4. Amberlog was stopped, 20 more lines were appended, and then Amberlog was restarted. Both collectors sent the data again after retries, and both datasets returned 100 records.
5. A mock server answered with HTTP 400 and with HTTP 503. This gave the results in the column "What the collector does" of the table above. In test copies of the configurations, the retry limits were then made smaller (Fluent Bit `retry_limit` 2, Vector `request.retry_attempts` 3) and the server kept answering 503, to test what happens when the retries are used up: both collectors dropped the batch and wrote an error to their logs.

Steps 1 to 4 were tested again with a build made before the release of Amberlog 0.1.0, with the same results. Step 5 depends only on the collectors themselves. It was tested again on 2026-10-24 with the same collector versions, with the same results.
