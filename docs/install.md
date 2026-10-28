# Installation and deployment guide

[中文](install.zh-CN.md)

> Applies to Amberlog 0.1.0.

This guide is for people who want to deploy Amberlog in their own environment. It covers how to install, how to connect object storage, how to give read-only credentials to external engines, how to configure TLS and a reverse proxy, and how to estimate memory. All commands in this guide were run in a test environment (Linux, with the S3-compatible storage MinIO). Each place without a test says so.

Before you deploy, read section 1 of [Known limitations and roadmap](known-limitations.md). For configuration keys, see the [Configuration reference](configuration.md). For day-to-day operations, see the [Operations guide](operations.md). If you only want to see Amberlog work first, use the [Quickstart](../examples/quickstart/README.md).

## 1. Terms used in this guide

- **Write instance**: the process that runs as `amberlog serve`. It accepts ingestion and search, and does commits, compaction, lake entry and other work in the background. On one storage (the same `object_store.url` and `storage.prefix`), only one write instance can run at a time.
- **WAL** (write-ahead log): local files under `wal.dir`. Amberlog writes an ingestion request to the WAL and calls fsync before it returns 200. After that, a background task commits the records to object storage.
- **Hot tier** and **cold tier**: records are first stored in the hot tier, in Amberlog's own format. An hour bucket **enters the lake** `lifecycle.lake_watermark` (default 72 hours) after its hour ends. It then becomes part of a standard Apache Iceberg table in the cold tier, and external engines can read it directly.
- **Read-only catalog mode**: the process that runs as `amberlog catalog`. It only provides the Iceberg REST catalog endpoints to external engines. It does not accept ingestion or search, and it uses only read-only credentials.
- **Plaintext declaration**: the configuration key `server.behind_tls_proxy = true`. It tells Amberlog that a reverse proxy in front of it handles TLS.

## 2. What to prepare before you deploy

| Item | Requirement |
|---|---|
| Operating system | Linux x86_64 or aarch64, glibc 2.28 or later (for example RHEL 8 and its derivatives, Kylin V10, Debian 10 or later, Ubuntu 20.04 or later). With the container image, the host only needs to run Docker or a compatible container engine |
| Object storage | MinIO (tested), or AWS S3 and Ceph RGW (supported through the S3-compatible API, not yet tested); see section 6. For a functional trial only, you can also use local disk |
| Local disk | Holds the WAL. When there is a backlog, the WAL grows; its size is about the volume of the backlogged records. The disk must be reliable: records that are acknowledged but not yet committed exist only here |
| Memory | See section 9. The largest item is cold compaction, which grows with one day of data of a single dataset |
| Network | The write instance listens on port 8800 by default. The read-only catalog mode uses another port |

## 3. Three installation methods

| Method | Suitable for | See |
|---|---|---|
| Binary | Physical machines and virtual machines, managed with systemd | Section 4 |
| Container image | Environments that already have a container platform | Section 5 |
| Compose file | A trial on your own computer, not for production | [Quickstart](../examples/quickstart/README.md) |

All three methods use the same executable, `amberlog`, and exactly the same configuration keys.

## 4. Install with the binary

The examples below use version 0.1.0 on x86_64. For aarch64, replace `x86_64` in the file names with `aarch64`. Run the commands as root.

In the test, the checksum verification, unpacking, configuration file and startup checks were all done, with an ordinary user and the same configuration file. The two steps that create the user (`useradd`) and use `systemctl` were not run.

### 4.1 Download and verify

1. Download the package and the checksum file from the GitHub Release page:

   ```sh
   VERSION=0.1.0
   BASE=https://github.com/visdata-com/amberlog/releases/download/v$VERSION
   curl -LO "$BASE/amberlog-$VERSION-x86_64-unknown-linux-gnu.tar.gz"
   curl -LO "$BASE/SHA256SUMS"
   ```

2. Verify the package. The output must be `OK`:

   ```sh
   sha256sum -c --ignore-missing SHA256SUMS
   ```

   ```text
   amberlog-0.1.0-x86_64-unknown-linux-gnu.tar.gz: OK
   ```

3. Unpack it. The package contains the `amberlog` executable and `README.md`, `README.zh-CN.md`, `LICENSE`, `NOTICE`, `THIRD-PARTY-NOTICES.txt` and `CHANGELOG.md`:

   ```sh
   tar -xzf amberlog-$VERSION-x86_64-unknown-linux-gnu.tar.gz
   ```

### 4.2 Install the files and create the user

1. Install the executable and check the version:

   ```sh
   install -m 0755 amberlog-$VERSION-x86_64-unknown-linux-gnu/amberlog /usr/local/bin/amberlog
   amberlog --version
   ```

2. Create a dedicated system user. Amberlog does not need root privileges:

   ```sh
   useradd --system --home-dir /var/lib/amberlog --shell /usr/sbin/nologin amberlog
   ```

3. Create the WAL directory and the configuration directory:

   ```sh
   install -d -o amberlog -g amberlog -m 0700 /var/lib/amberlog /var/lib/amberlog/wal
   install -d -o root -g amberlog -m 0750 /etc/amberlog
   ```

### 4.3 Generate a token

The token is the only credential for access to Amberlog. Whoever has it can read and write all data. Use a long random value. Do not use the values in the examples:

```sh
openssl rand -hex 32
```

Write the token into the configuration file, and make the file readable only by the user that runs Amberlog (section 4.4). Do not put the token in command-line arguments. Avoid environment variables where you can: other processes of the same user can see environment variables, and in a container `docker inspect` can see them too. To change the token, restart the process.

### 4.4 Write the configuration file

Write `/etc/amberlog/amberlog.toml`. The example below uses MinIO and in-process TLS. For other object-storage configurations, see section 6. For other ways to do TLS, see section 8. Replace the values in angle brackets with your own:

```toml
[server]
http_addr = "0.0.0.0:8800"

[server.tls]
cert_file = "/etc/amberlog/tls/tls.crt"
key_file = "/etc/amberlog/tls/tls.key"

[auth]
token = "<token generated in section 4.3>"

[wal]
dir = "/var/lib/amberlog/wal"

[object_store]
url = "s3://logs"

[object_store.s3]
endpoint = "https://minio.example.com:9000"
force_path_style = true
conditional_put = "create_only"
access_key_id = "amberlog-writer"
secret_access_key = "<Amberlog's secret access key>"
```

Set the file permissions:

```sh
chown root:amberlog /etc/amberlog/amberlog.toml
chmod 0640 /etc/amberlog/amberlog.toml
```

Put the certificate and the private key under `/etc/amberlog/tls/`. Set the private key to `root:amberlog` and `0640` as well.

For a trial without a certificate from your own CA, you can make a test CA and a server certificate with openssl. Put every name that clients use to connect into `subjectAltName`. Do not use these certificates in production:

```sh
cd /etc/amberlog/tls
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
  -subj "/CN=Amberlog test CA" -keyout ca.key -out ca.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=amberlog.example.com" -keyout tls.key -out tls.csr
printf 'subjectAltName=DNS:amberlog.example.com,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > ext.cnf
openssl x509 -req -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 \
  -extfile ext.cnf -out tls.crt
rm -f tls.csr ext.cnf ca.srl
chown root:amberlog tls.crt tls.key && chmod 0640 tls.key && chmod 0600 ca.key
```

Clients must then trust `ca.crt`, for example `curl --cacert /etc/amberlog/tls/ca.crt https://localhost:8800/readyz`. If the trial runs on one machine only, you can also skip TLS and listen on `127.0.0.1` (section 8.1).

### 4.5 systemd unit

Write `/etc/systemd/system/amberlog.service`:

```ini
[Unit]
Description=Amberlog
Documentation=https://github.com/visdata-com/amberlog
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=amberlog
Group=amberlog
ExecStart=/usr/local/bin/amberlog serve --config /etc/amberlog/amberlog.toml
# Exit code 2 is a configuration error; a restart does not help.
Restart=on-failure
RestartPreventExitStatus=2
RestartSec=5s
# Greater than server.shutdown_timeout (default 10 seconds), to leave time to commit all acknowledged records.
TimeoutStopSec=60s
LimitNOFILE=65536
# Set this from the machine's memory and the estimate in section 9.
MemoryMax=40G

[Install]
WantedBy=multi-user.target
```

Why these lines:

- `LimitNOFILE=65536`: the systemd default soft limit is 1024. With about 1000 concurrent connections, the WAL cannot open new segment files, and ingestion returns 503.
- `MemoryMax`: sets a memory limit for the process, so that it cannot bring down the whole machine.

This unit file was checked with `systemd-analyze verify`. It was not actually started as a system service.

### 4.6 Start and check

1. Start the service:

   ```sh
   systemctl daemon-reload
   systemctl enable --now amberlog
   ```

2. Read the startup log. The log goes to stderr, one JSON object per line:

   ```sh
   journalctl -u amberlog -n 20
   ```

   You must see these lines (sample output, an excerpt; each line keeps only some of its fields; start-up under systemd was not tested):

   ```text
   {"level":"info","msg":"starting amberlog","event":"starting","version":"0.1.0"}
   {"level":"info","msg":"open file limit is 65536","soft":65536,"hard":65536}
   {"level":"info","msg":"amberlog is ready","event":"ready","version":"0.1.0","http_addr":"0.0.0.0:8800","object_store":"s3://logs","mode":"serve","tls":true}
   ```

   - The value of `open file limit is` is not lower than 65536.
   - In the `ready` line, `tls` is `true` (when you use in-process TLS).

3. Check readiness. The client must trust the CA that issued the certificate:

   ```sh
   curl --cacert /etc/amberlog/tls/ca.crt https://amberlog.example.com:8800/readyz
   ```

   ```text
   {"status":"ready"}
   ```

4. Write one record, then search for it. `$TOKEN` is the token. Search times are in microseconds:

   ```sh
   now=$(($(date +%s) * 1000))
   curl --cacert /etc/amberlog/tls/ca.crt -X POST \
     "https://amberlog.example.com:8800/api/default/app/_json" \
     -H "Authorization: Bearer $TOKEN" \
     -d "[{\"_timestamp\": $now, \"level\": \"info\", \"message\": \"installed\"}]"
   ```

   ```text
   {"code":200,"status":[{"name":"app","successful":1,"failed":0,"errors":[]}]}
   ```

   ```sh
   curl --cacert /etc/amberlog/tls/ca.crt -X POST \
     "https://amberlog.example.com:8800/api/default/_search" \
     -H "Authorization: Bearer $TOKEN" \
     -d "{\"query\": {\"sql\": \"SELECT level, message FROM app\",
          \"start_time\": $(( (now - 3600000) * 1000 )), \"end_time\": $(( (now + 60000) * 1000 ))}}"
   ```

   The `hits` in the result contain the record you just wrote.

If startup fails, use the exit code to find the cause:

| Exit code | Meaning |
|---|---|
| 2 | Configuration error. stderr names the configuration key that is wrong |
| 3 | The WAL directory or the object storage is not available. This includes an object-storage certificate that is not trusted and credentials that are rejected |
| 4 | Amberlog cannot listen on the listen address, for example because the port is already in use |
| 1 | Other startup failures |

### 4.7 Shutdown and upgrade

- `systemctl stop amberlog` sends SIGTERM. Amberlog stops accepting new requests, waits for in-flight requests to finish, commits the acknowledged records to object storage, and then exits with exit code 0.
- Upgrade: read the changelog of the new version, stop the service, replace `/usr/local/bin/amberlog`, and start it again. For the rules on data format and upgrades, see section 6.2 of [Known limitations and roadmap](known-limitations.md). For the steps, see section 5 of the [Operations guide](operations.md).

## 5. Use the container image

The image is in the GitHub Container Registry, for `linux/amd64` and `linux/arm64`:

```text
ghcr.io/visdata-com/amberlog:<version>
```

Key points of the image:

- The entry point is `amberlog`. The default command is `serve`.
- It runs as a non-root user. The user ID and group ID are both 10001.
- The working directory is `/var/lib/amberlog`, owned by user 10001. Put the WAL under it, on a mounted volume.
- It declares port 8800.
- The image itself sets no `AMBERLOG_` environment variables.

Steps:

1. Write the configuration file on the host (section 4.4). Set `wal.dir` to `/var/lib/amberlog/wal`. Write certificate paths as paths inside the container, for example `/etc/amberlog/tls/tls.crt`.
2. Change the owner of the configuration directory to 10001. Otherwise the user in the container cannot read the `0600` files, and startup reports `cannot read config file: Permission denied`:

   ```sh
   chown -R 10001:10001 /etc/amberlog
   chmod 0600 /etc/amberlog/amberlog.toml /etc/amberlog/tls/tls.key
   ```

3. Start the container:

   ```sh
   docker run -d --name amberlog \
     -p 8800:8800 \
     --ulimit nofile=65536:65536 \
     --stop-timeout 60 \
     -v /etc/amberlog:/etc/amberlog:ro \
     -v amberlog-wal:/var/lib/amberlog \
     ghcr.io/visdata-com/amberlog:0.1.0 \
     serve --config /etc/amberlog/amberlog.toml
   ```

   - `--ulimit nofile`: same reason as in section 4.5.
   - `--stop-timeout 60`: by default, `docker stop` waits only 10 seconds and then kills the process.
   - The token is in the configuration file. Do not pass it with `-e`, so that it does not show in `docker inspect`.

4. Check the log, readiness and ingestion as in section 4.6. Use `docker logs amberlog` for the log.

Measured: the container ran as user 10001, the log showed `open file limit is 65536`, `tls` in the `ready` line was `true`, and search worked. After `docker stop -t 30`, the exit code was 0.

On Kubernetes, mount the configuration file and the certificates from a Secret, use a persistent volume for the WAL, and set `terminationGracePeriodSeconds` to 60 or more. Only one write instance can run on one storage, so the replica count is 1 and the update strategy is `Recreate`. This paragraph was not tested on Kubernetes.

## 6. Object storage

### 6.1 Requirements

- Amberlog supports MinIO, AWS S3, Ceph RGW and local disk (`file://`). It was tested on MinIO and local disk. AWS S3 and Ceph RGW are supported through the S3-compatible API, but have not been tested yet. Huawei Cloud OBS, Alibaba Cloud OSS and Tencent Cloud COS are not supported.
- The storage must support "create only if it does not exist", implemented with the request header `If-None-Match: *`. After your operations team confirms this, write `conditional_put = "create_only"` in the configuration (required for `serve` and `retire` with `s3://`).
- At startup, Amberlog probes this once: it writes the same new key twice under `<storage.prefix>/_system/`. If the storage does not support it, Amberlog exits with exit code 2.
- Use an `https://` endpoint. With `http://`, Amberlog works as usual, but it writes an `object_store_plaintext` warning at startup.
- If the object-storage certificate is issued by an internal enterprise CA, put the CA certificate in `object_store.s3.ca_file`. Amberlog always verifies the object-storage certificate. There is no configuration key to skip verification.

### 6.2 Amberlog's own credentials

The credentials you give Amberlog grant only its own prefix. The policy below is for the storage bucket `logs` and the default `storage.prefix` (`amberlog`). If you set `object_store.url` to `s3://logs/<path>`, replace `amberlog/` in the policy with `<path>/amberlog/`.

```json
{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow",
   "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
              "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
   "Resource": ["arn:aws:s3:::logs/amberlog/*"]},
  {"Effect": "Allow", "Action": ["s3:ListBucket"],
   "Resource": ["arn:aws:s3:::logs"],
   "Condition": {"StringLike": {"s3:prefix": ["amberlog/*"]}}},
  {"Effect": "Allow", "Action": ["s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
   "Resource": ["arn:aws:s3:::logs"]}
]}
```

Measured (MinIO): with credentials that have this policy, the write instance started, and ingestion, search, lake entry and deletion at expiry all worked. Writes outside the prefix and listing the root of the storage bucket were rejected.

### 6.3 MinIO

The test used MinIO `RELEASE.2025-09-07T16-13-09Z`. The MinIO project has archived its repository and no longer publishes images or binaries, so this version can now only be built from source.

The steps of this section and of section 6.2 were tested again on Silo `RELEASE.2026-10-18T00-00-00Z` (image `pgsty/silo`), a community fork of MinIO maintained by PGSTY, with the same commands. That test covered startup, ingestion, lake entry, the rejection of writes outside the prefix and of listing the root of the storage bucket, versioning, and `mc ilm rule add`. It did not cover deletion at expiry. The [Quickstart](../examples/quickstart/README.md) also uses Silo.

The commands below use the MinIO client `mc`. The `pgsty/silo` image includes it; it is also available as the image `pgsty/mc`. First register your MinIO under the alias `myminio`, with the MinIO administrator credentials:

```sh
mc alias set myminio https://minio.example.com:9000 <administrator user> '<administrator password>'
```

1. Create the storage bucket and enable versioning (for the reason, see section 6.6):

   ```sh
   mc mb myminio/logs
   mc version enable myminio/logs
   ```

2. Create a user. Save the policy from section 6.2 as `amberlog-writer.json` and attach it to this user:

   ```sh
   mc admin user add myminio amberlog-writer '<secret access key>'
   mc admin policy create myminio amberlog-writer amberlog-writer.json
   mc admin policy attach myminio amberlog-writer --user amberlog-writer
   ```

3. Configure Amberlog:

   ```toml
   [object_store]
   url = "s3://logs"

   [object_store.s3]
   endpoint = "https://minio.example.com:9000"
   force_path_style = true
   conditional_put = "create_only"
   access_key_id = "amberlog-writer"
   secret_access_key = "<secret access key>"
   ```

### 6.4 AWS S3

1. Create the storage bucket and enable versioning.
2. Create an IAM user or role, and grant it the policy from section 6.2.
3. Configure Amberlog. Do not set `endpoint`. Set `region` to the region of the storage bucket. Keep `force_path_style` at its default, `false`:

   ```toml
   [object_store]
   url = "s3://logs"

   [object_store.s3]
   region = "<region of the bucket>"
   conditional_put = "create_only"
   ```

4. Credentials: in EC2 and similar environments, do not set `access_key_id` and `secret_access_key`. Amberlog then gets credentials from the AWS credential environment variables and the credential chain (section 7 of the [Configuration reference](configuration.md)).

This section was not tested on AWS S3.

### 6.5 Ceph RGW

Ceph RGW is supported through the S3-compatible API, but has not been tested yet. At startup, the conditional-write probe (section 6.1) checks in your own environment whether the storage supports `If-None-Match: *`.

1. Create an RGW user and a storage bucket, and enable versioning.
2. Restrict this user with a bucket policy: add `"Principal": {"AWS": ["arn:aws:iam:::user/<user name>"]}` to the policy from section 6.2, and set it on the storage bucket as a bucket policy.
3. Configure Amberlog as for MinIO (section 6.3). Set `endpoint` to the RGW address and `force_path_style = true`.

This section was not tested on Ceph RGW.

### 6.6 Versioning and lifecycle rules

- We recommend that you enable versioning on the storage bucket. If someone deletes or changes objects in the storage bucket without going through Amberlog, the old versions remain and you can recover them. Amberlog detects external deletes and changes only at limited points in time (section 6.1 of [Known limitations and roadmap](known-limitations.md)).
- Measured (MinIO): with versioning enabled, Amberlog started, ingested and deleted as usual. A delete leaves only a delete marker; the old version stays in the storage bucket. In the test there were 18 current objects and 43 versions in total.
- So configure a lifecycle rule that removes noncurrent versions after a period, for example 30 days. The period must be longer than the time within which you may need to recover data. For MinIO, the command is `mc ilm rule add --noncurrent-expire-days 30 myminio/logs`. AWS S3 and Ceph use their own lifecycle configuration. This `mc` command ran successfully on the test bucket; the test did not wait long enough to see the rule remove expired versions.
- Give write access to the object storage only to Amberlog (section 6.2).

### 6.7 Server-side encryption

- `object_store.s3.server_side_encryption = "sse_s3"`: every write request carries `x-amz-server-side-encryption: AES256`.
- `"sse_kms"`: requests carry `aws:kms`. If you also set `sse_kms_key_id`, it selects the key; if not, the storage provider's default key is used.
- If the storage rejects writes that carry the encryption header (for example because the key service is not available), `serve` exits with exit code 3 at startup.
- By default, MinIO and Ceph accept requests with the encryption header only over TLS connections, so the endpoint must use `https://`.
- With SSE-KMS, the read-only credentials for external engines and for audit also need permission to decrypt with that key.
- SSE-C is not supported.

The tests in this guide did not enable server-side encryption.

### 6.8 Local disk

- `object_store.url = "file:///var/lib/amberlog/data"`. The path must be absolute. Create the directory first; otherwise Amberlog exits with exit code 3.
- Suitable only for a functional trial. If the storage directory is moved away while Amberlog runs, or its volume is unmounted, `/readyz` does not report not-ready (section 8 of the [Operations guide](operations.md)).

## 7. Read-only credentials for external engines

External engines read the cold-tier tables in object storage directly, without going through Amberlog. External engines must use read-only credentials. Amberlog's catalog endpoints do not provide table creation or table dropping, but they cannot stop an engine from writing data files into the storage bucket first. Only read-only credentials can stop that. You must configure this step yourself on the object storage. For how each engine uses these credentials, see the [External engine guide](external-engines.md).

### 7.1 Scope of the grant

The read-only credentials grant only the prefix of the cold-tier tables, `<storage.prefix>/_iceberg/`. Do not grant the hot tier:

```json
{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow", "Action": ["s3:GetObject"],
   "Resource": ["arn:aws:s3:::logs/amberlog/_iceberg",
                "arn:aws:s3:::logs/amberlog/_iceberg/*"]},
  {"Effect": "Allow", "Action": ["s3:ListBucket"],
   "Resource": ["arn:aws:s3:::logs"],
   "Condition": {"StringLike": {"s3:prefix": ["amberlog/_iceberg", "amberlog/_iceberg/", "amberlog/_iceberg/*"]}}},
  {"Effect": "Allow", "Action": ["s3:GetBucketLocation"],
   "Resource": ["arn:aws:s3:::logs"]}
]}
```

- The prefix key `amberlog/_iceberg` itself (without the trailing slash) must also be readable and listable. The hadoop-type catalog of some engines sends requests for it; without this item, those engines cannot list the namespaces. The prefix key is not an object, so granting it gives access to no additional content.
- With SSE-KMS, also add permission to decrypt with that key.

On MinIO, save the policy above as `lake-reader.json`, then create the user and attach the policy (`myminio` is the alias from section 6.3):

```sh
mc admin user add myminio lake-reader '<secret access key>'
mc admin policy create myminio lake-reader lake-reader.json
mc admin policy attach myminio lake-reader --user lake-reader
```

Measured (MinIO): credentials with this policy could list the files of the cold-tier tables. Listing the hot-tier prefix and writing objects into the cold-tier prefix were rejected.

### 7.2 Read-only catalog mode

A token that you give to an external engine can also ingest and search on the write instance. To narrow the permissions, start a separate process in read-only catalog mode, and give it another token and the read-only credentials from section 7.1:

1. Write `/etc/amberlog/catalog.toml`:

   ```toml
   [server]
   http_addr = "0.0.0.0:8801"

   [server.tls]
   cert_file = "/etc/amberlog/tls/tls.crt"
   key_file = "/etc/amberlog/tls/tls.key"

   [auth]
   token = "<another generated token>"

   [object_store]
   url = "s3://logs"

   [object_store.s3]
   endpoint = "https://minio.example.com:9000"
   force_path_style = true
   access_key_id = "lake-reader"
   secret_access_key = "<secret access key of the read-only credentials>"
   ```

   `storage.prefix` must be the same as on the write instance. `wal.dir` and `conditional_put` are not needed.

2. Start it:

   ```sh
   amberlog catalog --config /etc/amberlog/catalog.toml
   ```

   In the `ready` line, `mode` is `catalog`.

3. List the tables:

   ```sh
   curl --cacert /etc/amberlog/tls/ca.crt \
     -H "Authorization: Bearer $CATALOG_TOKEN" \
     https://amberlog.example.com:8801/iceberg/v1/amberlog/namespaces/default/tables
   ```

   ```text
   {"identifiers":[{"namespace":["default"],"name":"app"}]}
   ```

Measured: the read-only catalog mode started with the read-only credentials and listed the tables. Access with the write instance's token returned 401. A search request to it returned 404. It can run at the same time as the write instance.

To manage it with systemd, write another unit as in section 4.5, and change `ExecStart` to `amberlog catalog --config /etc/amberlog/catalog.toml`.

## 8. TLS and secure deployment

### 8.1 Choose how to listen

| Method | Configuration | When to use |
|---|---|---|
| Listen on the local machine only | `server.http_addr = "127.0.0.1:8800"` | A trial on the local machine only; or the reverse proxy and Amberlog are on the same machine |
| In-process TLS | Set `server.tls.cert_file` and `server.tls.key_file` | Clients connect to Amberlog directly |
| TLS terminated by a reverse proxy | No certificate; `server.behind_tls_proxy = true` | A reverse proxy or gateway is already in front |

- The default listen address `0.0.0.0:8800` is not a loopback address. If you set neither a certificate nor the plaintext declaration, `serve` and `catalog` refuse to start, with exit code 2.
- If you set both a certificate and the plaintext declaration, Amberlog also exits with exit code 2.
- With the plaintext declaration, the startup log has a `plaintext_listen` warning. Amberlog does not check whether a reverse proxy is really in front.

### 8.2 In-process TLS

- Only TLS 1.2 and TLS 1.3 are accepted. After you set a certificate, requests sent as plain HTTP get no HTTP response at all.
- `/healthz`, `/readyz` and `/metrics` also answer only over TLS. Health probes and Prometheus must change to `https://` and trust the CA that issued the certificate. `/metrics` needs the token; Prometheus reads a token file with `authorization.credentials_file` (section 3.1 of the [Operations guide](operations.md)).
- The certificate file is PEM, with the leaf certificate first; intermediate certificates can follow. The private key must not have a passphrase. For the accepted private-key algorithms, see "Inbound TLS" in the [Configuration reference](configuration.md).
- You can replace the certificate and the private key while Amberlog runs, without a restart. The process reads the two files at least every 60 seconds, and new connections use the new certificate after 120 seconds at most. If the new content is not valid, Amberlog keeps the old certificate and writes `tls_reload_failed`. To replace them, first write the two new files, then rename them over the old files together, so that Amberlog does not read one new file and one old file.
- There is no metric for certificate expiry. Monitor the expiry time with an external probe.

### 8.3 Reverse proxy

When Amberlog faces a larger network directly, put a reverse proxy in front of it. In the current release, the reverse proxy must handle connection time limits, connection counts, request rates and security response headers (section 2.2 of [Known limitations and roadmap](known-limitations.md)).

When Amberlog and the proxy are on the same machine, Amberlog listens only on the loopback address and does not need the plaintext declaration. When they are not on the same machine, Amberlog listens on an internal network address and you write the plaintext declaration. That network segment is still plaintext, and clients must not bypass the proxy to connect to Amberlog directly.

An nginx example (Amberlog's `server.http_addr` is set to `127.0.0.1:8802`):

```nginx
limit_req_zone  $binary_remote_addr zone=amberlog_req:10m rate=50r/s;
limit_conn_zone $binary_remote_addr zone=amberlog_conn:10m;

server {
    listen 443 ssl;
    server_name amberlog.example.com;

    ssl_certificate     /etc/nginx/tls/tls.crt;
    ssl_certificate_key /etc/nginx/tls/tls.key;
    ssl_protocols       TLSv1.2 TLSv1.3;

    # Equal to or smaller than ingest.max_body_bytes (default 32 MiB).
    client_max_body_size  32m;
    # Time limits for reading the request header and body, and for idle connections.
    client_header_timeout 30s;
    client_body_timeout   60s;
    keepalive_timeout     75s;
    # Connection count and request rate per source address. Adjust to the number of your collectors.
    limit_conn amberlog_conn 100;
    limit_req  zone=amberlog_req burst=200 nodelay;

    add_header X-Content-Type-Options nosniff always;
    add_header X-Frame-Options DENY always;
    add_header Referrer-Policy no-referrer always;
    add_header Strict-Transport-Security "max-age=31536000" always;

    location / {
        proxy_pass http://127.0.0.1:8802;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        # A search over a large time range can take several minutes.
        proxy_read_timeout 300s;
    }
}
```

- Forward the `Authorization` request header unchanged. nginx forwards it by default.
- Do not record the `Authorization` request header in the access log. The default nginx log format does not record it.
- The rate and connection-count values are only examples. When many collectors send through the same egress address, raise them.

Measured (nginx 1.31.2, listening on 8443): through the proxy, readiness, ingestion, search and `/ui` all worked. All four security response headers were in the responses. A request without the token returned 401. A 34 MB request body got 413 from the proxy. The access log did not contain `Authorization`.

### 8.4 Log events to alert on

The events below are stable log events. The event name is in the `event` field of the JSON log, and `log.level` does not filter them. When one appears, the deployment has moved away from the secure practice:

| Event | Meaning |
|---|---|
| `auth_disabled` | Authentication is turned off (`auth.enabled = false`), so anyone can access Amberlog. Use this only for development on the local machine |
| `plaintext_listen` | Amberlog listens with plaintext on a non-loopback address |
| `object_store_plaintext` | The object-storage endpoint is `http://` |
| `tls_reload_failed` | The replacement certificate or private key is not valid; the old certificate is still in use |

For the other alerts, see section 3 of the [Operations guide](operations.md).

### 8.5 Other recommendations

- Mutual TLS is not supported. If you need client-certificate authentication, let the reverse proxy verify the certificates.
- The Chinese national cryptographic (SM) transport protocols are not supported. If you need them, use an SM gateway at the network boundary. Between the gateway and Amberlog, use ordinary TLS or the plaintext declaration.
- Write the tokens for collectors, Prometheus and external engines in their own configuration files, not in command-line arguments.
- The search UI `/ui` opens without a token. The page itself contains no data; you must enter the token to search.

## 9. Capacity planning

### 9.1 Memory

Estimate the process memory as the sum of the items below, then add a margin:

| Item | How to estimate |
|---|---|
| Ingestion backlog | At most `ingest.max_backlog_memory_bytes`, default 256 MiB |
| Search | `query.max_concurrency` (default 4) × the peak of a single query. In a measurement, one full-text search over a 160-hour range used about 1 GB, so reserve about 4 GB for the default configuration |
| Seal compaction | Not measured separately. For reference: in a measurement with 20,000 records per second across 4 datasets, the highest anonymous resident memory (RssAnon) of the whole process was about 740 MiB. It occurred when the first hour bucket was sealed (section 4.3 of [Known limitations and roadmap](known-limitations.md)) |
| Lake entry | About 2.4 times one hour of raw data of a single dataset |
| Cold compaction | About 2.4 times one day of raw data of a single dataset (an estimate from the lake-entry ratio). This is the largest item |

With a process memory limit of 40 GB, the limit for a single dataset is about 15 GB of raw data per day (an estimate). With less available memory, the limit is lower: about "memory available to cold compaction ÷ 2.4".

- "Raw data" is the number of bytes of the JSON records that the collectors send.
- The cache of the hot-tier inspection grows with uptime. Reserve an additional margin for it, and monitor it every day (section 4.3 of [Known limitations and roadmap](known-limitations.md)).
- If the data volume of a single source is above the limit, split it and write it into several datasets. The limit applies to each dataset separately.

The measured figures above come from single measurements on one virtual machine with 16 cores and 46 GB of memory, on development builds before the 0.1.0 release. The object storage was a single-disk MinIO on the same machine. They are not capacity commitments. Before you go into production, measure once on your own machines, object storage and logs.

### 9.2 Throughput

The highest sustained ingestion rate measured: about 130,000 records per second with 4 datasets written at the same time, and about 80,000 records per second when only 1 dataset is written (500 bytes per record). These are measured values, not commitments. The test environment is the same as in the previous section. For details, see section 4.1 of [Known limitations and roadmap](known-limitations.md).

### 9.3 Object-storage capacity

In addition to the data itself, count the manifest history of the hot tier and the metadata of the cold tier. The manifest history is not reclaimed within `hot_retention.age` (default 168 hours); see sections 4.4 and 4.5 of [Known limitations and roadmap](known-limitations.md). First write for a few hours in your own way of writing, then measure how many bytes each dataset uses.

After an hour bucket is sealed, Amberlog merges the small files of similar size in that hour bucket. The files that were merged away are physically deleted only after a further `storage.delete_delay` (default 1 hour). So for some time after sealing, the space used by the hot-tier data files temporarily increases. In the high-write-volume measurement described in sections 4.3 and 4.4 of [Known limitations and roadmap](known-limitations.md), the space used by the hot-tier data files during the run was at most about 1.65 times the space at the end (read every 5 minutes; even if all data written between two readings is added as an estimate, it does not exceed 2 times).

### 9.4 File descriptors

Set the process file descriptor limit to 65536 or higher (`LimitNOFILE` for systemd, `--ulimit nofile` for containers). At startup, Amberlog raises the soft limit to the hard limit and writes `open file limit is <N>`. If the value is lower than 65536, an additional warning line follows.

## 10. Checks before going into production

- [ ] The token is a long random value, written only in a configuration file with permissions `0640` or `0600`.
- [ ] Amberlog uses in-process TLS, or sits behind a reverse proxy that terminates TLS. The startup log has no unexpected `plaintext_listen`.
- [ ] When Amberlog faces a larger network directly, the reverse proxy handles connection time limits, rate limits and security response headers.
- [ ] The object-storage endpoint uses `https://`. The log has no `object_store_plaintext`.
- [ ] Amberlog's credentials grant only its own prefix. The storage bucket has versioning enabled and a lifecycle rule that removes noncurrent versions.
- [ ] External engines use read-only credentials that grant only the `_iceberg/` prefix. If needed, a separate read-only catalog mode process runs.
- [ ] The WAL is on a reliable local disk.
- [ ] `open file limit is` is not lower than 65536.
- [ ] Memory is estimated as in section 9, a process memory limit is set, and resident memory is monitored every day.
- [ ] Alerts are set up as in section 3 of the [Operations guide](operations.md), including the four events in section 8.4.
