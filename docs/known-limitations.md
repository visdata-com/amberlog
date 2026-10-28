# Known limitations and roadmap

[中文](known-limitations.zh-CN.md)

> Applies to Amberlog 0.1.0.

This document is for people who plan to try Amberlog: what this release cannot do yet, what to do when you meet a known limitation, and the direction for later releases. Read section 1 before you deploy, and then decide how far to use Amberlog in your trial.

The configuration keys are in the [Configuration reference](configuration.md). The HTTP interface is in the [API reference](api.md). Day-to-day operations are in the [Operations guide](operations.md). Known security advisories for dependencies are in [SECURITY.md](../SECURITY.md).

## 1. Read these first

- **There is no permission model.** Everyone who has the token can read and write all data (section 2.1). Use Amberlog only on an internal network, or put a reverse proxy with access control in front of it.
- **One process, no high availability.** There is only one copy of the local WAL (section 3.1). The disk that holds the WAL must be reliable.
- **During long runs, memory use rises, and the manifests in object storage keep increasing** (section 4). Before you go into production, apply the recommendations in section 4 and monitor every day.
- **External engines can read only the cold tier.** The most recent data is in the hot tier, and external engines cannot read it (section 5.1).
- **Only AWS S3, MinIO, Ceph RGW and local disk are supported. Of these object storages, only MinIO was tested; AWS S3 and Ceph RGW have not been tested yet** (section 3.3).

## 2. Access control and security

### 2.1 Only one token, no permission model

- Amberlog accepts only one fixed Bearer token, the configuration key `auth.token`.
- Everyone who has this token can do everything: write to any dataset, search any dataset, and see the full list of data assets in the ledger.
- There are no roles, no read-only tokens, no per-dataset authorization and no single sign-on.
- With `auth.enabled = false`, anyone who can connect to the port can do all of these things.
- One process serves only one organization, `default`. To keep several organizations apart, deploy one Amberlog for each organization.
- External engines read the cold-tier tables in object storage directly, without going through Amberlog. The credentials on the object storage decide what an external engine can read. This is a design boundary, and it does not change in later releases.

**Recommendations:**

- Use Amberlog only on an internal network. When it faces a larger network directly, put a reverse proxy with access control in front of it.
- Use a long, randomly generated token, for example the output of `openssl rand -hex 32`. Do not use the token from the examples.
- Give external engines a separate read-only credential that allows reading only the prefix of the cold-tier tables.

### 2.2 Protections that the program itself still lacks

| What happens | What to do |
|---|---|
| There is no minimum token length. Amberlog starts even with a very short token, and gives no warning at startup | Generate the token as recommended in section 2.1 |
| Connections have no time limit. A client without a token can also keep a connection open for as long as it wants | When Amberlog listens on an outside-facing address directly, put a reverse proxy in front of it, and let the proxy limit connection duration, number of connections and request rate |
| Responses do not have security response headers. Other websites can embed the search UI `/ui` | Put the UI behind a reverse proxy, and let the proxy add response headers such as `X-Content-Type-Options`, `X-Frame-Options` (or `frame-ancestors` in `Content-Security-Policy`) and `Referrer-Policy` |
| There is no limit on failed authentication attempts | Let the reverse proxy limit them by source address |

The first three items are to be fixed in the next stage (section 7.1). The fourth item is left to the reverse proxy. There is no plan to do it in the program itself.

### 2.3 Encryption in transit

- Inbound connections support TLS 1.2 and TLS 1.3. You can replace the certificate while the process runs. See "Inbound TLS" in the [Configuration reference](configuration.md).
- Client certificates are not verified (there is no mutual TLS). When you need certificate authentication, let the reverse proxy verify the client certificates.
- The Chinese national cryptography transport protocol (TLCP) is not supported. When you need it, let a national-cryptography gateway at the network boundary handle it.

## 3. Deployment

### 3.1 One process, no high availability

- On the same storage, only one write instance (`amberlog serve`) can run at a time. There are no multiple nodes and no automatic failover.
- Amberlog first writes an ingestion request to the local WAL and runs fsync, and only then returns 200. After that, the background commits the records to object storage.
- However the process exits (including `kill -9` and crashes), acknowledged records are not lost and not duplicated, provided that you restart with the same `wal.dir`.
- The WAL has only one copy. Records that are acknowledged but not yet committed to object storage exist only in the WAL. If the disk that holds the WAL is damaged or lost at that time, these records are lost. Normally this period is about one `flush.interval` (default 5 seconds) plus the time of one commit. It is longer when commits have a backlog or object storage is not available.
- Object storage is the only long-term location. Amberlog is not "fully stateless": the local WAL is the state described above.

**Recommendation:** put the WAL on a reliable local disk. Use `amberlog_manifest_commits_total{result="failed"}` and the WAL backlog alert (section 3.2 of the [Operations guide](operations.md)) to find stalled commits early.

### 3.2 Release platforms

- Release binaries exist only for Linux x86_64 and Linux aarch64. They require glibc 2.28 or later (for example RHEL 8 and its derivatives, Kylin V10, Debian 10 or later, Ubuntu 20.04 or later).
- Container images exist for `linux/amd64` and `linux/arm64`.
- There are no binaries for Windows or macOS.

### 3.3 Supported object storage

- MinIO and local disk (`file://`) are supported, and both were tested.
- AWS S3 and Ceph RGW are supported through the S3-compatible API, but have not been tested yet. Try them once in your own environment before you use them.
- The storage must support "create only if it does not exist", implemented with the request header `If-None-Match: *`. `serve` and `retire` probe this once at startup. If the storage does not support it, they do not start ("Conditional-write probe" in the [Configuration reference](configuration.md)).
- Huawei Cloud OBS, Alibaba Cloud OSS and Tencent Cloud COS are not supported and have not been tested.
- With local disk storage, when the storage directory is moved away or its volume is unmounted while the process runs, `/readyz` does not report not-ready (section 8 of the [Operations guide](operations.md)). Protect the storage volume with a mount check.

### 3.4 Ingestion protocol

- There is only Amberlog's own JSON ingestion endpoint `_json` (section 2 of the [API reference](api.md)). Amberlog does not accept OTLP, Elasticsearch `_bulk`, Loki, syslog or other protocols.
- Existing collectors must be changed to send in this format. The [Collector examples](../examples/collectors/README.md) give configurations for Vector and Fluent Bit. Both were tested.

## 4. Capacity and long runs

Unless noted otherwise, the numbers in this section were all measured on one test machine: a virtual machine with 16 cores and 46 GB of memory, running Linux; the object storage was a single-disk MinIO on the same machine; the process memory limit was 40 GB; synthetic records of 500 bytes each; no TLS. On another machine, with remote or cloud object storage, or with real logs, the results are different. Measure once in your own environment.

These measurements were made on development builds before the 0.1.0 release, and not all on the same build. Later changes can make the results for 0.1.0 different.

### 4.1 Ingestion throughput has a limit

| Scenario | Highest ingestion rate that can be sustained |
|---|---|
| 4 datasets written at the same time | About 130,000 records per second (about 65 MB/s) |
| Only 1 dataset written | About 80,000 records per second (about 40 MB/s) |

- Within one dataset, commits are done one after another, so a single dataset has a limit. Different datasets do not wait for each other.
- While the rate can be sustained, the measured p99 time from acknowledgement until a new record can be searched was within 0.4 seconds. This is a measured value, not a commitment.
- What Amberlog promises: within the throughput limit, acknowledged records can be searched within 10 seconds. When the backlog exceeds its memory limit (`ingest.max_backlog_memory_bytes`, default 256 MiB), overflow records are kept only in the WAL and can be searched only after they are read back into memory.
- With 4 datasets, above the limit, records build up a backlog in the WAL. Amberlog does not refuse them. The longer the backlog lasts, the longer new records wait until they can be searched (measured p99 30 to 60 seconds).
- With 1 dataset, when the clients tried to send more, the rate stayed at about 80,000 records per second: acknowledgements became slower, and no backlog built up.
- The bottleneck is on the commit path, not the CPU: while the rate could be sustained, the process used only 2 to 5 cores.

### 4.2 The memory for lake entry and cold compaction grows with the data volume

- **Lake entry** reads all the data of one dataset for one hour at a time. The measured memory needed was about 2.4 times the raw data of that hour. (This measurement used the synthetic logs described in section 4.4, not the 500-byte records.)
- **Cold compaction** reads all the candidate files of one dataset for one day at a time. Estimated with the lake-entry factor, the memory needed is about 2.4 times the raw data of that day (cold compaction itself was not measured).
- With a process memory limit of 40 GB, the limit for a single dataset is about 15 GB of raw data per day (about 0.6 GB per hour). This limit is tighter than the one for lake entry. Use it when you plan a deployment.
- Above the limit, the operating system kills the process. After a restart, the process tries again and is killed again (estimated from the mechanism, not measured).
- For a source with a large data volume, consider writing its data into several datasets (this recommendation follows from the mechanism above and was not measured).

### 4.3 The hot-tier inspection cache grows with run time

Hot-tier inspection caches in the process the manifest pieces it has read. It deletes them only after the snapshots that reference them are reclaimed. The content of each manifest piece is stored only once, and adjacent versions of an hour-bucket manifest share it. So this cache grows with the manifest pieces written within `hot_retention.age` (default 168 hours), that is, with the number of commits and the number of datasets. It does not depend on the data volume.

For example, after 384 commits to the same hour bucket, the cache has 1,472 manifest-piece entries. If each version stored a full copy, there would be 73,920.

One measurement under a high write load, the same run as the measurement in section 4.4 (4 datasets with 20,000 records per second in total, written continuously for 2 hours, default configuration):

| Item | Measured value |
|---|---|
| Rise in anonymous resident memory (RssAnon) caused by each hot-tier inspection | 47, 49, 109 and 26 MiB for the 4 inspections. It did not grow with the number of commits |
| Anonymous resident memory after 10 minutes of idle time after writing stopped | About 213 MiB |
| Highest value over the whole run (sampled every 20 milliseconds) | About 740 MiB, when the first hour bucket was sealed |

This measurement ran for only about 4 hours. It did not run for the full `hot_retention.age`. It is one measurement on one machine, not a capacity commitment.

Amberlog returns memory that the glibc allocator frees to the system at once: at startup it sets the glibc trim threshold (128 KiB). When `glibc.malloc.trim_threshold` is set in the environment variable `GLIBC_TUNABLES`, that setting is used. You do not need any other setting for this.

**Recommendation:** check the anonymous resident memory of the process every day (`RssAnon` in `/proc/<pid>/status`, or `MemoryCurrent` in systemd). Before it comes close to the memory limit, restart the process at a time of low business load. The restart clears the cache. Acknowledged records are replayed from the WAL and are not lost.

### 4.4 The hot-tier manifest history is not reclaimed within the retention period

Each hot-tier commit writes one snapshot, and one manifest piece for each hour bucket that it changes (a JSON file under `manifest/buckets/` in object storage). The hour-bucket manifest of one hour bucket consists of a number of manifest pieces:

- A **delta piece** records only the files added in this commit and the files removed in this commit. Most commits write only delta pieces.
- A **full piece** records all live files of the hour bucket at this moment. When an hour bucket has 64 manifest pieces counted from its latest full piece, the next commit writes a full piece. When an hour bucket has no live files left, a full piece is also written.

So each file entry is written once, and after that again with a full piece about every 64 commits. The amount of manifest data written grows roughly in proportion to the number of commits.

Measured: with 70 consecutive commits to the same hour bucket, each adding one data file, the 1st and 65th pieces were full pieces and the others were delta pieces. Each delta piece was about 690 bytes. The 65th piece had 65 file entries and was about 26 KB.

- Snapshots and manifest pieces are reclaimed only after `hot_retention.age` (default 168 hours). With continuous writes, all manifests written during this period stay in object storage.
- The shorter `flush.interval` is, the more often commits happen, and the faster the manifests grow.

One measurement under a high write load: the test machine described at the start of this section, 4 datasets with 20,000 synthetic log records per second in total, written continuously for 2 hours, with default values for everything except the settings required for deployment. These synthetic logs had 4 formats (web access, application JSON, system security, container logs), with an average of about 620 bytes per record (estimated). This is different from the 500 bytes per record given at the start of this section.

| Item | Measured value |
|---|---|
| Records written | 144 million |
| Hot-tier data files | 5,682.2 MB |
| Total under `manifest/` | 217.3 MB, which is 3.82% of the hot-tier data files. The highest of the 4 datasets was 6.86% |
| Manifest growth | 99.7 MB in the first hour, 112.8 MB in the second hour |

- The second hour grew about 13% more than the first. The extra came mostly from the commits of seal compaction after the first hour bucket was sealed (inferred). The manifests grow roughly linearly with the write duration.
- This is one measurement on one machine with one write pattern, not a capacity commitment. With a different number of datasets, number of commits per dataset or record size, the ratio is different. The measurement did not run for the full `hot_retention.age`.

**Recommendations:**

- Include the manifests when you plan object-storage capacity. First write for a few hours with your own write pattern, measure the bytes under `manifest/` for each dataset, and then scale the result to `hot_retention.age`.
- When the write volume is high and storage is tight, decrease `hot_retention.age`, for example to `1h`. The manifests that remain are a little more than the amount written in the most recent `hot_retention.age` (estimated, not measured). The cost: the interval between audit verifications must be shorter than `hot_retention.age`. Otherwise the snapshot anchored by the previous evidence has already been reclaimed, and the verification records the result as unprovable (section 6 of the [Operations guide](operations.md)).
- Decreasing `hot_retention.age` does not decrease the amount that each commit writes. It only decreases the amount that remains.

### 4.5 Cold-tier metadata only grows

- Each commit to a cold-tier table writes a new metadata file `v<N>.metadata.json`. Amberlog deletes none of the metadata files it has written. With the default configuration, each dataset accumulates about 1.2 to 4.6 GB of metadata per year (estimated). The number of commits decides this, not the data volume. Decreasing `cold_retention.age` (default 168 hours) makes newly written metadata files smaller.
- After cold compaction, the current snapshot of a cold-tier table still references manifests that contain only "deleted" entries (empty manifests). Their number grows with each lake entry: about 8,800 per dataset per year, about 55 MB (estimated). Cold-side queries, cold compaction and external engines read the manifest list each time. The larger the list, the slower the query planning. No configuration key avoids this.

## 5. Search and external engines

### 5.1 External engines can read only the cold tier

- An hour bucket enters the lake `lifecycle.lake_watermark` (default 72 hours) after it ends, and becomes part of a cold-tier Apache Iceberg table. Until then, the data is in the hot tier and can be searched only through Amberlog.
- External engines read the cold-tier tables directly. There is no promise that the hot tier can be read directly at any time.
- When you stop using Amberlog, use `amberlog retire` to convert all hot-tier data into cold-tier tables (section 7 of the [Operations guide](operations.md)).
- How each engine reads the cold-tier tables, and the known differences between engines, are in the [External engine guide](external-engines.md).

### 5.2 Full-text search has no index

- Full-text matching (`str_match`, `match_all`) scans all the text in the time range. The longer the time range, the slower the search.
- When you add a full-text condition in the search UI, the time range is automatically reduced to the last 24 hours. You can extend it to at most 7 days. When you write SQL, there is no limit, but long ranges are very slow.
- First narrow the range with field conditions or by dataset, and then add full-text conditions.

### 5.3 Time zones

The search UI shows times with the browser's time zone data. When the browser and Amberlog have different versions of the time zone data, some time intervals may not line up.

## 6. Audit and data format

### 6.1 Changes made outside Amberlog are found only at certain points

- When someone bypasses Amberlog and changes the data files, hour-bucket manifests or count objects in the object-storage bucket directly, Amberlog does not always find this at once.
- When a data file is replaced by another file with the same name, this is found only during seal compaction, the digest check before lake entry, and audit verification.
- Recommendations: give write permission on the object storage only to Amberlog; turn on versioning for the object-storage bucket; run audit verification at regular intervals (section 6 of the [Operations guide](operations.md)).

### 6.2 Upgrades and data format

- Patch releases (for example 0.1.0 → 0.1.1) do not change the data format. You can upgrade and downgrade freely.
- For a minor release that changes the data format, the changelog contains upgrade notes. Upgrade one minor release at a time. Do not skip a minor release, and do not downgrade.
- Before an upgrade, stop the process normally as described in section 5.1 of the [Operations guide](operations.md).
- The cold-tier tables always stay readable. Later releases only add columns and do not change existing columns.

## 7. Roadmap

This section gives directions only, not dates. The changelog shows the actual progress.

### 7.1 Next stage

- **Access control**: service tokens (randomly generated, with an optional expiry time); built-in roles, with per-dataset authorization. The existing `auth.token` stays. After the upgrade it is equal to an administrator token, and what it could do before does not change.
- **Single sign-on**: connect to an OIDC identity platform, so that you can log in to the search UI. SAML 2.0 and LDAP connect through an identity proxy that converts them to OIDC.
- **The protections in section 2.2**: a warning for short tokens, connection time limits, and security response headers.
- **Memory of cold compaction**: cold compaction merges only part of the candidate files at a time, so that its memory no longer grows with the data volume of one dataset for one day (section 4.2). The memory that lake entry needs still grows with the data volume of one dataset for one hour.
- **Empty manifests of the cold tier** no longer stay in the manifest list (section 4.5).
- **Local disk storage**: when the directory disappears while the process runs, `/readyz` reports not-ready (section 3.3).

### 7.2 To be decided later as needed

The items below have no schedule now. If you need one of them in your trial, open an issue and describe your scenario.

- One process serving several organizations.
- An HTTP interface to manage users, roles and tokens.
- Native SAML 2.0 and LDAP.
- Inbound mutual TLS.
- Decreasing the memory that lake entry needs, so that it no longer grows with the data volume of one dataset for one hour.
- OTLP and other ingestion protocols.
- An index to speed up substring search.
- Chinese cloud object storage (Huawei Cloud OBS, Alibaba Cloud OSS, Tencent Cloud COS).
- The Chinese national cryptography transport protocol.
- Deleting old cold-tier metadata files.

### 7.3 Not planned for now

- Built-in alerting channels (email, SMS, instant messaging). Alerting sends only generic webhooks, and the receiver connects them to downstream systems.
- Local accounts and passwords. Login is left to the identity platform.
