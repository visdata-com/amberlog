#!/usr/bin/env python3
"""用 DuckDB 直接读对象存储里的冷层表，执行一段 SQL（可以含多条语句）并打印结果。

用法：lake-query "<SQL>"
连接对象存储用的端点与只读凭据来自环境变量 LAKE_S3_ENDPOINT、LAKE_S3_KEY_ID、LAKE_S3_SECRET（见 compose.yaml）。
冷层表在桶内的位置是 s3://<桶>/<storage.prefix>/_iceberg/<org>/<stream>，例如 s3://amberlog-data/amberlog/_iceberg/default/app。

Read the cold-tier tables in object storage directly with DuckDB, run one piece of SQL (it may contain several
statements), and print the result.

Usage: lake-query "<SQL>"
The endpoint and read-only credentials come from the environment variables LAKE_S3_ENDPOINT, LAKE_S3_KEY_ID and
LAKE_S3_SECRET (see compose.yaml). A cold-tier table is at s3://<bucket>/<storage.prefix>/_iceberg/<org>/<stream>
in the bucket, for example s3://amberlog-data/amberlog/_iceberg/default/app.
"""

import os
import sys

import duckdb

if len(sys.argv) != 2:
    sys.exit(__doc__.strip())

con = duckdb.connect()
for ext in ("httpfs", "avro", "iceberg"):
    con.load_extension(ext)
con.execute(
    "CREATE SECRET lake (TYPE S3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')",
    [os.environ["LAKE_S3_KEY_ID"], os.environ["LAKE_S3_SECRET"], os.environ["LAKE_S3_ENDPOINT"]],
)
result = con.sql(sys.argv[1])
if result is not None:
    result.show(max_width=200)
