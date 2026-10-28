#!/bin/sh
# 一键试用的演示：写入、检索、外部引擎直读冷层。先在本目录执行 docker compose up -d --wait，再执行本脚本。
# 需要 curl。脚本按空环境编写；再演示一遍前，先用 docker compose down -v 删掉容器与数据，再重新 up。
# Quickstart demo: ingest, search, and an external engine reading the cold tier directly. First run docker compose up -d --wait
# in this directory, then this script. Needs curl. The script assumes an empty environment; before running it again,
# remove the containers and data with docker compose down -v and bring them up again. Step titles are printed in Chinese.
set -eu
cd "$(dirname "$0")"

URL=http://127.0.0.1:8800
TOKEN=quickstart-token # 与 compose.yaml 里的 AMBERLOG_AUTH__TOKEN 相同 / same as AMBERLOG_AUTH__TOKEN in compose.yaml
TABLE="s3://amberlog-data/amberlog/_iceberg/default/app"

step() { printf '\n== %s\n' "$*"; }

step "1. 等 Amberlog 就绪"
i=0
until curl -fsS "$URL/readyz" > /dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -lt 60 ] || { echo "60 秒内没有就绪，用 docker compose logs amberlog 查看日志 / not ready within 60 seconds; see docker compose logs amberlog" >&2; exit 1; }
  sleep 1
done
curl -sS "$URL/readyz"
echo

# _timestamp 写成毫秒数。4 天前的记录已越过默认的入湖水位（72 小时），很快进入冷层；现在的记录 72 小时后才进入。
# 3 条旧记录取 4 天前那个小时的第 10 分钟，落在同一个小时桶里，一次入湖。
# _timestamp is in milliseconds. Records from 4 days ago are already past the default lake-entry watermark (72 hours) and
# soon enter the cold tier; current records enter it only after 72 hours. The 3 old records use minute 10 of the hour
# 4 days ago, so they fall into one hour bucket and enter the lake together.
now_ms=$(($(date +%s) * 1000))
old_ms=$(((now_ms - 4 * 86400 * 1000) / 3600000 * 3600000 + 600000))

step "2. 写入 5 条记录到数据集 default/app：3 条是 4 天前的，2 条是现在的"
curl -sS -X POST "$URL/api/default/app/_json" -H "Authorization: Bearer $TOKEN" -d "[
  {\"_timestamp\": $old_ms, \"level\": \"info\", \"message\": \"user login\", \"user\": {\"name\": \"alice\"}},
  {\"_timestamp\": $((old_ms + 1000)), \"level\": \"error\", \"message\": \"db timeout\", \"latency_ms\": 3000.0},
  {\"_timestamp\": $((old_ms + 2000)), \"level\": \"warn\", \"message\": \"slow request\", \"latency_ms\": 870.0},
  {\"_timestamp\": $now_ms, \"level\": \"info\", \"message\": \"user logout\", \"user\": {\"name\": \"alice\"}},
  {\"_timestamp\": $((now_ms + 1000)), \"level\": \"error\", \"message\": \"disk full\"}
]"
echo

step "3. 用 Amberlog 检索最近 5 天：新旧记录都能查到"
start_us=$(((old_ms - 3600 * 1000) * 1000))
end_us=$(((now_ms + 3600 * 1000) * 1000))
curl -sS -X POST "$URL/api/default/_search" -H "Authorization: Bearer $TOKEN" -d "{\"query\": {
  \"sql\": \"SELECT level, count(*) AS n FROM app GROUP BY level ORDER BY level\",
  \"start_time\": $start_us, \"end_time\": $end_us}}"
echo
curl -sS -X POST "$URL/api/default/_search" -H "Authorization: Bearer $TOKEN" -d "{\"query\": {
  \"sql\": \"SELECT _timestamp, level, message FROM app WHERE level = 'error' ORDER BY _timestamp\",
  \"start_time\": $start_us, \"end_time\": $end_us}}"
echo

step "4. 等 4 天前的记录进入冷层（最多 3 分钟）"
i=0
until curl -fsS "$URL/iceberg/v1/amberlog/namespaces/default/tables" -H "Authorization: Bearer $TOKEN" 2> /dev/null | grep -q '"name":"app"'; do
  i=$((i + 1))
  [ "$i" -lt 180 ] || { echo "3 分钟内冷层表没有出现，用 docker compose logs amberlog 查看日志 / the cold-tier table did not appear within 3 minutes; see docker compose logs amberlog" >&2; exit 1; }
  sleep 1
done
echo "冷层表 default.app 已出现（等了约 $i 秒）"

step "5. 用 DuckDB 和只读凭据直接读对象存储里的冷层表，不经过 Amberlog：只看得到已入湖的旧记录"
docker compose exec -T duckdb lake-query \
  "SELECT level, count(*) AS n FROM iceberg_scan('$TABLE') GROUP BY level ORDER BY level"
docker compose exec -T duckdb lake-query \
  "SELECT _timestamp, level, message, \"user.name\", latency_ms FROM iceberg_scan('$TABLE') ORDER BY _timestamp"
