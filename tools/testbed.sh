#!/bin/sh
# 测试台架起停。标签（K / B / C / D / E）只用于隔离：每个标签独占一个桶（本地模式下另占一个 compose project
# 与一组宿主端口），几份工作区可以各用一个标签同时跑测试。
#   tools/testbed.sh <标签> up     起台架（或建桶）并打印要 export 的环境变量
#   tools/testbed.sh <标签> env    只打印环境变量
#   tools/testbed.sh <标签> down   停台架（数据在 tmpfs，随之清空）
# 两种模式：
# - 共享模式：有 ~/.config/amberlog/testbed-shared（内容为一行服务地址，如 http://192.0.2.10:19900）时，连一台常驻的
#   共享对象存储。桶为 amberlog-testbed-<本机主机名>-<标签>，up 只建桶，down 不做事（旧对象由服务端定时删除）。不用本机 docker。
# - 本地模式（没有上述文件）：本机 docker 起 Silo（MinIO 的社区分支，镜像见 tests/testbed/compose.yaml），每个标签一个 compose project、一组宿主端口：S3 端口见下面的 base
#   （K 19000、B 19100、C 19200、D 19300、E 19400），控制台端口为 base + 1。
# 台架变量不用 AMBERLOG_ 前缀：被测进程会把 AMBERLOG_* 当配置键（REQ-AMB-CFG-STARTUP）。
set -eu
usage() { echo "usage: $0 <K|B|C|D|E> <up|env|down>" >&2; exit 2; }
[ $# -eq 2 ] || usage
case "$1" in
  K|k) track=k; base=19000 ;;
  B|b) track=b; base=19100 ;;
  C|c) track=c; base=19200 ;;
  D|d) track=d; base=19300 ;;
  E|e) track=e; base=19400 ;;
  *) usage ;;
esac
root=$(cd "$(dirname "$0")/.." && pwd)
shared_conf=${XDG_CONFIG_HOME:-$HOME/.config}/amberlog/testbed-shared
if [ -r "$shared_conf" ]; then
  mode=shared
  endpoint=$(tr -d ' \t\r\n' < "$shared_conf")
  [ -n "$endpoint" ] || { echo "$shared_conf 为空：应为共享台架的地址，如 http://192.0.2.10:19900" >&2; exit 2; }
  # 桶名只能是小写字母、数字与连字符
  host=$(uname -n | cut -d. -f1 | tr 'A-Z' 'a-z' | tr -c 'a-z0-9-\n' '-')
  export TESTBED_S3_BUCKET="amberlog-testbed-$host-$track"
else
  mode=local
  endpoint=http://127.0.0.1:$base
  export COMPOSE_PROJECT_NAME="amberlog-testbed-$track"
  export TESTBED_S3_PORT=$base
  export TESTBED_CONSOLE_PORT=$((base + 1))
  export TESTBED_S3_BUCKET="amberlog-testbed-$track"
fi
compose() { docker compose -f "$root/tests/testbed/compose.yaml" "$@"; }
# 共享模式的 up：以 root 凭据建本机本标签的桶；已存在（409 BucketAlreadyOwnedByYou）也算成功
make_bucket() {
  code=$(curl -sS -o /dev/null -w '%{http_code}' --aws-sigv4 'aws:amz:us-east-1:s3' \
    --user testbed:testbed-secret -X PUT "$endpoint/$TESTBED_S3_BUCKET") || {
    echo "连不上共享台架 $endpoint" >&2; exit 1; }
  case "$code" in
    200|409) ;;
    *) echo "共享台架 $endpoint 建桶 $TESTBED_S3_BUCKET 返回 HTTP $code" >&2; exit 1 ;;
  esac
}
print_env() {
  cat <<EOV
export TESTBED_S3_ENDPOINT=$endpoint
export TESTBED_S3_ACCESS_KEY_ID=testbed
export TESTBED_S3_SECRET_ACCESS_KEY=testbed-secret
export TESTBED_S3_BUCKET=$TESTBED_S3_BUCKET
export TESTBED_S3_APP_ACCESS_KEY_ID=amberlog-app
export TESTBED_S3_APP_SECRET_ACCESS_KEY=amberlog-app-secret
EOV
}
case "$2:$mode" in
  up:shared) make_bucket && print_env ;;
  down:shared) echo "共享台架（$endpoint）：down 不做事，旧对象由服务端定时删除" >&2 ;;
  up:local) compose up -d --wait minio >&2 && compose run --rm bucket >&2 && print_env ;;
  down:local) compose down -v >&2 ;;
  env:*) print_env ;;
  *) usage ;;
esac
