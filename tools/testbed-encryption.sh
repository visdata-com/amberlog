#!/bin/sh
# 加密场景的测试台架（REQ-AMB-ARCH-ENCRYPTION 的出站场景用，测试在 crates/server/tests/arch_encryption.rs）：以自签 CA「CA-S」开 TLS 的
# 对象存储 Silo（MinIO 的社区分支，下称 MinIO），旁边的密钥服务 KES 内置密钥 k-a；另有一台 MinIO 的 KES 在起台架的最后被停掉，
# 供「密钥服务已停止」的场景用。与 tools/testbed.sh 的普通台架互不相干：各自的进程、端口与桶，不改它的服务定义。
#   tools/testbed-encryption.sh <K|B|C|D|E> up|env|down   本机：docker 容器（每个标签一套）
#   tools/testbed-encryption.sh ci up                     维护者内部 CI 的作业里：下载固定版本的二进制、核 sha256，以后台进程起
#                                                         （本机请用上一行的标签模式）
# up 打印要 export 的环境变量（含 CA-S 与 MinIO 服务端证书的 PEM，供测试进程内的请求日志转发层用）。
# 端口（只绑 127.0.0.1）：<基址>+11 为 TLS+KES 的 MinIO，+13 为 KES 已停的 MinIO，+10、+12 为两个 KES
# （K 19011/19013 … E 19411/19413，内部 CI 用 E 的端口）；测试进程要能经 127.0.0.1 连到这两个 MinIO 端口。
# 加密台架总在本机（或内部 CI 的作业容器里）起，与 tools/testbed.sh 是否为共享模式无关。
# 证书、私钥与 KES 的密钥库放在 ${TMPDIR:-/tmp} 下，不进仓库。台架变量一律 TESTBED_ 前缀（不得用 AMBERLOG_）。
# 固定版本：Silo RELEASE.2026-10-18、KES 2023-07-26（连法与各种情形下的状态码按这两个版本实测）。
set -eu
usage() { echo "usage: $0 <K|B|C|D|E> <up|env|down> | $0 ci up" >&2; exit 2; }
[ $# -eq 2 ] || usage
mode=docker
case "$1" in
  K|k) track=k; base=19000 ;;
  B|b) track=b; base=19100 ;;
  C|c) track=c; base=19200 ;;
  D|d) track=d; base=19300 ;;
  E|e) track=e; base=19400 ;;
  ci) track=ci; base=19400; mode=process ;;
  *) usage ;;
esac
# Silo：MinIO 上游已归档，Docker Hub 上的 minio/minio 已删除。与 tests/testbed/compose.yaml 同一个镜像，按摘要固定
MINIO_IMG=pgsty/silo:RELEASE.2026-10-18T00-00-00Z@sha256:2636fc4155d9a7e41ff58cdb65ddd13ba9304e67184e13c25bc5cb34016269d7
# KES 2023-07-26T11-13-07Z；Docker Hub 上已没有 minio/kes，取 quay.io 并按摘要固定
KES_IMG=quay.io/minio/kes@sha256:fb15af611149892f357a8a99d1bcd8bf5dae713bd64c15e6eb27fbdb88fc208b
# 内部 CI 用的二进制：与上面两个镜像同一版本的官方发布包；地址写死版本，不经 api.github.com（匿名访问有频率限制）
SILO_URL=https://github.com/pgsty/silo/releases/download/RELEASE.2026-10-18T00-00-00Z/silo_20261018000000.0.0_linux_amd64.tar.gz
SILO_SHA=e069b6425f0de944fea6c301773d5620fe8992bf0d16b6c5e42be57d666ee794
KES_URL=https://github.com/minio/kes/releases/download/2023-07-26T11-13-07Z/kes-linux-amd64
KES_SHA=0b06e99cd17d2ca079027696f87fde3a0e9e138ab7e5508b2fa06dfb22a25386
name=amberlog-testbed-enc-$track
state=${TMPDIR:-/tmp}/$name
certs=$state/certs
bucket=$name
p_kes=$((base + 10))
p_tls=$((base + 11))
p_kes_down=$((base + 12))
p_down=$((base + 13))

gencerts() {
  [ -f "$certs/ca-s.crt" ] && return 0
  mkdir -p "$certs"
  ec() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null; }
  ec "$certs/ca-s.key"
  openssl req -x509 -new -key "$certs/ca-s.key" -sha256 -days 3650 -subj "/CN=CA-S" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$certs/ca-s.crt"
  leaf() { # 名字 CN 主题备用名 EKU 序列号
    ec "$certs/$1.key"
    openssl req -new -key "$certs/$1.key" -subj "/CN=$2" -out "$certs/$1.csr"
    printf 'basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=%s\nsubjectAltName=%s\n' \
      "$4" "$3" > "$certs/$1.ext"
    openssl x509 -req -in "$certs/$1.csr" -CA "$certs/ca-s.crt" -CAkey "$certs/ca-s.key" -set_serial "$5" \
      -days 825 -sha256 -extfile "$certs/$1.ext" -out "$certs/$1.crt" 2>/dev/null
    rm -f "$certs/$1.csr" "$certs/$1.ext"
  }
  leaf minio minio "IP:127.0.0.1,DNS:minio,DNS:minio-down,DNS:localhost" serverAuth 11
  leaf kes kes "IP:127.0.0.1,DNS:kes,DNS:kes-down" serverAuth 12
  leaf minio-client minio-kes-client "DNS:minio-kes-client" clientAuth 13
  chmod 0644 "$certs"/*.key
  mkdir -p "$certs/minio/CAs"
  cp "$certs/minio.crt" "$certs/minio/public.crt"
  cp "$certs/minio.key" "$certs/minio/private.key"
  cp "$certs/ca-s.crt" "$certs/minio/CAs/ca-s.crt"
}

# KES 配置：MinIO 的客户端身份（客户端证书公钥 SubjectPublicKeyInfo 的 SHA-256）可建、生成、解密密钥
kes_config() { # 输出文件 监听地址 证书目录 密钥库目录
  identity=$(openssl x509 -in "$certs/minio-client.crt" -noout -pubkey \
    | openssl pkey -pubin -outform DER | openssl dgst -sha256 -r | cut -d' ' -f1)
  cat > "$1" <<EOK
address: $2
admin:
  identity: disabled
tls:
  key: $3/kes.key
  cert: $3/kes.crt
  ca: $3/ca-s.crt
policy:
  minio:
    allow: [/v1/key/create/*, /v1/key/generate/*, /v1/key/decrypt/*, /v1/key/bulk/decrypt, /v1/key/list/*, /v1/status]
    identities: [$identity]
keystore:
  fs:
    path: $4
EOK
}

retry() { # 次数 命令…
  n=$1; shift
  for _ in $(seq 1 "$n"); do "$@" 2>/dev/null && return 0; sleep 1; done
  echo "$0: 等不到：$*" >&2
  return 1
}

kes_ready() { # 主机名 端口：探活后建密钥 k-a（已存在时 KES 回错误也算就绪）
  curl -fsS -o /dev/null --cacert "$certs/ca-s.crt" --resolve "$1:$2:127.0.0.1" \
    --cert "$certs/minio-client.crt" --key "$certs/minio-client.key" "https://$1:$2/v1/status" || return 1
  curl -sS -o /dev/null --cacert "$certs/ca-s.crt" --resolve "$1:$2:127.0.0.1" \
    --cert "$certs/minio-client.crt" --key "$certs/minio-client.key" -X POST "https://$1:$2/v1/key/create/k-a"
}

minio_ready() { # 端口：探活后以 SigV4 签名的 PUT 建桶（409 为已存在）
  curl -fsS -o /dev/null --cacert "$certs/ca-s.crt" "https://127.0.0.1:$1/minio/health/live" || return 1
  code=$(curl -s -o /dev/null -w '%{http_code}' --cacert "$certs/ca-s.crt" -X PUT \
    --aws-sigv4 "aws:amz:us-east-1:s3" --user testbed:testbed-secret "https://127.0.0.1:$1/$bucket")
  [ "$code" = 200 ] || [ "$code" = 409 ]
}

# ---- 本机：docker 容器 ----

docker_kes() { # 名字（兼网络别名） 宿主端口
  docker run -d --name "$name-$1" --network "$name" --network-alias "$1" -p "127.0.0.1:$2:7373" \
    -v "$certs:/certs:ro" -v "$state/kes.yml:/kes.yml:ro" "$KES_IMG" server --config /kes.yml --auth on >/dev/null
  retry 60 kes_ready "$1" "$2"
}

docker_minio() { # 名字 宿主端口 KES 别名
  docker run -d --name "$name-$1" --network "$name" -p "127.0.0.1:$2:9000" \
    -e MINIO_ROOT_USER=testbed -e MINIO_ROOT_PASSWORD=testbed-secret \
    -e MINIO_KMS_KES_ENDPOINT="https://$3:7373" \
    -e MINIO_KMS_KES_CERT_FILE=/certs/minio-client.crt -e MINIO_KMS_KES_KEY_FILE=/certs/minio-client.key \
    -e MINIO_KMS_KES_CAPATH=/certs/ca-s.crt -e MINIO_KMS_KES_KEY_NAME=k-a \
    -v "$certs:/certs:ro" --tmpfs /data "$MINIO_IMG" server /data --address :9000 --certs-dir /certs/minio >/dev/null
  retry 60 minio_ready "$2"
}

docker_down() {
  docker rm -f "$name-kes" "$name-kes-down" "$name-minio" "$name-minio-down" >/dev/null 2>&1 || true
  docker network rm "$name" >/dev/null 2>&1 || true
}

docker_up() {
  docker_down
  kes_config "$state/kes.yml" 0.0.0.0:7373 /certs /data/keys
  docker network create "$name" >/dev/null
  docker_kes kes "$p_kes"
  docker_kes kes-down "$p_kes_down"
  docker_minio minio "$p_tls" kes
  docker_minio minio-down "$p_down" kes-down
  # 「密钥服务已停止」：这台 MinIO 已带着 KMS 起来，此后它的 KES 不再可达
  docker stop "$name-kes-down" >/dev/null
}

# ---- 内部 CI：下载的二进制，后台进程 ----

fetch() { # 地址 sha256 输出：已有的（内部 CI 缓存命中）也每次核 sha256，不符就重新下载
  if [ -f "$3" ] && echo "$2  $3" | sha256sum -c - >/dev/null 2>&1; then
    chmod +x "$3"
    return 0
  fi
  curl -fsSL -o "$3.part" "$1"
  echo "$2  $3.part" | sha256sum -c - >/dev/null
  chmod +x "$3.part"
  mv "$3.part" "$3"
}

process_minio() { # 名字 端口 KES 端口
  mkdir -p "$state/$1/data"
  MINIO_ROOT_USER=testbed MINIO_ROOT_PASSWORD=testbed-secret MINIO_KMS_KES_ENDPOINT="https://127.0.0.1:$3" \
    MINIO_KMS_KES_CERT_FILE="$certs/minio-client.crt" MINIO_KMS_KES_KEY_FILE="$certs/minio-client.key" \
    MINIO_KMS_KES_CAPATH="$certs/ca-s.crt" MINIO_KMS_KES_KEY_NAME=k-a \
    "$bin/silo" server "$state/$1/data" --address "127.0.0.1:$2" --console-address "127.0.0.1:$(($2 + 100))" \
    --certs-dir "$certs/minio" > "$state/$1.log" 2>&1 &
  echo $! > "$state/$1.pid"
  retry 60 minio_ready "$2"
}

process_kes() { # 名字 端口
  mkdir -p "$state/$1"
  kes_config "$state/$1.yml" "127.0.0.1:$2" "$certs" "$state/$1/keys"
  "$bin/kes" server --config "$state/$1.yml" --auth on > "$state/$1.log" 2>&1 &
  echo $! > "$state/$1.pid"
  retry 60 kes_ready "$1" "$2"
}

process_up() {
  bin=${TESTBED_ENC_BIN:-$state/bin}
  mkdir -p "$bin"
  fetch "$SILO_URL" "$SILO_SHA" "$bin/silo.tar.gz"
  tar -xzf "$bin/silo.tar.gz" -C "$bin" silo
  fetch "$KES_URL" "$KES_SHA" "$bin/kes"
  process_kes kes "$p_kes"
  process_kes kes-down "$p_kes_down"
  process_minio minio "$p_tls" "$p_kes"
  process_minio minio-down "$p_down" "$p_kes_down"
  kill "$(cat "$state/kes-down.pid")"
}

pem() { printf "'%s'" "$(cat "$1")"; }

print_env() {
  cat <<EOV
export TESTBED_TLS_S3_ENDPOINT=https://127.0.0.1:$p_tls
export TESTBED_TLS_S3_KMS_DOWN_ENDPOINT=https://127.0.0.1:$p_down
export TESTBED_TLS_S3_BUCKET=$bucket
export TESTBED_TLS_S3_ACCESS_KEY_ID=testbed
export TESTBED_TLS_S3_SECRET_ACCESS_KEY=testbed-secret
export TESTBED_TLS_KMS_KEY=k-a
export TESTBED_TLS_CA_PEM=$(pem "$certs/ca-s.crt")
export TESTBED_TLS_SERVER_CERT_PEM=$(pem "$certs/minio.crt")
export TESTBED_TLS_SERVER_KEY_PEM=$(pem "$certs/minio.key")
EOV
}

case "$mode:$2" in
  docker:up)
    gencerts >&2
    docker_up >&2
    echo "加密台架已在本机起好（与 tools/testbed.sh 是否为共享模式无关）；测试进程经 127.0.0.1 的端口 $p_tls 与 $p_down 连接" >&2
    print_env
    ;;
  process:up)
    gencerts >&2
    process_up >&2
    print_env
    ;;
  *:env) print_env ;;
  docker:down) docker_down; rm -rf "$state" ;;
  *) usage ;;
esac
