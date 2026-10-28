#!/bin/sh
# 生成第三方许可声明，供预编译二进制与容器镜像附带。生成的文件不提交，发版时生成。
#
# 用法：sh tools/third-party-notices.sh [输出文件]      默认输出 THIRD-PARTY-NOTICES.txt
#
# 需要 cargo 与 cargo-about（版本见 CARGO_ABOUT_VERSION）；会先 `cargo fetch --locked` 取齐依赖源码，
# cargo-about 从源码里读许可文件。声明由两部分拼成：
#   1. cargo-about 按 about.toml 与 third-party-notices.hbs 列出随二进制分发的 crate 及其许可原文；
#   2. third-party-notices-bundled.txt：pg_query 内嵌 C 源码的许可原文（crate 元数据没有申报，见 deny.toml 的 clarify），
#      与检索界面页面内嵌的前端库（ui/ 的运行时依赖）的许可原文（版本由 ui-deps-check.mjs 核对）。
set -eu

CARGO_ABOUT_VERSION=0.9.2
# 第 2 部分照录的是这一版 pg_query 内嵌的源码；Cargo.lock 里的版本变了就停下，先重新核对。
PG_QUERY_VERSION=6.2.0

here=$(cd "$(dirname "$0")" && pwd)
conf=$(dirname "$here")
out=${1:-THIRD-PARTY-NOTICES.txt}

# 工作区根目录：从配置所在目录往上找 Cargo.lock。
root=$conf
while [ ! -f "$root/Cargo.lock" ]; do
  [ "$root" != / ] || { echo "third-party-notices: 找不到 Cargo.lock" >&2; exit 1; }
  root=$(dirname "$root")
done

have=$(cargo-about --version 2>/dev/null || true)
[ "$have" = "cargo-about $CARGO_ABOUT_VERSION" ] || {
  echo "third-party-notices: 需要 cargo-about $CARGO_ABOUT_VERSION，当前为「${have:-未安装}」" >&2
  echo "  安装：cargo install --locked cargo-about@$CARGO_ABOUT_VERSION --features cli" >&2
  exit 1
}

locked=$(awk '/^name = "pg_query"$/ { getline; gsub(/^version = "|"$/, ""); print }' "$root/Cargo.lock")
[ "$locked" = "$PG_QUERY_VERSION" ] || {
  echo "third-party-notices: Cargo.lock 里 pg_query 是 ${locked:-（没有）}，third-party-notices-bundled.txt 照录的是 $PG_QUERY_VERSION。" >&2
  echo "  重新核对 pg_query 内嵌的 C 源码及其许可，更新那份文件、deny.toml 的 clarify 与本脚本的 PG_QUERY_VERSION。" >&2
  exit 1
}

cargo fetch --locked --manifest-path "$root/Cargo.toml"

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
cargo-about generate --locked --fail --manifest-path "$root/Cargo.toml" --config "$conf/about.toml" \
  --output-file "$tmp" "$here/third-party-notices.hbs"
{ cat "$tmp"; cat "$here/third-party-notices-bundled.txt"; } > "$out"
echo "third-party-notices: 已写入 $out" >&2
