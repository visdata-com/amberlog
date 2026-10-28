#!/bin/sh
# 检索界面（REQ-AMB-UI-CONSOLE、REQ-AMB-UI-SEARCH、REQ-AMB-UI-GOVERNANCE）系统测试用的无头浏览器：Chrome for Testing 的 chrome-headless-shell，版本固定。
#   tools/chrome.sh [目录]   下载并解开到 <目录>（缺省 ~/.cache/amberlog/chrome-headless-shell/<版本>），核对系统库，
#                            打印要 export 的 TESTBED_CHROME。已下载过则只核对。
# 测试在 TESTBED_CHROME 未设置时找缺省目录（crates/server/tests/common/browser.rs）。本脚本要在运行测试的那台机器上执行。
# 浏览器所需的系统库不随压缩包提供，缺了由本脚本列出。Debian / Ubuntu 上可装：
#   unzip libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 libcups2 libdbus-1-3 libxkbcommon0
#   libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 libexpat1
#   libglib2.0-0 libx11-6 libxcb1 libxext6
set -eu
VERSION=154.0.8037.92
dest=${1:-$HOME/.cache/amberlog/chrome-headless-shell/$VERSION}
bin="$dest/chrome-headless-shell-linux64/chrome-headless-shell"
if [ ! -x "$bin" ]; then
  mkdir -p "$dest"
  # --retry-all-errors：TLS 连接中断这类错误 curl 默认不重试。
  curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 -o "$dest/chs.zip" \
    "https://storage.googleapis.com/chrome-for-testing-public/$VERSION/linux64/chrome-headless-shell-linux64.zip"
  if command -v unzip > /dev/null; then
    unzip -q -o "$dest/chs.zip" -d "$dest"
  else
    python3 -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])' "$dest/chs.zip" "$dest"
  fi
  rm -f "$dest/chs.zip"
  chmod +x "$bin"
fi
missing=$(ldd "$bin" | grep "not found" || true)
if [ -n "$missing" ]; then
  echo "chrome-headless-shell 缺系统库：" >&2
  echo "$missing" >&2
  exit 1
fi
echo "export TESTBED_CHROME=$bin"
