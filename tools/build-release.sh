#!/bin/sh
# 编译发版用的 amberlog 二进制并打包。发版时，维护者从发布仓库的版本 tag 检出，在 AlmaLinux 8 容器里调用它；
# 本地复现用同一条命令（在仓库根目录执行，<BUILD_IMAGE> 为 almalinux:8@sha256:8b469a3a78515e8a18ea8fc727e6a3679e1d0c5ba6f58d5b30be3d0d9b86cffe）：
#
#   docker run --rm -v "$PWD:/src" -w /src -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" <BUILD_IMAGE> \
#     sh tools/build-release.sh <x86_64|aarch64> <版本号 X.Y.Z> <第三方许可声明文件>
#
# 产出 dist/amberlog-<版本号>-<目标>.tar.gz 与同名 .sha256。包里有 amberlog、LICENSE、NOTICE、THIRD-PARTY-NOTICES.txt、
# CHANGELOG.md（有 README.md、README.zh-CN.md 时也带上）。
#
# 为什么在 AlmaLinux 8 容器里编：二进制动态链接 glibc，能在编译环境的 glibc 版本及以上运行。AlmaLinux 8 是 glibc 2.28，
# 与 RHEL 8 及其衍生版、麒麟 V10 相同，比 GitHub 运行器（Ubuntu 24.04，glibc 2.39）能覆盖更多发行版。
# 编完检查二进制要求的 glibc 符号版本，高于 2.28 就失败：依赖里新加的调用若只在较新的 glibc 里有，在这一步拦下。
# C 代码（pg_query、ring、zstd 等）与链接都用 clang 和 lld。aarch64 是交叉编译：用 dnf 把 AlmaLinux 8 的 aarch64 版
# glibc、libgcc 装进一个 sysroot，clang 与 bindgen 都指向它。
set -eu

arch=${1:-}
version=${2:-}
notices=${3:-}
case $arch in
  x86_64 | aarch64) ;;
  *) echo "build-release: 第一个参数要是 x86_64 或 aarch64" >&2; exit 2 ;;
esac
if [ -z "$version" ] || [ ! -f "$notices" ]; then echo "build-release: 用法见本文件开头" >&2; exit 2; fi
target=$arch-unknown-linux-gnu
max_glibc=2.28

# rustup 安装器钉版本并核对 SHA-256；工具链版本由 rust-toolchain.toml 决定。
RUSTUP_VERSION=1.28.2
RUSTUP_SHA256=20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c

# 联网的步骤失败时等 30 秒重试，最多 5 次：dnf 取镜像列表超时不会自己重试，一次超时就要整个重来。
retry() {
  n=1
  until "$@"; do
    [ "$n" -lt 5 ] || return 1
    echo "build-release: 第 $n 次失败，30 秒后重试：$1 $2" >&2
    n=$((n + 1))
    sleep 30
  done
}

# 网络到 AlmaLinux 的镜像站慢时，可以设 ALMALINUX_MIRROR 换成一个镜像站（其下是 8/BaseOS、8/AppStream 等目录）。
if [ -n "${ALMALINUX_MIRROR:-}" ]; then
  sed -i -e 's|^mirrorlist=|#mirrorlist=|' \
    -e "s|^# baseurl=https://repo.almalinux.org/almalinux|baseurl=$ALMALINUX_MIRROR|" /etc/yum.repos.d/almalinux*.repo
fi
retry dnf -y -q --setopt=install_weak_deps=False install gcc clang lld llvm git tar gzip cpio 'dnf-command(download)' > /dev/null
sysroot=/opt/sysroot-$arch
if [ "$arch" = aarch64 ]; then
  # 只下载 aarch64 的 glibc、内核头文件与 gcc 运行库，解包到 sysroot，不安装、不执行包里的脚本。
  # /lib64、/lib 照 AlmaLinux 8 的布局做成指向 usr/ 的链接，libc.so 里写的 /lib64/libc.so.6 才找得到。
  mkdir -p "$sysroot/usr/lib64" "$sysroot/usr/lib" /tmp/rpms-aarch64
  ln -s usr/lib64 "$sysroot/lib64"
  ln -s usr/lib "$sysroot/lib"
  # 元数据另放一个缓存目录：与上面 x86_64 的安装共用缓存时，dnf 会按 x86_64 的元数据解析出 x86_64、i686 的包名，
  # 再到 aarch64 的目录下去取，全部 404。--arch 只取 aarch64 与 noarch 的包。-q 下失败不打印原因，这里补一句。
  retry dnf -q --forcearch=aarch64 --releasever=8 --setopt=cachedir=/var/cache/dnf-aarch64 download \
    --arch aarch64 --arch noarch --destdir /tmp/rpms-aarch64 \
    glibc glibc-devel glibc-headers kernel-headers libgcc gcc libxcrypt libxcrypt-devel \
    || { echo "build-release: 下载 AlmaLinux 8 的 aarch64 系统库失败（可设 ALMALINUX_MIRROR 换镜像站）" >&2; exit 1; }
  for rpm in /tmp/rpms-aarch64/*.rpm; do (cd "$sysroot" && rpm2cpio "$rpm" | cpio -idm --quiet); done
fi

retry curl -sSfL -o /tmp/rustup-init "https://static.rust-lang.org/rustup/archive/$RUSTUP_VERSION/x86_64-unknown-linux-gnu/rustup-init"
echo "$RUSTUP_SHA256  /tmp/rustup-init" | sha256sum -c - > /dev/null
chmod +x /tmp/rustup-init
/tmp/rustup-init -y -q --profile minimal --default-toolchain none
PATH=$HOME/.cargo/bin:$PATH
rustup toolchain install
rustup target add "$target"

# 版本号要和 Cargo.toml 一致；--version 里的提交号由 build.rs 取 git，挂载进来的仓库属主不同，先放行。
cargo_version=$(sed -n '/^\[workspace\.package\]/,/^\[/s/^version = "\(.*\)"$/\1/p' Cargo.toml)
[ "$cargo_version" = "$version" ] || { echo "build-release: Cargo.toml 版本号是 $cargo_version，不是 $version" >&2; exit 1; }
git config --global --add safe.directory "$PWD"

# cc 等构建脚本对目标为 clang 时自己加 --target；链接器参数、sysroot 由这里给。
tvar=$(echo "$target" | tr 'a-z-' 'A-Z_')
uvar=$(echo "$target" | tr '-' '_')
export "CC_$uvar=clang" "CXX_$uvar=clang++" "AR_$uvar=llvm-ar"
export "CARGO_TARGET_${tvar}_LINKER=clang"
rustflags="-C link-arg=-fuse-ld=lld"
if [ "$arch" = aarch64 ]; then
  export "CFLAGS_$uvar=--sysroot=$sysroot" "CXXFLAGS_$uvar=--sysroot=$sysroot"
  export "BINDGEN_EXTRA_CLANG_ARGS_$uvar=--sysroot=$sysroot"
  rustflags="$rustflags -C link-arg=--target=$target -C link-arg=--sysroot=$sysroot"
fi
export "CARGO_TARGET_${tvar}_RUSTFLAGS=$rustflags"
# 编译目录放在容器里：挂载的仓库里不留 root 属主的文件。
export CARGO_TARGET_DIR=/tmp/target
cargo build --release --locked --target "$target" -p amberlog-server --bin amberlog

bin=$CARGO_TARGET_DIR/$target/release/amberlog
case $arch in
  x86_64) machine="X86-64" ;;
  aarch64) machine="AArch64" ;;
esac
readelf -h "$bin" | grep -q "Machine:.*$machine" || { echo "build-release: $bin 不是 $arch 二进制" >&2; exit 1; }
# 二进制要求的 glibc 符号版本（.gnu.version_r 里的 GLIBC_x.y），取最高的一个与 max_glibc 比较。
need=$(readelf -W -V "$bin" | sed -n 's/.*Name: GLIBC_\([0-9.]*\).*/\1/p' | sort -uV | tail -n 1)
[ -n "$need" ] || { echo "build-release: 读不出 $bin 要求的 glibc 版本" >&2; exit 1; }
if [ "$(printf '%s\n%s\n' "$need" "$max_glibc" | sort -V | tail -n 1)" != "$max_glibc" ]; then
  echo "build-release: $bin 要求 GLIBC_$need，高于 $max_glibc：" >&2
  readelf -W --dyn-syms "$bin" | grep -F "@GLIBC_$need " >&2 || true
  exit 1
fi
echo "build-release: $bin 要求 glibc $need 及以上" >&2

name=amberlog-$version-$target
pkg=/tmp/pkg/$name
rm -rf /tmp/pkg && mkdir -p "$pkg" dist
cp "$bin" "$pkg/amberlog"
cp LICENSE NOTICE CHANGELOG.md "$pkg/"
for f in README.md README.zh-CN.md; do [ ! -f "$f" ] || cp "$f" "$pkg/"; done
cp "$notices" "$pkg/THIRD-PARTY-NOTICES.txt"
tar -C /tmp/pkg --owner=0 --group=0 --numeric-owner -czf "dist/$name.tar.gz" "$name"
(cd dist && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
[ -z "${HOST_UID:-}" ] || chown -R "$HOST_UID:${HOST_GID:-$HOST_UID}" dist
echo "build-release: dist/$name.tar.gz" >&2
