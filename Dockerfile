# Amberlog 运行镜像：把发版包（tools/build-release.sh 的产出）装进 Debian 13 精简镜像，以非 root 用户运行。
# 发版时，维护者以两个架构的包为上下文，用它构建 linux/amd64 与 linux/arm64 两种镜像。本地复现时，先按
# tools/build-release.sh 开头的命令把包编进 dist/，再以 dist/ 为构建上下文：
#
#   docker buildx build --platform linux/amd64 --build-arg VERSION=<版本号 X.Y.Z> -f Dockerfile -t amberlog:<版本号> dist
#
# 镜像内：
#   /usr/local/bin/amberlog                   入口，缺省子命令 serve
#   /usr/share/doc/amberlog/                  LICENSE、NOTICE、THIRD-PARTY-NOTICES.txt
#   /var/lib/amberlog                         工作目录，属 amberlog 用户（10001:10001）；可挂卷放 wal.dir
#
# 镜像不设任何 AMBERLOG_ 环境变量：环境变量优先于配置文件，设在镜像里会悄悄盖掉用户配置文件里的同名项。
# 所以 object_store.url、wal.dir 等必填项由运行时的配置文件或环境变量给出。server.http_addr 的缺省值是 0.0.0.0:8800，
# 非回环地址上以明文监听要么配证书，要么声明 server.behind_tls_proxy = true，否则以退出码 2 拒绝启动。
#
# 最终镜像一层 RUN 都没有：解包、建用户、取根证书都在构建机本身的架构上做，构建 arm64 镜像不需要 QEMU。

ARG DEBIAN_IMAGE=debian:13-slim@sha256:28de0877c2189802884ccd20f15ee41c203573bd87bb6b883f5f46362d24c5c2

FROM --platform=$BUILDPLATFORM ${DEBIAN_IMAGE} AS unpack
ARG TARGETARCH
ARG VERSION
COPY amberlog-*.tar.gz amberlog-*.tar.gz.sha256 /dist/
RUN set -eu; \
    [ -n "${VERSION:-}" ] || { echo "Dockerfile: 要用 --build-arg VERSION=<版本号> 指定版本" >&2; exit 2; }; \
    case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-gnu ;; \
      arm64) target=aarch64-unknown-linux-gnu ;; \
      *) echo "Dockerfile: 不支持的架构 $TARGETARCH" >&2; exit 2 ;; \
    esac; \
    name=amberlog-$VERSION-$target; \
    cd /dist && sha256sum -c "$name.tar.gz.sha256"; \
    tar -xzf "$name.tar.gz" -C /tmp && mv "/tmp/$name" /pkg; \
    groupadd --system --gid 10001 amberlog; \
    useradd --system --uid 10001 --gid 10001 --home-dir /var/lib/amberlog --no-create-home --shell /usr/sbin/nologin amberlog; \
    mkdir -p /var/lib/amberlog; \
    export DEBIAN_FRONTEND=noninteractive; \
    apt-get update -qq; \
    apt-get install -y -qq --no-install-recommends ca-certificates > /dev/null; \
    rm -rf /var/lib/apt/lists/*

FROM ${DEBIAN_IMAGE}
ARG VERSION
LABEL org.opencontainers.image.title="Amberlog" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"
# passwd 与 group 取自同一个基础镜像加上 amberlog 用户，与目标架构无关。根证书供连 https 的对象存储与 webhook 时验证对端。
COPY --from=unpack /etc/passwd /etc/group /etc/
COPY --from=unpack /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=unpack /pkg/amberlog /usr/local/bin/amberlog
COPY --from=unpack /pkg/LICENSE /pkg/NOTICE /pkg/THIRD-PARTY-NOTICES.txt /usr/share/doc/amberlog/
COPY --from=unpack --chown=10001:10001 /var/lib/amberlog /var/lib/amberlog
USER 10001:10001
WORKDIR /var/lib/amberlog
EXPOSE 8800
ENTRYPOINT ["/usr/local/bin/amberlog"]
CMD ["serve"]
