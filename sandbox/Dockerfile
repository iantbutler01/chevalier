# syntax=docker/dockerfile:1.7
FROM rust:1.96-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libssl-dev pkg-config
WORKDIR /src
COPY hash hash
COPY sandbox/Cargo.toml sandbox/Cargo.lock sandbox/
COPY sandbox/crates sandbox/crates
COPY sandbox/vmd sandbox/vmd
COPY sandbox/portproxy sandbox/portproxy
COPY sandbox/proto sandbox/proto
COPY sandbox/patches/fuser-0.17.0 sandbox/patches/fuser-0.17.0
WORKDIR /src/sandbox
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/sandbox/target \
    cargo build --locked --release -p vmd --bin vmd --bin vmdctl -p portproxy --bin portproxy \
    && mkdir /out && cp target/release/vmd target/release/vmdctl target/release/portproxy /out/

FROM envoyproxy/envoy:v1.33.0 AS envoy
FROM coredns/coredns:1.12.1 AS coredns
FROM rust:1.96-bookworm AS virtiofsd
RUN apt-get update && apt-get install -y --no-install-recommends curl libcap-ng-dev libseccomp-dev patch pkg-config
COPY sandbox/patches/virtiofsd-1.13.0-lock-forwarding.patch /src/patches/virtiofsd-1.13.0-lock-forwarding.patch
COPY sandbox/scripts/build-patched-virtiofsd.sh /src/scripts/build-patched-virtiofsd.sh
RUN VIRTIOFSD_INSTALL_ROOT=/usr/local VIRTIOFSD_SKIP_TESTS=1 /src/scripts/build-patched-virtiofsd.sh
FROM ubuntu:26.04
ARG TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive
ENV PROXY_BIN=/opt/chevalier/portproxy/bin
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates cryptsetup-bin docker.io docker-buildx dosfstools e2fsprogs extlinux fdisk fuse3 \
    iproute2 iptables kpartx parted qemu-system-x86 qemu-system-arm qemu-efi-aarch64 \
    qemu-utils util-linux virtiofsd \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/vmd /out/vmdctl /usr/local/bin/
COPY --from=builder /out/portproxy /opt/chevalier/portproxy/bin/portproxy-linux-${TARGETARCH}
COPY --from=virtiofsd /usr/local/bin/virtiofsd /usr/libexec/virtiofsd
COPY --from=envoy /usr/local/bin/envoy /usr/local/bin/envoy
COPY --from=coredns /coredns /usr/local/bin/coredns
COPY sandbox/vmd/assets/threat-domains.seed.hosts /opt/chevalier/network/threat-domains.hosts
EXPOSE 8052
ENTRYPOINT ["vmd"]
