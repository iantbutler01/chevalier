FROM coredns/coredns:1.12.1 AS dns
FROM envoyproxy/envoy:v1.33.0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 iproute2 iptables && rm -rf /var/lib/apt/lists/*
COPY --from=dns /coredns /usr/local/bin/coredns
ENTRYPOINT ["python3", "/fixture/verify.py"]
