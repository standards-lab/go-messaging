# The harness's NATS: the JetStream server the nats module's integration
# suite connects to through MESSAGING_NATS_URL, creating and deleting a
# stream per test. This FROM line is NATS's one pin. It runs JetStream with
# its store in /data, which compose mounts as tmpfs, and is healthy once
# JetStream is enabled, read from the unpublished monitoring port.
FROM nats:2.15.3-alpine

CMD ["-js", "-sd", "/data", "-m", "8222"]

HEALTHCHECK --start-period=30s --start-interval=1s --interval=5s --timeout=3s --retries=5 \
    CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8222/healthz?js-enabled-only=true"]
