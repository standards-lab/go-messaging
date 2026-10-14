# The harness's NATS: the JetStream server the nats module's integration
# suite connects to through MESSAGING_NATS_URL, creating and deleting a
# stream per test. This FROM line is NATS's one pin.
FROM nats:2.15.1-alpine

# JetStream on, its store in /data, which compose mounts as tmpfs, and the
# monitoring endpoint on 8222 for the health check.
CMD ["-js", "-sd", "/data", "-m", "8222"]

# Healthy once JetStream is enabled, not merely once the server listens: a
# suite that starts earlier would fail to create its streams. The probe uses
# the image's busybox wget.
HEALTHCHECK --start-period=30s --start-interval=1s --interval=5s --timeout=3s --retries=5 \
    CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8222/healthz?js-enabled-only=true"]
