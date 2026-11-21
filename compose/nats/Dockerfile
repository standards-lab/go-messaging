# The harness's NATS with JetStream, for the nats module's integration suite.
# See docs/internals.md
FROM nats:2.15.3-alpine

CMD ["-js", "-sd", "/data", "-m", "8222"]

HEALTHCHECK --start-period=30s --start-interval=1s --interval=5s --timeout=3s --retries=5 \
    CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8222/healthz?js-enabled-only=true"]
