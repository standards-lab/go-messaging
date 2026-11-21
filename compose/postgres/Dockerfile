# The harness's PostgreSQL: the server the integration suite connects to
# through MESSAGING_DSN, creating and dropping a database per test. This
# FROM line is PostgreSQL's one pin. Its user, password, and database are
# the development values MESSAGING_DSN names, not secrets. It is healthy
# once the server accepts TCP connections to the app database: the first
# start's initialization runs a temporary server on the socket alone, which
# would report ready before the real server starts.
FROM postgres:19.0-alpine

ENV POSTGRES_USER=app \
    POSTGRES_PASSWORD=app \
    POSTGRES_DB=app

HEALTHCHECK --start-period=30s --start-interval=1s --interval=5s --timeout=3s --retries=5 \
    CMD ["pg_isready", "-h", "127.0.0.1", "-U", "app", "-d", "app"]
