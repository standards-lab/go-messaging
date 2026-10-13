--| tier: standard
-- Counts the outbox rows awaiting publication, neither published nor
-- quarantined: the outbox's lag, for a consumer that reports or monitors
-- it. A quarantined row awaits an operator, not the relay, so it is not
-- counted. The relay never runs it.
SELECT count(*) FROM messaging_outbox
WHERE published_at IS NULL AND quarantined_at IS NULL
