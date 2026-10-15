--| transaction: none
-- The outbox's retention reads the published rows in published_at order,
-- oldest first. The index's predicate is the retention's own, so it holds
-- only the rows the retention can delete. CONCURRENTLY builds it without
-- blocking the outbox's writes, which a transaction block refuses, so the
-- migration runs outside one and holds this one statement.
CREATE INDEX CONCURRENTLY messaging_ix_outbox_published ON messaging_outbox (published_at)
    WHERE published_at IS NOT NULL AND quarantined_at IS NULL
