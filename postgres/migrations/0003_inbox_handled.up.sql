--| transaction: none
-- The inbox's retention reads the handled rows in handled_at order, oldest
-- first. CONCURRENTLY builds the index without blocking the inbox's
-- claims, which a transaction block refuses, so the migration runs outside
-- one and holds this one statement.
CREATE INDEX CONCURRENTLY messaging_ix_inbox_handled ON messaging_inbox (handled_at)
