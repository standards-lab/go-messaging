--| tier: standard
-- Deletes at most budget published outbox rows published before the
-- cutoff, oldest publication first: the outbox's retention, run by
-- OutboxRetention's pass on its own, outside any transaction the relay
-- holds. A row awaiting publication or quarantined is never deleted, and
-- quarantined_at IS NULL states that rule although a published row is
-- never quarantined. The subquery's predicate matches
-- messaging_ix_outbox_published's, so the pass reads that index in
-- published_at order and stops at the budget. The relay never runs it.
DELETE FROM messaging_outbox
WHERE seq IN (
    SELECT seq FROM messaging_outbox
    WHERE published_at IS NOT NULL AND quarantined_at IS NULL
      AND published_at < {{before}}
    ORDER BY published_at
    FETCH FIRST {{budget:bigint}} ROWS ONLY
)
