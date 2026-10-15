--| tier: standard
-- Deletes at most budget inbox rows handled before the cutoff, oldest
-- first: the inbox's retention, run by InboxRetention's pass on its own. A
-- redelivery of a deleted row's event is claimed again, so the retention
-- age must outlast every redelivery the broker can make. The subquery
-- reads messaging_ix_inbox_handled in handled_at order and stops at the
-- budget. The inbox's claim never runs it.
DELETE FROM messaging_inbox
WHERE (consumer, source, id) IN (
    SELECT consumer, source, id FROM messaging_inbox
    WHERE handled_at < {{before}}
    ORDER BY handled_at
    FETCH FIRST {{budget:bigint}} ROWS ONLY
)
