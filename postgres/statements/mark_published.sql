--| tier: standard
--| transaction: required
-- Marks a claimed row published, in the transaction that claimed it, so
-- the mark commits only after the handler published the event.
UPDATE messaging_outbox
SET published_at = CURRENT_TIMESTAMP
WHERE seq = {{seq:bigint}}
