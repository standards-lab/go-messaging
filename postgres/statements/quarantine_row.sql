--| tier: standard
--| transaction: required
-- Quarantines a claimed row the relay cannot publish, in the transaction
-- that claimed it, recording why, so the claim never returns the row again.
-- The row stays in the outbox for an operator to inspect.
UPDATE messaging_outbox
SET quarantined_at = CURRENT_TIMESTAMP, quarantine_reason = {{reason}}
WHERE seq = {{seq:bigint}}
