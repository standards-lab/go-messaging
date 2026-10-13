--| tier: standard
--| transaction: required
-- Writes one event into the outbox, in the transaction that makes the state
-- it reports true. The header is event.Encode's headers as JSON, typed by
-- the column it is inserted into, so the statement needs no cast. It runs on
-- the command's own sqlate *Tx, through the outbox's sink. A second event
-- with the same source and id is messaging_uq_outbox_event's unique
-- violation, which fails the transaction.
INSERT INTO messaging_outbox (source, id, header, data)
VALUES ({{source}}, {{id}}, {{header}}, {{data}})
