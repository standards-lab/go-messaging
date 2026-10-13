--| tier: native
--| transaction: required
--| native: INSERT ... ON CONFLICT DO NOTHING, so a repeated claim changes no row instead of failing the handler's transaction. Port: an engine must insert or skip in one statement (SQLite has ON CONFLICT DO NOTHING; MySQL has INSERT IGNORE; SQL Server and Oracle have MERGE).
-- Records that a consumer handled an event, in the handler's own
-- transaction, a sqlate *Tx. One row affected is the first claim.
INSERT INTO messaging_inbox (consumer, source, id)
VALUES ({{consumer}}, {{source}}, {{id}})
ON CONFLICT DO NOTHING
