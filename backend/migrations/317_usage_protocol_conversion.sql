-- Persist bounded protocol conversion diagnostics only when cross-protocol IR
-- conversion, Responses client-tool adaptation, or native trc1 carrier replay
-- ran on the usage-producing attempt; lossless native passthrough rows stay NULL.
ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS protocol_conversion JSONB;
