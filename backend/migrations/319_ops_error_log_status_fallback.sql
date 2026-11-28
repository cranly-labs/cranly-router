-- Older writers can omit the wire status even when a logical code is present.
-- Keep their display fallback. Current writers explicitly disable it, including
-- for WebSocket messages that have no per-message HTTP response.
ALTER TABLE ops_error_logs
    ADD COLUMN IF NOT EXISTS legacy_client_status BOOLEAN NOT NULL DEFAULT TRUE;

-- Both Ops and request lookup use this projection. NULL means no observed HTTP
-- response for current writers or no usable status from an older writer.
CREATE OR REPLACE FUNCTION ops_client_http_status(wire INT, logical INT, legacy BOOLEAN)
RETURNS INT LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT COALESCE(wire, CASE WHEN legacy THEN logical END)
$$;
