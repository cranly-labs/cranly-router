-- Persist the committed HTTP wire status sent to the client separately from
-- status_code when in-band stream failures record a logical 4xx/5xx status
-- after an earlier 2xx commitment. Historical rows keep NULL and fall back to
-- status_code when queried.
ALTER TABLE ops_error_logs
    ADD COLUMN IF NOT EXISTS client_status_code INT;
