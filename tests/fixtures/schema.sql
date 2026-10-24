-- Apply explicitly as the application owner; never run migrations at startup.
CREATE TABLE IF NOT EXISTS uploads (
    row_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id TEXT NOT NULL UNIQUE CHECK (id ~ '^[0-9a-f]{32}$'),
    filename TEXT NOT NULL CHECK (octet_length(filename) BETWEEN 1 AND 200),
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    size BIGINT NOT NULL CHECK (size >= 0),
    ttl BIGINT NOT NULL CHECK (ttl BETWEEN 1 AND 604800),
    state TEXT NOT NULL CHECK (state IN ('uploading','verifying','publishing','ready','deleting','deleted','expired','failed')),
    created_at BIGINT NOT NULL,
    ready_at BIGINT,
    expires_at BIGINT,
    deleted_at BIGINT,
    terminal TEXT CHECK (terminal IN ('deleted','expired','failed')),
    error TEXT,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    CHECK (state <> 'ready' OR (ready_at IS NOT NULL AND expires_at IS NOT NULL)),
    CHECK (state <> 'deleting' OR terminal IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS uploads_pending ON uploads (state, created_at)
    WHERE state IN ('uploading','verifying','publishing','deleting');
CREATE INDEX IF NOT EXISTS uploads_expiry ON uploads (expires_at) WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS uploads_retention ON uploads (deleted_at)
    WHERE state IN ('deleted','expired','failed');
COMMENT ON TABLE uploads IS 'Authoritative File Friend lifecycle and capacity reservations; bytes reside on Apollo';
COMMENT ON COLUMN uploads.id IS 'External 128-bit random capability; never log';
COMMENT ON COLUMN uploads.created_at IS 'Unix seconds at admission';
COMMENT ON COLUMN uploads.version IS 'Optimistic concurrency version; intents commit before filesystem operations';
