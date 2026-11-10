-- Content can live in an object store, leaving only a pointer here.
ALTER TABLE blobs ALTER COLUMN content DROP NOT NULL;
ALTER TABLE blobs ADD COLUMN location text NOT NULL DEFAULT 'db';
ALTER TABLE blobs ADD COLUMN stored_bytes bigint;
ALTER TABLE blobs ADD COLUMN created_at timestamptz NOT NULL DEFAULT now();

-- A blob is attached once a snapshot or checkpoint refers to it. One that never
-- is, for instance from an upload that was interrupted, is swept after a grace
-- period. Rows that exist now are all attached; the default is dropped so new
-- rows start out unattached.
ALTER TABLE blobs ADD COLUMN attached_at timestamptz DEFAULT now();
ALTER TABLE blobs ALTER COLUMN attached_at DROP DEFAULT;
CREATE INDEX blobs_unattached ON blobs (created_at) WHERE attached_at IS NULL;
CREATE INDEX blobs_in_objects ON blobs (session_id) WHERE location <> 'db';
