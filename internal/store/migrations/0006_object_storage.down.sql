DROP INDEX blobs_in_objects;
DROP INDEX blobs_unattached;
ALTER TABLE blobs DROP COLUMN attached_at;
ALTER TABLE blobs DROP COLUMN created_at;
ALTER TABLE blobs DROP COLUMN stored_bytes;
ALTER TABLE blobs DROP COLUMN location;
ALTER TABLE blobs ALTER COLUMN content SET NOT NULL;
