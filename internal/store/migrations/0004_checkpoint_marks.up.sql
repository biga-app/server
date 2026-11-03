ALTER TABLE checkpoints ADD COLUMN marker_key text;
CREATE UNIQUE INDEX checkpoint_marker_key ON checkpoints (session_id, marker_key) WHERE marker_key IS NOT NULL;
