CREATE TABLE session_extensions (
    session_id text NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    id text NOT NULL,
    name text NOT NULL,
    token_hash bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, id),
    UNIQUE (session_id, token_hash)
);
