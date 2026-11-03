package store

import (
	"context"
	"time"
)

type Extension struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

func (q *Queries) AddExtension(ctx context.Context, sessionID, id, name string, tokenHash []byte) (Extension, error) {
	extension := Extension{ID: id, Name: name}
	err := q.db.QueryRow(ctx, `
		INSERT INTO session_extensions (session_id, id, name, token_hash)
		VALUES ($1, $2, $3, $4) RETURNING created_at`, sessionID, id, name, tokenHash).
		Scan(&extension.CreatedAt)
	return extension, err
}

func (q *Queries) Extensions(ctx context.Context, sessionID string) ([]Extension, error) {
	rows, err := q.db.Query(ctx, `
		SELECT id, name, created_at FROM session_extensions
		WHERE session_id = $1 ORDER BY created_at, id`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	extensions := []Extension{}
	for rows.Next() {
		var extension Extension
		if err := rows.Scan(&extension.ID, &extension.Name, &extension.CreatedAt); err != nil {
			return nil, err
		}
		extensions = append(extensions, extension)
	}
	return extensions, rows.Err()
}

func (q *Queries) DeleteExtension(ctx context.Context, sessionID, id string) error {
	_, err := q.db.Exec(ctx, `DELETE FROM session_extensions WHERE session_id = $1 AND id = $2`, sessionID, id)
	return err
}

func (q *Queries) CheckExtension(ctx context.Context, sessionID string, tokenHash []byte) (bool, error) {
	var allowed bool
	err := q.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM session_extensions WHERE session_id = $1 AND token_hash = $2
		)`, sessionID, tokenHash).Scan(&allowed)
	return allowed, err
}

type CheckpointEventRow struct {
	Seq          int
	CreatedAt    time.Time
	FilesChanged int
	Additions    int
	Deletions    int
}

func (q *Queries) CheckpointEvents(ctx context.Context, sessionID string, after, limit int) ([]CheckpointEventRow, error) {
	rows, err := q.db.Query(ctx, `
		SELECT seq, created_at, files_changed, additions, deletions
		FROM checkpoints WHERE session_id = $1 AND seq > $2
		ORDER BY seq LIMIT $3`, sessionID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []CheckpointEventRow{}
	for rows.Next() {
		var event CheckpointEventRow
		if err := rows.Scan(&event.Seq, &event.CreatedAt, &event.FilesChanged, &event.Additions, &event.Deletions); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}
