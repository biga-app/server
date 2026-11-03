package store

import (
	"context"
	"time"
)

func (q *Queries) MarkByKey(ctx context.Context, sessionID, key string) (CheckpointRow, error) {
	var row CheckpointRow
	err := q.db.QueryRow(ctx, `SELECT seq, label, created_at FROM checkpoints WHERE session_id = $1 AND marker_key = $2`, sessionID, key).
		Scan(&row.Seq, &row.Label, &row.CreatedAt)
	row.Named = true
	return row, notFound(err)
}

func (q *Queries) InsertMark(ctx context.Context, sessionID, key, label string, seq int, createdAt time.Time) error {
	_, err := q.db.Exec(ctx, `
		INSERT INTO checkpoints (session_id, seq, label, created_at, marker_key)
		VALUES ($1, $2, $3, $4, $5)`, sessionID, seq, label, createdAt, key)
	return err
}
