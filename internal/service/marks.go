package service

import (
	"context"
	"errors"

	"github.com/biga-app/server/internal/store"
	"github.com/biga-app/server/internal/wire"
)

var ErrMarkKeyConflict = errors.New("checkpoint mark already exists with a different name")

func (s *Service) MarkCheckpoint(ctx context.Context, id, ownerToken, key, label string) (wire.CheckpointSummary, error) {
	var result wire.CheckpointSummary
	created := false
	err := s.store.Tx(ctx, func(q *store.Queries) error {
		sess, err := q.LockSession(ctx, id)
		if err != nil {
			return sessionErr(err)
		}
		if err := checkOwner(sess, ownerToken); err != nil {
			return err
		}
		row, err := q.MarkByKey(ctx, id, key)
		switch {
		case err == nil:
			if row.Label != label {
				return ErrMarkKeyConflict
			}
			result = summary(id, row)
			return nil
		case !errors.Is(err, store.ErrNotFound):
			return err
		}
		seq, err := q.MaxSeq(ctx, id)
		if err != nil {
			return err
		}
		if seq < 0 {
			return ErrNoVersion
		}
		row = store.CheckpointRow{Seq: seq + 1, Label: label, CreatedAt: s.now(), Named: true}
		if err := q.InsertMark(ctx, id, key, row.Label, row.Seq, row.CreatedAt); err != nil {
			return err
		}
		result, created = summary(id, row), true
		return nil
	})
	if err == nil && created {
		s.publishCheckpoint(ctx, &wire.Checkpoint{CheckpointSummary: result, Changes: []wire.CheckpointChange{}})
	}
	return result, err
}
