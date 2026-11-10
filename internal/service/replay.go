package service

import (
	"context"
	"errors"

	"github.com/biga-app/server/internal/store"
	"github.com/biga-app/server/internal/wire"
)

// MaxReplay is the most checkpoints a reconnecting viewer is sent. It matches
// the live buffer: a viewer further behind than that is better off reloading
// the session state.
const MaxReplay = 64

// Replay returns the checkpoints after a viewer's position, oldest first, so a
// viewer that dropped can catch up without reloading the session. It reports
// false when the viewer must reload instead: the tree was replaced since the
// snapshot it holds, or it is more than [MaxReplay] checkpoints behind.
func (s *Service) Replay(ctx context.Context, id string, after int, snapshot int64) ([]*wire.Checkpoint, bool, error) {
	var (
		out []*wire.Checkpoint
		ok  bool
	)
	err := s.store.ReadTx(ctx, func(q *store.Queries) error {
		if _, err := q.Session(ctx, id); err != nil {
			return sessionErr(err)
		}
		snap, err := q.Snapshot(ctx, id)
		if errors.Is(err, store.ErrNotFound) || (err == nil && snap.Timestamp.UnixMilli() != snapshot) {
			return nil
		}
		if err != nil {
			return err
		}
		rows, err := q.CheckpointsAfter(ctx, id, after, MaxReplay+1)
		if err != nil || len(rows) > MaxReplay {
			return err
		}
		for _, r := range rows {
			out = append(out, &wire.Checkpoint{CheckpointSummary: summary(id, r.Row), Changes: r.Changes})
		}
		ok = true
		return nil
	})
	return out, ok, err
}
