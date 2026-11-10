package service

import (
	"context"
	"time"
)

// CleanupResult reports what a retention sweep did.
type CleanupResult struct {
	// Ended counts active sessions marked ended because their CLI went
	// silent past the abandon window.
	Ended int64 `json:"ended"`
	// Deleted counts ended sessions removed after the retention period.
	Deleted int64 `json:"deleted"`
	// Orphans counts uploaded blobs removed because no snapshot or
	// checkpoint ever referred to them.
	Orphans int64 `json:"orphans"`
	// Skipped is true when another instance was already sweeping.
	Skipped bool `json:"skipped,omitempty"`
}

const (
	// deleteBatch is how many sessions one sweep removes.
	deleteBatch = 200
	// orphanBatch is how many orphaned blobs one sweep removes.
	orphanBatch = 1000
	// defaultOrphanAfter is how long unreferenced uploads are kept, long
	// enough for a slow first upload to finish.
	defaultOrphanAfter = 24 * time.Hour
)

// Cleanup runs the retention sweep once. Only one instance sweeps at a
// time.
func (s *Service) Cleanup(ctx context.Context) (CleanupResult, error) {
	var res CleanupResult
	now := s.now()
	ran, err := s.store.WithCleanupLock(ctx, func() error {
		var err error
		if res.Ended, err = s.store.Q().MarkAbandoned(ctx, now.Add(-s.opts.AbandonAfter)); err != nil {
			return err
		}
		if res.Deleted, err = s.deleteExpired(ctx, now); err != nil {
			return err
		}
		res.Orphans, err = s.deleteOrphans(ctx, now)
		return err
	})
	res.Skipped = !ran && err == nil
	return res, err
}

// deleteExpired removes sessions past retention. A session's objects go first:
// if that fails the session is left for the next sweep, since deleting its
// rows would strand the objects.
func (s *Service) deleteExpired(ctx context.Context, now time.Time) (int64, error) {
	ids, err := s.store.Q().ExpiredSessionIDs(ctx, now.Add(-s.opts.Retention), s.opts.Protected, deleteBatch)
	if err != nil {
		return 0, err
	}
	removable := ids[:0]
	for _, id := range ids {
		if err := s.store.DeleteObjects(ctx, id); err != nil {
			s.log.Error("delete session objects", "session", id, "error", err)
			continue
		}
		removable = append(removable, id)
	}
	if len(removable) == 0 {
		return 0, nil
	}
	return s.store.Q().DeleteSessions(ctx, removable)
}

// deleteOrphans removes blobs that were uploaded but never referenced. Rows go
// first and only the ones still unreferenced at that moment, so content a late
// request has just claimed is never taken from under it.
func (s *Service) deleteOrphans(ctx context.Context, now time.Time) (int64, error) {
	after := s.opts.OrphanAfter
	if after <= 0 {
		after = defaultOrphanAfter
	}
	q := s.store.Q()
	refs, err := q.UnattachedBlobs(ctx, now.Add(-after), "", orphanBatch)
	if err != nil || len(refs) == 0 {
		return 0, err
	}
	gone, err := q.DeleteBlobs(ctx, refs)
	if err != nil {
		return 0, err
	}
	if err := s.store.DeleteBlobObjects(ctx, gone); err != nil {
		s.log.Error("delete orphaned objects", "count", len(gone), "error", err)
	}
	return int64(len(gone)), nil
}

// RunCleanup sweeps every interval until ctx is done.
func (s *Service) RunCleanup(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		res, err := s.Cleanup(ctx)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			s.log.Error("retention sweep failed", "error", err)
		case res.Ended > 0 || res.Deleted > 0 || res.Orphans > 0:
			s.log.Info("retention sweep", "ended", res.Ended, "deleted", res.Deleted, "orphans", res.Orphans)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
