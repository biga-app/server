package service

import (
	"context"

	"github.com/biga-app/server/internal/linediff"
	"github.com/biga-app/server/internal/store"
	"github.com/biga-app/server/internal/wire"
)

// maxMissingReported bounds how many absent hashes an error names.
const maxMissingReported = 100

// MissingBlobs reports which of the hashes have no stored content yet, in the
// order asked.
func (s *Service) MissingBlobs(ctx context.Context, id, token string, hashes []string) ([]string, error) {
	if err := s.Authorize(ctx, id, token); err != nil {
		return nil, err
	}
	have, err := s.store.Q().ExistingBlobs(ctx, id, hashes)
	if err != nil {
		return nil, err
	}
	missing := make([]string, 0, len(hashes))
	seen := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		if !have[h] && !seen[h] {
			missing = append(missing, h)
		}
		seen[h] = true
	}
	return missing, nil
}

// StoreBlobs saves content ahead of the snapshot that will refer to it. It
// counts against the session's storage quota like any other content.
func (s *Service) StoreBlobs(ctx context.Context, id, token string, blobs []wire.File) error {
	encrypted, err := s.Encrypted(ctx, id)
	if err != nil {
		return err
	}
	encoded := encodeFiles(blobs, encrypted)
	return s.store.Tx(ctx, func(q *store.Queries) error {
		if _, err := s.lockForWrite(ctx, q, id, token); err != nil {
			return err
		}
		if err := s.storeBlobs(ctx, q, id, encoded); err != nil {
			return err
		}
		return q.TouchSession(ctx, id)
	})
}

// requireBlobs fails with a [MissingBlobsError] when a snapshot's deferred
// files have no stored content.
func requireBlobs(ctx context.Context, q *store.Queries, id string, files []wire.File) error {
	var hashes []string
	for _, f := range files {
		if f.Deferred {
			hashes = append(hashes, f.Hash)
		}
	}
	have, err := q.ExistingBlobs(ctx, id, hashes)
	if err != nil {
		return err
	}
	var missing []string
	for _, h := range hashes {
		if !have[h] {
			missing = append(missing, h)
			if len(missing) == maxMissingReported {
				break
			}
		}
	}
	if len(missing) > 0 {
		return &MissingBlobsError{Hashes: missing}
	}
	return nil
}

type lineCount struct{ added, removed int }

// deferredCounts computes the line counts of files whose content is in the
// database rather than in the request, a few files at a time so a large
// snapshot does not have to fit in memory.
func (s *Service) deferredCounts(ctx context.Context, q *store.Queries, sess store.SessionRow, files []wire.File, prior map[string]store.HeadFile) (map[string]lineCount, error) {
	const batch = 100
	var pending []wire.File
	for _, f := range files {
		if p, ok := prior[f.Path]; f.Deferred && (!ok || p.Hash != f.Hash) {
			pending = append(pending, f)
		}
	}
	out := make(map[string]lineCount, len(pending))
	for start := 0; start < len(pending); start += batch {
		group := pending[start:min(start+batch, len(pending))]
		var hashes []string
		for _, f := range group {
			hashes = append(hashes, f.Hash)
			if p, ok := prior[f.Path]; ok && !p.Binary {
				hashes = append(hashes, p.Hash)
			}
		}
		blobs, err := s.readBlobs(ctx, q, sess, hashes)
		if err != nil {
			return nil, err
		}
		for _, f := range group {
			p, existed := prior[f.Path]
			switch {
			case existed && !p.Binary:
				added, removed := linediff.Count(blobs[p.Hash].Content, blobs[f.Hash].Content)
				out[f.Path] = lineCount{added, removed}
			case !existed:
				out[f.Path] = lineCount{added: linediff.Lines(blobs[f.Hash].Content)}
			}
		}
	}
	return out, nil
}
