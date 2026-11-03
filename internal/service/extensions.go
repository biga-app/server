package service

import (
	"context"
	"crypto/rand"
	"errors"
	"strconv"

	"github.com/biga-sh/server/internal/auth"
	"github.com/biga-sh/server/internal/store"
	"github.com/biga-sh/server/internal/wire"
)

var (
	ErrExtensionLimit = errors.New("session extension limit reached")
	ErrExtensionToken = errors.New("extension token missing or invalid")
)

func extensionView(row store.Extension) wire.Extension {
	return wire.Extension{ID: row.ID, Name: row.Name, CreatedAt: wire.Time(row.CreatedAt)}
}

func (s *Service) CreateExtension(ctx context.Context, sessionID, ownerToken, name string) (wire.ExtensionGrant, error) {
	token := auth.NewExtensionToken()
	var grant wire.ExtensionGrant
	err := s.store.Tx(ctx, func(q *store.Queries) error {
		session, err := q.LockSession(ctx, sessionID)
		if err != nil {
			return sessionErr(err)
		}
		if err := checkOwner(session, ownerToken); err != nil {
			return err
		}
		extensions, err := q.Extensions(ctx, sessionID)
		if err != nil {
			return err
		}
		if len(extensions) >= 20 {
			return ErrExtensionLimit
		}
		row, err := q.AddExtension(ctx, sessionID, rand.Text(), name, auth.HashToken(token))
		if err == nil {
			grant = wire.ExtensionGrant{Extension: extensionView(row), Token: token}
		}
		return err
	})
	return grant, err
}

func (s *Service) Extensions(ctx context.Context, sessionID, ownerToken string) ([]wire.Extension, error) {
	var extensions []wire.Extension
	err := s.store.ReadTx(ctx, func(q *store.Queries) error {
		session, err := q.Session(ctx, sessionID)
		if err != nil {
			return sessionErr(err)
		}
		if err := checkOwner(session, ownerToken); err != nil {
			return err
		}
		rows, err := q.Extensions(ctx, sessionID)
		if err != nil {
			return err
		}
		extensions = make([]wire.Extension, 0, len(rows))
		for _, row := range rows {
			extensions = append(extensions, extensionView(row))
		}
		return nil
	})
	return extensions, err
}

func (s *Service) RevokeExtension(ctx context.Context, sessionID, ownerToken, id string) error {
	return s.store.Tx(ctx, func(q *store.Queries) error {
		session, err := q.LockSession(ctx, sessionID)
		if err != nil {
			return sessionErr(err)
		}
		if err := checkOwner(session, ownerToken); err != nil {
			return err
		}
		return q.DeleteExtension(ctx, sessionID, id)
	})
}

func (s *Service) ExtensionFeed(ctx context.Context, sessionID, token string, after, limit int) (wire.ExtensionFeed, error) {
	feed := wire.ExtensionFeed{Events: []wire.CheckpointEvent{}, NextAfter: after}
	if !auth.IsExtensionToken(token) {
		return feed, ErrExtensionToken
	}
	err := s.store.ReadTx(ctx, func(q *store.Queries) error {
		allowed, err := q.CheckExtension(ctx, sessionID, auth.HashToken(token))
		if err != nil {
			return err
		}
		if !allowed {
			return ErrExtensionToken
		}
		rows, err := q.CheckpointEvents(ctx, sessionID, after, limit)
		if err != nil {
			return err
		}
		for _, row := range rows {
			eventType := "checkpoint.created"
			if row.MarkName != "" {
				eventType = "checkpoint.marked"
			}
			feed.Events = append(feed.Events, wire.CheckpointEvent{
				ID:   sessionID + ":" + strconv.Itoa(row.Seq),
				Type: eventType, Name: row.MarkName, Version: 1, SessionID: sessionID,
				Seq: row.Seq, CreatedAt: wire.Time(row.CreatedAt),
				FilesChanged: row.FilesChanged, Additions: row.Additions, Deletions: row.Deletions,
			})
			feed.NextAfter = row.Seq
		}
		return nil
	})
	return feed, err
}
