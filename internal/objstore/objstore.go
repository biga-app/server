// Package objstore keeps content in an object store, so the database holds
// only what it needs to find it.
//
// Objects are named sessions/<session>/<hash>, so everything one session owns
// shares a prefix and goes with it when the session is deleted.
package objstore

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"
)

// ErrNotFound reports that no object has the key.
var ErrNotFound = errors.New("object not found")

// Store is a place to keep content by key.
type Store interface {
	// Put stores data under key, replacing what was there. Storing the same
	// bytes twice is harmless.
	Put(ctx context.Context, key string, data []byte) error
	// Get returns the bytes under key, or [ErrNotFound].
	Get(ctx context.Context, key string) ([]byte, error)
	// DeletePrefix removes every object whose key starts with prefix.
	DeletePrefix(ctx context.Context, prefix string) error
	// Delete removes the keys; a key that is already gone is not an error.
	Delete(ctx context.Context, keys ...string) error
	// Ping checks that the store is reachable.
	Ping(ctx context.Context) error
}

// Key names a session's blob.
func Key(sessionID, hash string) string { return Prefix(sessionID) + hash }

// Prefix is the prefix shared by everything a session owns.
func Prefix(sessionID string) string { return "sessions/" + sessionID + "/" }

// Memory is a [Store] in process memory, for tests and for trying things out.
type Memory struct {
	mu      sync.Mutex
	objects map[string][]byte
	// FailPuts makes the next n Put calls fail, to test what happens when the
	// store is down.
	FailPuts int
}

// NewMemory returns an empty store.
func NewMemory() *Memory { return &Memory{objects: make(map[string][]byte)} }

var errInjected = errors.New("objstore: injected failure")

func (m *Memory) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailPuts > 0 {
		m.FailPuts--
		return errInjected
	}
	m.objects[key] = slices.Clone(data)
	return nil
}

func (m *Memory) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return slices.Clone(data), nil
}

func (m *Memory) DeletePrefix(_ context.Context, prefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			delete(m.objects, key)
		}
	}
	return nil
}

func (m *Memory) Delete(_ context.Context, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		delete(m.objects, key)
	}
	return nil
}

func (m *Memory) Ping(context.Context) error { return nil }

// Keys lists what is stored, sorted.
func (m *Memory) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objects))
	for key := range m.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
