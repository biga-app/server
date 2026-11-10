package objstore

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
)

func contract(t *testing.T, s Store) {
	t.Helper()
	ctx := t.Context()
	a, b := Key("sa", "h1"), Key("sa", "h2")
	other := Key("sb", "h1")

	if _, err := s.Get(ctx, a); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get of a missing key = %v, want ErrNotFound", err)
	}
	for key, data := range map[string][]byte{a: []byte("one"), b: {}, other: []byte("keep")} {
		if err := s.Put(ctx, key, data); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	if err := s.Put(ctx, a, []byte("one")); err != nil {
		t.Fatalf("storing the same bytes twice: %v", err)
	}
	if got, err := s.Get(ctx, a); err != nil || string(got) != "one" {
		t.Errorf("Get = %q, %v", got, err)
	}
	if got, err := s.Get(ctx, b); err != nil || len(got) != 0 {
		t.Errorf("empty object = %q, %v", got, err)
	}
	big := bytes.Repeat([]byte("x"), 3<<20)
	if err := s.Put(ctx, Key("sa", "big"), big); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, Key("sa", "big")); err != nil || !bytes.Equal(got, big) {
		t.Errorf("large object round trip failed: %d bytes, %v", len(got), err)
	}

	if err := s.Delete(ctx, a, Key("sa", "never-existed")); err != nil {
		t.Errorf("deleting a missing key: %v", err)
	}
	if _, err := s.Get(ctx, a); !errors.Is(err, ErrNotFound) {
		t.Errorf("deleted key still there: %v", err)
	}
	if err := s.DeletePrefix(ctx, Prefix("sa")); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{b, Key("sa", "big")} {
		if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s survived DeletePrefix: %v", key, err)
		}
	}
	if got, err := s.Get(ctx, other); err != nil || string(got) != "keep" {
		t.Errorf("another session's object: %q, %v", got, err)
	}
	if err := s.DeletePrefix(ctx, Prefix("empty")); err != nil {
		t.Errorf("DeletePrefix with nothing under it: %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Errorf("Ping: %v", err)
	}
	_ = s.DeletePrefix(ctx, Prefix("sb"))
}

func TestMemory(t *testing.T) { contract(t, NewMemory()) }

func TestMemoryFailsOnRequest(t *testing.T) {
	m := NewMemory()
	m.FailPuts = 1
	if err := m.Put(t.Context(), "k", nil); err == nil {
		t.Fatal("injected failure did not happen")
	}
	if err := m.Put(t.Context(), "k", nil); err != nil {
		t.Fatalf("second Put: %v", err)
	}
}

func TestKeysShareASessionPrefix(t *testing.T) {
	if got, want := Key("abc", "h"), "sessions/abc/h"; got != want {
		t.Errorf("Key = %q, want %q", got, want)
	}
	if got := Prefix("abc"); got != "sessions/abc/" {
		t.Errorf("Prefix = %q", got)
	}
}

// TestS3 runs the contract against a real service. Point BIGA_TEST_S3 at
// "endpoint,bucket,accesskey,secretkey" of a scratch bucket that exists,
// e.g. a local MinIO.
func TestS3(t *testing.T) {
	spec := os.Getenv("BIGA_TEST_S3")
	if spec == "" {
		t.Skip("BIGA_TEST_S3 is not set")
	}
	parts := strings.Split(spec, ",")
	if len(parts) != 4 {
		t.Fatalf("BIGA_TEST_S3 = %q, want endpoint,bucket,accesskey,secretkey", spec)
	}
	s, err := NewS3(S3Config{Endpoint: parts[0], Bucket: parts[1], AccessKey: parts[2], SecretKey: parts[3], Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(t.Context()); err != nil {
		t.Fatal(err)
	}
	contract(t, s)
}
