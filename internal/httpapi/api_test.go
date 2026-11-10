package httpapi

// These tests drive the real API against Postgres. Point
// BIGA_TEST_DATABASE_URL at a scratch database to run them, e.g.
//
//	docker run -d -p 127.0.0.1:55432:5432 -e POSTGRES_PASSWORD=test postgres:17-alpine
//	BIGA_TEST_DATABASE_URL=postgres://postgres:test@127.0.0.1:55432/postgres go test ./...

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"

	"github.com/biga-app/server/internal/config"
	"github.com/biga-app/server/internal/events"
	"github.com/biga-app/server/internal/ratelimit"
	"github.com/biga-app/server/internal/service"
	"github.com/biga-app/server/internal/store"
	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

var (
	migrateOnce sync.Once
	migrateErr  error
	quiet       = slog.New(slog.NewTextHandler(io.Discard, nil))
)

type harness struct {
	t   *testing.T
	srv *httptest.Server
	st  *store.Store
}

func newHarness(t *testing.T, env map[string]string) *harness {
	t.Helper()
	dsn := os.Getenv("BIGA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("BIGA_TEST_DATABASE_URL is not set")
	}
	migrateOnce.Do(func() { migrateErr = store.Migrate(context.Background(), dsn, quiet) })
	if migrateErr != nil {
		t.Fatalf("migrate: %v", migrateErr)
	}
	vars := map[string]string{"DATABASE_URL": dsn, "BIGA_PUBLIC_URL": "https://biga.example"}
	maps.Copy(vars, env)
	cfg, err := config.Load(func(k string) string { return vars[k] })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	st, err := store.Open(t.Context(), dsn, quiet)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)
	hub := events.NewHub(nil, quiet)
	svc := service.New(st, hub, service.Options{
		SessionStale:   cfg.SessionStale,
		EagerMaxBytes:  cfg.EagerMaxBytes,
		EagerMaxFiles:  cfg.EagerMaxFiles,
		StorageQuota:   cfg.Limits.SessionStorageBytes,
		PublicURL:      cfg.PublicURL,
		BlobCacheBytes: cfg.BlobCacheBytes,
		Limits: wire.Limits{
			MaxSnapshotFiles:  cfg.Limits.SnapshotFiles,
			MaxFileBytes:      cfg.Limits.FileBytes,
			MaxBlobBatchBytes: cfg.Limits.CheckpointBytes,
		},
	}, quiet)
	done := make(chan struct{})
	api := New(Options{Service: svc, Events: hub, Config: cfg, Limiter: ratelimit.NewMemory(), Log: quiet, Done: done, Ready: st.Ping})
	srv := httptest.NewServer(api)
	t.Cleanup(func() { close(done); srv.Close() })
	return &harness{t: t, srv: srv, st: st}
}

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) isProto() bool {
	mt, _, _ := mime.ParseMediaType(r.header.Get("Content-Type"))
	return mt == protoType
}

func (r response) data(t *testing.T, v any) {
	t.Helper()
	if msg, ok := v.(proto.Message); ok {
		if !r.isProto() || r.status >= 300 {
			t.Fatalf("not a protobuf success (%d %s): %s", r.status, r.header.Get("Content-Type"), r.body)
		}
		if err := proto.Unmarshal(r.body, msg); err != nil {
			t.Fatalf("decode %T: %v", msg, err)
		}
		return
	}
	var env struct {
		OK   bool            `json:"ok"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(r.body, &env); err != nil || !env.OK {
		t.Fatalf("not a success envelope (%d): %s", r.status, r.body)
	}
	if err := json.Unmarshal(env.Data, v); err != nil {
		t.Fatalf("decode data: %v", err)
	}
}

func (r response) code() string {
	if r.isProto() {
		var e bigav1.Error
		_ = proto.Unmarshal(r.body, &e)
		return e.GetCode()
	}
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(r.body, &env)
	return env.Error.Code
}

type req struct {
	method, path string
	body         proto.Message
	raw          []byte
	token, key   string
	header       map[string]string
}

func (h *harness) do(q req) response {
	h.t.Helper()
	var body io.Reader
	switch {
	case q.raw != nil:
		body = bytes.NewReader(q.raw)
	case q.body != nil:
		b, err := proto.Marshal(q.body)
		if err != nil {
			h.t.Fatal(err)
		}
		body = bytes.NewReader(b)
	}
	r, err := http.NewRequestWithContext(h.t.Context(), q.method, h.srv.URL+q.path, body)
	if err != nil {
		h.t.Fatal(err)
	}
	r.Header.Set("Accept", protoType)
	if body != nil {
		r.Header.Set("Content-Type", protoType)
	}
	if q.token != "" {
		r.Header.Set("Authorization", "Bearer "+q.token)
	}
	if q.key != "" {
		r.Header.Set("X-Biga-Key", q.key)
	}
	for k, v := range q.header {
		r.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, header: resp.Header, body: b}
}

func (h *harness) expect(q req, status int, code string) response {
	h.t.Helper()
	r := h.do(q)
	if r.status != status || (code != "" && r.code() != code) {
		h.t.Fatalf("%s %s: got %d %q, want %d %q: %s", q.method, q.path, r.status, r.code(), status, code, r.body)
	}
	return r
}

func newSessionID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "t" + hex.EncodeToString(b)
}

func putSession(title string) *bigav1.PutSessionRequest {
	if title == "" {
		return &bigav1.PutSessionRequest{}
	}
	return &bigav1.PutSessionRequest{Title: new(title)}
}

// create makes a session and returns its owner token.
func (h *harness) create(id string) string {
	h.t.Helper()
	var grant bigav1.SessionGrant
	h.expect(req{method: "PUT", path: "/api/sessions/" + id, body: putSession("demo")}, 201, "").data(h.t, &grant)
	if !strings.HasPrefix(grant.GetToken(), "bgs_") {
		h.t.Fatalf("create returned no owner token: %+v", &grant)
	}
	return grant.GetToken()
}

func fileNode(name, path, hash, content string) *bigav1.FileNode {
	return &bigav1.FileNode{Name: name, Path: path, Kind: &bigav1.FileNode_File{File: &bigav1.FileInfo{
		Size: int64(len(content)), Hash: hash, Content: []byte(content),
	}}}
}

func file(name, content string) *bigav1.FileNode {
	return fileNode(name, name, wire.HashOf(content), content)
}

func snapshot(id string, files ...*bigav1.FileNode) *bigav1.FileSnapshot {
	return &bigav1.FileSnapshot{
		SessionId: id, Timestamp: time.Now().UnixMilli(),
		Tree: &bigav1.FileNode{Name: "proj", Path: ".", Kind: &bigav1.FileNode_Directory{
			Directory: &bigav1.DirectoryInfo{Children: files},
		}},
	}
}

func changeType(name string) bigav1.ChangeType {
	return bigav1.ChangeType(bigav1.ChangeType_value["CHANGE_TYPE_"+strings.ToUpper(name)])
}

func entry(path, change, content string) *bigav1.CheckpointEntry {
	e := &bigav1.CheckpointEntry{Path: path, ChangeType: changeType(change), Size: int64(len(content))}
	if change != "deleted" {
		e.Hash, e.Content = new(wire.HashOf(content)), []byte(content)
	}
	return e
}

func checkpoint(id string, entries ...*bigav1.CheckpointEntry) *bigav1.CheckpointUpload {
	return &bigav1.CheckpointUpload{SessionId: id, Timestamp: time.Now().UnixMilli(), Entries: entries}
}

func TestOwnerTokenGuardsEveryWrite(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	base := "/api/sessions/" + id

	var grant bigav1.SessionGrant
	r := h.expect(req{method: "PUT", path: base, body: putSession("demo")}, 201, "")
	r.data(t, &grant)
	token := grant.GetToken()
	if grant.GetShareUrl() != "https://biga.example/session/"+id {
		t.Errorf("shareUrl = %q", grant.GetShareUrl())
	}
	if r.header.Get("Cache-Control") != "no-store" || r.header.Get("Content-Encoding") != "" {
		t.Errorf("a response carrying a token must be uncached and uncompressed: %v", r.header)
	}
	other := h.create(newSessionID(t)) // a valid token for a different session

	// Without the owner token, nobody can take the session over.
	h.expect(req{method: "PUT", path: base, body: putSession("pwned")}, 401, "unauthorized")
	h.expect(req{method: "PUT", path: base, token: other, body: putSession("pwned")}, 403, "forbidden")
	h.expect(req{method: "POST", path: base + "/snapshot", body: snapshot(id, file("a.txt", "evil\n"))}, 401, "unauthorized")
	h.expect(req{method: "POST", path: base + "/snapshot", token: other, body: snapshot(id, file("a.txt", "evil\n"))}, 403, "forbidden")
	h.expect(req{method: "POST", path: base + "/checkpoint", token: "bgs_guess", body: checkpoint(id, entry("a.txt", "added", "x"))}, 403, "forbidden")
	h.expect(req{method: "POST", path: base + "/heartbeat"}, 401, "unauthorized")
	h.expect(req{method: "GET", path: base + "/manifest"}, 401, "unauthorized")
	h.expect(req{method: "DELETE", path: base}, 401, "unauthorized")

	// The owner can do everything.
	var again bigav1.SessionGrant
	h.expect(req{method: "PUT", path: base, token: token, body: putSession("")}, 200, "").data(t, &again)
	if again.GetToken() != "" || again.GetSession().GetTitle() != "demo" {
		t.Errorf("reactivation leaked a token or lost the title: %+v", &again)
	}
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "one\ntwo\n"), file("b.txt", "keep\n"))}, 202, "")
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, entry("a.txt", "modified", "one\n2\nthree\n"))}, 202, "")
	h.expect(req{method: "POST", path: base + "/heartbeat", token: token}, 204, "")

	var m bigav1.Manifest
	h.expect(req{method: "GET", path: base + "/manifest", token: token}, 200, "").data(t, &m)
	if len(m.GetFiles()) != 2 {
		t.Errorf("manifest = %+v", &m)
	}

	// Viewers read without credentials.
	var st bigav1.SessionState
	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(t, &st)
	if st.GetMode() != bigav1.LoadMode_LOAD_MODE_EAGER || len(st.GetCheckpoints()) != 2 || st.GetSnapshot() == nil {
		t.Fatalf("state = %+v", &st)
	}
	if root := st.GetSnapshot().GetTree(); root.GetName() != "proj" || root.GetPath() != "." || len(root.GetDirectory().GetChildren()) != 2 {
		t.Errorf("the tree root keeps the broadcast's name and path: %+v", root)
	}
	latest := st.GetCheckpoints()[0]
	if latest.GetSeq() != 1 || latest.GetLabel() != "Updated a.txt" || latest.GetAdditions() != 2 || latest.GetDeletions() != 1 {
		t.Errorf("latest checkpoint = %+v", latest)
	}
	if first := st.GetCheckpoints()[1]; first.GetLabel() != "Initial snapshot" || first.GetAdditions() != 3 {
		t.Errorf("initial checkpoint = %+v", first)
	}

	var fc bigav1.FileContent
	h.expect(req{method: "GET", path: base + "/file?path=a.txt"}, 200, "").data(t, &fc)
	if string(fc.GetContent()) != "one\n2\nthree\n" {
		t.Errorf("latest content = %q", fc.GetContent())
	}
	h.expect(req{method: "GET", path: base + "/file?path=a.txt&ref=0"}, 200, "").data(t, &fc)
	if string(fc.GetContent()) != "one\ntwo\n" {
		t.Errorf("content at seq 0 = %q", fc.GetContent())
	}
	r = h.expect(req{method: "GET", path: base + "/file?path=a.txt&ref=" + wire.HashOf("one\ntwo\n")}, 200, "")
	if !strings.Contains(r.header.Get("Cache-Control"), "immutable") {
		t.Errorf("hash refs should be immutable: %q", r.header.Get("Cache-Control"))
	}

	var cp bigav1.Checkpoint
	h.expect(req{method: "GET", path: base + "/checkpoints/1"}, 200, "").data(t, &cp)
	if len(cp.GetChanges()) != 1 || cp.GetChanges()[0].GetBeforeHash() != wire.HashOf("one\ntwo\n") {
		t.Errorf("checkpoint 1 = %+v", &cp)
	}

	// Ending is the owner's call, and only once.
	h.expect(req{method: "DELETE", path: base, token: token}, 204, "")
	h.expect(req{method: "DELETE", path: base, token: token}, 409, "invalid_state")
	h.expect(req{method: "POST", path: base + "/heartbeat", token: token}, 409, "inactive_session")
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, entry("a.txt", "deleted", ""))}, 409, "invalid_state")
}

func TestRejoinSnapshotRecordsDeletions(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "a\n"), file("gone.txt", "1\n2\n"))}, 202, "")
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "a\n"))}, 202, "")

	var list bigav1.ListCheckpointsResponse
	h.expect(req{method: "GET", path: base + "/checkpoints?limit=10"}, 200, "").data(t, &list)
	if got := list.GetCheckpoints(); len(got) != 2 || got[0].GetLabel() != "Resumed session" || got[0].GetDeletions() != 2 {
		t.Fatalf("checkpoints = %+v", &list)
	}
	h.expect(req{method: "GET", path: base + "/file?path=gone.txt"}, 404, "not_found")
	var history bigav1.FileHistoryResponse
	h.expect(req{method: "GET", path: base + "/file/history?path=gone.txt"}, 200, "").data(t, &history)
	if got := history.GetHistory(); len(got) != 2 || got[0].GetChangeType() != bigav1.ChangeType_CHANGE_TYPE_DELETED {
		t.Errorf("history = %+v", &history)
	}
}

func TestStateAfterDeletingLastFile(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "hello"))}, 202, "")
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, entry("a.txt", "deleted", ""))}, 202, "")

	var state bigav1.SessionState
	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(t, &state)
	if state.GetSnapshot() == nil {
		t.Fatal("state has no snapshot")
	}
	tree := state.GetSnapshot().GetTree()
	if tree.GetDirectory() == nil || len(tree.GetDirectory().GetChildren()) != 0 || len(state.GetContents()) != 0 {
		t.Fatalf("expected an empty tree and no contents, got tree %+v and %v", tree, state.GetContents())
	}
}

func TestSessionsWithoutATokenAreReadOnly(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	// A row with no owner token, as created before tokens existed.
	if _, _, err := h.st.Q().InsertSession(t.Context(), id, "legacy", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	base := "/api/sessions/" + id
	h.expect(req{method: "GET", path: base}, 200, "")
	h.expect(req{method: "PUT", path: base, body: putSession("")}, 403, "session_locked")
	h.expect(req{method: "PUT", path: base, token: "bgs_anything", body: putSession("")}, 403, "session_locked")
	h.expect(req{method: "POST", path: base + "/snapshot", token: "bgs_anything", body: snapshot(id, file("a", "x"))}, 403, "session_locked")
	h.expect(req{method: "DELETE", path: base}, 403, "session_locked")
}

func TestRejectsMalformedUploads(t *testing.T) {
	t.Parallel()
	h := newHarness(t, map[string]string{"BIGA_MAX_CHECKPOINT_BYTES": "4096"})
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id

	forged := file("a.txt", "real\n")
	forged.GetFile().Content = []byte("something else\n")
	traversal := entry("../../etc/passwd", "added", "x")
	nul := entry("a\x00b", "added", "x")
	nested := fileNode("x", "elsewhere/x", wire.HashOf("y"), "y")

	for name, q := range map[string]req{
		"hash that does not match content": {method: "POST", path: base + "/snapshot", body: snapshot(id, forged)},
		"path that lies about its place":   {method: "POST", path: base + "/snapshot", body: snapshot(id, nested)},
		"duplicate paths":                  {method: "POST", path: base + "/snapshot", body: snapshot(id, file("a", "1"), file("a", "1"))},
		"sessionId of another session":     {method: "POST", path: base + "/snapshot", body: snapshot("someoneelse", file("a", "1"))},
		"path traversal":                   {method: "POST", path: base + "/checkpoint", body: checkpoint(id, traversal)},
		"NUL byte in a path":               {method: "POST", path: base + "/checkpoint", body: checkpoint(id, nul)},
		"unknown change type":              {method: "POST", path: base + "/checkpoint", body: checkpoint(id, &bigav1.CheckpointEntry{Path: "a"})},
		"empty checkpoint":                 {method: "POST", path: base + "/checkpoint", body: checkpoint(id)},
		"not protobuf":                     {method: "POST", path: base + "/checkpoint", raw: []byte{0xff, 0xff, 0xff, 0xff}},
		"title with control characters":    {method: "PUT", path: base, body: putSession("a\x1b[31mb")},
	} {
		q.token = token
		r := h.do(q)
		if r.status != http.StatusBadRequest || r.code() != "invalid_request" {
			t.Errorf("%s: got %d %q: %s", name, r.status, r.code(), r.body)
		}
	}

	h.expect(req{method: "GET", path: "/api/sessions/bad..id/state"}, 400, "invalid_request")
	h.expect(req{method: "GET", path: base + "/file?path=../secret"}, 400, "invalid_request")
	h.expect(req{method: "GET", path: base + "/checkpoints?limit=-1"}, 400, "invalid_request")

	// Size limits apply to the decoded body, so a gzip bomb is refused.
	bombMsg, err := proto.Marshal(checkpoint(id, &bigav1.CheckpointEntry{
		Path: "a", ChangeType: bigav1.ChangeType_CHANGE_TYPE_ADDED, Content: bytes.Repeat([]byte("A"), 1<<20),
	}))
	if err != nil {
		t.Fatal(err)
	}
	var bomb bytes.Buffer
	zw := gzip.NewWriter(&bomb)
	_, _ = zw.Write(bombMsg)
	_ = zw.Close()
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, raw: bomb.Bytes(), header: map[string]string{"Content-Encoding": "gzip"}}, 413, "payload_too_large")
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, raw: []byte("x"), header: map[string]string{"Content-Encoding": "br"}}, 415, "unsupported_encoding")
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, raw: []byte("a=b"), header: map[string]string{"Content-Type": "application/x-www-form-urlencoded"}}, 415, "unsupported_media_type")
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, raw: []byte("{}"), header: map[string]string{"Content-Type": "application/json"}}, 415, "unsupported_media_type")

	// Messages nested beyond the decoder's depth are refused, not recursed into.
	deep := &bigav1.FileNode{Name: "d", Path: "d", Kind: &bigav1.FileNode_Directory{Directory: &bigav1.DirectoryInfo{}}}
	for range maxMessageDepth + 50 {
		deep = &bigav1.FileNode{Name: "d", Path: "d", Kind: &bigav1.FileNode_Directory{Directory: &bigav1.DirectoryInfo{Children: []*bigav1.FileNode{deep}}}}
	}
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, deep)}, 400, "invalid_request")

	// Nothing above was stored.
	var st bigav1.SessionState
	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(t, &st)
	if st.GetSnapshot() != nil || len(st.GetCheckpoints()) != 0 {
		t.Errorf("rejected uploads left data behind: %+v", &st)
	}
}

func TestClientsThatDoNotSpeakProtobufGetAReadableError(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	base := "/api/sessions/" + id

	for _, q := range []req{
		{method: "PUT", path: base, raw: []byte(`{"title":"x"}`), header: map[string]string{"Accept": "application/json", "Content-Type": "application/json"}},
		{method: "GET", path: base + "/state", header: map[string]string{"Accept": "application/json"}},
		{method: "GET", path: base + "/state", header: map[string]string{"Accept": "*/*"}},
	} {
		r := h.do(q)
		var env struct {
			Error struct{ Code, Message string } `json:"error"`
		}
		if err := json.Unmarshal(r.body, &env); err != nil || r.status != http.StatusNotAcceptable || env.Error.Code != "not_acceptable" || !strings.Contains(env.Error.Message, "protobuf") {
			t.Errorf("%s %v: got %d %s", q.method, q.header, r.status, r.body)
		}
	}
}

func TestAccessKeyGatesWrites(t *testing.T) {
	t.Parallel()
	key := "bgk_" + strings.Repeat("k", 40)
	h := newHarness(t, map[string]string{"BIGA_ACCESS_KEYS": key})
	id := newSessionID(t)
	base := "/api/sessions/" + id

	h.expect(req{method: "PUT", path: base, body: putSession("")}, 401, "access_key_required")
	h.expect(req{method: "PUT", path: base, key: key + "x", body: putSession("")}, 401, "access_key_invalid")
	var grant bigav1.SessionGrant
	h.expect(req{method: "PUT", path: base, key: key, body: putSession("")}, 201, "").data(t, &grant)
	// Even the owner needs the key, so revoking a key stops its broadcasts.
	h.expect(req{method: "POST", path: base + "/heartbeat", token: grant.GetToken()}, 401, "access_key_required")
	h.expect(req{method: "POST", path: base + "/heartbeat", token: grant.GetToken(), key: key}, 204, "")
	// Viewers never need one.
	h.expect(req{method: "GET", path: base + "/state"}, 200, "")
}

func TestRepeatedBadCredentialsAreLockedOut(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	for range limitAuthFailures.n {
		h.expect(req{method: "POST", path: base + "/heartbeat", token: "bgs_wrong"}, 403, "forbidden")
	}
	r := h.expect(req{method: "POST", path: base + "/heartbeat", token: "bgs_wrong"}, 429, "rate_limited")
	if r.header.Get("Retry-After") == "" {
		t.Error("lockout should say when to retry")
	}
	// The lockout is per client: it blocks even a correct guess from it.
	h.expect(req{method: "POST", path: base + "/heartbeat", token: token}, 429, "rate_limited")
}

func TestStorageQuota(t *testing.T) {
	t.Parallel()
	h := newHarness(t, map[string]string{"BIGA_MAX_SESSION_STORAGE_BYTES": "1000"})
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	big := make([]byte, 3000)
	_, _ = rand.Read(big) // incompressible
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("big.txt", hex.EncodeToString(big)))}, 413, "storage_quota_exceeded")
	var st bigav1.SessionState
	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(t, &st)
	if st.GetSnapshot() != nil {
		t.Error("a snapshot over quota was stored")
	}
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("small.txt", "ok\n"))}, 202, "")
}

func TestCORSIsReadOnly(t *testing.T) {
	t.Parallel()
	h := newHarness(t, map[string]string{"BIGA_CORS_ORIGINS": "https://viewer.example"})
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	origin := map[string]string{"Origin": "https://viewer.example"}

	r := h.expect(req{method: "GET", path: base, header: origin}, 200, "")
	if r.header.Get("Access-Control-Allow-Origin") != "https://viewer.example" {
		t.Errorf("read ACAO = %q", r.header.Get("Access-Control-Allow-Origin"))
	}
	r = h.expect(req{method: "GET", path: base, header: map[string]string{"Origin": "https://evil.example"}}, 200, "")
	if r.header.Get("Access-Control-Allow-Origin") != "" {
		t.Error("an unlisted origin got CORS access")
	}
	r = h.expect(req{method: "POST", path: base + "/heartbeat", token: token, header: origin}, 204, "")
	if r.header.Get("Access-Control-Allow-Origin") != "" {
		t.Error("a write route sent CORS headers")
	}
	for _, header := range []string{"X-Content-Type-Options", "Content-Security-Policy", "X-Request-Id"} {
		if r.header.Get(header) == "" {
			t.Errorf("missing %s", header)
		}
	}
}

// watch opens a session's live stream and returns a function that reads its
// next frame.
func (h *harness) watch(path string) func() *bigav1.StreamEvent {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), 10*time.Second)
	h.t.Cleanup(cancel)
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, h.srv.URL+path, nil)
	r.Header.Set("Accept", streamType)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { resp.Body.Close() })
	if ct := resp.Header.Get("Content-Type"); ct != streamType {
		h.t.Fatalf("content type %q", ct)
	}
	frames := make(chan *bigav1.StreamEvent, 128)
	go func() {
		defer close(frames)
		in := bufio.NewReader(resp.Body)
		for {
			var ev bigav1.StreamEvent
			if err := protodelim.UnmarshalFrom(in, &ev); err != nil {
				return
			}
			frames <- &ev
		}
	}()
	return func() *bigav1.StreamEvent {
		h.t.Helper()
		select {
		case ev, ok := <-frames:
			if !ok {
				h.t.Fatal("the stream closed early")
			}
			return ev
		case <-ctx.Done():
			h.t.Fatal("timed out waiting for a frame")
		}
		return nil
	}
}

func TestStreamDeliversLiveEvents(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "1\n"))}, 202, "")

	next := h.watch(base + "/stream")
	if ev := next(); ev.GetConnected().GetSessionId() != id || ev.GetConnected().GetReplayed() {
		t.Fatalf("first frame = %+v", ev)
	}
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, entry("a.txt", "modified", "2\n"))}, 202, "")
	ev := next()
	if cp := ev.GetCheckpoint(); cp.GetSummary().GetSeq() != 1 || len(cp.GetChanges()) != 1 || cp.GetChanges()[0].GetPath() != "a.txt" {
		t.Fatalf("checkpoint frame = %+v", ev)
	}
	h.expect(req{method: "DELETE", path: base, token: token}, 204, "")
	if ev := next(); ev.GetSessionEnded().GetSessionId() != id {
		t.Fatalf("last frame = %+v", ev)
	}
}

// replaySession makes a session with checkpoints 0 to saves and returns its
// owner token and the timestamp of its snapshot.
func (h *harness) replaySession(id string, saves int) (token string, snapshotTS int64) {
	h.t.Helper()
	token = h.create(id)
	base := "/api/sessions/" + id
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "0\n"))}, 202, "")
	for i := 1; i <= saves; i++ {
		h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, entry("a.txt", "modified", strings.Repeat("x", i)+"\n"))}, 202, "")
	}
	var st bigav1.SessionState
	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(h.t, &st)
	return token, st.GetSnapshot().GetTimestamp()
}

func TestStreamReplaysWhatTheViewerMissed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token, ts := h.replaySession(id, 3)
	base := "/api/sessions/" + id
	resume := func(after int, snapshot int64) string {
		return fmt.Sprintf("%s/stream?after=%d&snapshot=%d", base, after, snapshot)
	}

	next := h.watch(resume(0, ts))
	if ev := next(); !ev.GetConnected().GetReplayed() {
		t.Fatalf("a viewer one snapshot behind should be replayed to: %+v", ev)
	}
	for seq := int32(1); seq <= 3; seq++ {
		cp := next().GetCheckpoint()
		if cp.GetSummary().GetSeq() != seq || len(cp.GetChanges()) != 1 || cp.GetChanges()[0].GetBeforeHash() == "" {
			t.Fatalf("replayed checkpoint %d = %+v", seq, cp)
		}
	}
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, entry("a.txt", "modified", "live\n"))}, 202, "")
	if cp := next().GetCheckpoint(); cp.GetSummary().GetSeq() != 4 {
		t.Fatalf("the live checkpoint should follow the replay once, got %+v", cp)
	}

	caughtUp := h.watch(resume(4, ts))
	if ev := caughtUp(); !ev.GetConnected().GetReplayed() {
		t.Fatalf("a viewer that is up to date resumes with nothing to replay: %+v", ev)
	}
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, entry("a.txt", "modified", "again\n"))}, 202, "")
	if cp := caughtUp().GetCheckpoint(); cp.GetSummary().GetSeq() != 5 {
		t.Fatalf("checkpoint after resuming = %+v", cp)
	}
}

func TestStreamAsksViewersToReloadWhenReplayIsNotSafe(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token, ts := h.replaySession(id, 2)
	base := "/api/sessions/" + id

	for name, path := range map[string]string{
		"no position":              base + "/stream",
		"unreadable position":      base + "/stream?after=x&snapshot=" + strconv.FormatInt(ts, 10),
		"no snapshot":              fmt.Sprintf("%s/stream?after=1", base),
		"a snapshot it never held": fmt.Sprintf("%s/stream?after=1&snapshot=%d", base, ts-1),
	} {
		if ev := h.watch(path)(); ev.GetConnected() == nil || ev.GetConnected().GetReplayed() {
			t.Errorf("%s: connected = %+v, want not replayed", name, ev)
		}
	}

	// A new snapshot replaces the tree, which a replay of checkpoints cannot express.
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "0\n"), file("b.txt", "new\n"))}, 202, "")
	if ev := h.watch(fmt.Sprintf("%s/stream?after=2&snapshot=%d", base, ts))(); ev.GetConnected().GetReplayed() {
		t.Errorf("a viewer holding an older snapshot must reload: %+v", ev)
	}
}

func TestStreamReplayIsBounded(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	_, ts := h.replaySession(id, service.MaxReplay+2)
	base := "/api/sessions/" + id

	if ev := h.watch(fmt.Sprintf("%s/stream?after=0&snapshot=%d", base, ts))(); ev.GetConnected().GetReplayed() {
		t.Errorf("a viewer %d checkpoints behind must reload: %+v", service.MaxReplay+2, ev)
	}
	next := h.watch(fmt.Sprintf("%s/stream?after=%d&snapshot=%d", base, service.MaxReplay+2-service.MaxReplay, ts))
	if ev := next(); !ev.GetConnected().GetReplayed() {
		t.Errorf("a viewer exactly at the limit can be replayed: %+v", ev)
	}
	for range service.MaxReplay {
		next()
	}
}

func TestStreamDoesNotReplayAnEndedSession(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token, ts := h.replaySession(id, 1)
	base := "/api/sessions/" + id
	h.expect(req{method: "DELETE", path: base, token: token}, 204, "")
	if ev := h.watch(fmt.Sprintf("%s/stream?after=0&snapshot=%d", base, ts))(); ev.GetConnected().GetReplayed() {
		t.Errorf("an ended session has nothing to resume: %+v", ev)
	}
}

func TestUnknownRoutesAndOpsEndpointsAnswerJSON(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	json := map[string]string{"Accept": "application/json"}
	h.expect(req{method: "GET", path: "/api/nope", header: json}, 404, "not_found")
	r := h.expect(req{method: "GET", path: "/readyz", header: json}, 200, "")
	if ct := r.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("readyz content type = %q", ct)
	}
	h.expect(req{method: "GET", path: "/api/cron/cleanup", header: json}, 404, "not_found") // disabled without a secret
}

func TestResponsesAreCompressedForGzipClients(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", strings.Repeat("hello world, again and again\n", 500)))}, 202, "")

	r, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, h.srv.URL+base+"/state", nil)
	r.Header.Set("Accept", protoType)
	r.Header.Set("Accept-Encoding", "gzip")
	resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Encoding") != "gzip" || resp.Header.Get("Accept-Encoding") != "gzip" {
		t.Fatalf("headers = %v", resp.Header)
	}
	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	var st bigav1.SessionState
	if err := proto.Unmarshal(plain, &st); err != nil || st.GetSession().GetId() != id || len(st.GetContents()) != 1 {
		t.Fatalf("decoded = %+v, %v", &st, err)
	}
}
