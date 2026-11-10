package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/biga-app/server/internal/objstore"
	"github.com/biga-app/server/internal/service"
	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

func scratchDatabase(t *testing.T) string {
	t.Helper()
	base := os.Getenv("BIGA_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("BIGA_TEST_DATABASE_URL is not set")
	}
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	name := "biga_scratch_" + hex.EncodeToString(suffix)
	admin, err := pgx.Connect(t.Context(), base)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(context.Background())
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), base)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func (h *harness) query(sql string, args ...any) pgx.Row {
	h.t.Helper()
	conn, err := pgx.Connect(h.t.Context(), h.dsn)
	if err != nil {
		h.t.Fatalf("connect: %v", err)
	}
	h.t.Cleanup(func() { conn.Close(context.Background()) })
	return conn.QueryRow(h.t.Context(), sql, args...)
}

func objectKeys(m *objstore.Memory, id string) []string {
	var keys []string
	for _, k := range m.Keys() {
		if strings.HasPrefix(k, objstore.Prefix(id)) {
			keys = append(keys, k)
		}
	}
	return keys
}

func TestContentLivesInObjectStorage(t *testing.T) {
	t.Parallel()
	objects := objstore.NewMemory()
	h := newHarness(t, nil, withObjects(objects))
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	one, two := "one\ntwo\n", "changed\nlines\n"

	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", one))}, 202, "")
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, entry("a.txt", "modified", two))}, 202, "")

	want := []string{objstore.Key(id, wire.HashOf(one)), objstore.Key(id, wire.HashOf(two))}
	slices.Sort(want)
	if got := objectKeys(objects, id); !slices.Equal(got, want) {
		t.Fatalf("objects = %v, want %v", got, want)
	}

	var inline, remote int
	if err := h.query(`SELECT count(*) FILTER (WHERE content IS NOT NULL), count(*) FILTER (WHERE location = 'objects') FROM blobs WHERE session_id = $1`, id).Scan(&inline, &remote); err != nil {
		t.Fatal(err)
	}
	if inline != 0 || remote != 2 {
		t.Errorf("rows holding content = %d, rows pointing at objects = %d", inline, remote)
	}

	var fc bigav1.FileContent
	h.expect(req{method: "GET", path: base + "/file?path=a.txt"}, 200, "").data(t, &fc)
	if string(fc.GetContent()) != two {
		t.Errorf("served %q, want %q", fc.GetContent(), two)
	}
	h.expect(req{method: "GET", path: base + "/file?path=a.txt&ref=" + wire.HashOf(one)}, 200, "").data(t, &fc)
	if string(fc.GetContent()) != one {
		t.Errorf("served %q, want %q", fc.GetContent(), one)
	}

	var cp bigav1.Checkpoint
	h.expect(req{method: "GET", path: base + "/checkpoints/1"}, 200, "").data(t, &cp)
	if got := cp.GetChanges()[0]; got.GetAdditions() != 2 || got.GetDeletions() != 2 {
		t.Errorf("line counts read through object storage: +%d -%d", got.GetAdditions(), got.GetDeletions())
	}
}

func TestObjectStorageOutageFailsTheUploadAndLeavesNothingHalfStored(t *testing.T) {
	t.Parallel()
	objects := objstore.NewMemory()
	h := newHarness(t, nil, withObjects(objects))
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id

	objects.FailPuts = 100
	r := h.do(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "one\n"))})
	if r.status < 500 {
		t.Fatalf("an unavailable store answered %d: %s", r.status, r.body)
	}
	var st bigav1.SessionState
	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(t, &st)
	if st.GetSnapshot() != nil {
		t.Error("a snapshot was recorded although its content was not stored")
	}
	var rows int
	if err := h.query(`SELECT count(*) FROM blobs WHERE session_id = $1`, id).Scan(&rows); err != nil || rows != 0 {
		t.Errorf("blob rows = %d, err = %v", rows, err)
	}

	objects.FailPuts = 0
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "one\n"))}, 202, "")
}

func TestUploadsNeedTheOwnerBeforeTouchingObjectStorage(t *testing.T) {
	t.Parallel()
	objects := objstore.NewMemory()
	h := newHarness(t, nil, withObjects(objects))
	id := newSessionID(t)
	h.create(id)
	h.expect(req{method: "POST", path: "/api/sessions/" + id + "/blobs", token: "bgs_wrong", body: &bigav1.UploadBlobsRequest{Blobs: []*bigav1.Blob{blob("x\n")}}}, 403, "")
	if got := objectKeys(objects, id); len(got) != 0 {
		t.Errorf("a rejected upload left objects: %v", got)
	}
}

func TestSweepRemovesExpiredSessionsAndUnreferencedUploads(t *testing.T) {
	t.Parallel()
	objects := objstore.NewMemory()
	h := newHarness(t, nil, withObjects(objects), withDatabase(scratchDatabase(t)), withService(func(o *service.Options) {
		o.Retention, o.OrphanAfter, o.AbandonAfter = time.Nanosecond, time.Nanosecond, time.Hour
	}))

	kept, ended := newSessionID(t), newSessionID(t)
	keptToken, endedToken := h.create(kept), h.create(ended)
	for id, token := range map[string]string{kept: keptToken, ended: endedToken} {
		h.expect(req{method: "POST", path: "/api/sessions/" + id + "/snapshot", token: token, body: snapshot(id, file("a.txt", id+"\n"))}, 202, "")
	}
	h.expect(req{method: "POST", path: "/api/sessions/" + kept + "/blobs", token: keptToken, body: &bigav1.UploadBlobsRequest{Blobs: []*bigav1.Blob{blob("never used\n")}}}, 202, "")
	h.expect(req{method: "DELETE", path: "/api/sessions/" + ended, token: endedToken}, 204, "")
	if len(objectKeys(objects, kept)) != 2 || len(objectKeys(objects, ended)) != 1 {
		t.Fatalf("objects before the sweep: %v", objects.Keys())
	}
	var storedBefore int64
	if err := h.query(`SELECT storage_bytes FROM sessions WHERE id = $1`, kept).Scan(&storedBefore); err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)
	res, err := h.svc.Cleanup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 1 || res.Orphans != 1 {
		t.Errorf("sweep = %+v, want one session and one orphan", res)
	}
	if got := objectKeys(objects, ended); len(got) != 0 {
		t.Errorf("the expired session's objects survived: %v", got)
	}
	if got := objectKeys(objects, kept); len(got) != 1 || got[0] != objstore.Key(kept, wire.HashOf(kept+"\n")) {
		t.Errorf("the live session lost or kept the wrong objects: %v", got)
	}
	var storedAfter int64
	if err := h.query(`SELECT storage_bytes FROM sessions WHERE id = $1`, kept).Scan(&storedAfter); err != nil {
		t.Fatal(err)
	}
	if storedAfter >= storedBefore {
		t.Errorf("storage_bytes %d -> %d: removed uploads should be given back", storedBefore, storedAfter)
	}
	var st bigav1.SessionState
	h.expect(req{method: "GET", path: "/api/sessions/" + kept + "/state"}, 200, "").data(t, &st)
}

func TestSweepKeepsUploadsAWaitingSnapshotWillUse(t *testing.T) {
	t.Parallel()
	objects := objstore.NewMemory()
	h := newHarness(t, nil, withObjects(objects), withDatabase(scratchDatabase(t)), withService(func(o *service.Options) {
		o.Retention, o.OrphanAfter, o.AbandonAfter = time.Hour, time.Hour, time.Hour
	}))
	id := newSessionID(t)
	token := h.create(id)
	h.expect(req{method: "POST", path: "/api/sessions/" + id + "/blobs", token: token, body: &bigav1.UploadBlobsRequest{Blobs: []*bigav1.Blob{blob("waiting\n")}}}, 202, "")
	res, err := h.svc.Cleanup(t.Context())
	if err != nil || res.Orphans != 0 {
		t.Fatalf("sweep = %+v, %v", res, err)
	}
	h.expect(req{method: "POST", path: "/api/sessions/" + id + "/snapshot", token: token, body: snapshot(id, deferred("a.txt", "waiting\n"))}, 202, "")
}

func TestMigrateMovesDatabaseContentToObjectStorage(t *testing.T) {
	t.Parallel()
	plain := newHarness(t, nil, withDatabase(scratchDatabase(t)))
	id := newSessionID(t)
	token := plain.create(id)
	base := "/api/sessions/" + id
	plain.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "one\ntwo\n"), file("b.txt", "three\n"))}, 202, "")

	objects := objstore.NewMemory()
	moving := newHarness(t, nil, withObjects(objects), withDatabase(plain.dsn))
	moved, err := moving.st.MigrateBlobs(t.Context(), 1, nil)
	if err != nil || moved != 2 {
		t.Fatalf("moved %d, %v", moved, err)
	}
	if got := len(objectKeys(objects, id)); got != 2 {
		t.Errorf("objects = %d", got)
	}
	var left int
	if err := moving.query(`SELECT count(*) FROM blobs WHERE session_id = $1 AND (location = 'db' OR content IS NOT NULL)`, id).Scan(&left); err != nil || left != 0 {
		t.Errorf("rows still holding content = %d, %v", left, err)
	}
	var fc bigav1.FileContent
	moving.expect(req{method: "GET", path: base + "/file?path=a.txt"}, 200, "").data(t, &fc)
	if string(fc.GetContent()) != "one\ntwo\n" {
		t.Errorf("served %q after migration", fc.GetContent())
	}
	if again, err := moving.st.MigrateBlobs(t.Context(), 10, nil); err != nil || again != 0 {
		t.Errorf("a second run moved %d, %v", again, err)
	}
}
