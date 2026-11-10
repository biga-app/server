package httpapi

import (
	"crypto/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

func deferred(name, content string) *bigav1.FileNode {
	n := file(name, content)
	n.GetFile().Content = nil
	return n
}

func blob(content string) *bigav1.Blob {
	return &bigav1.Blob{Hash: wire.HashOf(content), Content: []byte(content)}
}

func (h *harness) missing(base, token string, contents ...string) []string {
	h.t.Helper()
	hashes := make([]string, len(contents))
	for i, c := range contents {
		hashes[i] = wire.HashOf(c)
	}
	var res bigav1.MissingBlobsResponse
	h.expect(req{method: "POST", path: base + "/blobs/missing", token: token, body: &bigav1.MissingBlobsRequest{Hashes: hashes}}, 200, "").data(h.t, &res)
	return res.GetHashes()
}

func TestSnapshotFromBlobsUploadedFirst(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	one, two, three := "one\ntwo\n", "keep\n", "changed\nlines\nhere\n"

	if got := h.missing(base, token, one, two, one); len(got) != 2 || got[0] != wire.HashOf(one) || got[1] != wire.HashOf(two) {
		t.Fatalf("nothing is stored yet, so everything is missing once: %v", got)
	}
	h.expect(req{method: "POST", path: base + "/blobs", token: token, body: &bigav1.UploadBlobsRequest{Blobs: []*bigav1.Blob{blob(one)}}}, 202, "")
	if got := h.missing(base, token, one, two); len(got) != 1 || got[0] != wire.HashOf(two) {
		t.Fatalf("only the second blob is still missing: %v", got)
	}

	r := h.do(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, deferred("a.txt", one), deferred("b.txt", two))})
	if r.status != http.StatusConflict || r.code() != "missing_blob" {
		t.Fatalf("a snapshot referring to a blob that was never uploaded: %d %s", r.status, r.body)
	}
	var st bigav1.SessionState
	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(t, &st)
	if st.GetSnapshot() != nil {
		t.Error("a snapshot with missing content was stored")
	}

	h.expect(req{method: "POST", path: base + "/blobs", token: token, body: &bigav1.UploadBlobsRequest{Blobs: []*bigav1.Blob{blob(two)}}}, 202, "")
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, deferred("a.txt", one), deferred("b.txt", two))}, 202, "")

	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(t, &st)
	initial := st.GetCheckpoints()[0]
	if initial.GetLabel() != "Initial snapshot" || initial.GetAdditions() != 3 || initial.GetFilesChanged() != 2 {
		t.Errorf("line counts come from the stored blobs: %+v", initial)
	}
	var fc bigav1.FileContent
	h.expect(req{method: "GET", path: base + "/file?path=a.txt"}, 200, "").data(t, &fc)
	if string(fc.GetContent()) != one {
		t.Errorf("a.txt = %q", fc.GetContent())
	}

	// A rejoin sends a new tree: unchanged files cost nothing, a changed one is diffed against what was there.
	h.expect(req{method: "POST", path: base + "/blobs", token: token, body: &bigav1.UploadBlobsRequest{Blobs: []*bigav1.Blob{blob(three)}}}, 202, "")
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, deferred("a.txt", three), deferred("b.txt", two))}, 202, "")
	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(t, &st)
	resumed := st.GetCheckpoints()[0]
	if resumed.GetLabel() != "Resumed session" || resumed.GetFilesChanged() != 1 || resumed.GetAdditions() != 3 || resumed.GetDeletions() != 2 {
		t.Errorf("a changed file is diffed against its previous content: %+v", resumed)
	}
}

func TestSnapshotsStillAcceptInlineContentAndEmptyFiles(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	empty := fileNode("empty.txt", "empty.txt", wire.HashOf(""), "")
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "x\n"), empty)}, 202, "")
	var m bigav1.Manifest
	h.expect(req{method: "GET", path: base + "/manifest", token: token}, 200, "").data(t, &m)
	if len(m.GetFiles()) != 2 {
		t.Errorf("manifest = %+v", &m)
	}
}

func TestBlobEndpointsValidateWhatTheyAreSent(t *testing.T) {
	t.Parallel()
	h := newHarness(t, map[string]string{"BIGA_MAX_CHECKPOINT_BYTES": "4096", "BIGA_MAX_SESSION_STORAGE_BYTES": "1500"})
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	upload := func(blobs ...*bigav1.Blob) req {
		return req{method: "POST", path: base + "/blobs", token: token, body: &bigav1.UploadBlobsRequest{Blobs: blobs}}
	}

	h.expect(req{method: "POST", path: base + "/blobs", body: &bigav1.UploadBlobsRequest{Blobs: []*bigav1.Blob{blob("x")}}}, 401, "unauthorized")
	h.expect(req{method: "POST", path: base + "/blobs/missing", body: &bigav1.MissingBlobsRequest{}}, 401, "unauthorized")
	h.expect(upload(), 400, "invalid_request")
	h.expect(upload(&bigav1.Blob{Hash: wire.HashOf("real"), Content: []byte("forged")}), 400, "invalid_request")
	h.expect(upload(&bigav1.Blob{Hash: "nope", Content: []byte("x")}), 400, "invalid_request")
	h.expect(upload(blob("same"), blob("same")), 400, "invalid_request")
	h.expect(req{method: "POST", path: base + "/blobs/missing", token: token, body: &bigav1.MissingBlobsRequest{Hashes: []string{"zz"}}}, 400, "invalid_request")
	h.expect(upload(blob(strings.Repeat("a", 5000))), 413, "payload_too_large")

	noise, other := make([]byte, 1200), make([]byte, 1200)
	_, _ = rand.Read(noise)
	_, _ = rand.Read(other)
	h.expect(upload(&bigav1.Blob{Hash: wire.HashOf(string(noise)), Content: noise}), 202, "")
	h.expect(upload(&bigav1.Blob{Hash: wire.HashOf(string(other)), Content: other}), 413, "storage_quota_exceeded")

	h.expect(req{method: "DELETE", path: base, token: token}, 204, "")
	h.expect(upload(blob("late")), 409, "invalid_state")
}

func TestGrantReportsTheServerLimits(t *testing.T) {
	t.Parallel()
	h := newHarness(t, map[string]string{"BIGA_MAX_SNAPSHOT_FILES": "123"})
	var grant bigav1.SessionGrant
	h.expect(req{method: "PUT", path: "/api/sessions/" + newSessionID(t), body: putSession("t")}, 201, "").data(t, &grant)
	limits := grant.GetLimits()
	if limits.GetMaxSnapshotFiles() != 123 || limits.GetMaxFileBytes() != 6<<20 || limits.GetMaxBlobBatchBytes() != 8<<20 {
		t.Errorf("limits = %+v", limits)
	}
}
