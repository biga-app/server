package httpapi

import (
	"strings"
	"testing"

	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

func putEncrypted(title string) *bigav1.PutSessionRequest {
	r := putSession(title)
	r.Encrypted = true
	return r
}

func (h *harness) createEncrypted(id string) string {
	h.t.Helper()
	var grant bigav1.SessionGrant
	h.expect(req{method: "PUT", path: "/api/sessions/" + id, body: putEncrypted("sealed")}, 201, "").data(h.t, &grant)
	if !grant.GetSession().GetEncrypted() {
		h.t.Fatalf("the session was not created encrypted: %+v", &grant)
	}
	return grant.GetToken()
}

// sealedID stands in for the keyed id a client names content by: a well-formed
// hash that is not the SHA-256 of what follows.
func sealedID(name string) string { return wire.HashOf("id of " + name) }

func sealedNode(name, sealed string, lines int32) *bigav1.FileNode {
	n := fileNode(name, name, sealedID(name), sealed)
	n.GetFile().Lines = lines
	n.GetFile().Size = 100
	return n
}

func deferredSealedNode(name string, size int64, lines int32) *bigav1.FileNode {
	return &bigav1.FileNode{Name: name, Path: name, Kind: &bigav1.FileNode_File{File: &bigav1.FileInfo{
		Hash: sealedID(name), Size: size, Lines: lines,
	}}}
}

func TestEncryptedSessionKeepsSealedBytesAsTheyAre(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.createEncrypted(id)
	base := "/api/sessions/" + id

	// Highly compressible, so a server that stored it normally would shrink it.
	sealedA := strings.Repeat("A", 4000)
	sealedB := "\x00\xff\x10 not text at all \xfe"
	h.expect(req{method: "POST", path: base + "/blobs", token: token, body: &bigav1.UploadBlobsRequest{Blobs: []*bigav1.Blob{
		{Hash: sealedID("a.txt"), Content: []byte(sealedA)},
		{Hash: sealedID("b.txt"), Content: []byte(sealedB)},
	}}}, 202, "")
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id,
		deferredSealedNode("a.txt", 4000, 120), deferredSealedNode("b.txt", 30, 7))}, 202, "")

	row, err := h.st.Q().Session(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if row.StorageBytes < int64(len(sealedA)+len(sealedB)) || !row.Encrypted {
		t.Errorf("sealed content is stored as it is, without compression: %d bytes counted, encrypted %v", row.StorageBytes, row.Encrypted)
	}

	var st bigav1.SessionState
	h.expect(req{method: "GET", path: base + "/state"}, 200, "").data(t, &st)
	if !st.GetSession().GetEncrypted() {
		t.Error("the state does not say the session is encrypted")
	}
	initial := st.GetCheckpoints()[0]
	if initial.GetAdditions() != 127 || initial.GetFilesChanged() != 2 {
		t.Errorf("the client's line counts are used: %+v", initial)
	}
	if got := string(st.GetContents()[sealedID("a.txt")]); got != sealedA {
		t.Errorf("eager content differs from what was uploaded (%d bytes)", len(got))
	}

	var fc bigav1.FileContent
	h.expect(req{method: "GET", path: base + "/file?path=b.txt"}, 200, "").data(t, &fc)
	if string(fc.GetContent()) != sealedB || !fc.GetEncrypted() || fc.GetHash() != sealedID("b.txt") {
		t.Errorf("file = %+v", &fc)
	}
}

func TestEncryptedCheckpointsReportTheirOwnCounts(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.createEncrypted(id)
	base := "/api/sessions/" + id
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id,
		sealedNode("a.txt", "sealed-a", 10), sealedNode("gone.txt", "sealed-gone", 4))}, 202, "")

	hashOf := func(name string) *string { return new(sealedID(name + "-v2")) }
	modified := &bigav1.CheckpointEntry{
		Path: "a.txt", ChangeType: bigav1.ChangeType_CHANGE_TYPE_MODIFIED, Hash: hashOf("a.txt"),
		Size: 99, Content: []byte("sealed-a-v2"), Additions: 6, Deletions: 3,
	}
	added := &bigav1.CheckpointEntry{
		Path: "new.txt", ChangeType: bigav1.ChangeType_CHANGE_TYPE_ADDED, Hash: hashOf("new.txt"),
		Size: 50, Content: []byte("sealed-new"), Additions: 12,
	}
	deleted := &bigav1.CheckpointEntry{Path: "gone.txt", ChangeType: bigav1.ChangeType_CHANGE_TYPE_DELETED, Deletions: 4}
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, modified, added, deleted)}, 202, "")

	var cp bigav1.Checkpoint
	h.expect(req{method: "GET", path: base + "/checkpoints/1"}, 200, "").data(t, &cp)
	counts := map[string][2]int32{}
	for _, c := range cp.GetChanges() {
		counts[c.GetPath()] = [2]int32{c.GetAdditions(), c.GetDeletions()}
	}
	if counts["a.txt"] != [2]int32{6, 3} || counts["new.txt"] != [2]int32{12, 0} || counts["gone.txt"] != [2]int32{0, 4} {
		t.Errorf("counts = %v", counts)
	}
	if s := cp.GetSummary(); s.GetAdditions() != 18 || s.GetDeletions() != 7 || s.GetFilesChanged() != 3 {
		t.Errorf("summary = %+v", s)
	}

	var fc bigav1.FileContent
	h.expect(req{method: "GET", path: base + "/file?path=a.txt"}, 200, "").data(t, &fc)
	if string(fc.GetContent()) != "sealed-a-v2" || !fc.GetEncrypted() {
		t.Errorf("file = %+v", &fc)
	}
}

func TestUnencryptedSessionsIgnoreClientCountsAndVerifyHashes(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.create(id)
	base := "/api/sessions/" + id
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, file("a.txt", "one\ntwo\n"))}, 202, "")

	lying := entry("a.txt", "modified", "one\n2\nthree\n")
	lying.Additions, lying.Deletions = 999, 999
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, lying)}, 202, "")
	var cp bigav1.Checkpoint
	h.expect(req{method: "GET", path: base + "/checkpoints/1"}, 200, "").data(t, &cp)
	if got := cp.GetChanges()[0]; got.GetAdditions() != 2 || got.GetDeletions() != 1 {
		t.Errorf("the server counts for itself: %+v", got)
	}

	bad := &bigav1.Blob{Hash: sealedID("x"), Content: []byte("not what the hash says")}
	h.expect(req{method: "POST", path: base + "/blobs", token: token, body: &bigav1.UploadBlobsRequest{Blobs: []*bigav1.Blob{bad}}}, 400, "invalid_request")
	var fc bigav1.FileContent
	h.expect(req{method: "GET", path: base + "/file?path=a.txt"}, 200, "").data(t, &fc)
	if fc.GetEncrypted() {
		t.Error("an unencrypted file is not marked encrypted")
	}
}

func TestASessionCannotChangeBetweenEncryptedAndNot(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)

	sealed, open := newSessionID(t), newSessionID(t)
	sealedToken, openToken := h.createEncrypted(sealed), h.create(open)

	h.expect(req{method: "PUT", path: "/api/sessions/" + sealed, token: sealedToken, body: putSession("")}, 409, "encryption_mismatch")
	h.expect(req{method: "PUT", path: "/api/sessions/" + open, token: openToken, body: putEncrypted("")}, 409, "encryption_mismatch")

	var again bigav1.SessionGrant
	h.expect(req{method: "PUT", path: "/api/sessions/" + sealed, token: sealedToken, body: putEncrypted("")}, 200, "").data(t, &again)
	if !again.GetSession().GetEncrypted() {
		t.Error("reactivating an encrypted session keeps it encrypted")
	}
	var s bigav1.Session
	h.expect(req{method: "GET", path: "/api/sessions/" + sealed}, 200, "").data(t, &s)
	if !s.GetEncrypted() {
		t.Error("the session view says it is encrypted")
	}
}

func TestEncryptedStreamCarriesCountsAndIds(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	token := h.createEncrypted(id)
	base := "/api/sessions/" + id
	h.expect(req{method: "POST", path: base + "/snapshot", token: token, body: snapshot(id, sealedNode("a.txt", "sealed-a", 3))}, 202, "")

	next := h.watch(base + "/stream")
	if ev := next(); ev.GetConnected() == nil {
		t.Fatalf("first frame = %+v", ev)
	}
	h.expect(req{method: "POST", path: base + "/checkpoint", token: token, body: checkpoint(id, &bigav1.CheckpointEntry{
		Path: "a.txt", ChangeType: bigav1.ChangeType_CHANGE_TYPE_MODIFIED, Hash: new(sealedID("a2")), Size: 1, Content: []byte("sealed-2"), Additions: 5, Deletions: 2,
	})}, 202, "")
	cp := next().GetCheckpoint()
	if cp.GetSummary().GetAdditions() != 5 || cp.GetChanges()[0].GetAfterHash() != sealedID("a2") {
		t.Errorf("live checkpoint = %+v", cp)
	}
}
