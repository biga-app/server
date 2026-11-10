package httpapi

import (
	"testing"

	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

func TestMarkCheckpointRetriesAndAppearsInFeed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	owner := h.create(id)
	base := "/api/sessions/" + id
	mark := req{method: "PUT", path: base + "/checkpoints/marks/retry-key-123456", body: &bigav1.MarkCheckpointRequest{Name: " Ready for review "}}
	h.expect(mark, 401, "unauthorized")
	mark.token = owner
	h.expect(mark, 404, "not_found")
	h.expect(req{method: "POST", path: base + "/snapshot", token: owner, body: snapshot(id, file("draft.txt", "hello"))}, 202, "")
	var grant bigav1.ExtensionGrant
	h.expect(req{method: "POST", path: base + "/extensions", token: owner, body: &bigav1.CreateExtensionRequest{Name: "Recaps"}}, 201, "").data(t, &grant)

	for range 2 {
		var summary bigav1.CheckpointSummary
		h.expect(mark, 200, "").data(t, &summary)
		if summary.GetSeq() != 1 || !summary.GetNamed() || summary.GetLabel() != "Ready for review" || summary.GetFilesChanged() != 0 {
			t.Fatalf("unexpected named checkpoint: %+v", &summary)
		}
	}
	mark.body = &bigav1.MarkCheckpointRequest{Name: "Another name"}
	h.expect(mark, 409, "mark_key_conflict")

	var got bigav1.Checkpoint
	h.expect(req{method: "GET", path: base + "/checkpoints/1"}, 200, "").data(t, &got)
	if !got.GetSummary().GetNamed() || got.GetSummary().GetLabel() != "Ready for review" || len(got.GetChanges()) != 0 {
		t.Fatalf("unexpected checkpoint detail: %+v", &got)
	}

	var feed wire.ExtensionFeed
	h.expect(req{method: "GET", path: base + "/extension-events?after=-1", token: grant.GetToken()}, 200, "").data(t, &feed)
	if len(feed.Events) != 2 || feed.NextAfter != 1 || feed.Events[0].Name != "" ||
		feed.Events[1].Type != "checkpoint.marked" || feed.Events[1].Name != got.GetSummary().GetLabel() {
		t.Fatalf("unexpected feed: %+v", feed)
	}
}
