package httpapi

import (
	"testing"

	"github.com/biga-app/server/internal/wire"
)

func TestMarkCheckpointRetriesAndAppearsInFeed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	id := newSessionID(t)
	owner := h.create(id)
	base := "/api/sessions/" + id
	mark := req{method: "PUT", path: base + "/checkpoints/marks/retry-key-123456", body: map[string]string{"name": " Ready for review "}}
	h.expect(mark, 401, "unauthorized")
	mark.token = owner
	h.expect(mark, 404, "not_found")
	h.expect(req{method: "POST", path: base + "/snapshot", token: owner, body: snapshot(id, file("draft.txt", "hello"))}, 202, "")
	var grant wire.ExtensionGrant
	h.expect(req{method: "POST", path: base + "/extensions", token: owner, body: map[string]string{"name": "Recaps"}}, 201, "").data(t, &grant)

	for range 2 {
		var summary wire.CheckpointSummary
		h.expect(mark, 200, "").data(t, &summary)
		if summary.Seq != 1 || !summary.Named || summary.Label != "Ready for review" || summary.FilesChanged != 0 {
			t.Fatalf("unexpected named checkpoint: %+v", summary)
		}
	}
	mark.body = map[string]string{"name": "Another name"}
	h.expect(mark, 409, "mark_key_conflict")

	var got wire.Checkpoint
	h.expect(req{method: "GET", path: base + "/checkpoints/1"}, 200, "").data(t, &got)
	if !got.Named || got.Label != "Ready for review" || len(got.Changes) != 0 {
		t.Fatalf("unexpected checkpoint detail: %+v", got)
	}

	var feed wire.ExtensionFeed
	h.expect(req{method: "GET", path: base + "/extension-events?after=-1", token: grant.Token}, 200, "").data(t, &feed)
	if len(feed.Events) != 2 || feed.NextAfter != 1 || feed.Events[0].Name != "" ||
		feed.Events[1].Type != "checkpoint.marked" || feed.Events[1].Name != got.Label {
		t.Fatalf("unexpected feed: %+v", feed)
	}
}
