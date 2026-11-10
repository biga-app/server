package httpapi

import (
	"strings"
	"testing"

	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

func TestExtensionCheckpointFeed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	sessionID := newSessionID(t)
	owner := h.create(sessionID)
	base := "/api/sessions/" + sessionID
	extensions := base + "/extensions"
	feedPath := base + "/extension-events"
	name := &bigav1.CreateExtensionRequest{Name: "Build monitor"}

	h.expect(req{method: "POST", path: extensions, body: name}, 401, "unauthorized")
	var grant bigav1.ExtensionGrant
	created := h.expect(req{method: "POST", path: extensions, token: owner, body: name}, 201, "")
	created.data(t, &grant)
	if grant.GetExtension().GetId() == "" || !strings.HasPrefix(grant.GetToken(), "bgx_") || !strings.Contains(created.header.Get("Cache-Control"), "no-store") {
		t.Fatal("expected a one-time, non-cacheable extension grant")
	}
	listed := h.expect(req{method: "GET", path: extensions, token: owner}, 200, "")
	var list bigav1.ListExtensionsResponse
	listed.data(t, &list)
	if len(list.GetExtensions()) != 1 || list.GetExtensions()[0].GetId() != grant.GetExtension().GetId() || strings.Contains(string(listed.body), grant.GetToken()) {
		t.Fatal("expected the extension metadata without its token")
	}

	h.expect(req{method: "GET", path: feedPath, token: owner}, 401, "extension_token_invalid")
	otherSession := newSessionID(t)
	h.create(otherSession)
	h.expect(req{method: "GET", path: "/api/sessions/" + otherSession + "/extension-events", token: grant.GetToken()}, 401, "extension_token_invalid")
	h.expect(req{method: "POST", path: base + "/heartbeat", token: grant.GetToken()}, 403, "forbidden")
	h.expect(req{method: "GET", path: extensions, token: grant.GetToken()}, 403, "forbidden")
	h.expect(req{method: "GET", path: feedPath + "?after=-2", token: grant.GetToken()}, 400, "invalid_request")

	h.expect(req{method: "POST", path: base + "/snapshot", token: owner, body: snapshot(sessionID, file("private-name.txt", "private-body"))}, 202, "")
	h.expect(req{method: "POST", path: base + "/checkpoint", token: owner, body: checkpoint(sessionID, entry("private-name.txt", "modified", "new body"))}, 202, "")
	var feed wire.ExtensionFeed
	first := h.expect(req{method: "GET", path: feedPath + "?limit=1", token: grant.GetToken()}, 200, "")
	first.data(t, &feed)
	if len(feed.Events) != 1 || feed.NextAfter != 0 || feed.Events[0].ID != sessionID+":0" || feed.Events[0].Type != "checkpoint.created" || feed.Events[0].Version != 1 {
		t.Fatalf("unexpected first page: %+v", feed)
	}
	second := h.expect(req{method: "GET", path: feedPath + "?after=0", token: grant.GetToken()}, 200, "")
	second.data(t, &feed)
	if len(feed.Events) != 1 || feed.NextAfter != 1 || feed.Events[0].Seq != 1 || feed.Events[0].FilesChanged != 1 {
		t.Fatalf("unexpected second page: %+v", feed)
	}
	if !strings.Contains(second.header.Get("Cache-Control"), "no-store") || strings.Contains(string(second.body), "private-name.txt") || strings.Contains(string(second.body), "new body") {
		t.Fatal("feed must be non-cacheable and omit file names and content")
	}
	h.expect(req{method: "GET", path: feedPath + "?after=1", token: grant.GetToken()}, 200, "").data(t, &feed)
	if len(feed.Events) != 0 || feed.NextAfter != 1 {
		t.Fatalf("unexpected empty page: %+v", feed)
	}

	h.expect(req{method: "DELETE", path: extensions + "/" + grant.GetExtension().GetId(), token: owner}, 204, "")
	h.expect(req{method: "DELETE", path: extensions + "/" + grant.GetExtension().GetId(), token: owner}, 204, "")
	h.expect(req{method: "GET", path: feedPath, token: grant.GetToken()}, 401, "extension_token_invalid")
}
