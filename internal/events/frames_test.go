package events

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protodelim"

	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

func decode(t *testing.T, e Event) *bigav1.StreamEvent {
	t.Helper()
	var ev bigav1.StreamEvent
	if err := protodelim.UnmarshalFrom(bytes.NewReader(e.Data), &ev); err != nil {
		t.Fatalf("%s frame does not decode: %v", e.Name, err)
	}
	return &ev
}

func TestFramesCarryTheirEvent(t *testing.T) {
	t.Parallel()
	now := time.UnixMilli(1_700_000_000_123)

	e, err := Connected("s1", now, true)
	if got := decode(t, e).GetConnected(); err != nil || e.Name != NameConnected || got.GetTimestamp() != now.UnixMilli() || !got.GetReplayed() {
		t.Errorf("connected = %+v, %v", e, err)
	}
	if e, _ = Connected("s1", now, false); decode(t, e).GetConnected().GetReplayed() {
		t.Error("a stream that is not resuming must not say it replayed")
	}
	e, err = SessionEnded("s1", now)
	if err != nil || e.Name != NameSessionEnded || decode(t, e).GetSessionEnded().GetSessionId() != "s1" {
		t.Errorf("session_ended = %+v, %v", e, err)
	}
	e, err = Keepalive(now)
	if err != nil || e.Name != NameKeepalive || decode(t, e).GetKeepalive() == nil {
		t.Errorf("keepalive = %+v, %v", e, err)
	}

	cp := &wire.Checkpoint{
		CheckpointSummary: wire.CheckpointSummary{SessionID: "s1", Seq: 3, Label: "Updated a.txt", FilesChanged: 1},
		Changes:           []wire.CheckpointChange{{Path: "a.txt", ChangeType: wire.Modified}},
	}
	e, err = Checkpoint(cp)
	if got := decode(t, e).GetCheckpoint(); err != nil || e.Name != NameCheckpoint || e.Seq != 3 || got.GetSummary().GetSeq() != 3 || got.GetChanges()[0].GetPath() != "a.txt" {
		t.Errorf("checkpoint = %+v, %v", e, err)
	}

	e, err = Snapshot(wire.FileSnapshot{SessionID: "s1", Timestamp: 7, Tree: &wire.FileNode{Type: wire.NodeDirectory, Name: "p", Path: "."}})
	if got := decode(t, e).GetSnapshot(); err != nil || e.Name != NameSnapshot || got.GetSessionId() != "s1" || got.GetTree().GetDirectory() == nil {
		t.Errorf("snapshot = %+v, %v", e, err)
	}
}

func TestFramesAreSelfDelimiting(t *testing.T) {
	t.Parallel()
	a, _ := Connected("s1", time.Now(), false)
	b, _ := Keepalive(time.Now())
	in := bytes.NewReader(append(append([]byte{}, a.Data...), b.Data...))

	var first, second bigav1.StreamEvent
	if err := protodelim.UnmarshalFrom(in, &first); err != nil || first.GetConnected() == nil {
		t.Fatalf("first frame: %+v, %v", &first, err)
	}
	if err := protodelim.UnmarshalFrom(in, &second); err != nil || second.GetKeepalive() == nil {
		t.Fatalf("second frame: %+v, %v", &second, err)
	}
}

func TestSnapshotFrameRejectsUnknownNodes(t *testing.T) {
	t.Parallel()
	if _, err := Snapshot(wire.FileSnapshot{Tree: &wire.FileNode{Type: "link"}}); err == nil {
		t.Error("an unknown node type should not encode")
	}
}
