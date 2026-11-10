package events

import (
	"bytes"
	"time"

	"google.golang.org/protobuf/encoding/protodelim"

	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

const (
	NameConnected    = "connected"
	NameSnapshot     = "snapshot"
	NameCheckpoint   = "checkpoint"
	NameSessionEnded = "session_ended"
	NameKeepalive    = "keepalive"
)

func frame(name string, ev *bigav1.StreamEvent) (Event, error) {
	var buf bytes.Buffer
	if _, err := protodelim.MarshalTo(&buf, ev); err != nil {
		return Event{}, err
	}
	return Event{Name: name, Data: buf.Bytes()}, nil
}

func Connected(sessionID string, at time.Time, replayed bool) (Event, error) {
	return frame(NameConnected, &bigav1.StreamEvent{Event: &bigav1.StreamEvent_Connected{
		Connected: &bigav1.Connected{SessionId: sessionID, Timestamp: at.UnixMilli(), Replayed: replayed},
	}})
}

func Snapshot(s wire.FileSnapshot) (Event, error) {
	snapshot, err := wire.SnapshotToPB(s)
	if err != nil {
		return Event{}, err
	}
	return frame(NameSnapshot, &bigav1.StreamEvent{Event: &bigav1.StreamEvent_Snapshot{Snapshot: snapshot}})
}

func Checkpoint(c *wire.Checkpoint) (Event, error) {
	e, err := frame(NameCheckpoint, &bigav1.StreamEvent{Event: &bigav1.StreamEvent_Checkpoint{
		Checkpoint: wire.CheckpointToPB(c),
	}})
	e.Seq = c.Seq
	return e, err
}

func SessionEnded(sessionID string, at time.Time) (Event, error) {
	return frame(NameSessionEnded, &bigav1.StreamEvent{Event: &bigav1.StreamEvent_SessionEnded{
		SessionEnded: &bigav1.SessionEnded{SessionId: sessionID, Timestamp: at.UnixMilli()},
	}})
}

func Keepalive(at time.Time) (Event, error) {
	return frame(NameKeepalive, &bigav1.StreamEvent{Event: &bigav1.StreamEvent_Keepalive{
		Keepalive: &bigav1.Keepalive{Timestamp: at.UnixMilli()},
	}})
}
