package wire

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/proto"

	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

func uploadedTree(content string) *bigav1.FileNode {
	return &bigav1.FileNode{Name: "proj", Path: ".", Kind: &bigav1.FileNode_Directory{Directory: &bigav1.DirectoryInfo{
		Children: []*bigav1.FileNode{
			{Name: "a.txt", Path: "a.txt", Kind: &bigav1.FileNode_File{File: &bigav1.FileInfo{
				Size: int64(len(content)), Hash: HashOf(content), Content: []byte(content),
			}}},
			{Name: "dist", Path: "dist", Kind: &bigav1.FileNode_Directory{Directory: &bigav1.DirectoryInfo{Ignored: true}}},
		},
	}}}
}

func TestSnapshotFromPBKeepsContentBytes(t *testing.T) {
	content := "ok\xffok\n"
	snap := SnapshotFromPB(&bigav1.FileSnapshot{SessionId: "s", Tree: uploadedTree(content), Timestamp: 5, SequenceNum: 2})
	if snap.SessionID != "s" || snap.Timestamp != 5 || snap.SequenceNum != 2 {
		t.Fatalf("snapshot = %+v", snap)
	}
	files, err := ValidateSnapshot(&snap, "s", TreeLimits{MaxFiles: 10, MaxFileBytes: 1 << 20})
	if err != nil {
		t.Fatalf("a file with invalid UTF-8 and the SHA-256 of its raw bytes should be accepted: %v", err)
	}
	if len(files) != 1 || files[0].Content != content {
		t.Errorf("files = %+v", files)
	}
}

func TestSnapshotToPBDropsContent(t *testing.T) {
	snap := SnapshotFromPB(&bigav1.FileSnapshot{SessionId: "s", Tree: uploadedTree("hi\n"), Timestamp: 1})
	msg, err := SnapshotToPB(snap)
	if err != nil {
		t.Fatal(err)
	}
	children := msg.GetTree().GetDirectory().GetChildren()
	if len(children) != 2 || len(children[0].GetFile().GetContent()) != 0 || children[0].GetFile().GetHash() != HashOf("hi\n") {
		t.Errorf("children = %+v", children)
	}
	if dir := children[1].GetDirectory(); dir == nil || !dir.GetIgnored() {
		t.Errorf("stub directory = %+v", children[1])
	}
}

func TestSnapshotToPBRejectsUnknownNodeTypes(t *testing.T) {
	if _, err := SnapshotToPB(FileSnapshot{Tree: &FileNode{Type: "symlink"}}); err == nil {
		t.Error("an unknown node type should fail")
	}
}

func TestStoredSnapshotToPBReadsStoredJSON(t *testing.T) {
	tree := &FileNode{Type: NodeDirectory, Name: "proj", Path: ".", Children: []*FileNode{
		{Type: NodeFile, Name: "a.txt", Path: "a.txt", Size: 3, Hash: HashOf("hi\n"), Binary: true},
	}}
	raw, err := json.Marshal(tree)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := StoredSnapshotToPB(&StoredSnapshot{SessionID: "s", Tree: raw, Timestamp: 9, SequenceNum: 1})
	if err != nil {
		t.Fatal(err)
	}
	file := msg.GetTree().GetDirectory().GetChildren()[0].GetFile()
	if msg.GetSessionId() != "s" || msg.GetTimestamp() != 9 || file.GetSize() != 3 || !file.GetBinary() {
		t.Errorf("snapshot = %+v", msg)
	}
	if got, err := StoredSnapshotToPB(nil); got != nil || err != nil {
		t.Errorf("no snapshot: %v, %v", got, err)
	}
	if _, err := StoredSnapshotToPB(&StoredSnapshot{Tree: json.RawMessage(`{"type":`)}); err == nil {
		t.Error("corrupt stored JSON should fail")
	}
}

func TestCheckpointUploadFromPB(t *testing.T) {
	hash := HashOf("x")
	up := CheckpointUploadFromPB(&bigav1.CheckpointUpload{SessionId: "s", Timestamp: 3, Entries: []*bigav1.CheckpointEntry{
		{Path: "a", ChangeType: bigav1.ChangeType_CHANGE_TYPE_MODIFIED, Hash: &hash, Size: 1, Content: []byte("x")},
		{Path: "b", ChangeType: bigav1.ChangeType_CHANGE_TYPE_DELETED},
		{Path: "c"},
	}})
	if up.SessionID != "s" || up.Timestamp != 3 || len(up.Entries) != 3 {
		t.Fatalf("upload = %+v", up)
	}
	if e := up.Entries[0]; e.ChangeType != Modified || e.Hash == nil || *e.Hash != hash || e.Content != "x" {
		t.Errorf("entry 0 = %+v", e)
	}
	if e := up.Entries[1]; e.ChangeType != Deleted || e.Hash != nil {
		t.Errorf("entry 1 = %+v", e)
	}
	if up.Entries[2].ChangeType.Valid() {
		t.Errorf("an unspecified change type must not be valid: %q", up.Entries[2].ChangeType)
	}
}

func TestCheckpointToPB(t *testing.T) {
	before, after := HashOf("a"), HashOf("b")
	msg := CheckpointToPB(&Checkpoint{
		CheckpointSummary: CheckpointSummary{SessionID: "s", Seq: 4, Label: "Updated a", Named: true, FilesChanged: 1, Additions: 2, Deletions: 1},
		Changes:           []CheckpointChange{{Path: "a", ChangeType: Renamed, OldPath: "z", BeforeHash: &before, AfterHash: &after, Additions: 2, Deletions: 1}},
	})
	ch := msg.GetChanges()[0]
	if msg.GetSummary().GetSeq() != 4 || !msg.GetSummary().GetNamed() || ch.GetChangeType() != bigav1.ChangeType_CHANGE_TYPE_RENAMED ||
		ch.GetOldPath() != "z" || ch.GetBeforeHash() != before || ch.GetAfterHash() != after {
		t.Errorf("checkpoint = %+v", msg)
	}
}

func TestStateToPB(t *testing.T) {
	desc := "about"
	raw, _ := json.Marshal(&FileNode{Type: NodeDirectory, Name: "proj", Path: "."})
	state, err := StateToPB(&SessionState{
		Session:     Session{ID: "s", Title: "t", Description: &desc, Status: StatusActive, CreatedAt: "c", UpdatedAt: "u"},
		Snapshot:    &StoredSnapshot{SessionID: "s", Tree: raw},
		Checkpoints: []CheckpointSummary{{SessionID: "s", Seq: 1}},
		Mode:        ModeEager,
		Contents:    map[string]string{HashOf("hi"): "hi"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.GetSession().GetStatus() != bigav1.SessionStatus_SESSION_STATUS_ACTIVE || state.GetSession().GetDescription() != desc ||
		state.GetSession().EndedAt != nil || state.GetMode() != bigav1.LoadMode_LOAD_MODE_EAGER ||
		string(state.GetContents()[HashOf("hi")]) != "hi" || len(state.GetCheckpoints()) != 1 {
		t.Errorf("state = %+v", state)
	}
	round := &bigav1.SessionState{}
	if data, err := proto.Marshal(state); err != nil || proto.Unmarshal(data, round) != nil || !proto.Equal(state, round) {
		t.Errorf("state does not survive the wire: %v", err)
	}
}
