package wire

import (
	"encoding/json"
	"fmt"

	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

func statusToPB(s string) bigav1.SessionStatus {
	switch s {
	case StatusActive:
		return bigav1.SessionStatus_SESSION_STATUS_ACTIVE
	case StatusEnded:
		return bigav1.SessionStatus_SESSION_STATUS_ENDED
	}
	return bigav1.SessionStatus_SESSION_STATUS_UNSPECIFIED
}

func changeToPB(c ChangeType) bigav1.ChangeType {
	switch c {
	case Added:
		return bigav1.ChangeType_CHANGE_TYPE_ADDED
	case Modified:
		return bigav1.ChangeType_CHANGE_TYPE_MODIFIED
	case Deleted:
		return bigav1.ChangeType_CHANGE_TYPE_DELETED
	case Renamed:
		return bigav1.ChangeType_CHANGE_TYPE_RENAMED
	}
	return bigav1.ChangeType_CHANGE_TYPE_UNSPECIFIED
}

func changeFromPB(c bigav1.ChangeType) ChangeType {
	switch c {
	case bigav1.ChangeType_CHANGE_TYPE_ADDED:
		return Added
	case bigav1.ChangeType_CHANGE_TYPE_MODIFIED:
		return Modified
	case bigav1.ChangeType_CHANGE_TYPE_DELETED:
		return Deleted
	case bigav1.ChangeType_CHANGE_TYPE_RENAMED:
		return Renamed
	}
	return ""
}

func modeToPB(m string) bigav1.LoadMode {
	switch m {
	case ModeEager:
		return bigav1.LoadMode_LOAD_MODE_EAGER
	case ModeLazy:
		return bigav1.LoadMode_LOAD_MODE_LAZY
	}
	return bigav1.LoadMode_LOAD_MODE_UNSPECIFIED
}

func SessionToPB(s Session) *bigav1.Session {
	return &bigav1.Session{
		Id:          s.ID,
		Title:       s.Title,
		Description: s.Description,
		Status:      statusToPB(s.Status),
		CreatedAt:   s.CreatedAt,
		UpdatedAt:   s.UpdatedAt,
		EndedAt:     s.EndedAt,
		GroupId:     s.GroupID,
	}
}

func GrantToPB(g SessionGrant) *bigav1.SessionGrant {
	return &bigav1.SessionGrant{Session: SessionToPB(g.Session), Token: g.Token, ShareUrl: g.ShareURL}
}

func SessionInputFromPB(r *bigav1.PutSessionRequest) CreateSessionInput {
	return CreateSessionInput{Title: r.Title, Description: r.Description, GroupID: r.GroupId}
}

func nodeToPB(n *FileNode, withContent bool) (*bigav1.FileNode, error) {
	if n == nil {
		return nil, nil
	}
	out := &bigav1.FileNode{Name: n.Name, Path: n.Path}
	switch n.Type {
	case NodeFile:
		info := &bigav1.FileInfo{Size: n.Size, Hash: n.Hash, Binary: n.Binary}
		if withContent {
			info.Content = []byte(n.Content)
		}
		out.Kind = &bigav1.FileNode_File{File: info}
	case NodeDirectory:
		dir := &bigav1.DirectoryInfo{Ignored: n.Ignored, Children: make([]*bigav1.FileNode, 0, len(n.Children))}
		for _, child := range n.Children {
			c, err := nodeToPB(child, withContent)
			if err != nil {
				return nil, err
			}
			dir.Children = append(dir.Children, c)
		}
		out.Kind = &bigav1.FileNode_Directory{Directory: dir}
	default:
		return nil, fmt.Errorf("wire: unknown node type %q", n.Type)
	}
	return out, nil
}

func nodeFromPB(n *bigav1.FileNode) *FileNode {
	if n == nil {
		return nil
	}
	out := &FileNode{Name: n.GetName(), Path: n.GetPath()}
	switch kind := n.GetKind().(type) {
	case *bigav1.FileNode_File:
		out.Type = NodeFile
		out.Size, out.Hash, out.Binary = kind.File.GetSize(), kind.File.GetHash(), kind.File.GetBinary()
		out.Content = string(kind.File.GetContent())
	case *bigav1.FileNode_Directory:
		out.Type = NodeDirectory
		out.Ignored = kind.Directory.GetIgnored()
		for _, child := range kind.Directory.GetChildren() {
			out.Children = append(out.Children, nodeFromPB(child))
		}
	}
	return out
}

func SnapshotToPB(s FileSnapshot) (*bigav1.FileSnapshot, error) {
	tree, err := nodeToPB(s.Tree, false)
	if err != nil {
		return nil, err
	}
	return &bigav1.FileSnapshot{SessionId: s.SessionID, Tree: tree, Timestamp: s.Timestamp, SequenceNum: s.SequenceNum}, nil
}

func SnapshotFromPB(s *bigav1.FileSnapshot) FileSnapshot {
	return FileSnapshot{
		SessionID: s.GetSessionId(), Tree: nodeFromPB(s.GetTree()),
		Timestamp: s.GetTimestamp(), SequenceNum: s.GetSequenceNum(),
	}
}

func StoredSnapshotToPB(s *StoredSnapshot) (*bigav1.FileSnapshot, error) {
	if s == nil {
		return nil, nil
	}
	var tree FileNode
	if err := json.Unmarshal(s.Tree, &tree); err != nil {
		return nil, fmt.Errorf("decode stored tree: %w", err)
	}
	return SnapshotToPB(FileSnapshot{SessionID: s.SessionID, Tree: &tree, Timestamp: s.Timestamp, SequenceNum: s.SequenceNum})
}

func CheckpointUploadFromPB(u *bigav1.CheckpointUpload) CheckpointUpload {
	out := CheckpointUpload{
		SessionID: u.GetSessionId(), Timestamp: u.GetTimestamp(),
		Entries: make([]CheckpointEntry, 0, len(u.GetEntries())),
	}
	for _, e := range u.GetEntries() {
		out.Entries = append(out.Entries, CheckpointEntry{
			Path: e.GetPath(), ChangeType: changeFromPB(e.GetChangeType()), OldPath: e.GetOldPath(),
			Hash: e.Hash, Size: e.GetSize(), Binary: e.GetBinary(), Content: string(e.GetContent()),
		})
	}
	return out
}

func SummaryToPB(c CheckpointSummary) *bigav1.CheckpointSummary {
	return &bigav1.CheckpointSummary{
		Named: c.Named, SessionId: c.SessionID, Seq: int32(c.Seq), Label: c.Label, CreatedAt: c.CreatedAt,
		FilesChanged: int32(c.FilesChanged), Additions: int32(c.Additions), Deletions: int32(c.Deletions),
	}
}

func SummariesToPB(list []CheckpointSummary) []*bigav1.CheckpointSummary {
	out := make([]*bigav1.CheckpointSummary, len(list))
	for i, c := range list {
		out[i] = SummaryToPB(c)
	}
	return out
}

func CheckpointToPB(c *Checkpoint) *bigav1.Checkpoint {
	changes := make([]*bigav1.CheckpointChange, len(c.Changes))
	for i, ch := range c.Changes {
		changes[i] = &bigav1.CheckpointChange{
			Path: ch.Path, ChangeType: changeToPB(ch.ChangeType), OldPath: ch.OldPath,
			BeforeHash: ch.BeforeHash, AfterHash: ch.AfterHash,
			Additions: int32(ch.Additions), Deletions: int32(ch.Deletions), Binary: ch.Binary,
		}
	}
	return &bigav1.Checkpoint{Summary: SummaryToPB(c.CheckpointSummary), Changes: changes}
}

func HistoryToPB(list []FileHistoryEntry) *bigav1.FileHistoryResponse {
	out := make([]*bigav1.FileHistoryEntry, len(list))
	for i, h := range list {
		out[i] = &bigav1.FileHistoryEntry{
			Seq: int32(h.Seq), Label: h.Label, CreatedAt: h.CreatedAt, ChangeType: changeToPB(h.ChangeType),
			BeforeHash: h.BeforeHash, AfterHash: h.AfterHash,
			Additions: int32(h.Additions), Deletions: int32(h.Deletions), Binary: h.Binary,
		}
	}
	return &bigav1.FileHistoryResponse{History: out}
}

func ManifestToPB(m Manifest) *bigav1.Manifest {
	files := make([]*bigav1.ManifestEntry, len(m.Files))
	for i, f := range m.Files {
		files[i] = &bigav1.ManifestEntry{Path: f.Path, Hash: f.Hash, Size: f.Size, Binary: f.Binary}
	}
	return &bigav1.Manifest{Files: files}
}

func StateToPB(s *SessionState) (*bigav1.SessionState, error) {
	snapshot, err := StoredSnapshotToPB(s.Snapshot)
	if err != nil {
		return nil, err
	}
	var contents map[string][]byte
	if len(s.Contents) > 0 {
		contents = make(map[string][]byte, len(s.Contents))
		for hash, content := range s.Contents {
			contents[hash] = []byte(content)
		}
	}
	return &bigav1.SessionState{
		Session:     SessionToPB(s.Session),
		Snapshot:    snapshot,
		Checkpoints: SummariesToPB(s.Checkpoints),
		Mode:        modeToPB(s.Mode),
		Contents:    contents,
	}, nil
}

func FileContentToPB(f FileContent) *bigav1.FileContent {
	return &bigav1.FileContent{Content: []byte(f.Content), Binary: f.Binary, Hash: f.Hash}
}

func ExtensionToPB(e Extension) *bigav1.Extension {
	return &bigav1.Extension{Id: e.ID, Name: e.Name, CreatedAt: e.CreatedAt}
}

func ExtensionGrantToPB(g ExtensionGrant) *bigav1.ExtensionGrant {
	return &bigav1.ExtensionGrant{Extension: ExtensionToPB(g.Extension), Token: g.Token}
}

func ExtensionsToPB(list []Extension) *bigav1.ListExtensionsResponse {
	out := make([]*bigav1.Extension, len(list))
	for i, e := range list {
		out[i] = ExtensionToPB(e)
	}
	return &bigav1.ListExtensionsResponse{Extensions: out}
}
