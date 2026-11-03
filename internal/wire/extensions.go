package wire

type Extension struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt string `json:"createdAt"`
}

type ExtensionGrant struct {
	Extension
	Token string `json:"token"`
}

type CheckpointEvent struct {
	Name         string `json:"name,omitempty"`
	ID           string `json:"id"`
	Type         string `json:"type"`
	Version      int    `json:"version"`
	SessionID    string `json:"sessionId"`
	Seq          int    `json:"seq"`
	CreatedAt    string `json:"createdAt"`
	FilesChanged int    `json:"filesChanged"`
	Additions    int    `json:"additions"`
	Deletions    int    `json:"deletions"`
}

type ExtensionFeed struct {
	Events    []CheckpointEvent `json:"events"`
	NextAfter int               `json:"nextAfter"`
}
