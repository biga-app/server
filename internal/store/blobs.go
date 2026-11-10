package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/biga-app/server/internal/objstore"
)

// BlobInput is file content to store.
type BlobInput struct {
	Hash    string
	Content string
	Size    int64
	Binary  bool
	// Raw stores the content as it is. Sealed content is already random, so
	// compressing it would only cost time.
	Raw bool
}

// EncodedBlob is a blob compressed and ready to insert. Encode outside a
// transaction: compression is the expensive part of a write.
type EncodedBlob struct {
	Hash        string
	Data        []byte
	Compression string
	Size        int32
	Binary      bool
	// Remote means Data has been put in the object store, so the row records
	// where it is and holds no content itself.
	Remote bool
}

// EncodeBlobs compresses blobs for [Queries.InsertBlobs].
func EncodeBlobs(in []BlobInput) []EncodedBlob {
	out := make([]EncodedBlob, len(in))
	for i, b := range in {
		data, codec := []byte{}, compressionNone
		if !b.Binary {
			if b.Raw {
				data, codec = []byte(b.Content), compressionNone
			} else {
				data, codec = compress(b.Content)
			}
		}
		out[i] = EncodedBlob{Hash: b.Hash, Data: data, Compression: codec, Size: int32(b.Size), Binary: b.Binary}
	}
	return out
}

// Locations a blob's content can be in.
const (
	locationDB      = "db"
	locationObjects = "objects"
)

// objectConcurrency bounds how many objects are moved at once.
const objectConcurrency = 8

// insertChunkBytes bounds one INSERT statement's payload.
const insertChunkBytes = 16 << 20

// UsesObjects reports whether new content goes to an object store.
func (s *Store) UsesObjects() bool { return s.objects != nil }

// PutObjects stores the content of blobs the session does not have yet in the
// object store, ahead of the transaction that records them, so no database
// lock is held across the network. Blobs it handles come back marked remote;
// without an object store, or for ones the session already has, they come back
// as they were.
func (s *Store) PutObjects(ctx context.Context, sessionID string, blobs []EncodedBlob) ([]EncodedBlob, error) {
	if s.objects == nil {
		return blobs, nil
	}
	hashes := make([]string, len(blobs))
	for i, b := range blobs {
		hashes[i] = b.Hash
	}
	have, err := s.Q().ExistingBlobs(ctx, sessionID, hashes)
	if err != nil {
		return nil, err
	}
	out := make([]EncodedBlob, len(blobs))
	copy(out, blobs)
	var todo []int
	for i, b := range out {
		if !b.Binary && !have[b.Hash] {
			todo = append(todo, i)
		}
	}
	err = forEachConcurrently(ctx, todo, func(i int) error {
		if err := s.objects.Put(ctx, objstore.Key(sessionID, out[i].Hash), out[i].Data); err != nil {
			return fmt.Errorf("store %s: %w", out[i].Hash, err)
		}
		out[i].Remote = true
		return nil
	})
	return out, err
}

// DeleteObjects removes everything a session owns from the object store.
func (s *Store) DeleteObjects(ctx context.Context, sessionID string) error {
	if s.objects == nil {
		return nil
	}
	return s.objects.DeletePrefix(ctx, objstore.Prefix(sessionID))
}

// PingObjects checks the object store, if there is one.
func (s *Store) PingObjects(ctx context.Context) error {
	if s.objects == nil {
		return nil
	}
	return s.objects.Ping(ctx)
}

// forEachConcurrently runs fn for each item, a few at a time, and returns the
// first error after everything started has finished.
func forEachConcurrently(ctx context.Context, items []int, fn func(int) error) error {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
		slots    = make(chan struct{}, objectConcurrency)
	)
	for _, item := range items {
		mu.Lock()
		failed := firstErr != nil
		mu.Unlock()
		if failed || ctx.Err() != nil {
			break
		}
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-slots; wg.Done() }()
			if err := fn(item); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	return firstErr
}

// InsertBlobs stores blobs not already stored for the session and returns
// how many bytes the new ones occupy.
func (q *Queries) InsertBlobs(ctx context.Context, sessionID string, blobs []EncodedBlob) (int64, error) {
	var stored int64
	for start := 0; start < len(blobs); {
		end, size := start, 0
		for end < len(blobs) && (end == start || size+inlineBytes(blobs[end]) <= insertChunkBytes) {
			size += inlineBytes(blobs[end])
			end++
		}
		n, err := q.insertBlobChunk(ctx, sessionID, blobs[start:end])
		if err != nil {
			return 0, err
		}
		stored += n
		start = end
	}
	return stored, nil
}

// inlineBytes is how much of a blob goes into the database.
func inlineBytes(b EncodedBlob) int {
	if b.Remote {
		return 0
	}
	return len(b.Data)
}

func (q *Queries) insertBlobChunk(ctx context.Context, sessionID string, blobs []EncodedBlob) (int64, error) {
	hashes := make([]string, len(blobs))
	data := make([][]byte, len(blobs))
	codecs := make([]string, len(blobs))
	sizes := make([]int32, len(blobs))
	stored := make([]int64, len(blobs))
	locations := make([]string, len(blobs))
	binary := make([]bool, len(blobs))
	for i, b := range blobs {
		hashes[i], codecs[i], sizes[i], binary[i], stored[i] = b.Hash, b.Compression, b.Size, b.Binary, int64(len(b.Data))
		if b.Remote {
			locations[i] = locationObjects
		} else {
			locations[i], data[i] = locationDB, b.Data
		}
	}
	rows, err := q.db.Query(ctx, `
		INSERT INTO blobs (session_id, hash, content, compression, size, "binary", location, stored_bytes)
		SELECT $1, t.hash, t.content, t.codec, t.size, t.bin, t.loc, t.stored
		FROM unnest($2::text[], $3::bytea[], $4::text[], $5::int[], $6::bool[], $7::text[], $8::bigint[])
			AS t(hash, content, codec, size, bin, loc, stored)
		ON CONFLICT (session_id, hash) DO NOTHING
		RETURNING stored_bytes`,
		sessionID, hashes, data, codecs, sizes, binary, locations, stored)
	if err != nil {
		return 0, fmt.Errorf("insert blobs: %w", err)
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return 0, err
		}
		total += n
	}
	return total, rows.Err()
}

// Blob is stored file content.
type Blob struct {
	Content string
	Binary  bool
}

// storedBlob is a row as read, before any content held in the object store is
// fetched.
type storedBlob struct {
	hash   string
	data   []byte
	codec  string
	binary bool
	remote bool
}

// fetch fills in the content of rows that live in the object store.
func (q *Queries) fetch(ctx context.Context, sessionID string, rows []storedBlob) error {
	var remote []int
	for i, r := range rows {
		if r.remote {
			remote = append(remote, i)
		}
	}
	if len(remote) == 0 {
		return nil
	}
	if q.objects == nil {
		return fmt.Errorf("blob %s is in object storage, but none is configured", rows[remote[0]].hash)
	}
	return forEachConcurrently(ctx, remote, func(i int) error {
		data, err := q.objects.Get(ctx, objstore.Key(sessionID, rows[i].hash))
		if err != nil {
			return fmt.Errorf("read blob %s: %w", rows[i].hash, err)
		}
		rows[i].data = data
		return nil
	})
}

// Blobs reads the listed blobs of a session; hashes without a blob are
// absent from the result.
func (q *Queries) Blobs(ctx context.Context, sessionID string, hashes []string) (map[string]Blob, error) {
	out := make(map[string]Blob, len(hashes))
	if len(hashes) == 0 {
		return out, nil
	}
	rows, err := q.db.Query(ctx,
		`SELECT hash, content, compression, "binary", location <> 'db' FROM blobs WHERE session_id = $1 AND hash = ANY($2)`,
		sessionID, hashes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var found []storedBlob
	for rows.Next() {
		var r storedBlob
		if err := rows.Scan(&r.hash, &r.data, &r.codec, &r.binary, &r.remote); err != nil {
			return nil, err
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := q.fetch(ctx, sessionID, found); err != nil {
		return nil, err
	}
	for _, r := range found {
		content, err := decompress(r.data, r.codec)
		if err != nil {
			return nil, err
		}
		out[r.hash] = Blob{Content: content, Binary: r.binary}
	}
	return out, nil
}

// HeadContents maps hash to content for every current text file.
func (q *Queries) HeadContents(ctx context.Context, sessionID string) (map[string]string, error) {
	rows, err := q.db.Query(ctx, `
		SELECT DISTINCT b.hash, b.content, b.compression, b."binary", b.location <> 'db'
		FROM files f
		JOIN blobs b ON b.session_id = f.session_id AND b.hash = f.hash
		WHERE f.session_id = $1 AND NOT b."binary"`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var found []storedBlob
	for rows.Next() {
		var r storedBlob
		if err := rows.Scan(&r.hash, &r.data, &r.codec, &r.binary, &r.remote); err != nil {
			return nil, err
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := q.fetch(ctx, sessionID, found); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(found))
	for _, r := range found {
		if out[r.hash], err = decompress(r.data, r.codec); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ExistingBlobs reports which of the hashes the session already has a blob for.
func (q *Queries) ExistingBlobs(ctx context.Context, sessionID string, hashes []string) (map[string]bool, error) {
	out := make(map[string]bool, len(hashes))
	if len(hashes) == 0 {
		return out, nil
	}
	rows, err := q.db.Query(ctx, `SELECT hash FROM blobs WHERE session_id = $1 AND hash = ANY($2)`, sessionID, hashes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		out[hash] = true
	}
	return out, rows.Err()
}

// AttachBlobs records that a snapshot or checkpoint now refers to the blobs, so
// the sweep leaves them alone.
func (q *Queries) AttachBlobs(ctx context.Context, sessionID string, hashes []string) error {
	if len(hashes) == 0 {
		return nil
	}
	_, err := q.db.Exec(ctx, `
		UPDATE blobs SET attached_at = now()
		WHERE session_id = $1 AND hash = ANY($2) AND attached_at IS NULL`, sessionID, hashes)
	return err
}

// BlobRef names one blob.
type BlobRef struct {
	SessionID string
	Hash      string
	// Remote means its content is in the object store.
	Remote bool
}

// UnattachedBlobs lists blobs no snapshot or checkpoint ever referred to that
// were stored before cutoff, oldest first. A session ID narrows it to one
// session.
func (q *Queries) UnattachedBlobs(ctx context.Context, cutoff time.Time, sessionID string, limit int) ([]BlobRef, error) {
	rows, err := q.db.Query(ctx, `
		SELECT session_id, hash, location <> 'db'
		FROM blobs
		WHERE attached_at IS NULL AND created_at < $1 AND ($2 = '' OR session_id = $2)
		ORDER BY created_at
		LIMIT $3`, cutoff, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BlobRef
	for rows.Next() {
		var r BlobRef
		if err := rows.Scan(&r.SessionID, &r.Hash, &r.Remote); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteBlobs removes blob rows, but only ones still unattached, and gives back
// the bytes they occupied to each session's storage total. It returns the rows
// it removed.
func (q *Queries) DeleteBlobs(ctx context.Context, refs []BlobRef) ([]BlobRef, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	sessions, hashes := make([]string, len(refs)), make([]string, len(refs))
	for i, r := range refs {
		sessions[i], hashes[i] = r.SessionID, r.Hash
	}
	rows, err := q.db.Query(ctx, `
		DELETE FROM blobs b USING unnest($1::text[], $2::text[]) AS t(session_id, hash)
		WHERE b.session_id = t.session_id AND b.hash = t.hash AND b.attached_at IS NULL
		RETURNING b.session_id, b.hash, b.location <> 'db', coalesce(b.stored_bytes, octet_length(b.content), 0)`,
		sessions, hashes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var (
		gone  []BlobRef
		freed = make(map[string]int64)
	)
	for rows.Next() {
		var (
			r     BlobRef
			bytes int64
		)
		if err := rows.Scan(&r.SessionID, &r.Hash, &r.Remote, &bytes); err != nil {
			return nil, err
		}
		gone = append(gone, r)
		freed[r.SessionID] += bytes
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for id, n := range freed {
		if _, err := q.db.Exec(ctx,
			`UPDATE sessions SET storage_bytes = greatest(storage_bytes - $2, 0) WHERE id = $1`, id, n); err != nil {
			return nil, err
		}
	}
	return gone, nil
}

// LocalBlobs lists text blobs whose content is still in the database.
func (q *Queries) LocalBlobs(ctx context.Context, limit int) ([]LocalBlob, error) {
	rows, err := q.db.Query(ctx, `
		SELECT session_id, hash, content
		FROM blobs
		WHERE location = 'db' AND NOT "binary"
		ORDER BY session_id, hash
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LocalBlob
	for rows.Next() {
		var b LocalBlob
		if err := rows.Scan(&b.SessionID, &b.Hash, &b.Data); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// LocalBlob is a blob's stored bytes, as read from the database.
type LocalBlob struct {
	SessionID string
	Hash      string
	Data      []byte
}

// MoveBlob records that a blob's content is now in the object store and drops
// it from the database. It does nothing if the blob has already moved.
func (q *Queries) MoveBlob(ctx context.Context, b LocalBlob) error {
	_, err := q.db.Exec(ctx, `
		UPDATE blobs SET location = 'objects', content = NULL, stored_bytes = $3
		WHERE session_id = $1 AND hash = $2 AND location = 'db'`, b.SessionID, b.Hash, int64(len(b.Data)))
	return err
}

// PutLocal stores a database blob's bytes in the object store, ahead of
// [Queries.MoveBlob].
func (s *Store) PutLocal(ctx context.Context, b LocalBlob) error {
	if s.objects == nil {
		return errors.New("no object store is configured")
	}
	return s.objects.Put(ctx, objstore.Key(b.SessionID, b.Hash), b.Data)
}

// DeleteBlobObjects removes the object-store content of blobs.
func (s *Store) DeleteBlobObjects(ctx context.Context, refs []BlobRef) error {
	if s.objects == nil {
		return nil
	}
	var keys []string
	for _, r := range refs {
		if r.Remote {
			keys = append(keys, objstore.Key(r.SessionID, r.Hash))
		}
	}
	if len(keys) == 0 {
		return nil
	}
	return s.objects.Delete(ctx, keys...)
}

// MigrateBlobs moves every text blob still held in the database into the
// object store, a batch at a time, and reports how many it moved. It can be
// stopped and run again: a blob is dropped from the database only after its
// object is stored.
func (s *Store) MigrateBlobs(ctx context.Context, batch int, progress func(moved int64)) (int64, error) {
	if s.objects == nil {
		return 0, errors.New("no object store is configured")
	}
	var moved int64
	for {
		blobs, err := s.Q().LocalBlobs(ctx, batch)
		if err != nil {
			return moved, err
		}
		if len(blobs) == 0 {
			return moved, nil
		}
		idx := make([]int, len(blobs))
		for i := range idx {
			idx[i] = i
		}
		err = forEachConcurrently(ctx, idx, func(i int) error {
			if err := s.PutLocal(ctx, blobs[i]); err != nil {
				return err
			}
			return s.Q().MoveBlob(ctx, blobs[i])
		})
		if err != nil {
			return moved, err
		}
		moved += int64(len(blobs))
		if progress != nil {
			progress(moved)
		}
	}
}
