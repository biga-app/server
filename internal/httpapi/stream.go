package httpapi

import (
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/biga-app/server/internal/events"
	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

// Live-stream timing.
const (
	keepaliveEvery = 15 * time.Second
	// streamWriteTimeout cuts off a viewer that stops reading.
	streamWriteTimeout = 10 * time.Second
)

// streamGate caps open streams, overall and per client, so one client
// cannot hold every connection the server has.
type streamGate struct {
	mu       sync.Mutex
	max, per int
	total    int
	byClient map[string]int
}

func newStreamGate(max, per int) *streamGate {
	return &streamGate{max: max, per: per, byClient: make(map[string]int)}
}

func (g *streamGate) acquire(client string) (ok, full bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.total >= g.max {
		return false, true
	}
	if g.byClient[client] >= g.per {
		return false, false
	}
	g.total++
	g.byClient[client]++
	return true, false
}

func (g *streamGate) release(client string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.total--
	if g.byClient[client]--; g.byClient[client] <= 0 {
		delete(g.byClient, client)
	}
}

// resumePosition reads where a reconnecting viewer left off: the last
// checkpoint it applied and the timestamp of the snapshot it holds.
func resumePosition(r *http.Request) (after int, snapshot int64, ok bool) {
	q := r.URL.Query()
	after, errAfter := strconv.Atoi(q.Get("after"))
	snapshot, errSnapshot := strconv.ParseInt(q.Get("snapshot"), 10, 64)
	if errAfter != nil || errSnapshot != nil || after < -1 || after > math.MaxInt32 || snapshot <= 0 {
		return 0, 0, false
	}
	return after, snapshot, true
}

// stream sends a session's live events as length-delimited protobuf frames.
// A viewer that says where it left off (?after=&snapshot=) is sent the
// checkpoints it missed when that is cheap, and told to reload otherwise.
func (a *API) stream(w http.ResponseWriter, r *http.Request) {
	id, ok := sessionID(w, r)
	if !ok {
		return
	}
	sess, err := a.svc.Session(r.Context(), id)
	if err != nil {
		a.serviceError(w, r, err, bigav1.ErrorCodeInvalidState, "")
		return
	}

	client := clientKey(clientAddr(r.Context()))
	ok, full := a.streams.acquire(client)
	if !ok {
		retryAfter(w, 30*time.Second)
		if full {
			fail(w, r, http.StatusServiceUnavailable, bigav1.ErrorCodeServerBusy, "The server is at its live-stream capacity.", nil)
		} else {
			fail(w, r, http.StatusTooManyRequests, bigav1.ErrorCodeTooManyStreams, "Too many open live streams from this client.", nil)
		}
		return
	}
	defer a.streams.release(client)

	// Subscribe before the first write, so no event slips between the two.
	sub := a.hub.Subscribe(id)
	defer sub.Close()

	rc := http.NewResponseController(w)
	// No read deadline: net/http cancels the request context when one
	// expires, which would end every stream. Headers are already bounded by
	// ReadHeaderTimeout, and each write below carries its own deadline.
	_ = rc.SetReadDeadline(time.Time{})
	h := w.Header()
	h.Set("Content-Type", streamType)
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("X-Accel-Buffering", "no") // nginx: do not buffer
	w.WriteHeader(http.StatusOK)

	send := func(chunk []byte) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
		if _, err := w.Write(chunk); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	frame := func(e events.Event, err error) bool {
		return err == nil && send(e.Data)
	}

	var (
		missed  []*wire.Checkpoint
		resumed bool
		lastSeq = -1
	)
	if after, snapshot, ok := resumePosition(r); ok && sess.Status == wire.StatusActive {
		var err error
		if missed, resumed, err = a.svc.Replay(r.Context(), id, after, snapshot); err != nil {
			a.log.Warn("stream replay failed", "session", id, "error", err)
			missed, resumed = nil, false
		}
		if resumed {
			lastSeq = after
		}
	}
	if !frame(events.Connected(id, time.Now(), resumed)) || sess.Status != wire.StatusActive {
		return
	}
	for _, cp := range missed {
		if !frame(events.Checkpoint(cp)) {
			return
		}
		lastSeq = cp.Seq
	}

	tick := time.NewTicker(keepaliveEvery)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-a.done:
			return
		case e, ok := <-sub.C:
			if !ok {
				return // fell too far behind; the browser reconnects and resyncs
			}
			if e.Name == events.NameCheckpoint && e.Seq <= lastSeq {
				continue // already sent as part of the replay
			}
			if !send(e.Data) || e.Name == events.NameSessionEnded {
				return
			}
		case <-tick.C:
			stale, err := a.svc.Stale(r.Context(), id)
			if err != nil {
				a.log.Warn("stream staleness check failed", "session", id, "error", err)
				return
			}
			if stale {
				frame(events.SessionEnded(id, time.Now()))
				return
			}
			if !frame(events.Keepalive(time.Now())) {
				return
			}
		}
	}
}
