// Package httpapi serves the session API the CLI and the viewer use.
//
// Reads are public: holding a session's ID is the permission to watch it.
// Writes need the session's owner token, and an access key when the server
// is configured with any. Every write is authorized before its body is read.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/biga-app/server/internal/auth"
	"github.com/biga-app/server/internal/config"
	"github.com/biga-app/server/internal/events"
	"github.com/biga-app/server/internal/ratelimit"
	"github.com/biga-app/server/internal/service"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

// Options configures the API.
type Options struct {
	Service *service.Service
	Events  *events.Hub
	Config  config.Config
	Limiter ratelimit.Limiter
	Log     *slog.Logger
	// Ready reports whether the server's dependencies answer.
	Ready func(context.Context) error
	// Done is closed when the server shuts down, ending live streams.
	Done <-chan struct{}
}

// API is the HTTP handler.
type API struct {
	svc         *service.Service
	hub         *events.Hub
	keys        *auth.Keys
	limiter     ratelimit.Limiter
	clients     clientResolver
	limits      config.Limits
	corsOrigins []string
	corp        string
	cronSecret  string
	tls         bool
	log         *slog.Logger
	ready       func(context.Context) error
	streams     *streamGate
	done        <-chan struct{}

	mux *http.ServeMux
}

// Fixed rate limits. Checkpoints are frequent (a burst of saves can flush
// every ~120 ms), so their budget is high; session creation is keyed by
// client to blunt mass session creation on a zero-account service.
var (
	limitSession    = rateLimit{60, time.Hour}
	limitSnapshot   = rateLimit{15, time.Minute}
	limitCheckpoint = rateLimit{240, time.Minute}
	limitHeartbeat  = rateLimit{30, time.Minute}
	limitBlobs      = rateLimit{600, time.Minute}
	limitOwnerRead  = rateLimit{30, time.Minute}
	// limitAuthFailures caps wrong tokens and keys per client.
	limitAuthFailures = rateLimit{30, 10 * time.Minute}
)

type rateLimit struct {
	n      int
	window time.Duration
}

// Request deadlines.
const (
	readTimeout   = 30 * time.Second
	writeTimeout  = 60 * time.Second
	uploadTimeout = 6 * time.Minute
)

// New returns the API handler.
func New(o Options) *API {
	a := &API{
		svc:         o.Service,
		hub:         o.Events,
		keys:        auth.NewKeys(o.Config.AccessKeys),
		limiter:     o.Limiter,
		clients:     clientResolver{trusted: o.Config.TrustedProxies},
		limits:      o.Config.Limits,
		corsOrigins: o.Config.CORSOrigins,
		corp:        "same-site",
		cronSecret:  o.Config.CronSecret,
		tls:         o.Config.TLS(),
		log:         o.Log,
		ready:       o.Ready,
		streams:     newStreamGate(o.Config.Limits.Streams, o.Config.Limits.StreamsPerClient),
		done:        o.Done,
		mux:         http.NewServeMux(),
	}
	if slices.Contains(a.corsOrigins, "*") {
		a.corp = "cross-origin"
	}
	a.routes()
	return a
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.base(a.mux).ServeHTTP(w, r)
}

func (a *API) routes() {
	m := a.mux
	read := func(h http.HandlerFunc) http.HandlerFunc {
		return deadlines(readTimeout, writeTimeout, a.cors(a.readLimit(negotiate(h))))
	}
	write := func(timeout time.Duration, h http.HandlerFunc) http.HandlerFunc {
		return deadlines(timeout, timeout+time.Minute, negotiate(h))
	}

	// Written by the CLI.
	m.HandleFunc(bigav1.RoutePutSession, write(readTimeout, a.putSession))
	m.HandleFunc(bigav1.RouteEndSession, write(readTimeout, a.endSession))
	m.HandleFunc(bigav1.RoutePostSnapshot, write(uploadTimeout, a.snapshot))
	m.HandleFunc(bigav1.RoutePutCheckpointMark, write(readTimeout, a.markCheckpoint))
	m.HandleFunc(bigav1.RouteMissingBlobs, write(readTimeout, a.missingBlobs))
	m.HandleFunc(bigav1.RouteUploadBlobs, write(uploadTimeout, a.uploadBlobs))
	m.HandleFunc(bigav1.RoutePostCheckpoint, write(uploadTimeout, a.checkpoint))
	m.HandleFunc(bigav1.RouteHeartbeat, write(readTimeout, a.heartbeat))
	m.HandleFunc(bigav1.RouteGetManifest, write(readTimeout, a.manifest))
	m.HandleFunc(bigav1.RouteCreateExtension, write(readTimeout, a.createExtension))
	m.HandleFunc(bigav1.RouteListExtensions, write(readTimeout, a.listExtensions))
	m.HandleFunc(bigav1.RouteRevokeExtension, write(readTimeout, a.revokeExtension))
	m.HandleFunc(bigav1.RouteExtensionEvents, deadlines(readTimeout, writeTimeout, a.readLimit(a.extensionEvents)))

	// Read by the viewer.
	m.HandleFunc(bigav1.RouteGetSession, read(a.getSession))
	m.HandleFunc(bigav1.RouteGetState, read(a.state))
	m.HandleFunc(bigav1.RouteListCheckpoints, read(a.checkpoints))
	m.HandleFunc(bigav1.RouteGetCheckpoint, read(a.checkpointBySeq))
	m.HandleFunc(bigav1.RouteGetFile, read(a.file))
	m.HandleFunc(bigav1.RouteGetFileHistory, read(a.fileHistory))
	m.HandleFunc(bigav1.RouteStream, a.cors(a.readLimit(negotiate(a.stream))))
	m.HandleFunc(bigav1.RoutePreflight, a.cors(a.preflight))

	if a.cronSecret != "" {
		m.HandleFunc(bigav1.RouteCronCleanup, deadlines(writeTimeout, writeTimeout+time.Minute, a.cron))
	}
	m.HandleFunc(bigav1.RouteHealthz, a.healthz)
	m.HandleFunc(bigav1.RouteReadyz, a.readyz)
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fail(w, r, http.StatusNotFound, bigav1.ErrorCodeNotFound, "No such endpoint.", nil)
	})
}
