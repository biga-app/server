package httpapi

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

// acceptedEncodings is advertised on every response (RFC 7694). The CLI
// compresses uploads only after seeing it.
const acceptedEncodings = "gzip"

// minGzipBytes is the smallest response worth compressing.
const minGzipBytes = 1400

type envelope struct {
	OK    bool      `json:"ok"`
	Data  any       `json:"data,omitempty"`
	Error *apiError `json:"error,omitempty"`
}

type apiError struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Details map[string]string `json:"details,omitempty"`
}

// wantsProto reports whether the client asked for protobuf, as the CLI and
// the viewer do. Everyone else (curl, health checks, clients from before the
// protobuf wire) gets JSON errors they can read.
func wantsProto(r *http.Request) bool {
	for _, v := range r.Header.Values("Accept") {
		for part := range strings.SplitSeq(v, ",") {
			mt, _, err := mime.ParseMediaType(strings.TrimSpace(part))
			if err == nil && (mt == protoType || mt == streamType) {
				return true
			}
		}
	}
	return false
}

// negotiate answers clients that did not ask for protobuf with a JSON error
// that says what to do, instead of a body they cannot parse.
func negotiate(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !wantsProto(r) {
			fail(w, r, http.StatusNotAcceptable, "not_acceptable",
				"This server speaks protobuf (Accept: application/x-protobuf). Update the biga CLI and reload the viewer.", nil)
			return
		}
		h(w, r)
	}
}

// respond writes msg as the success body.
func respond(w http.ResponseWriter, r *http.Request, status int, msg proto.Message) {
	writeProto(w, r, status, msg, true)
}

// respondPrivate is respond for bodies holding a credential: never
// compressed, so a compression side channel (BREACH) has nothing to measure,
// and never cached.
func respondPrivate(w http.ResponseWriter, r *http.Request, status int, msg proto.Message) {
	w.Header().Set("Cache-Control", "no-store")
	writeProto(w, r, status, msg, false)
}

// accepted answers an upload that was taken in.
func accepted(w http.ResponseWriter) {
	w.Header().Set("Accept-Encoding", acceptedEncodings)
	w.WriteHeader(http.StatusAccepted)
}

// fail writes the error body: protobuf for clients that asked for it, the
// JSON envelope for the rest.
func fail(w http.ResponseWriter, r *http.Request, status int, code, message string, details map[string]string) {
	if status == http.StatusUnauthorized && w.Header().Get("WWW-Authenticate") == "" {
		w.Header().Set("WWW-Authenticate", `Bearer realm="biga"`)
	}
	w.Header().Set("Cache-Control", "no-store")
	if wantsProto(r) {
		writeProto(w, r, status, &bigav1.Error{Code: code, Message: message, Details: details}, false)
		return
	}
	writeJSON(w, r, status, envelope{Error: &apiError{Code: code, Message: message, Details: details}}, false)
}

// respondJSON writes the JSON success envelope, for the endpoints scripts and
// operators call: health checks, the retention cron and the extension feed.
func respondJSON(w http.ResponseWriter, r *http.Request, status int, data any) {
	writeJSON(w, r, status, envelope{OK: true, Data: data}, true)
}

func respondJSONPrivate(w http.ResponseWriter, r *http.Request, status int, data any) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, r, status, envelope{OK: true, Data: data}, false)
}

func writeProto(w http.ResponseWriter, r *http.Request, status int, msg proto.Message, compressible bool) {
	body, err := proto.Marshal(msg)
	if err != nil {
		http.Error(w, `{"ok":false,"error":{"code":"internal_error","message":"An unexpected error occurred."}}`, http.StatusInternalServerError)
		return
	}
	writeBody(w, r, status, protoType, body, compressible)
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, v any, compressible bool) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		http.Error(w, `{"ok":false,"error":{"code":"internal_error","message":"An unexpected error occurred."}}`, http.StatusInternalServerError)
		return
	}
	writeBody(w, r, status, "application/json; charset=utf-8", buf.Bytes(), compressible)
}

func writeBody(w http.ResponseWriter, r *http.Request, status int, contentType string, body []byte, compressible bool) {
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Accept-Encoding", acceptedEncodings)
	if compressible {
		h.Add("Vary", "Accept-Encoding")
	}
	if compressible && len(body) >= minGzipBytes && acceptsGzip(r) {
		var z bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&z, gzip.BestSpeed)
		_, _ = zw.Write(body)
		if zw.Close() == nil {
			h.Set("Content-Encoding", "gzip")
			body = z.Bytes()
		}
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body) //nolint:gosec // G705: a protobuf or JSON body, never HTML
	}
}

func acceptsGzip(r *http.Request) bool {
	for _, v := range r.Header.Values("Accept-Encoding") {
		for enc := range strings.SplitSeq(v, ",") {
			name, params, _ := strings.Cut(strings.TrimSpace(enc), ";")
			if strings.EqualFold(strings.TrimSpace(name), "gzip") && !strings.Contains(strings.ReplaceAll(params, " ", ""), "q=0") {
				return true
			}
		}
	}
	return false
}

// retryAfter sets a Retry-After header in whole seconds, at least one.
func retryAfter(w http.ResponseWriter, d time.Duration) {
	secs := int((d + time.Second - 1) / time.Second)
	w.Header().Set("Retry-After", strconv.Itoa(max(secs, 1)))
}
