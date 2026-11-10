package httpapi

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/biga-app/server/internal/auth"
	"github.com/biga-app/server/internal/service"
	"github.com/biga-app/server/internal/wire"
)

func (a *API) createExtension(w http.ResponseWriter, r *http.Request) {
	id, token, ok := a.owned(w, r, "extensions", limitOwnerRead)
	if !ok {
		return
	}
	var in struct {
		Name string `json:"name"`
	}
	if err := readJSON(w, r, 1024, &in, true); err != nil {
		bodyError(w, r, err, "Extension")
		return
	}
	name := strings.TrimSpace(in.Name)
	if name == "" || len(name) > 80 || strings.ContainsRune(name, 0) {
		fail(w, r, http.StatusBadRequest, "invalid_request", "Extension name must be between 1 and 80 bytes and contain no NUL characters.", nil)
		return
	}
	grant, err := a.svc.CreateExtension(r.Context(), id, token, name)
	if errors.Is(err, service.ErrExtensionLimit) {
		fail(w, r, http.StatusConflict, "extension_limit", "This session has reached its limit of 20 extensions.", nil)
		return
	}
	if err != nil {
		a.serviceError(w, r, err, "invalid_state", "")
		return
	}
	respondPrivate(w, r, http.StatusCreated, grant)
}

func (a *API) listExtensions(w http.ResponseWriter, r *http.Request) {
	id, token, ok := a.writeGate(w, r, "extensions:"+r.PathValue("id"), limitOwnerRead)
	if !ok {
		return
	}
	extensions, err := a.svc.Extensions(r.Context(), id, token)
	if err != nil {
		a.serviceError(w, r, err, "invalid_state", "")
		return
	}
	respondPrivate(w, r, http.StatusOK, map[string][]wire.Extension{"extensions": extensions})
}

func (a *API) revokeExtension(w http.ResponseWriter, r *http.Request) {
	id, token, ok := a.writeGate(w, r, "extensions:"+r.PathValue("id"), limitOwnerRead)
	if !ok {
		return
	}
	if err := a.svc.RevokeExtension(r.Context(), id, token, r.PathValue("extension")); err != nil {
		a.serviceError(w, r, err, "invalid_state", "")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) extensionEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := sessionID(w, r)
	if !ok {
		return
	}
	query := r.URL.Query()
	after, limit := -1, service.DefaultPage
	if raw := query.Get("after"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < -1 || n > math.MaxInt32 {
			fail(w, r, http.StatusBadRequest, "invalid_request", "after must be -1 or a checkpoint sequence number.", nil)
			return
		}
		after = n
	}
	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > service.MaxPage {
			fail(w, r, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 500.", nil)
			return
		}
		limit = n
	}
	feed, err := a.svc.ExtensionFeed(r.Context(), id, auth.BearerToken(r), after, limit)
	if errors.Is(err, service.ErrExtensionToken) {
		fail(w, r, http.StatusUnauthorized, "extension_token_invalid", "A valid extension token for this session is required.", nil)
		return
	}
	if err != nil {
		a.serviceError(w, r, err, "invalid_state", "")
		return
	}
	respondPrivate(w, r, http.StatusOK, feed)
}
