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
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

func (a *API) createExtension(w http.ResponseWriter, r *http.Request) {
	id, token, ok := a.owned(w, r, "extensions", limitOwnerRead)
	if !ok {
		return
	}
	var in bigav1.CreateExtensionRequest
	if err := readProto(w, r, 1024, &in, true); err != nil {
		bodyError(w, r, err, "Extension")
		return
	}
	name := strings.TrimSpace(in.GetName())
	if name == "" || len(name) > 80 || strings.ContainsRune(name, 0) {
		fail(w, r, http.StatusBadRequest, bigav1.ErrorCodeInvalidRequest, "Extension name must be between 1 and 80 bytes and contain no NUL characters.", nil)
		return
	}
	grant, err := a.svc.CreateExtension(r.Context(), id, token, name)
	if errors.Is(err, service.ErrExtensionLimit) {
		fail(w, r, http.StatusConflict, bigav1.ErrorCodeExtensionLimit, "This session has reached its limit of 20 extensions.", nil)
		return
	}
	if err != nil {
		a.serviceError(w, r, err, bigav1.ErrorCodeInvalidState, "")
		return
	}
	respondPrivate(w, r, http.StatusCreated, wire.ExtensionGrantToPB(grant))
}

func (a *API) listExtensions(w http.ResponseWriter, r *http.Request) {
	id, token, ok := a.writeGate(w, r, "extensions:"+r.PathValue("id"), limitOwnerRead)
	if !ok {
		return
	}
	extensions, err := a.svc.Extensions(r.Context(), id, token)
	if err != nil {
		a.serviceError(w, r, err, bigav1.ErrorCodeInvalidState, "")
		return
	}
	respondPrivate(w, r, http.StatusOK, wire.ExtensionsToPB(extensions))
}

func (a *API) revokeExtension(w http.ResponseWriter, r *http.Request) {
	id, token, ok := a.writeGate(w, r, "extensions:"+r.PathValue("id"), limitOwnerRead)
	if !ok {
		return
	}
	if err := a.svc.RevokeExtension(r.Context(), id, token, r.PathValue("extension")); err != nil {
		a.serviceError(w, r, err, bigav1.ErrorCodeInvalidState, "")
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
			fail(w, r, http.StatusBadRequest, bigav1.ErrorCodeInvalidRequest, "after must be -1 or a checkpoint sequence number.", nil)
			return
		}
		after = n
	}
	if raw := query.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > service.MaxPage {
			fail(w, r, http.StatusBadRequest, bigav1.ErrorCodeInvalidRequest, "limit must be between 1 and 500.", nil)
			return
		}
		limit = n
	}
	feed, err := a.svc.ExtensionFeed(r.Context(), id, auth.BearerToken(r), after, limit)
	if errors.Is(err, service.ErrExtensionToken) {
		fail(w, r, http.StatusUnauthorized, bigav1.ErrorCodeExtensionTokenInvalid, "A valid extension token for this session is required.", nil)
		return
	}
	if err != nil {
		a.serviceError(w, r, err, bigav1.ErrorCodeInvalidState, "")
		return
	}
	respondJSONPrivate(w, r, http.StatusOK, feed)
}
