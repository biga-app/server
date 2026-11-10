package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/biga-app/server/internal/service"
	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

// serviceError answers a failed service call. inactiveCode is the error
// code for an ended session, which differs by endpoint for compatibility
// with existing clients.
func (a *API) serviceError(w http.ResponseWriter, r *http.Request, err error, inactiveCode, notFoundMsg string) {
	var (
		verr    *wire.ValidationError
		missing *service.MissingBlobsError
	)
	switch {
	case errors.As(err, &missing):
		fail(w, r, http.StatusConflict, bigav1.ErrorCodeMissingBlob,
			"The snapshot refers to file content that was not uploaded. Upload it first, then send the snapshot again.",
			map[string]string{"count": strconv.Itoa(len(missing.Hashes)), "first": missing.Hashes[0]})
	case errors.As(err, &verr):
		fail(w, r, http.StatusBadRequest, bigav1.ErrorCodeInvalidRequest, "The request is invalid: "+verr.Error(),
			map[string]string{"field": verr.Field, "reason": verr.Reason})
	case errors.Is(err, service.ErrNotFound):
		fail(w, r, http.StatusNotFound, bigav1.ErrorCodeNotFound, "Session was not found.", nil)
	case errors.Is(err, service.ErrNoVersion):
		fail(w, r, http.StatusNotFound, bigav1.ErrorCodeNotFound, notFoundMsg, nil)
	case errors.Is(err, service.ErrInactive):
		fail(w, r, http.StatusConflict, inactiveCode, "Session is not active.", nil)
	case errors.Is(err, service.ErrUnauthorized):
		a.authFailed(r)
		fail(w, r, http.StatusUnauthorized, bigav1.ErrorCodeUnauthorized, "Writing to this session requires its owner token.", nil)
	case errors.Is(err, service.ErrForbidden):
		a.authFailed(r)
		fail(w, r, http.StatusForbidden, bigav1.ErrorCodeForbidden, "The session token does not match this session.", nil)
	case errors.Is(err, service.ErrLocked):
		a.authFailed(r)
		fail(w, r, http.StatusForbidden, bigav1.ErrorCodeSessionLocked,
			"This session was created before owner tokens and can no longer be written to. Start a new session.", nil)
	case errors.Is(err, service.ErrEncryptionMismatch):
		fail(w, r, http.StatusConflict, bigav1.ErrorCodeEncryptionMismatch,
			"A session cannot change between encrypted and not. Start a new one.", nil)
	case errors.Is(err, service.ErrQuota):
		fail(w, r, http.StatusRequestEntityTooLarge, bigav1.ErrorCodeStorageQuotaExceeded,
			"This session has reached its storage limit. Start a new session.", nil)
	case errors.Is(err, context.Canceled) && r.Context().Err() != nil:
		// The client went away; nobody is listening for an answer.
	default:
		a.log.Error("request failed", "request_id", requestID(r.Context()), "path", r.URL.Path, "error", err)
		fail(w, r, http.StatusInternalServerError, bigav1.ErrorCodeInternalError, "An unexpected error occurred.", nil)
	}
}

// bodyError answers a body that could not be read.
func bodyError(w http.ResponseWriter, r *http.Request, err error, what string) {
	switch {
	case errors.Is(err, errTooLarge):
		fail(w, r, http.StatusRequestEntityTooLarge, bigav1.ErrorCodePayloadTooLarge, what+" payload is too large.", nil)
	case errors.Is(err, errUnsupportedEncoding):
		fail(w, r, http.StatusUnsupportedMediaType, bigav1.ErrorCodeUnsupportedEncoding, "Unsupported Content-Encoding; send gzip or nothing.", nil)
	case errors.Is(err, errUnsupportedType):
		fail(w, r, http.StatusUnsupportedMediaType, bigav1.ErrorCodeUnsupportedMediaType, "Content-Type must be application/x-protobuf.", nil)
	default:
		fail(w, r, http.StatusBadRequest, bigav1.ErrorCodeInvalidRequest, "Invalid "+lower(what)+" payload: "+err.Error(), nil)
	}
}

func lower(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if 'A' <= b[0] && b[0] <= 'Z' {
		b[0] += 'a' - 'A'
	}
	return string(b)
}
