package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"unicode"

	"github.com/biga-app/server/internal/service"
	"github.com/biga-app/server/internal/wire"
)

func (a *API) markCheckpoint(w http.ResponseWriter, r *http.Request) {
	id, token, ok := a.owned(w, r, "mark", limitOwnerRead)
	if !ok {
		return
	}
	key := r.PathValue("key")
	if !wire.ValidSessionID(key) || len(key) < 16 {
		fail(w, r, http.StatusBadRequest, "invalid_request", "Invalid checkpoint mark key.", nil)
		return
	}
	var input struct {
		Name string `json:"name"`
	}
	if err := readJSON(w, r, 1024, &input, true); err != nil {
		bodyError(w, r, err, "Checkpoint mark")
		return
	}
	label := strings.TrimSpace(input.Name)
	if label == "" || len(label) > 120 || strings.ContainsFunc(label, unicode.IsControl) {
		fail(w, r, http.StatusBadRequest, "invalid_request", "Use a single-line checkpoint name of 1 to 120 bytes.", nil)
		return
	}
	result, err := a.svc.MarkCheckpoint(r.Context(), id, token, key, label)
	if errors.Is(err, service.ErrMarkKeyConflict) {
		fail(w, r, http.StatusConflict, "mark_key_conflict", "This mark key was already used with another name.", nil)
		return
	}
	if err != nil {
		a.serviceError(w, r, err, "invalid_state", "Broadcast at least one checkpoint before adding a mark.")
		return
	}
	respondPrivate(w, r, http.StatusOK, result)
}
