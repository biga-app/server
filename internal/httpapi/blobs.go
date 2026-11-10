package httpapi

import (
	"net/http"

	"github.com/biga-app/server/internal/wire"
	bigav1 "github.com/biga-app/server/internal/wirepb/biga/v1"
)

// missingBlobs says which of the content hashes a client holds the server has
// not stored yet, so an upload can send only those.
func (a *API) missingBlobs(w http.ResponseWriter, r *http.Request) {
	id, token, ok := a.owned(w, r, "blobs", limitBlobs)
	if !ok {
		return
	}
	var req bigav1.MissingBlobsRequest
	if err := readProto(w, r, a.limits.CheckpointBytes, &req, true); err != nil {
		bodyError(w, r, err, "Blob list")
		return
	}
	if err := wire.ValidateHashes(req.GetHashes(), a.limits.SnapshotFiles); err != nil {
		a.serviceError(w, r, err, "invalid_state", "")
		return
	}
	missing, err := a.svc.MissingBlobs(r.Context(), id, token, req.GetHashes())
	if err != nil {
		a.serviceError(w, r, err, "invalid_state", "")
		return
	}
	respond(w, r, http.StatusOK, &bigav1.MissingBlobsResponse{Hashes: missing})
}

// uploadBlobs stores file content ahead of the snapshot that will refer to it.
func (a *API) uploadBlobs(w http.ResponseWriter, r *http.Request) {
	id, token, ok := a.owned(w, r, "blobs", limitBlobs)
	if !ok {
		return
	}
	var req bigav1.UploadBlobsRequest
	if err := readProto(w, r, a.limits.CheckpointBytes, &req, false); err != nil {
		bodyError(w, r, err, "Blob")
		return
	}
	blobs := wire.BlobsFromPB(&req)
	if err := wire.ValidateBlobs(blobs, a.limits.CheckpointEntries, a.limits.FileBytes); err != nil {
		a.serviceError(w, r, err, "invalid_state", "")
		return
	}
	if err := a.svc.StoreBlobs(r.Context(), id, token, blobs); err != nil {
		a.serviceError(w, r, err, "invalid_state", "")
		return
	}
	accepted(w)
}
