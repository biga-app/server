package httpapi

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"google.golang.org/protobuf/proto"
)

const (
	protoType  = "application/x-protobuf"
	streamType = "application/vnd.biga.stream+protobuf"

	// maxMessageDepth bounds how deeply a request's messages may nest, well
	// above the snapshot tree depth the validator allows.
	maxMessageDepth = 300
)

var (
	errTooLarge            = errors.New("request body is too large")
	errUnsupportedEncoding = errors.New("unsupported Content-Encoding")
	errUnsupportedType     = errors.New("Content-Type must be application/x-protobuf")
	errMalformed           = errors.New("request body is not valid protobuf")
)

// readProto decodes a request body into msg, enforcing max on the decoded
// bytes: a small gzip body cannot expand past the limit, and a missing or
// lying Content-Length cannot smuggle a larger one through. An empty body
// leaves msg untouched when allowEmpty is set.
func readProto(w http.ResponseWriter, r *http.Request, max int64, msg proto.Message, allowEmpty bool) error {
	if r.ContentLength > 0 || r.Header.Get("Content-Type") != "" {
		mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mt != protoType {
			return errUnsupportedType
		}
	}
	encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	if encoding != "" && encoding != "identity" && encoding != "gzip" {
		return errUnsupportedEncoding
	}
	// The raw body can never legitimately exceed the decoded limit.
	if r.ContentLength > max {
		return errTooLarge
	}
	var body io.Reader = http.MaxBytesReader(w, r.Body, max)
	if encoding == "gzip" {
		zr, err := gzip.NewReader(body)
		if err != nil {
			return bodyErr(err)
		}
		defer zr.Close()
		body = zr
	}

	data, err := io.ReadAll(io.LimitReader(body, max+1))
	if err != nil {
		return bodyErr(err)
	}
	if int64(len(data)) > max {
		return errTooLarge
	}
	if len(data) == 0 {
		if allowEmpty {
			return nil
		}
		return errMalformed
	}
	if err := (proto.UnmarshalOptions{RecursionLimit: maxMessageDepth}).Unmarshal(data, msg); err != nil {
		return fmt.Errorf("%w: %s", errMalformed, protoProblem(err))
	}
	return nil
}

func bodyErr(err error) error {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return errTooLarge
	}
	return fmt.Errorf("%w: the body is truncated or corrupt", errMalformed)
}

// protoProblem describes a decode error without echoing the input.
func protoProblem(err error) string {
	if strings.Contains(err.Error(), "exceeded maximum recursion depth") {
		return "messages are nested too deeply"
	}
	return "malformed"
}
