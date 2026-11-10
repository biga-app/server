// Package h3server serves the API over HTTP/3 (QUIC) beside the TCP listener,
// and tells TCP clients it is there with an Alt-Svc header.
package h3server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// advertiseFor is how long clients may remember that HTTP/3 is available.
const advertiseFor = 24 * time.Hour

// Server is an HTTP/3 server on a UDP socket.
type Server struct {
	srv  *http3.Server
	conn net.PacketConn
}

// Listen opens the UDP socket for address (a "host:port" the TCP listener
// shares) and prepares to serve handler on it. network is "udp", "udp4" or
// "udp6". QUIC needs TLS 1.3, so the certificate is the only TLS setting.
func Listen(ctx context.Context, network, address string, cert tls.Certificate, handler http.Handler, log *slog.Logger) (*Server, error) {
	var lc net.ListenConfig
	conn, err := lc.ListenPacket(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("listen on udp %s: %w", address, err)
	}
	srv := &http3.Server{
		Handler:        handler,
		MaxHeaderBytes: 64 << 10,
		IdleTimeout:    120 * time.Second,
		Logger:         log,
		TLSConfig: http3.ConfigureTLSConfig(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		}),
		QUICConfig: &quic.Config{
			MaxIdleTimeout:  60 * time.Second,
			KeepAlivePeriod: 20 * time.Second,
		},
	}
	return &Server{srv: srv, conn: conn}, nil
}

// Addr is the UDP address being served.
func (s *Server) Addr() net.Addr { return s.conn.LocalAddr() }

// Serve answers requests until the server is shut down.
func (s *Server) Serve() error { return s.srv.Serve(s.conn) }

// Shutdown stops accepting connections and lets requests in flight finish
// until ctx ends, then closes the rest.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)
	if err != nil {
		_ = s.srv.Close()
	}
	_ = s.conn.Close()
	return err
}

// AltSvc announces HTTP/3 on port to clients that reached next over TCP.
func AltSvc(next http.Handler, port int) http.Handler {
	value := `h3=":` + strconv.Itoa(port) + `"; ma=` + strconv.Itoa(int(advertiseFor.Seconds()))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor < 3 {
			w.Header().Set("Alt-Svc", value)
		}
		next.ServeHTTP(w, r)
	})
}
