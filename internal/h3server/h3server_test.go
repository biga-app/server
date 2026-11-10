package h3server

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func start(t *testing.T, handler http.Handler) (*Server, *http.Client) {
	t.Helper()
	s, err := Listen(t.Context(), "udp4", "127.0.0.1:0", selfSigned(t), handler, quiet)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := t.Context(), func() {}
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	tr := &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	t.Cleanup(func() { _ = tr.Close() })
	return s, &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

func TestRequestsAreServedOverHTTP3(t *testing.T) {
	s, client := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Proto", r.Proto)
		_, _ = w.Write([]byte(strings.ToUpper(string(body))))
	}))
	resp, err := client.Post("https://"+s.Addr().String()+"/x", "text/plain", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "HELLO" || resp.ProtoMajor != 3 || !strings.HasPrefix(resp.Header.Get("X-Proto"), "HTTP/3") {
		t.Errorf("got %q over %s (handler saw %q)", got, resp.Proto, resp.Header.Get("X-Proto"))
	}
}

func TestLiveStreamsFlushAndHonorDeadlinesOverHTTP3(t *testing.T) {
	release := make(chan struct{})
	s, client := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		if err := rc.SetWriteDeadline(time.Now().Add(time.Minute)); err != nil {
			http.Error(w, "write deadline: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := rc.SetReadDeadline(time.Time{}); err != nil {
			http.Error(w, "read deadline: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "first\n")
		if err := rc.Flush(); err != nil {
			return
		}
		select {
		case <-release:
			_, _ = io.WriteString(w, "second\n")
		case <-r.Context().Done():
		}
	}))
	resp, err := client.Get("https://" + s.Addr().String() + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("%d: %s", resp.StatusCode, body)
	}
	lines := bufio.NewReader(resp.Body)
	line, err := lines.ReadString('\n')
	if err != nil || line != "first\n" {
		t.Fatalf("the first chunk must arrive while the response is still open: %q, %v", line, err)
	}
	close(release)
	if line, err = lines.ReadString('\n'); err != nil || line != "second\n" {
		t.Fatalf("second chunk: %q, %v", line, err)
	}
}

func TestAltSvcOnlyGoesToTCPClients(t *testing.T) {
	h := AltSvc(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}), 8443)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got, want := rec.Header().Get("Alt-Svc"), `h3=":8443"; ma=86400`; got != want {
		t.Errorf("Alt-Svc = %q, want %q", got, want)
	}
	r3 := httptest.NewRequest(http.MethodGet, "/", nil)
	r3.ProtoMajor = 3
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r3)
	if rec.Header().Get("Alt-Svc") != "" {
		t.Error("an HTTP/3 response advertised HTTP/3")
	}
}

func TestShutdownClosesTheSocket(t *testing.T) {
	s, err := Listen(t.Context(), "udp4", "127.0.0.1:0", selfSigned(t), http.NotFoundHandler(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve() }()
	_, port, _ := net.SplitHostPort(s.Addr().String())
	if _, err := strconv.Atoi(port); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}
}
