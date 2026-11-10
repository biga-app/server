package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/biga-app/server/internal/config"
)

func TestPlainListenersSpeakCleartextHTTP2(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	srv.Config.Protocols = protocols(config.Config{})
	srv.Start()
	t.Cleanup(srv.Close)

	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	h2c := &http.Client{Transport: &http.Transport{Protocols: &p}}
	resp, err := h2c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if body, _ := io.ReadAll(resp.Body); string(body) != "HTTP/2.0" {
		t.Errorf("a client that speaks h2c got %q", body)
	}

	resp, err = http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if body, _ := io.ReadAll(resp.Body); string(body) != "HTTP/1.1" {
		t.Errorf("a plain HTTP/1.1 client got %q", body)
	}
}

func TestHTTP3FollowsTheTCPListener(t *testing.T) {
	tcp := &fakeListener{addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 5), Port: 8443}}
	tls := config.Config{TLSCertFile: "c", TLSKeyFile: "k", HTTP3: true}

	cases := []struct {
		name, addr string
		cfg        config.Config
		ln         net.Listener
		network    string
		address    string
		ok         bool
	}{
		{"tls", "10.0.0.5:8443", tls, tcp, "udp", "10.0.0.5:8443", true},
		{"every interface", ":8443", tls, tcp, "udp", ":8443", true},
		{"ipv4 only", "tcp4:0.0.0.0:8443", tls, tcp, "udp4", "0.0.0.0:8443", true},
		{"ipv6 only", "tcp6:[::]:8443", tls, tcp, "udp6", "[::]:8443", true},
		{"turned off", ":8443", config.Config{TLSCertFile: "c", TLSKeyFile: "k"}, tcp, "", "", false},
		{"no tls", ":8443", config.Config{HTTP3: true}, tcp, "", "", false},
		{"unix socket", "unix:/run/biga.sock", tls, &fakeListener{addr: &net.UnixAddr{Name: "/run/biga.sock", Net: "unix"}}, "", "", false},
	}
	for _, c := range cases {
		network, address, port, ok := quicAddress(c.cfg, c.addr, c.ln)
		if network != c.network || address != c.address || ok != c.ok || (ok && port != 8443) {
			t.Errorf("%s: got %q %q %d %v, want %q %q 8443 %v", c.name, network, address, port, ok, c.network, c.address, c.ok)
		}
	}
}

type fakeListener struct{ addr net.Addr }

func (l *fakeListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (l *fakeListener) Close() error              { return nil }
func (l *fakeListener) Addr() net.Addr            { return l.addr }
