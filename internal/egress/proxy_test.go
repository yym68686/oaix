package egress

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHTTPSProxyTunnelKeepsCredentialsOffUpstream(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Proxy-Authorization") != "" {
			t.Error("proxy credentials leaked to upstream")
		}
		_, _ = io.WriteString(w, "tunnel-ok")
	}))
	defer upstream.Close()
	var connects atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != "upstream.example.com:443" || r.Header.Get("Proxy-Authorization") == "" {
			t.Error("invalid CONNECT request")
			w.WriteHeader(407)
			return
		}
		connects.Add(1)
		dest, err := net.Dial("tcp", strings.TrimPrefix(upstream.URL, "https://"))
		if err != nil {
			t.Error(err)
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			dest.Close()
			t.Error(err)
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		go func() { defer conn.Close(); defer dest.Close(); _, _ = io.Copy(dest, conn) }()
		go func() { defer conn.Close(); defer dest.Close(); _, _ = io.Copy(conn, dest) }()
	}))
	defer proxy.Close()
	base := http.DefaultTransport.(*http.Transport).Clone()
	// Only the local test upstream uses a self-signed certificate.
	base.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "8.8.8.8:8080" {
			t.Errorf("unexpected dial: %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(proxy.URL, "http://"))
	}
	client := &http.Client{Transport: NewTransport(base)}
	defer client.CloseIdleConnections()
	u, _ := Parse("8.8.8.8:8080:demo:password")
	req, _ := http.NewRequestWithContext(WithProxy(context.Background(), u), "GET", "https://upstream.example.com/", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "tunnel-ok" || connects.Load() != 1 {
		t.Fatalf("tunnel failed: %q count=%d", body, connects.Load())
	}
}

func TestSOCKS5AuthenticationAndRemoteDNS(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "socks-ok") }))
	defer upstream.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	completed := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			completed <- err
			return
		}
		defer conn.Close()
		read := func(n int) []byte { buf := make([]byte, n); _, err = io.ReadFull(conn, buf); return buf }
		header := read(2)
		read(int(header[1]))
		conn.Write([]byte{5, 2})
		auth := read(2)
		username := read(int(auth[1]))
		plen := read(1)
		password := read(int(plen[0]))
		if string(username) != "demo" || string(password) != "password" {
			completed <- errors.New("SOCKS authentication mismatch")
			return
		}
		conn.Write([]byte{1, 0})
		request := read(4)
		if request[3] != 3 {
			completed <- errors.New("SOCKS destination was resolved locally")
			return
		}
		size := read(1)
		host := read(int(size[0]))
		read(2)
		if err != nil || string(host) != "upstream.example.com" {
			completed <- errors.New("SOCKS hostname mismatch")
			return
		}
		dest, err := net.Dial("tcp", strings.TrimPrefix(upstream.URL, "http://"))
		if err != nil {
			completed <- err
			return
		}
		defer dest.Close()
		conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 80})
		go func() { _, _ = io.Copy(dest, conn); dest.Close() }()
		_, _ = io.Copy(conn, dest)
		completed <- nil
	}()
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "8.8.8.8:1080" {
			t.Errorf("unexpected dial: %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
	}
	client := &http.Client{Transport: NewTransport(base)}
	u, _ := Parse("socks5://demo:password@8.8.8.8:1080")
	req, _ := http.NewRequestWithContext(WithProxy(context.Background(), u), "GET", "http://upstream.example.com/", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	client.CloseIdleConnections()
	if string(body) != "socks-ok" {
		t.Fatalf("SOCKS request failed: %q", body)
	}
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
}

func TestParseProxy(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{" gateway.example.com:9999:td-customer-demo-country-US:secret ", "http://td-customer-demo-country-US:secret@gateway.example.com:9999"},
		{"proxy.example.com:9999:user:p:a@ss#?", "http://user:p%3Aa%40ss%23%3F@proxy.example.com:9999"},
		{"proxy.example.com:9999:user:p://word", "http://user:p%3A%2F%2Fword@proxy.example.com:9999"},
		{"proxy.example.com:8080", "http://proxy.example.com:8080"},
		{"[2606:4700:4700::1111]:1080:user:password", "http://user:password@[2606:4700:4700::1111]:1080"},
		{"socks5://user:p%40ss@proxy.example.com:1080/", "socks5://user:p%40ss@proxy.example.com:1080"},
		{"https://proxy.example.com:443", "https://proxy.example.com:443"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			u, err := Parse(tc.raw)
			if err != nil || u.String() != tc.want {
				t.Fatalf("parsed=%v error=%v want=%s", u, err, tc.want)
			}
		})
	}
	for _, raw := range []string{"", "proxy.example.com", "proxy.example.com:0", "proxy.example.com:65536", "proxy.example.com:80:user", "proxy.example.com:80::password", "ftp://proxy.example.com:21", "http://user:password@proxy.example.com:80/path", "http://proxy.example.com:80?url=x", "127.0.0.1:8080", "169.254.169.254:80", "100.64.0.1:80", "[::ffff:127.0.0.1]:80", "[fd00::1]:80", "proxy.local:80", "http://user:password%0A@proxy.example.com:80"} {
		if _, err := Parse(raw); err == nil {
			t.Errorf("accepted invalid proxy %q", raw)
		} else if strings.Contains(err.Error(), "password") {
			t.Errorf("error leaked credentials: %v", err)
		}
	}
}

func TestTransportUsesIsolatedProxyAuthenticationAndDoesNotFallBack(t *testing.T) {
	var calls atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Host != "upstream.example.com" || !r.URL.IsAbs() {
			t.Errorf("request did not use HTTP proxy: %v", r.URL)
		}
		_, _ = io.WriteString(w, r.Header.Get("Proxy-Authorization"))
	}))
	defer proxy.Close()
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.Proxy = nil
	base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if !strings.HasPrefix(address, "8.8.8.8:") {
			t.Errorf("unexpected unproxied dial %q", address)
			return nil, errors.New("unexpected dial")
		}
		return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(proxy.URL, "http://"))
	}
	client := &http.Client{Transport: NewTransport(base)}
	defer client.CloseIdleConnections()
	for _, username := range []string{"alice", "bob", "alice"} {
		u, _ := url.Parse("http://" + username + ":secret@8.8.8.8:8080")
		req, _ := http.NewRequestWithContext(WithProxy(context.Background(), u), "GET", "http://upstream.example.com/", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		want := &http.Request{Header: make(http.Header)}
		want.SetBasicAuth(username, "secret")
		if string(body) != want.Header.Get("Authorization") {
			t.Fatalf("cross-channel authentication: got %q", body)
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("proxy calls=%d", calls.Load())
	}
	ctx := ForToken(context.Background(), failingResolver{}, 42)
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://upstream.example.com/", nil)
	if _, err := client.Do(req); err == nil {
		t.Fatal("lookup failure fell back to direct")
	}
	private, _ := url.Parse("http://127.0.0.1:8080")
	req, _ = http.NewRequestWithContext(WithProxy(context.Background(), private), "GET", "http://upstream.example.com/", nil)
	if _, err := client.Do(req); err == nil {
		t.Fatal("private proxy was dialed")
	}
	if calls.Load() != 3 {
		t.Fatalf("failed routes reached network: %d", calls.Load())
	}
}

type failingResolver struct{}

func (failingResolver) ResolveTokenProxy(context.Context, int64) (*url.URL, error) {
	return nil, errors.New("sensitive connection string")
}

func TestProxyFailureDoesNotExposeCredentials(t *testing.T) {
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.DialContext = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("username:private-password")
	}
	client := &http.Client{Transport: NewTransport(base)}
	u, _ := Parse("8.8.8.8:8080:username:private-password")
	req, _ := http.NewRequestWithContext(WithProxy(context.Background(), u), "GET", "http://upstream.example.com/", nil)
	_, err := client.Do(req)
	if err == nil || strings.Contains(err.Error(), "private-password") || strings.Contains(err.Error(), "username") {
		t.Fatalf("unsafe error: %v", err)
	}
}
