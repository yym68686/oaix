// Package egress handles account-specific outbound proxy connections.
package egress

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// These safe errors distinguish proxy configuration/transport failures from
// upstream HTTP responses without exposing credentials or changing error text.
var (
	ErrProxyConfigurationUnavailable = errors.New("无法读取账号代理配置")
	ErrProxyUnavailable              = errors.New("代理连接失败，请检查地址、认证信息和网络可用性")
)

// Parse accepts a proxy URL or host:port[:username:password]. Errors never
// contain the supplied credentials. Passwords in the shorthand may contain ':'.
func Parse(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	invalid := errors.New("代理格式无效，请输入 host:port:用户名:密码 或完整代理 URL")
	if raw == "" || len(raw) > 4096 || strings.ContainsAny(raw, "\r\n\x00") {
		return nil, invalid
	}
	schemePrefix, _, hasScheme := strings.Cut(raw, "://")
	if !hasScheme || strings.Contains(schemePrefix, ":") {
		end := 0
		if strings.HasPrefix(raw, "[") {
			end = strings.Index(raw, "]")
			if end < 0 {
				return nil, invalid
			}
		}
		colon := strings.Index(raw[end:], ":")
		if colon < 0 {
			return nil, invalid
		}
		colon += end
		host := raw[:colon]
		parts := strings.SplitN(raw[colon+1:], ":", 3)
		if len(parts) == 2 {
			return nil, invalid
		}
		u := &url.URL{Scheme: "http", Host: host + ":" + parts[0]}
		if len(parts) == 3 {
			if parts[1] == "" || parts[2] == "" {
				return nil, invalid
			}
			u.User = url.UserPassword(parts[1], parts[2])
		}
		raw = u.String()
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, invalid
	}
	u.Scheme = strings.ToLower(u.Scheme)
	switch u.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return nil, errors.New("仅支持 HTTP、HTTPS 和 SOCKS5 代理")
	}
	if u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, invalid
	}
	host := strings.ToLower(u.Hostname())
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 || host == "" {
		return nil, invalid
	}
	if strings.IndexFunc(host, func(r rune) bool { return unicode.IsSpace(r) || r == '%' || r == '/' || r == '\\' }) >= 0 {
		return nil, invalid
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if !publicIP(ip) {
			return nil, errors.New("代理地址必须是公网地址")
		}
	} else {
		if !strings.Contains(host, ".") || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") {
			return nil, errors.New("代理地址必须是公网域名或 IP")
		}
		for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
			if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return nil, invalid
			}
			for _, r := range label {
				if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
					return nil, invalid
				}
			}
		}
	}
	if u.User != nil {
		password, hasPassword := u.User.Password()
		if u.User.Username() == "" || !hasPassword || password == "" || strings.ContainsAny(u.User.Username()+password, "\r\n\x00") {
			return nil, invalid
		}
	}
	u.Host = net.JoinHostPort(host, strconv.Itoa(port))
	u.Path, u.RawPath = "", ""
	return u, nil
}

func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32", "64:ff9b::/96", "2002::/16"} {
		if netip.MustParsePrefix(prefix).Contains(ip) {
			return false
		}
	}
	return true
}

type Resolver interface {
	ResolveTokenProxy(context.Context, int64) (*url.URL, error)
}
type SnapshotResolver interface {
	ResolveTokenProxySnapshot(context.Context, int64) (*url.URL, RouteSnapshot, error)
}
type contextKey struct{}
type route struct {
	proxy *url.URL
	err   error
}

func WithProxy(ctx context.Context, proxy *url.URL) context.Context {
	return context.WithValue(ctx, contextKey{}, route{proxy: proxy})
}

// ForToken resolves persisted intent before an outbound attempt. A failed
// lookup is carried to the transport and must never fall back to direct access.
func ForToken(ctx context.Context, source any, tokenID int64) context.Context {
	resolver, ok := source.(Resolver)
	if !ok {
		return ctx
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var proxy *url.URL
	var err error
	trace := TraceFrom(ctx)
	trace.Event("route_config", "start", "", nil, 0)
	if source, ok := source.(SnapshotResolver); ok && trace != nil {
		var snapshot RouteSnapshot
		proxy, snapshot, err = source.ResolveTokenProxySnapshot(lookupCtx, tokenID)
		trace.update(func(d *TraceRecord) { d.Route = snapshot })
	} else {
		proxy, err = resolver.ResolveTokenProxy(lookupCtx, tokenID)
	}
	trace.Event("route_config", "done", "", err, 0)
	if err != nil {
		err = ErrProxyConfigurationUnavailable
	}
	return context.WithValue(ctx, contextKey{}, route{proxy: proxy, err: err})
}

type proxyTransport struct{ inner *http.Transport }

// NewTransport retains Go's bounded connection pools, keyed by proxy URL
// (including credentials), so connections cannot leak across proxy channels.
func NewTransport(base *http.Transport) http.RoundTripper {
	tr := base.Clone()
	originalProxy := tr.Proxy
	tr.Proxy = func(req *http.Request) (*url.URL, error) {
		r, ok := req.Context().Value(contextKey{}).(route)
		if ok && (r.proxy != nil || r.err != nil) {
			TraceFrom(req.Context()).setRoute("account_proxy", r.proxy)
			return r.proxy, r.err
		}
		if originalProxy != nil {
			proxy, err := originalProxy(req)
			kind := "default_direct"
			if proxy != nil {
				kind = "environment_proxy"
			}
			TraceFrom(req.Context()).setRoute(kind, proxy)
			return proxy, err
		}
		TraceFrom(req.Context()).setRoute("default_direct", nil)
		return nil, nil
	}
	originalConnectResponse := tr.OnProxyConnectResponse
	tr.OnProxyConnectResponse = func(ctx context.Context, proxyURL *url.URL, req *http.Request, resp *http.Response) error {
		TraceFrom(ctx).Event("proxy_connect", "done", "", nil, resp.StatusCode)
		if originalConnectResponse != nil {
			return originalConnectResponse(ctx, proxyURL, req, resp)
		}
		return nil
	}
	dial := tr.DialContext
	if dial == nil {
		dial = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		r, _ := ctx.Value(contextKey{}).(route)
		if r.proxy == nil {
			conn, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return observeDialConn(ctx, conn), nil
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, errors.New("代理地址无效")
		}
		trace := TraceFrom(ctx)
		trace.Event("proxy_dns", "start", address, nil, 0)
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		trace.Event("proxy_dns", "done", address, err, 0)
		if err != nil || len(ips) == 0 {
			return nil, errors.New("代理域名解析失败")
		}
		// Validate every answer and dial the checked IP, preventing DNS rebinding.
		for _, ip := range ips {
			if !publicIP(ip) {
				return nil, errors.New("代理地址必须解析到公网 IP")
			}
		}
		for _, ip := range ips {
			conn, dialErr := dial(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
			if dialErr == nil {
				return observeDialConn(ctx, conn), nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		return nil, errors.New("代理连接失败")
	}
	return &proxyTransport{inner: tr}
}

func (t *proxyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r, _ := req.Context().Value(contextKey{}).(route)
	trace := TraceFrom(req.Context())
	if r.err != nil {
		return nil, r.err
	}
	trace.Event("round_trip", "start", "", nil, 0)
	resp, err := t.inner.RoundTrip(req)
	trace.Event("round_trip", "done", "", err, 0)
	if resp != nil && trace != nil {
		trace.Response(resp)
		if resp.Body != nil {
			resp.Body = &tracedBody{ReadCloser: resp.Body, trace: trace}
		}
	}
	if err != nil && r.proxy != nil {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
		return nil, ErrProxyUnavailable
	}
	return resp, err
}

func (t *Trace) setRoute(kind string, proxy *url.URL) {
	t.update(func(d *TraceRecord) {
		d.Route.Kind = kind
		d.ProxySelectionObserved = true
		if proxy != nil {
			d.Route.Protocol = knownValue(proxy.Scheme, "http", "https", "socks5", "socks5h")
			d.ProxyHandshakeCoverage = "connect_response_only"
			if proxy.Scheme == "socks5" || proxy.Scheme == "socks5h" {
				d.ProxyHandshakeCoverage = "socks_handshake_not_instrumented"
			}
			d.Route.Endpoint = safeEndpoint(proxy.Host)
		}
	})
}

func (t *proxyTransport) CloseIdleConnections() { t.inner.CloseIdleConnections() }

var DefaultTransport = NewTransport(http.DefaultTransport.(*http.Transport))
