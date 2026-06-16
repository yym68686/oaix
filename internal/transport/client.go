package transport

import (
	"context"
	"crypto/tls"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

type Client struct {
	httpClients []*http.Client
	cfg         config.UpstreamConfig
	requests    atomic.Int64
	errors      atomic.Int64
	active      atomic.Int64
	nextShard   atomic.Uint64
}

type Stats struct {
	RequestsTotal int64 `json:"requests_total"`
	ErrorsTotal   int64 `json:"errors_total"`
	Active        int64 `json:"active"`
}

func New(cfg config.UpstreamConfig) *Client {
	shards := cfg.ShardCount
	if shards <= 0 {
		shards = 1
	}
	clients := make([]*http.Client, 0, shards)
	for i := 0; i < shards; i++ {
		clients = append(clients, newHTTPClient(cfg))
	}
	return &Client{
		httpClients: clients,
		cfg:         cfg,
	}
}

func newHTTPClient(cfg config.UpstreamConfig) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     cfg.ForceAttemptHTTP2,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    cfg.DisableCompression,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return &http.Client{Transport: tr}
}

func (c *Client) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	c.requests.Add(1)
	c.active.Add(1)
	resp, err := c.clientFor(req).Do(req.WithContext(ctx))
	c.active.Add(-1)
	if err != nil {
		c.errors.Add(1)
	}
	return resp, err
}

func (c *Client) CloseIdleConnections() {
	for _, client := range c.httpClients {
		client.CloseIdleConnections()
	}
}

func (c *Client) Stats() Stats {
	return Stats{
		RequestsTotal: c.requests.Load(),
		ErrorsTotal:   c.errors.Load(),
		Active:        c.active.Load(),
	}
}

func CloseBody(body io.Closer) {
	if body != nil {
		_ = body.Close()
	}
}

func (c *Client) clientFor(req *http.Request) *http.Client {
	if c == nil || len(c.httpClients) == 0 {
		return http.DefaultClient
	}
	if len(c.httpClients) == 1 || req == nil || req.URL == nil {
		return c.httpClients[0]
	}
	key := req.URL.Scheme + "://" + req.URL.Host + "|" + req.Header.Get("X-Request-ID")
	if req.Header.Get("X-Request-ID") == "" {
		key = req.URL.Scheme + "://" + req.URL.Host + "|" + req.Header.Get("OpenAI-Request-ID")
	}
	if key == req.URL.Scheme+"://"+req.URL.Host+"|" {
		index := c.nextShard.Add(1)
		return c.httpClients[int(index%uint64(len(c.httpClients)))]
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return c.httpClients[int(hash.Sum64()%uint64(len(c.httpClients)))]
}
