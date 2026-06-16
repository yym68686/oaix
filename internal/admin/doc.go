package admin

// Package admin is the ownership boundary for management-plane behavior.
//
// The current HTTP routes are registered from internal/httpapi. This package is
// intentionally present as the stable destination for admin handlers and
// service objects as the management plane continues to move out of shared HTTP
// wiring.
