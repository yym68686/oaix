package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/yym68686/oaix/internal/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runtime.RunGateway(ctx); err != nil {
		slog.Error("oaix admin failed", "error", err)
		os.Exit(1)
	}
}
