// Command incident-copilot запускает HTTP-сервис.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/PKSonlem/incident-copilot/app/internal/config"
	"github.com/PKSonlem/incident-copilot/app/internal/httpapi"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

// shutdownTimeout — сколько ждём завершения текущих запросов после сигнала остановки.
const shutdownTimeout = 60 * time.Second

func run(logger *slog.Logger) error {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "gateway.yaml"
	}
	cfg, err := config.Load(configPath, os.Getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	handler, err := httpapi.New(cfg)
	if err != nil {
		return fmt.Errorf("build server: %w", err)
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	logger.Info("listening", "addr", addr)
	select {
	case err := <-serveErr:
		return fmt.Errorf("listen and serve: %w", err)
	case <-ctx.Done():
	}
	stop() // повторный сигнал снова действует по умолчанию и завершает процесс сразу
	logger.Info("shutting down: waiting for in-flight requests")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Не дождались: обрываем оставшиеся соединения, чтобы процесс не завис.
		if cerr := srv.Close(); cerr != nil {
			logger.Warn("close server", "error", cerr)
		}
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}
	return nil
}
