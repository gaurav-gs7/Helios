package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gauravgs7/helios/internal/api"
	"github.com/gauravgs7/helios/internal/config"
	"github.com/gauravgs7/helios/internal/dispatch"
	"github.com/gauravgs7/helios/internal/metrics"
	"github.com/gauravgs7/helios/internal/scheduler"
	"github.com/gauravgs7/helios/internal/store/postgres"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.LoadControlPlane()
	if err != nil {
		logger.Error("load control-plane config", "err", err)
		os.Exit(1)
	}

	registry := prometheus.NewRegistry()
	m := metrics.New(registry)
	store, err := postgres.New(ctx, cfg.DatabaseURL, logger, m)
	if err != nil {
		logger.Error("create postgres store", "err", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Ping(ctx); err != nil {
		logger.Error("ping postgres", "err", err)
		os.Exit(1)
	}
	if err := store.ApplyMigrations(ctx); err != nil {
		logger.Error("apply migrations", "err", err)
		os.Exit(1)
	}
	dispatcher, err := dispatch.New(cfg.NATSURL)
	if err != nil {
		logger.Error("connect dispatcher", "err", err)
		os.Exit(1)
	}
	defer dispatcher.Close()

	server := api.NewServer(
		store,
		logger,
		cfg.Version,
		cfg.PlannerURL,
		cfg.AdminToken,
		cfg.WorkerBootstrapToken,
		cfg.MaxRequestBodyBytes,
		registry,
		cfg.SubmissionRatePerMinute,
	)
	s := scheduler.New(store, dispatcher, logger, cfg.LeaseDuration, cfg.SchedulerPolicy)

	go scheduler.RunLoop(ctx, logger.With("loop", "scheduler"), cfg.SchedulerTick, func(loopCtx context.Context) error {
		if err := store.RefreshWorkerHealth(loopCtx, cfg.HeartbeatStaleAfter, cfg.HeartbeatDeadAfter); err != nil {
			return err
		}
		if err := store.PromoteRetryableTasks(loopCtx); err != nil {
			return err
		}
		return s.Tick(loopCtx)
	})
	go scheduler.RunLoop(ctx, logger.With("loop", "recovery"), cfg.RecoveryTick, func(loopCtx context.Context) error {
		if err := store.RefreshWorkerHealth(loopCtx, cfg.HeartbeatStaleAfter, cfg.HeartbeatDeadAfter); err != nil {
			return err
		}
		if err := store.PromoteRetryableTasks(loopCtx); err != nil {
			return err
		}
		return store.RecoverActiveTasks(loopCtx)
	})

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           server.Routes(),
		ReadTimeout:       cfg.HTTPReadTimeout,
		ReadHeaderTimeout: cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
	}
	grpcServer := server.NewGRPCServer()
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		logger.Error("listen grpc", "addr", cfg.GRPCAddress, "err", err)
		os.Exit(1)
	}
	defer grpcListener.Close()
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.HTTPShutdownTimeout)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	logger.Info("control plane listening", "addr", cfg.HTTPAddress)
	logger.Info("control plane grpc listening", "addr", cfg.GRPCAddress)
	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil && ctx.Err() == nil {
			logger.Error("control plane grpc failed", "err", err)
			cancel()
		}
	}()
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("control plane failed", "err", err)
		os.Exit(1)
	}
}
