package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gauravgs7/helios/internal/config"
	"github.com/gauravgs7/helios/internal/dispatch"
	"github.com/gauravgs7/helios/internal/handlers"
	"github.com/gauravgs7/helios/internal/worker"
)

func main() {
	cfg := config.LoadWorker()
	var (
		taskTypes        = flag.String("task-types", strings.Join(handlers.SupportedTaskTypes(), ","), "comma separated list of supported task types")
		capacity         = flag.Int("capacity", cfg.WorkerCapacity, "maximum concurrent tasks for this worker")
		cpuCapacityUnits = flag.Int("cpu-units", cfg.WorkerCPUCapacityUnits, "worker schedulable CPU capacity units")
		memoryCapacityMB = flag.Int("memory-mb", cfg.WorkerMemoryCapacityMB, "worker schedulable memory capacity in MB")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dispatcher, err := dispatch.New(cfg.NATSURL)
	if err != nil {
		logger.Error("connect dispatcher", "err", err)
		os.Exit(1)
	}
	defer dispatcher.Close()

	runtime := worker.New(cfg, logger, dispatcher)
	types := strings.Split(*taskTypes, ",")
	for i := range types {
		types[i] = strings.TrimSpace(types[i])
	}
	if err := runtime.Run(ctx, types, *capacity, *cpuCapacityUnits, *memoryCapacityMB); err != nil {
		logger.Error("worker failed", "err", err)
		os.Exit(1)
	}
}
