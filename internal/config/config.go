package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName             string
	Version                 string
	HTTPAddress             string
	ControlPlaneURL         string
	DatabaseURL             string
	NATSURL                 string
	SchedulerTick           time.Duration
	SchedulerPolicy         string
	RecoveryTick            time.Duration
	LeaseDuration           time.Duration
	HeartbeatStaleAfter     time.Duration
	HeartbeatDeadAfter      time.Duration
	WorkerHeartbeatInterval time.Duration
	PlannerURL              string
	DefaultNamespace        string
	AdminToken              string
	WorkerBootstrapToken    string
	MaxRequestBodyBytes     int64
	SubmissionRatePerMinute int
	HTTPReadTimeout         time.Duration
	HTTPWriteTimeout        time.Duration
	HTTPIdleTimeout         time.Duration
	HTTPShutdownTimeout     time.Duration
	WorkerCapacity          int
	WorkerCPUCapacityUnits  int
	WorkerMemoryCapacityMB  int
}

func LoadControlPlane() (Config, error) {
	cfg := base()
	cfg.ServiceName = "helios-control-plane"
	cfg.HTTPAddress = env("HELIOS_HTTP_ADDR", ":8080")
	cfg.DatabaseURL = env("HELIOS_DATABASE_URL", "postgres://helios:helios@localhost:5432/helios?sslmode=disable")
	cfg.NATSURL = env("HELIOS_NATS_URL", "nats://localhost:4222")
	cfg.SchedulerTick = envDuration("HELIOS_SCHEDULER_TICK", time.Second)
	cfg.SchedulerPolicy = env("HELIOS_SCHEDULER_POLICY", "resource-aware")
	cfg.RecoveryTick = envDuration("HELIOS_RECOVERY_TICK", 2*time.Second)
	cfg.LeaseDuration = envDuration("HELIOS_LEASE_DURATION", 20*time.Second)
	cfg.HeartbeatStaleAfter = envDuration("HELIOS_HEARTBEAT_STALE_AFTER", 15*time.Second)
	cfg.HeartbeatDeadAfter = envDuration("HELIOS_HEARTBEAT_DEAD_AFTER", 30*time.Second)
	cfg.PlannerURL = env("HELIOS_PLANNER_URL", "http://localhost:8090")
	cfg.DefaultNamespace = env("HELIOS_NAMESPACE", "local")
	cfg.AdminToken = env("HELIOS_ADMIN_TOKEN", "")
	cfg.WorkerBootstrapToken = env("HELIOS_WORKER_BOOTSTRAP_TOKEN", "")
	cfg.MaxRequestBodyBytes = envInt64("HELIOS_MAX_REQUEST_BODY_BYTES", 1<<20)
	cfg.SubmissionRatePerMinute = envInt("HELIOS_SUBMISSION_RATE_PER_MINUTE", 60)
	cfg.HTTPReadTimeout = envDuration("HELIOS_HTTP_READ_TIMEOUT", 10*time.Second)
	cfg.HTTPWriteTimeout = envDuration("HELIOS_HTTP_WRITE_TIMEOUT", 15*time.Second)
	cfg.HTTPIdleTimeout = envDuration("HELIOS_HTTP_IDLE_TIMEOUT", 60*time.Second)
	cfg.HTTPShutdownTimeout = envDuration("HELIOS_HTTP_SHUTDOWN_TIMEOUT", 10*time.Second)
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("HELIOS_DATABASE_URL is required")
	}
	if cfg.NATSURL == "" {
		return Config{}, fmt.Errorf("HELIOS_NATS_URL is required")
	}
	if cfg.AdminToken == "" {
		return Config{}, fmt.Errorf("HELIOS_ADMIN_TOKEN is required")
	}
	if cfg.WorkerBootstrapToken == "" {
		return Config{}, fmt.Errorf("HELIOS_WORKER_BOOTSTRAP_TOKEN is required")
	}
	return cfg, nil
}

func LoadWorker() Config {
	cfg := base()
	cfg.ServiceName = "helios-worker"
	cfg.ControlPlaneURL = env("HELIOS_CONTROL_PLANE_URL", "http://localhost:8080")
	cfg.NATSURL = env("HELIOS_NATS_URL", "nats://localhost:4222")
	cfg.WorkerHeartbeatInterval = envDuration("HELIOS_WORKER_HEARTBEAT_INTERVAL", 5*time.Second)
	cfg.Version = env("HELIOS_WORKER_VERSION", cfg.Version)
	cfg.WorkerBootstrapToken = env("HELIOS_WORKER_BOOTSTRAP_TOKEN", "")
	cfg.WorkerCapacity = envInt("HELIOS_WORKER_CAPACITY", 2)
	cfg.WorkerCPUCapacityUnits = envInt("HELIOS_WORKER_CPU_CAPACITY_UNITS", cfg.WorkerCapacity*1000)
	cfg.WorkerMemoryCapacityMB = envInt("HELIOS_WORKER_MEMORY_CAPACITY_MB", 1024)
	return cfg
}

func base() Config {
	return Config{
		Version: env("HELIOS_VERSION", "dev"),
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		parsed, err := time.ParseDuration(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.Atoi(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return fallback
}
