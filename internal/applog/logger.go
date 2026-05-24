package applog

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

func New(service string) (*slog.Logger, func()) {
	logDir := os.Getenv("HELIOS_LOG_DIR")
	if logDir == "" {
		logDir = "logs"
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})), func() {}
	}
	file, err := os.OpenFile(filepath.Join(logDir, service+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})), func() {}
	}
	writer := io.MultiWriter(os.Stdout, file)
	cleanup := func() {
		_ = file.Close()
	}
	return slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: slog.LevelInfo})), cleanup
}
