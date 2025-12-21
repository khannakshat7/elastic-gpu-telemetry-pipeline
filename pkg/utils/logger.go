package utils

import (
	"log/slog"
	"os"
)

var Logger *slog.Logger

// SetupLogger initializes the slog logger at INFO level.
func SetupLogger() {
	Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
