package helper

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPrettyHandler(t *testing.T) {
	var buf bytes.Buffer
	handler := NewPrettyHandler(&buf, PrettyHandlerOptions{})

	logger := slog.New(handler)
	logger.Info("test message", "key", "value")

	output := buf.String()
	assert.Contains(t, output, "test message")
	assert.Contains(t, output, "key")
	assert.Contains(t, output, "value")
	assert.Contains(t, output, "INFO")
}

func TestPrettyHandlerLevels(t *testing.T) {
	var buf bytes.Buffer
	handler := NewPrettyHandler(&buf, PrettyHandlerOptions{
		SlogOpts: slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	})

	logger := slog.New(handler)
	logger.Debug("debug msg")
	logger.Warn("warn msg")
	logger.Error("error msg")

	output := buf.String()
	assert.Contains(t, output, "debug msg")
	assert.Contains(t, output, "warn msg")
	assert.Contains(t, output, "error msg")
	assert.Contains(t, output, "DEBUG")
	assert.Contains(t, output, "WARN")
	assert.Contains(t, output, "ERROR")
}

func TestPrettyHandlerHandleDirect(t *testing.T) {
	var buf bytes.Buffer
	handler := NewPrettyHandler(&buf, PrettyHandlerOptions{})

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "direct handle", 0)

	err := handler.Handle(context.Background(), r)
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "direct handle")
}
