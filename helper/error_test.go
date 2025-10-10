package helper

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestError_Error(t *testing.T) {
	t.Run("Single trace error", func(t *testing.T) {
		originalErr := errors.New("original error message")
		customErr := Error{
			Original: originalErr,
			Trace:    []string{"first trace"},
		}

		result := customErr.Error()
		assert.Contains(t, result, "original error message", "should contain original error message")
		assert.Contains(t, result, "first trace", "should contain trace information")
		assert.Contains(t, result, " | Trace: ", "should contain trace separator")
	})

	t.Run("Multiple trace error", func(t *testing.T) {
		originalErr := errors.New("database connection failed")
		customErr := Error{
			Original: originalErr,
			Trace:    []string{"database.Connect", "service.Initialize", "main.Start"},
		}

		result := customErr.Error()
		assert.Contains(t, result, "database connection failed", "should contain original error message")
		assert.Contains(t, result, "database.Connect", "should contain first trace")
		assert.Contains(t, result, "service.Initialize", "should contain second trace")
		assert.Contains(t, result, "main.Start", "should contain third trace")
		assert.Contains(t, result, " | Trace: ", "should contain trace separator")

		tracePart := strings.Split(result, " | Trace: ")[1]
		assert.Contains(t, tracePart, "database.Connect, service.Initialize, main.Start", "traces should be comma-separated")
	})

	t.Run("Empty trace error", func(t *testing.T) {
		originalErr := errors.New("simple error")
		customErr := Error{
			Original: originalErr,
			Trace:    []string{},
		}

		result := customErr.Error()
		assert.Contains(t, result, "simple error", "should contain original error message")
		assert.Contains(t, result, " | Trace: ", "should contain trace separator even with empty trace")
	})
}

func TestNewError(t *testing.T) {
	t.Run("Create new error from standard error", func(t *testing.T) {
		originalErr := errors.New("file not found")
		trace := "reading configuration file"

		result := NewError(trace, originalErr)

		assert.Equal(t, originalErr, result.Original, "should preserve original error")
		require.Len(t, result.Trace, 1, "should have exactly one trace entry")
		assert.Contains(t, result.Trace[0], trace, "trace should contain provided trace message")
		assert.Contains(t, result.Trace[0], "TestNewError", "trace should contain calling function name")
	})

	t.Run("Chain existing Error", func(t *testing.T) {
		originalErr := errors.New("network timeout")
		firstTrace := "connecting to database"
		firstError := NewError(firstTrace, originalErr)

		secondTrace := "initializing service"
		result := NewError(secondTrace, firstError)

		assert.Equal(t, originalErr, result.Original, "should preserve original error")
		require.Len(t, result.Trace, 2, "should have two trace entries")
		assert.Contains(t, result.Trace[0], firstTrace, "first trace should contain first trace message")
		assert.Contains(t, result.Trace[1], secondTrace, "second trace should contain second trace message")
		assert.Contains(t, result.Trace[1], "TestNewError", "second trace should contain calling function name")
	})

	t.Run("Multiple chained errors", func(t *testing.T) {
		originalErr := errors.New("permission denied")

		err1 := NewError("opening file", originalErr)
		err2 := NewError("reading configuration", err1)
		result := NewError("starting application", err2)

		assert.Equal(t, originalErr, result.Original, "should preserve original error")
		require.Len(t, result.Trace, 3, "should have three trace entries")
		assert.Contains(t, result.Trace[0], "opening file", "first trace should contain first message")
		assert.Contains(t, result.Trace[1], "reading configuration", "second trace should contain second message")
		assert.Contains(t, result.Trace[2], "starting application", "third trace should contain third message")
	})

	t.Run("Function name extraction", func(t *testing.T) {
		originalErr := errors.New("test error")
		trace := "test operation"

		result := NewError(trace, originalErr)

		require.Len(t, result.Trace, 1, "should have exactly one trace entry")
		assert.Contains(t, result.Trace[0], "TestNewError", "trace should contain the calling function name")
		assert.Contains(t, result.Trace[0], " - ", "trace should contain separator between function name and trace message")
		assert.Contains(t, result.Trace[0], trace, "trace should contain the provided trace message")
	})
}
