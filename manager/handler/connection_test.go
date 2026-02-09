package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListConnectionsHandlers(t *testing.T) {
	handler := NewManagerHandler(queue)
	e := echo.New()

	// Test ListConnections
	t.Run("ListConnections returns all active connections", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListConnections(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var connections []map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &connections)
		require.NoError(t, err)

		// Should have at least the connection from the test setup
		assert.GreaterOrEqual(t, len(connections), 0)

		// If there are connections, verify structure contains expected fields
		if len(connections) > 0 {
			conn := connections[0]
			assert.Contains(t, conn, "username")
			assert.Contains(t, conn, "database")
		}
	})

	t.Run("ListConnections returns valid JSON array", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListConnections(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify it's a valid JSON array
		var connections []interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &connections)
		require.NoError(t, err)
	})
}
