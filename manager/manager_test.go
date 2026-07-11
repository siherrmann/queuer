package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupRoutes(t *testing.T) {
	e := echo.New()
	SetupRoutes(e, queue)

	t.Run("Health endpoint is registered", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "healthy")
	})

	t.Run("Root endpoint is registered", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "healthy")
	})

	t.Run("Jobs endpoints are registered", func(t *testing.T) {
		// POST /api/v1/jobs
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", nil)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		// Should not be 404
		assert.NotEqual(t, http.StatusNotFound, rec.Code)

		// GET /api/v1/jobs
		req = httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("Job archives endpoints are registered", func(t *testing.T) {
		// GET /api/v1/job-archives
		req := httptest.NewRequest(http.MethodGet, "/api/v1/job-archives", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("Workers endpoints are registered", func(t *testing.T) {
		// GET /api/v1/workers
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		var workers []map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &workers)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(workers), 1)
	})

	t.Run("Connections endpoint is registered", func(t *testing.T) {
		// GET /api/v1/connections
		req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("CORS middleware is enabled", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/api/v1/workers", nil)
		req.Header.Set("Origin", "http://example.com")
		req.Header.Set("Access-Control-Request-Method", "GET")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		// CORS should add these headers
		assert.NotEmpty(t, rec.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("Non-existent endpoint returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/nonexistent", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}
