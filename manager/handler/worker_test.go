package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetWorkerHandler(t *testing.T) {
	handler := NewManagerHandler(queue)
	e := echo.New()

	// Get the queuer's own worker RID
	var workerRID uuid.UUID
	t.Run("Setup - Get a worker RID", func(t *testing.T) {
		workerRid := queue.GetCurrentWorkerRID()
		require.NotEqual(t, uuid.Nil, workerRid)
		workerRID = workerRid
	})

	t.Run("GetWorker with valid RID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers/"+workerRID.String(), nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: workerRID.String()}})

		err := handler.GetWorker(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var worker map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &worker)
		require.NoError(t, err)
		assert.Equal(t, workerRID.String(), worker["rid"])
	})

	t.Run("GetWorker with invalid RID format", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers/invalid-uuid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: "invalid-uuid"}})

		err := handler.GetWorker(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid worker RID format")
	})

	t.Run("GetWorker with non-existent RID", func(t *testing.T) {
		nonExistentRID := uuid.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers/"+nonExistentRID.String(), nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: nonExistentRID.String()}})

		err := handler.GetWorker(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Body.String(), "Worker not found")
	})
}

func TestListWorkersHandler(t *testing.T) {
	handler := NewManagerHandler(queue)
	e := echo.New()

	t.Run("ListWorkers with default pagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListWorkers(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var workers []map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &workers)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(workers), 1)
	})

	t.Run("ListWorkers with custom limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers?limit=5", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListWorkers(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var workers []map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &workers)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(workers), 5)
	})

	t.Run("ListWorkers with custom lastId and limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers?lastId=0&limit=3", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListWorkers(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var workers []map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &workers)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(workers), 3)
	})

	t.Run("ListWorkers with invalid lastId", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers?lastId=invalid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListWorkers(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid lastId format")
	})

	t.Run("ListWorkers with negative lastId", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers?lastId=-1", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListWorkers(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid lastId format")
	})

	t.Run("ListWorkers with invalid limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers?limit=invalid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListWorkers(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid limit")
	})

	t.Run("ListWorkers with limit too high", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers?limit=101", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListWorkers(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid limit")
	})

	t.Run("ListWorkers with limit zero", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/workers?limit=0", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListWorkers(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid limit")
	})
}
