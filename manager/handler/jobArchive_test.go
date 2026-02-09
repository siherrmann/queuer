package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetJobArchiveHandler(t *testing.T) {
	handler := NewManagerHandler(queue)
	e := echo.New()

	// First, create and complete a job so we have an archived job to test with
	var archivedJobRID uuid.UUID
	t.Run("Setup - Create and complete a job", func(t *testing.T) {
		job, err := queue.AddJob("test-task", nil, 1)
		require.NoError(t, err)
		require.NotNil(t, job)
		archivedJobRID = job.RID

		// Wait for the job to be picked up and completed
		performedJob := queue.WaitForJobFinished(archivedJobRID, 5*time.Second)
		require.NoError(t, err)
		require.Equal(t, archivedJobRID, performedJob.RID)
	})

	t.Run("GetJobArchive with valid RID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs/"+archivedJobRID.String(), nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: archivedJobRID.String()}})

		err := handler.GetJobArchive(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var job map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &job)
		require.NoError(t, err)
		assert.Equal(t, archivedJobRID.String(), job["rid"])
	})

	t.Run("GetJobArchive with invalid RID format", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs/invalid-uuid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: "invalid-uuid"}})
		err := handler.GetJobArchive(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid job archive RID format")
	})

	t.Run("GetJobArchive with non-existent RID", func(t *testing.T) {
		nonExistentRID := uuid.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs/"+nonExistentRID.String(), nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: nonExistentRID.String()}})

		err := handler.GetJobArchive(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Body.String(), "Archived job not found")
	})
}

func TestListJobArchivesHandler(t *testing.T) {
	handler := NewManagerHandler(queue)
	e := echo.New()

	t.Run("ListJobArchives with default pagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobArchives(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var jobs []map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &jobs)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(jobs), 1)
	})

	t.Run("ListJobArchives with custom limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs?limit=5", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobArchives(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var jobs []map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &jobs)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(jobs), 5)
	})

	t.Run("ListJobArchives with custom lastId and limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs?lastId=0&limit=3", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobArchives(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var jobs []map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &jobs)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(jobs), 3)
	})

	t.Run("ListJobArchives with invalid lastId", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs?lastId=invalid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobArchives(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid lastId format")
	})

	t.Run("ListJobArchives with negative lastId", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs?lastId=-1", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobArchives(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid lastId format")
	})

	t.Run("ListJobArchives with invalid limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs?limit=invalid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobArchives(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid limit")
	})

	t.Run("ListJobArchives with limit too high", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs?limit=101", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobArchives(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid limit")
	})

	t.Run("ListJobArchives with limit zero", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/archives/jobs?limit=0", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobArchives(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid limit")
	})
}
