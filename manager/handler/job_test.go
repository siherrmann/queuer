package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/siherrmann/queuer/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddJobHandler(t *testing.T) {
	handler := NewManagerHandler(queue)
	e := echo.New()

	t.Run("AddJob with valid data", func(t *testing.T) {
		jobJSON := `{"task_name":"TestTask","parameters":[1]}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(jobJSON))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.AddJob(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusCreated, rec.Code)

		var job model.Job
		err = json.Unmarshal(rec.Body.Bytes(), &job)
		require.NoError(t, err)
		assert.Equal(t, "TestTask", job.TaskName)
		assert.NotEqual(t, uuid.Nil, job.RID)
	})

	t.Run("AddJob with invalid JSON", func(t *testing.T) {
		jobJSON := `{"task_name":"TestTask","parameters":invalid}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(jobJSON))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.AddJob(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid job data")
	})

	t.Run("AddJob with non-existent task", func(t *testing.T) {
		jobJSON := `{"task_name":"NonExistentTask","parameters":[]}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(jobJSON))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.AddJob(c)
		require.NoError(t, err)

		// Queuer allows adding jobs with any task name - validation happens at execution time
		assert.Equal(t, http.StatusCreated, rec.Code)

		var job model.Job
		err = json.Unmarshal(rec.Body.Bytes(), &job)
		require.NoError(t, err)
		assert.Equal(t, "NonExistentTask", job.TaskName)
	})
}

func TestGetJobHandler(t *testing.T) {
	handler := NewManagerHandler(queue)
	e := echo.New()

	t.Run("GetJob with valid RID", func(t *testing.T) {
		// First create a job
		job, err := queue.AddJob("TestTask", nil, 1)
		require.NoError(t, err)

		// Wait a bit for job to be queued
		time.Sleep(100 * time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+job.RID.String(), nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: job.RID.String()}})

		err = handler.GetJob(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, rec.Code)

		var fetchedJob model.Job
		err = json.Unmarshal(rec.Body.Bytes(), &fetchedJob)
		require.NoError(t, err)
		assert.Equal(t, job.RID, fetchedJob.RID)
		assert.Equal(t, "TestTask", fetchedJob.TaskName)
	})

	t.Run("GetJob with invalid RID format", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/invalid-uuid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: "invalid-uuid"}})

		err := handler.GetJob(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid job RID format")
	})

	t.Run("GetJob with non-existent RID", func(t *testing.T) {
		nonExistentRID := uuid.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+nonExistentRID.String(), nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: nonExistentRID.String()}})

		err := handler.GetJob(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Body.String(), "Job not found")
	})
}

func TestListJobsHandler(t *testing.T) {
	handler := NewManagerHandler(queue)
	e := echo.New()

	t.Run("ListJobs with default pagination", func(t *testing.T) {
		// Create multiple jobs
		for i := 0; i < 5; i++ {
			_, err := queue.AddJob("TestTask", nil, 1)
			require.NoError(t, err)
		}

		time.Sleep(100 * time.Millisecond)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobs(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, rec.Code)

		var jobs []*model.Job
		err = json.Unmarshal(rec.Body.Bytes(), &jobs)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(jobs), 1)
		assert.LessOrEqual(t, len(jobs), 10) // Default limit
	})

	t.Run("ListJobs with custom pagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs?lastId=0&limit=3", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobs(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, rec.Code)

		var jobs []*model.Job
		err = json.Unmarshal(rec.Body.Bytes(), &jobs)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(jobs), 3)
	})

	t.Run("ListJobs with invalid lastId", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs?lastId=invalid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobs(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid lastId")
	})

	t.Run("ListJobs with invalid limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs?limit=200", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.ListJobs(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid limit")
	})
}

func TestCancelJobHandler(t *testing.T) {
	handler := NewManagerHandler(queue)
	e := echo.New()

	t.Run("CancelJob with valid RID", func(t *testing.T) {
		// Create a job
		job, err := queue.AddJob("TestTask", nil, 10) // Long running
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/"+job.RID.String(), nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: job.RID.String()}})

		err = handler.CancelJob(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, rec.Code)

		var cancelledJob model.Job
		err = json.Unmarshal(rec.Body.Bytes(), &cancelledJob)
		require.NoError(t, err)
		assert.Equal(t, job.RID, cancelledJob.RID)
	})

	t.Run("CancelJob with invalid RID format", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/invalid-uuid", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: "invalid-uuid"}})

		err := handler.CancelJob(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Invalid job RID format")
	})

	t.Run("CancelJob with non-existent RID", func(t *testing.T) {
		nonExistentRID := uuid.New()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/"+nonExistentRID.String(), nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues([]echo.PathValue{{Name: "rid", Value: nonExistentRID.String()}})

		err := handler.CancelJob(c)
		require.NoError(t, err)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, rec.Body.String(), "Failed to cancel job")
	})
}
