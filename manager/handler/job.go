package handler

import (
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/siherrmann/queuer/model"
)

// AddJob handles the addition of a new job
func (m *ManagerHandler) AddJob(c echo.Context) error {
	job := model.Job{}
	err := c.Bind(&job)
	if err != nil {
		return c.String(http.StatusBadRequest, "Invalid job data")
	}

	jobAdded, err := m.queuer.AddJob(job.TaskName, job.Parameters...)
	if err != nil {
		return c.String(http.StatusInternalServerError, "Failed to add job to queuer")
	}

	return c.JSON(http.StatusCreated, jobAdded)
}

// GetJob retrieves a specific job by RID
func (m *ManagerHandler) GetJob(c echo.Context) error {
	ridStr := c.Param("rid")
	rid, err := uuid.Parse(ridStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "Invalid job RID format")
	}

	job, err := m.queuer.GetJob(rid)
	if err != nil {
		return c.String(http.StatusNotFound, "Job not found")
	}

	return c.JSON(http.StatusOK, job)
}

// ListJobs retrieves a paginated list of jobs
func (m *ManagerHandler) ListJobs(c echo.Context) error {
	lastIdStr := c.QueryParam("lastId")
	limitStr := c.QueryParam("limit")

	// Parse lastId with default
	lastId := 0
	if lastIdStr != "" {
		parsedLastId, err := strconv.Atoi(lastIdStr)
		if err != nil || parsedLastId < 0 {
			return c.String(http.StatusBadRequest, "Invalid lastId format")
		}
		lastId = parsedLastId
	}

	// Parse limit with default
	limit := 10
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit <= 0 || parsedLimit > 100 {
			return c.String(http.StatusBadRequest, "Invalid limit (must be 1-100)")
		}
		limit = parsedLimit
	}

	jobs, err := m.queuer.GetJobs(lastId, limit)
	if err != nil {
		return c.String(http.StatusInternalServerError, "Failed to retrieve jobs")
	}

	return c.JSON(http.StatusOK, jobs)
}

// CancelJob cancels a specific job by RID
func (m *ManagerHandler) CancelJob(c echo.Context) error {
	ridStr := c.Param("rid")
	rid, err := uuid.Parse(ridStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "Invalid job RID format")
	}

	cancelledJob, err := m.queuer.CancelJob(rid)
	if err != nil {
		return c.String(http.StatusInternalServerError, "Failed to cancel job")
	}

	return c.JSON(http.StatusOK, cancelledJob)
}
