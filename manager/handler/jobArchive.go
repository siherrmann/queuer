package handler

import (
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

// GetJobArchive retrieves a specific archived job by RID
func (m *ManagerHandler) GetJobArchive(c echo.Context) error {
	ridStr := c.Param("rid")
	rid, err := uuid.Parse(ridStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "Invalid job archive RID format")
	}

	job, err := m.queuer.GetJobEnded(rid)
	if err != nil {
		return c.String(http.StatusNotFound, "Archived job not found")
	}

	return c.JSON(http.StatusOK, job)
}

// ListJobArchives retrieves a paginated list of archived jobs
func (m *ManagerHandler) ListJobArchives(c echo.Context) error {
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

	jobArchives, err := m.queuer.GetJobsEnded(lastId, limit)
	if err != nil {
		return c.String(http.StatusInternalServerError, "Failed to retrieve archived jobs")
	}

	return c.JSON(http.StatusOK, jobArchives)
}
