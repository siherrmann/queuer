package handler

import (
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

// GetWorker retrieves a specific worker by RID
func (m *ManagerHandler) GetWorker(c echo.Context) error {
	ridStr := c.Param("rid")
	rid, err := uuid.Parse(ridStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "Invalid worker RID format")
	}

	worker, err := m.queuer.GetWorker(rid)
	if err != nil {
		return c.String(http.StatusNotFound, "Worker not found")
	}

	return c.JSON(http.StatusOK, worker)
}

// ListWorkers retrieves a paginated list of workers
func (m *ManagerHandler) ListWorkers(c echo.Context) error {
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

	workers, err := m.queuer.GetWorkers(lastId, limit)
	if err != nil {
		return c.String(http.StatusInternalServerError, "Failed to retrieve workers")
	}

	return c.JSON(http.StatusOK, workers)
}
