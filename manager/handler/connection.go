package handler

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// ListConnections retrieves all active connections
func (m *ManagerHandler) ListConnections(c echo.Context) error {
	connections, err := m.queuer.GetConnections()
	if err != nil {
		return c.String(http.StatusInternalServerError, "Failed to retrieve connections")
	}

	return c.JSON(http.StatusOK, connections)
}
