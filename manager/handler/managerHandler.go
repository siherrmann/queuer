package handler

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/siherrmann/queuer"
)

type ManagerHandler struct {
	queuer *queuer.Queuer
}

func NewManagerHandler(queuer *queuer.Queuer) *ManagerHandler {
	return &ManagerHandler{
		queuer: queuer,
	}
}

// Health check handler
func (m *ManagerHandler) HealthCheck(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{
		"status":  "healthy",
		"service": "queuer-manager",
	})
}
