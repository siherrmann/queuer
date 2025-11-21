package manager

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/siherrmann/queuer"
	"github.com/siherrmann/queuer/manager/handler"
)

// SetupRoutes configures all API routes for the manager service
func SetupRoutes(e *echo.Echo, queuerInstance *queuer.Queuer) {
	h := handler.NewManagerHandler(queuerInstance)

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	e.GET("/health", h.HealthCheck)
	e.GET("/", h.HealthCheck)

	v1 := e.Group("/api/v1")

	jobs := v1.Group("/jobs")
	jobs.POST("", h.AddJob)
	jobs.GET("", h.ListJobs)
	jobs.GET("/:rid", h.GetJob)
	jobs.DELETE("/:rid", h.CancelJob)

	jobArchives := v1.Group("/job-archives")
	jobArchives.GET("", h.ListJobArchives)
	jobArchives.GET("/:rid", h.GetJobArchive)

	workers := v1.Group("/workers")
	workers.GET("", h.ListWorkers)
	workers.GET("/:rid", h.GetWorker)

	connections := v1.Group("/connections")
	connections.GET("", h.ListConnections)
}
