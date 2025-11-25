package manager

import (
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/siherrmann/queuer"
)

func ManagerServer(port int, maxConcurrency int) {
	queuerInstance := queuer.NewQueuer("queuer-manager", maxConcurrency)

	e := echo.New()
	SetupRoutes(e, queuerInstance)

	e.Logger.Fatal(e.Start(":" + strconv.Itoa(port)))
}
