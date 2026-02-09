package manager

import (
	"strconv"

	"github.com/labstack/echo/v5"
	"github.com/siherrmann/queuer"
)

func ManagerServer(port int, maxConcurrency int) {
	queuerInstance := queuer.NewQueuer("queuer-manager", maxConcurrency)

	e := echo.New()
	SetupRoutes(e, queuerInstance)

	err := e.Start(":" + strconv.Itoa(port))
	if err != nil {
		panic(err)
	}
}
