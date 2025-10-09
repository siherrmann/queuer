package model

import "github.com/siherrmann/queuer"

type RootFlags struct {
	Verbose        bool
	QueuerInstance *queuer.Queuer
}
