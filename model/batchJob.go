package model

type BatchJob struct {
	Task            interface{}
	Parameters      []interface{}
	ParametersKeyed map[string]interface{}
	Options         *Options
}
