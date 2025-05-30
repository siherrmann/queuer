package model

type BatchJob struct {
	Task       interface{}
	Parameters []interface{}
	Options    *Options
}
