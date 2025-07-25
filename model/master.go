package model

import "time"

type Master struct {
	ID        string         `json:"id"`
	Settings  MasterSettings `json:"settings"`
	WorkerID  string         `json:"worker_id"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type MasterSettings struct {
	RetentionArchive int `json:"retention_archive"`
}
