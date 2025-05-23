package model

import (
	"time"

	"github.com/google/uuid"
)

type Worker struct {
	ID             int       `json:"id"`
	RID            uuid.UUID `json:"rid"`
	QueueName      string    `json:"queue_name"`
	Name           string    `json:"name"`
	AvailableTasks []string  `json:"available_tasks"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
